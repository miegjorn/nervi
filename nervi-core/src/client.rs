use anyhow::{Context, Result};
use async_nats::jetstream::{self, consumer::PullConsumer};

/// A received message from a JetStream subject.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub subject: String,
    pub payload: String,
}

/// Options for publishing a message.
pub struct PublishOptions {
    /// NATS subject to publish to (e.g. `ops.sre.alerts`).
    pub subject: String,
    /// Message payload as a plain string.
    pub payload: String,
}

/// Thin wrapper around the async-nats JetStream client.
pub struct NerviClient {
    js: jetstream::Context,
}

impl NerviClient {
    /// Connect to NATS and return a client backed by JetStream.
    pub async fn connect(nats_url: &str) -> Result<Self> {
        let nc = async_nats::connect(nats_url)
            .await
            .with_context(|| format!("connecting to NATS at {}", nats_url))?;
        let js = jetstream::new(nc);
        Ok(Self { js })
    }

    /// Publish a message to a subject. The stream covering the subject must
    /// already exist (created by the `ops` stream ConfigMap on cluster startup).
    pub async fn publish(&self, opts: PublishOptions) -> Result<()> {
        self.js
            .publish(opts.subject.clone(), opts.payload.into_bytes().into())
            .await
            .with_context(|| format!("publishing to {}", opts.subject))?
            .await
            .context("awaiting publish ack")?;
        Ok(())
    }

    /// Consume up to `max_messages` pending messages from a subject.
    ///
    /// Uses an ephemeral pull consumer so callers don't need to manage
    /// consumer lifecycle. Each call creates a fresh consumer and drains
    /// up to `max_messages` from the current head of the stream.
    pub async fn subscribe(&self, subject: &str, max_messages: u64) -> Result<Vec<Message>> {
        let stream_name = stream_name_for(subject);

        let stream = self
            .js
            .get_stream(&stream_name)
            .await
            .with_context(|| format!("getting stream {}", stream_name))?;

        let consumer: PullConsumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: subject.to_string(),
                ..Default::default()
            })
            .await
            .with_context(|| format!("creating ephemeral consumer for {}", subject))?;

        let mut batch = consumer
            .fetch()
            .max_messages(max_messages as usize)
            .messages()
            .await
            .context("fetching messages")?;

        let mut out = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            futures::StreamExt::next(&mut batch),
        )
        .await
        {
            let msg = msg.map_err(|e| anyhow::anyhow!("reading message from batch: {}", e))?;
            let payload = String::from_utf8_lossy(&msg.payload).to_string();
            out.push(Message {
                subject: msg.subject.to_string(),
                payload,
            });
            msg.ack().await.map_err(|e| anyhow::anyhow!("acking message: {}", e))?;
        }

        Ok(out)
    }
}

/// Derive the JetStream stream name from a subject.
/// Convention: the first token of the subject is the stream name.
/// `ops.sre.alerts` → `ops`, `infra.alerts` → `infra`.
fn stream_name_for(subject: &str) -> String {
    subject.split('.').next().unwrap_or(subject).to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_from_subject() {
        assert_eq!(stream_name_for("ops.sre.alerts"), "OPS");
        assert_eq!(stream_name_for("infra.metrics"), "INFRA");
        assert_eq!(stream_name_for("plain"), "PLAIN");
    }
}
