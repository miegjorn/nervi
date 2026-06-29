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

    /// Ensure the JetStream stream backing `subject` exists, creating it if it
    /// does not. Idempotent — safe to call on every startup.
    ///
    /// In the cluster the stream is provisioned by a Helm post-install hook, so
    /// callers normally rely on it already existing. For local development and
    /// the N-4 integration test (which run against an ephemeral broker) this
    /// lets nervi-core provision the stream itself, using the *same* naming
    /// convention as [`publish`](Self::publish) / [`subscribe`](Self::subscribe)
    /// (see [`stream_name_for`]) so the round-trip stays consistent.
    ///
    /// The stream covers `<root>.>` (e.g. `ops.>`), is file-backed for
    /// durability, and returns the resolved stream name.
    pub async fn ensure_stream(&self, subject: &str) -> Result<String> {
        let stream_name = stream_name_for(subject);
        let root = subject.split('.').next().unwrap_or(subject);

        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![format!("{}.>", root)],
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .with_context(|| format!("ensuring stream {} for subject {}", stream_name, subject))?;

        Ok(stream_name)
    }

    /// Publish a message to a subject. The stream covering the subject must
    /// already exist — provisioned by the cluster Helm hook, or via
    /// [`ensure_stream`](Self::ensure_stream) in local / test environments.
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
/// `occitan.ops.sre.alerts` → `OCCITAN`, `infra.alerts` → `INFRA`.
fn stream_name_for(subject: &str) -> String {
    subject.split('.').next().unwrap_or(subject).to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_name_from_subject() {
        assert_eq!(stream_name_for("occitan.ops.sre.alerts"), "OCCITAN");
        assert_eq!(stream_name_for("occitan.ops.metrics.cpu"), "OCCITAN");
        assert_eq!(stream_name_for("infra.metrics"), "INFRA");
        assert_eq!(stream_name_for("plain"), "PLAIN");
    }
}
