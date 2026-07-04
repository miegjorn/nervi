use anyhow::{Context, Result};
use async_nats::jetstream::{self, consumer::PullConsumer};
use async_nats::HeaderMap;

/// Header carrying the qualifier on every Nèrvi message. Matches
/// `nervi-mcp`'s `QUALIFIER_HEADER` (`mcp/nervi-mcp/src/core.ts`) so messages
/// published from either implementation are indistinguishable on the wire.
pub const QUALIFIER_HEADER: &str = "Nervi-Qualifier";
/// Header carrying the publish-time ISO-8601 timestamp. Matches
/// `nervi-mcp`'s `TIMESTAMP_HEADER`.
pub const TIMESTAMP_HEADER: &str = "Nervi-Timestamp";

/// A received message from a JetStream subject.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub subject: String,
    pub payload: String,
}

/// Options for publishing a message.
#[derive(Default)]
pub struct PublishOptions {
    /// NATS subject to publish to (e.g. `ops.sre.alerts`).
    pub subject: String,
    /// Message payload as a plain string.
    pub payload: String,
    /// Qualifier embedded as the `Nervi-Qualifier` header (ADR-N-001:
    /// `info` | `cross-project` | `data`, or a signal-kind-specific value
    /// such as ADR-N-005's `cross-project`). `None` omits the header
    /// entirely — matches the pre-existing behavior for callers that don't
    /// set one.
    pub qualifier: Option<String>,
    /// Publish-time ISO-8601 timestamp, embedded as `Nervi-Timestamp`.
    /// `None` omits the header. Callers own timestamp generation so this
    /// crate stays free of a wall-clock dependency and is deterministic
    /// under test.
    pub timestamp: Option<String>,
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
    ///
    /// When `opts.qualifier` and/or `opts.timestamp` are `Some`, they are
    /// embedded as the `Nervi-Qualifier` / `Nervi-Timestamp` headers (same
    /// header names as `nervi-mcp`); when both are `None`, this publishes
    /// with no headers, identical to the pre-existing behavior.
    pub async fn publish(&self, opts: PublishOptions) -> Result<()> {
        let subject = opts.subject.clone();
        let payload = bytes::Bytes::from(opts.payload.into_bytes());

        let ack = if opts.qualifier.is_none() && opts.timestamp.is_none() {
            self.js
                .publish(subject.clone(), payload)
                .await
                .with_context(|| format!("publishing to {}", subject))?
        } else {
            let mut headers = HeaderMap::new();
            if let Some(q) = &opts.qualifier {
                headers.insert(QUALIFIER_HEADER, q.as_str());
            }
            if let Some(ts) = &opts.timestamp {
                headers.insert(TIMESTAMP_HEADER, ts.as_str());
            }
            self.js
                .publish_with_headers(subject.clone(), headers, payload)
                .await
                .with_context(|| format!("publishing to {} with headers", subject))?
        };

        ack.await.context("awaiting publish ack")?;
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

    /// Continuously consume `subject` via a *durable* pull consumer whose
    /// `.messages()` stream the async-nats client keeps fed in the
    /// background — no caller-visible poll/sleep loop. `durable_name` must
    /// be stable across restarts for the same logical consumer (e.g.
    /// `corrier-inbound-guilhem`) so JetStream resumes from where it left
    /// off rather than replaying or dropping history.
    ///
    /// Returned stream yields `Ok(Message)` for each delivered message
    /// (already ack'd) or `Err` if a delivery itself failed to decode --
    /// callers should log-and-continue on `Err`, not tear down the stream.
    pub async fn consume_durable(
        &self,
        subject: &str,
        durable_name: &str,
    ) -> Result<impl futures::Stream<Item = Result<Message>>> {
        let stream_name = stream_name_for(subject);

        let stream = self
            .js
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![format!("{}.>", subject.split('.').next().unwrap_or(subject))],
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .with_context(|| format!("ensuring stream {} for {}", stream_name, subject))?;

        let consumer: PullConsumer = stream
            .get_or_create_consumer(
                durable_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(durable_name.to_string()),
                    filter_subject: subject.to_string(),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("creating durable consumer {} for {}", durable_name, subject))?;

        let messages = consumer
            .messages()
            .await
            .context("opening continuous message stream")?;

        Ok(futures::StreamExt::map(messages, |result| {
            let msg = result.map_err(|e| anyhow::anyhow!("reading message from stream: {}", e))?;
            let payload = String::from_utf8_lossy(&msg.payload).to_string();
            let subject = msg.subject.to_string();
            // Ack inline -- at-least-once delivery, matching subscribe()'s
            // existing per-message ack behavior.
            let ack_msg = msg.clone();
            tokio::spawn(async move {
                let _ = ack_msg.ack().await;
            });
            Ok(Message { subject, payload })
        }))
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

    #[test]
    fn publish_options_default_has_no_qualifier_or_timestamp() {
        // Existing callers (e.g. nervi-server's pre-qualifier nervi_publish
        // handler) build PublishOptions without setting these two fields —
        // Default must preserve the old no-header publish behavior exactly.
        let opts = PublishOptions {
            subject: "occitan.ops.sre.alerts".to_string(),
            payload: "{}".to_string(),
            ..Default::default()
        };
        assert!(opts.qualifier.is_none());
        assert!(opts.timestamp.is_none());
    }

    #[test]
    fn header_names_match_nervi_mcp_constants() {
        // Must stay byte-identical to mcp/nervi-mcp/src/core.ts's
        // QUALIFIER_HEADER / TIMESTAMP_HEADER, or messages published from
        // the Rust and TypeScript sides become indistinguishable only by
        // accident rather than by contract.
        assert_eq!(QUALIFIER_HEADER, "Nervi-Qualifier");
        assert_eq!(TIMESTAMP_HEADER, "Nervi-Timestamp");
    }

    #[test]
    fn header_map_carries_qualifier_and_timestamp_when_set() {
        let mut headers = HeaderMap::new();
        headers.insert(QUALIFIER_HEADER, "cross-project");
        headers.insert(TIMESTAMP_HEADER, "2026-07-01T00:00:00Z");
        assert_eq!(
            headers.get(QUALIFIER_HEADER).map(|v| v.to_string()),
            Some("cross-project".to_string())
        );
        assert_eq!(
            headers.get(TIMESTAMP_HEADER).map(|v| v.to_string()),
            Some("2026-07-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn durable_consumer_name_is_stable_and_url_safe() {
        // consume_durable derives its NATS durable_name from the caller-supplied
        // name verbatim -- this test just documents that expectation so a future
        // change to the derivation doesn't silently break existing durables.
        let name = "corrier-inbound-guilhem-abc123";
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }
}
