//! N-4 — End-to-end integration test for the Signal Bus Core.
//!
//! Proves the full bus chain against a **real** NATS JetStream server (not a
//! mock): an SRE-style producer publishes an operational alert on
//! `ops.sre.alerts`, and an independent developer consumer fetches it back with
//! its payload intact — even when the consumer subscribes *after* the producer
//! has finished (durability).
//!
//! Closes nervi#6.
//!
//! ## Running locally
//!
//! ```sh
//! nats-server -js &                 # JetStream-enabled broker on :4222
//! cargo test -p nervi-core --test integration
//! ```
//!
//! The broker URL is taken from `NERVI_TEST_NATS_URL` (default
//! `nats://127.0.0.1:4222`). The test requires a reachable JetStream broker; it
//! does not silently skip, so a broken bus surfaces as a failure in CI.

use nervi_core::{NerviClient, PublishOptions};
use serde::{Deserialize, Serialize};

/// The subject the SRE sensor publishes operational alerts on.
const ALERTS_SUBJECT: &str = "ops.sre.alerts";

/// A second `ops.*` subject used to prove subject filtering: a consumer of
/// `ops.sre.alerts` must not receive traffic published elsewhere on the stream.
const NOISE_SUBJECT: &str = "ops.metrics.cpu";

/// The shape of an SRE alert payload. Mirrors what the Epic 2 log watcher (N-5)
/// will emit; here a stand-in producer fills the same envelope so the fabric
/// can be validated before the real sensor lands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SreAlert {
    level: String,
    node: String,
    msg: String,
}

fn nats_url() -> String {
    std::env::var("NERVI_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

/// Start from a known-empty stream so the test is deterministic and repeatable
/// even against a persistent, file-backed broker (in CI the broker is fresh;
/// locally it may carry state from a prior run). Deletes the OPS stream if it
/// exists — `ensure_stream` then recreates it clean.
async fn reset_ops_stream(url: &str) {
    let nc = async_nats::connect(url)
        .await
        .expect("connect to NATS to reset the stream");
    let js = async_nats::jetstream::new(nc);
    // Ignore "stream not found": a clean broker is exactly the state we want.
    let _ = js.delete_stream("OPS").await;
}

/// Full chain: SRE producer → `ops.sre.alerts` → developer consumer, with
/// payload integrity, late (durable) subscription, and subject filtering.
#[tokio::test]
async fn sre_alert_reaches_developer_consumer_intact_after_late_subscription() {
    let url = nats_url();
    reset_ops_stream(&url).await;

    let client = NerviClient::connect(&url)
        .await
        .expect("connect to NATS JetStream — is a `nats-server -js` reachable?");

    // The ops stream must exist before anything is published. nervi-core owns
    // the convention (`stream_name_for`), so it provisions the matching stream
    // idempotently rather than depending on the cluster Helm hook.
    let stream = client
        .ensure_stream(ALERTS_SUBJECT)
        .await
        .expect("provision the ops stream");
    assert_eq!(stream, "OPS", "subject root maps to the OPS stream");

    // --- Producer: an SRE sensor publishes a critical alert, then exits. ------
    let alert = SreAlert {
        level: "critical".to_string(),
        node: "node-3".to_string(),
        msg: "disk 95% full".to_string(),
    };
    client
        .publish(PublishOptions {
            subject: ALERTS_SUBJECT.to_string(),
            payload: serde_json::to_string(&alert).unwrap(),
        })
        .await
        .expect("publish SRE alert to ops.sre.alerts");

    // Noise on a *different* ops subject — the developer consumer must not see it.
    client
        .publish(PublishOptions {
            subject: NOISE_SUBJECT.to_string(),
            payload: r#"{"cpu":0.42}"#.to_string(),
        })
        .await
        .expect("publish noise to ops.metrics.cpu");

    // --- Consumer is created only NOW — proves durable, async delivery. -------
    // The producer has already finished; the alert lives in the file-backed
    // stream and is fetched after the fact.
    let received = client
        .subscribe(ALERTS_SUBJECT, 10)
        .await
        .expect("subscribe to ops.sre.alerts");

    // Subject filtering: exactly the one alert, none of the noise.
    assert_eq!(
        received.len(),
        1,
        "developer consumer receives only ops.sre.alerts traffic, got: {received:?}"
    );

    let msg = &received[0];
    assert_eq!(msg.subject, ALERTS_SUBJECT, "subject round-trips");

    // Payload integrity: the JSON round-trips byte-for-byte into the same struct.
    let decoded: SreAlert =
        serde_json::from_str(&msg.payload).expect("payload is intact, decodable JSON");
    assert_eq!(decoded, alert, "alert payload survived the bus intact");
}
