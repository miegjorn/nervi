/**
 * N-4 integration test — SRE sensor → ops.sre.alerts → consumer.
 *
 * Runs against a real NATS server with JetStream enabled. The NATS URL is
 * read from the NATS_URL env var (default: nats://localhost:4222).
 *
 * The test provisions the OCCITAN stream, runs the full publish→subscribe
 * chain, and verifies:
 *   - payload integrity (round-trip byte-for-byte)
 *   - qualifier integrity (Nervi-Qualifier header survives round-trip)
 *   - durability (consumer created AFTER publish still receives messages)
 *   - ack semantics (acked messages not redelivered to same durable consumer)
 *   - independent cursor (fresh consumer name replays from stream beginning)
 *   - subject filtering (messages on different ops.* subjects not delivered)
 *   - subject validation (non-ops.* subject rejected before touching the bus)
 *
 * CI spins up `nats:2.10-alpine -js` as a service; locally you can run:
 *   docker run --rm -p 4222:4222 nats:2.10-alpine -js
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { connect, type JetStreamManager, type NatsConnection, AckPolicy, RetentionPolicy } from 'nats';
import { NatsBus } from '../src/bus.js';
import { STREAM_NAME, STREAM_SUBJECTS, ValidationError } from '../src/core.js';

// ---------------------------------------------------------------------------
// Stream provisioning helpers
// ---------------------------------------------------------------------------

const NATS_URL = process.env.NATS_URL ?? 'nats://localhost:4222';

/** Provision the OCCITAN stream — idempotent, purges on each test run. */
async function ensureStream(jsm: JetStreamManager): Promise<void> {
  try {
    await jsm.streams.info(STREAM_NAME);
    // Stream exists — purge so each test run starts from seq 1.
    await jsm.streams.purge(STREAM_NAME);
  } catch {
    // Stream absent — create it.
    await jsm.streams.add({
      name: STREAM_NAME,
      subjects: STREAM_SUBJECTS,
      retention: RetentionPolicy.Limits,
    });
  }
}

/** Delete a durable consumer (best-effort, ignore if absent). */
async function deleteConsumer(jsm: JetStreamManager, name: string): Promise<void> {
  try {
    await jsm.consumers.delete(STREAM_NAME, name);
  } catch {
    // Absent — that is fine.
  }
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe('N-4 integration: SRE sensor → ops.sre.alerts → consumer', () => {
  let nc: NatsConnection;
  let jsm: JetStreamManager;
  let bus: NatsBus;

  const CONSUMER_A = 'sre-test-consumer-a';
  const CONSUMER_B = 'sre-test-consumer-b';

  const ALERT_1 = JSON.stringify({ level: 'critical', node: 'node-3', msg: 'disk 95% full' });
  const ALERT_2 = JSON.stringify({ level: 'warning', node: 'node-7', msg: 'cpu spike detected' });

  beforeAll(async () => {
    nc = await connect({ servers: NATS_URL, name: 'nervi-n4-test' });
    jsm = await nc.jetstreamManager();
    await ensureStream(jsm);
    // Delete any leftover durable consumers from a previous run.
    await deleteConsumer(jsm, CONSUMER_A);
    await deleteConsumer(jsm, CONSUMER_B);
    bus = await NatsBus.connect(NATS_URL);
  });

  afterAll(async () => {
    await deleteConsumer(jsm, CONSUMER_A);
    await deleteConsumer(jsm, CONSUMER_B);
    await bus.close();
    await nc.drain();
  });

  // -------------------------------------------------------------------------
  // Step 1-2: publish two alerts (producer side).
  // The consumer has NOT been created yet — proving durable delivery.
  // -------------------------------------------------------------------------

  it('publishes first alert (info) with payload and qualifier', async () => {
    const result = await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    expect(result.stream).toBe(STREAM_NAME);
    expect(typeof result.seq).toBe('number');
    expect(result.seq).toBeGreaterThanOrEqual(1);
  });

  it('publishes second alert (cross-project) and seq increments', async () => {
    const first = await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    const second = await bus.publish('ops.sre.alerts', ALERT_2, 'cross-project');
    expect(second.seq).toBeGreaterThan(first.seq);
  });

  // -------------------------------------------------------------------------
  // Reset: purge & republish exactly two known messages so later assertions
  // have a deterministic baseline.
  // Note: NATS stream purge removes messages but does NOT reset the sequence
  // counter — we assert relative ordering, not absolute sequence numbers.
  // -------------------------------------------------------------------------

  it('baseline: purge stream and publish exactly the two test alerts', async () => {
    await jsm.streams.purge(STREAM_NAME);
    const r1 = await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    const r2 = await bus.publish('ops.sre.alerts', ALERT_2, 'cross-project');
    expect(r1.seq).toBeGreaterThanOrEqual(1);
    expect(r2.seq).toBeGreaterThan(r1.seq);
  });

  // -------------------------------------------------------------------------
  // Step 3-5: consumer created AFTER publication — durable delivery.
  // -------------------------------------------------------------------------

  it('consumer created after publication receives both alerts (durability)', async () => {
    // The consumer does not exist yet — NatsBus.fetch() creates it lazily.
    const messages = await bus.fetch('ops.sre.alerts', CONSUMER_A, 10);
    expect(messages).toHaveLength(2);
  });

  it('payload round-trips byte-for-byte', async () => {
    // Purge and republish, then use a fresh consumer name to replay.
    await jsm.streams.purge(STREAM_NAME);
    await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    await bus.publish('ops.sre.alerts', ALERT_2, 'cross-project');

    const messages = await bus.fetch('ops.sre.alerts', CONSUMER_B, 10);
    expect(messages[0].payload).toBe(ALERT_1);
    expect(messages[1].payload).toBe(ALERT_2);
  });

  it('qualifier header survives the round-trip', async () => {
    // CONSUMER_B already acked both messages (previous test).
    // Repopulate and use a new cursor.
    await jsm.streams.purge(STREAM_NAME);
    await deleteConsumer(jsm, CONSUMER_B);
    await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    await bus.publish('ops.sre.alerts', ALERT_2, 'cross-project');

    const messages = await bus.fetch('ops.sre.alerts', CONSUMER_B, 10);
    expect(messages[0].qualifier).toBe('info');
    expect(messages[1].qualifier).toBe('cross-project');
  });

  it('sequence numbers are ascending', async () => {
    await jsm.streams.purge(STREAM_NAME);
    await deleteConsumer(jsm, CONSUMER_A);
    await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    await bus.publish('ops.sre.alerts', ALERT_2, 'cross-project');

    const messages = await bus.fetch('ops.sre.alerts', CONSUMER_A, 10);
    expect(messages[0].sequence).toBeLessThan(messages[1].sequence);
  });

  it('timestamp is present and ISO-8601', async () => {
    await jsm.streams.purge(STREAM_NAME);
    await deleteConsumer(jsm, CONSUMER_A);
    await bus.publish('ops.sre.alerts', ALERT_1, 'info');

    const [msg] = await bus.fetch('ops.sre.alerts', CONSUMER_A, 1);
    expect(msg.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    expect(() => new Date(msg.timestamp)).not.toThrow();
    expect(new Date(msg.timestamp).getTime()).not.toBeNaN();
  });

  // -------------------------------------------------------------------------
  // Step 6: ack semantics — same durable consumer gets 0 messages on repeat.
  // -------------------------------------------------------------------------

  it('acked messages are not redelivered to the same durable consumer', async () => {
    // CONSUMER_A already acked all messages in the previous tests.
    // Ensure stream has messages but CONSUMER_A's cursor is past them.
    await jsm.streams.purge(STREAM_NAME);
    await deleteConsumer(jsm, CONSUMER_A);
    await bus.publish('ops.sre.alerts', ALERT_1, 'info');
    await bus.publish('ops.sre.alerts', ALERT_2, 'cross-project');

    // First fetch — consumes and acks both.
    const first = await bus.fetch('ops.sre.alerts', CONSUMER_A, 10);
    expect(first).toHaveLength(2);

    // Second fetch — cursor advanced, nothing pending.
    const second = await bus.fetch('ops.sre.alerts', CONSUMER_A, 10);
    expect(second).toHaveLength(0);
  });

  // -------------------------------------------------------------------------
  // Step 7: independent cursor — fresh consumer name replays from beginning.
  // -------------------------------------------------------------------------

  it('a fresh consumer name replays all messages from the stream', async () => {
    // The stream still holds the two alerts from the previous test.
    // CONSUMER_B has a cursor from an earlier test but we deleted and
    // re-created it, so let's use a truly new name.
    const CURSOR_NEW = 'sre-test-cursor-fresh';
    await deleteConsumer(jsm, CURSOR_NEW);

    try {
      const messages = await bus.fetch('ops.sre.alerts', CURSOR_NEW, 10);
      expect(messages).toHaveLength(2);
    } finally {
      await deleteConsumer(jsm, CURSOR_NEW);
    }
  });

  // -------------------------------------------------------------------------
  // Subject filtering: a message on ops.other is NOT delivered to
  // ops.sre.alerts consumer.
  // -------------------------------------------------------------------------

  it('messages on a different ops.* subject are not delivered to the ops.sre.alerts consumer', async () => {
    await jsm.streams.purge(STREAM_NAME);
    await deleteConsumer(jsm, CONSUMER_A);

    // Publish to a different subject (still in OCCITAN stream via ops.>).
    await bus.publish('ops.infra.metrics', '{"cpu":0.75}', 'data');

    // Consumer filtered to ops.sre.alerts should receive 0 messages.
    const messages = await bus.fetch('ops.sre.alerts', CONSUMER_A, 10);
    expect(messages).toHaveLength(0);
  });

  // -------------------------------------------------------------------------
  // Negative / edge: non-ops.* subject rejected before touching the bus.
  // -------------------------------------------------------------------------

  it('rejects a non-ops.* subject at the validation layer', async () => {
    // assertSubject is called inside handlePublish/handleSubscribe — but we
    // can also verify it at the bus level by checking that ValidationError is
    // thrown when the handlers are involved. Here we test the core guard
    // directly via the handlers to stay integration-clean.
    const { handlePublish } = await import('../src/handlers.js');
    await expect(
      handlePublish(bus, { subject: 'dev.sre.alerts', payload: ALERT_1, qualifier: 'info' }),
    ).rejects.toThrow(ValidationError);
  });

  // -------------------------------------------------------------------------
  // max_messages respects pending count — no hang beyond fetch expiry.
  // -------------------------------------------------------------------------

  it('max_messages larger than pending count returns only what is pending', async () => {
    await jsm.streams.purge(STREAM_NAME);
    await deleteConsumer(jsm, CONSUMER_A);
    await bus.publish('ops.sre.alerts', ALERT_1, 'info');

    const messages = await bus.fetch('ops.sre.alerts', CONSUMER_A, 100);
    // Only 1 message was published; we asked for 100.
    expect(messages).toHaveLength(1);
  });
});
