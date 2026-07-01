/**
 * Nèrvi MCP — NATS JetStream bus adapter.
 *
 * The only module that imports `nats`. It implements the SignalBus seam over a
 * JetStream connection: publishing carries the qualifier in a message header,
 * and subscribing uses a durable pull consumer fetched statelessly (no
 * long-running subscription). Stream/consumer creation is idempotent.
 */
import {
  AckPolicy,
  connect,
  headers as natsHeaders,
  StringCodec,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
} from 'nats';
import {
  buildHeaders,
  QUALIFIER_HEADER,
  STREAM_NAME,
  TIMESTAMP_HEADER,
  type PublishResult,
  type Qualifier,
  type ReceivedMessage,
  type SignalBus,
} from './core.js';

const sc = StringCodec();

export const DEFAULT_NATS_URL = 'nats://nervi-nats.nervi.svc.cluster.local:4222';

/** How long a pull fetch waits for messages before returning what it has. */
const FETCH_EXPIRES_MS = 5_000;

export class NatsBus implements SignalBus {
  private constructor(
    private readonly nc: NatsConnection,
    private readonly js: JetStreamClient,
    private readonly jsm: JetStreamManager,
  ) {}

  static async connect(url: string = process.env.NATS_URL ?? DEFAULT_NATS_URL): Promise<NatsBus> {
    const nc = await connect({ servers: url, name: 'nervi-mcp' });
    const js = nc.jetstream();
    const jsm = await nc.jetstreamManager();
    return new NatsBus(nc, js, jsm);
  }

  async close(): Promise<void> {
    await this.nc.drain();
  }

  async publish(subject: string, payload: string, qualifier: Qualifier, msgId?: string): Promise<PublishResult> {
    const timestamp = new Date().toISOString();
    const h = natsHeaders();
    for (const [key, value] of Object.entries(buildHeaders(qualifier, timestamp, msgId))) {
      h.set(key, value);
    }
    const ack = await this.js.publish(subject, sc.encode(payload), { headers: h });
    return { stream: ack.stream, seq: ack.seq };
  }

  async fetch(subject: string, consumerName: string, maxMessages: number): Promise<ReceivedMessage[]> {
    await this.ensureConsumer(subject, consumerName);
    const consumer = await this.js.consumers.get(STREAM_NAME, consumerName);

    const out: ReceivedMessage[] = [];
    const messages = await consumer.fetch({ max_messages: maxMessages, expires: FETCH_EXPIRES_MS });
    for await (const m of messages) {
      const qualifier = m.headers?.get(QUALIFIER_HEADER) || null;
      const headerTs = m.headers?.get(TIMESTAMP_HEADER);
      const timestamp = headerTs || new Date(m.info.timestampNanos / 1_000_000).toISOString();
      out.push({
        sequence: m.seq,
        subject: m.subject,
        qualifier,
        payload: sc.decode(m.data),
        timestamp,
      });
      m.ack();
    }
    return out;
  }

  /** Create the durable pull consumer for this subject if it is absent. */
  private async ensureConsumer(subject: string, consumerName: string): Promise<void> {
    try {
      await this.jsm.consumers.info(STREAM_NAME, consumerName);
    } catch {
      await this.jsm.consumers.add(STREAM_NAME, {
        durable_name: consumerName,
        ack_policy: AckPolicy.Explicit,
        filter_subject: subject,
      });
    }
  }
}
