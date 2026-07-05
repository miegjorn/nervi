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
  type ConsumerInfo,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
} from 'nats';
import {
  buildHeaders,
  QUALIFIER_HEADER,
  STREAM_NAME,
  SubjectBindingError,
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

/**
 * Extract the single filter subject a consumer is bound to. Consumers created
 * by this bus always use the singular `filter_subject`, but a consumer created
 * elsewhere may use the plural `filter_subjects` array — read whichever is set.
 * Returns '' for an unfiltered consumer (binds the whole stream).
 */
function consumerFilterSubject(info: ConsumerInfo): string {
  const { filter_subject, filter_subjects } = info.config;
  if (filter_subject) return filter_subject;
  if (filter_subjects && filter_subjects.length === 1) return filter_subjects[0];
  return filter_subject ?? '';
}

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

    // Guard against silent subject mismatch: a durable consumer keeps its
    // original filter subject for the lifetime of the consumer name, so
    // attaching to a pre-existing consumer bound to another subject would
    // deliver the wrong messages with no error. Fetch the live config and
    // refuse to hand back messages if the binding does not match the request.
    const boundSubject = consumerFilterSubject(await consumer.info());
    if (boundSubject !== subject) {
      throw new SubjectBindingError(consumerName, boundSubject, subject);
    }

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
