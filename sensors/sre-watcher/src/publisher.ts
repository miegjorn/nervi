/**
 * Nèrvi SRE watcher — NATS/JetStream alert publisher.
 *
 * The only file in the watcher that imports `nats`. It implements the
 * AlertPublisher seam over a JetStream connection; alerts land on
 * occitan.ops.sre.alerts, which the OCCITAN stream (subjects `occitan.>`)
 * captures. Mirrors the mcp/nervi-mcp NatsBus connection conventions.
 */
import {
  connect,
  StringCodec,
  type JetStreamClient,
  type NatsConnection,
} from 'nats';
import type { AlertPublisher } from './core.js';

const sc = StringCodec();

export const DEFAULT_NATS_URL = 'nats://nervi-nats.nervi.svc.cluster.local:4222';

/** Publishes structured SRE alerts onto the OCCITAN JetStream. */
export class NatsAlertPublisher implements AlertPublisher {
  private constructor(
    private readonly nc: NatsConnection,
    private readonly js: JetStreamClient,
  ) {}

  static async connect(
    url: string = process.env.NATS_URL ?? DEFAULT_NATS_URL,
  ): Promise<NatsAlertPublisher> {
    const nc = await connect({ servers: url, name: 'sre-watcher' });
    return new NatsAlertPublisher(nc, nc.jetstream());
  }

  async publish(subject: string, payload: string): Promise<void> {
    await this.js.publish(subject, sc.encode(payload));
  }

  async close(): Promise<void> {
    await this.nc.drain();
  }
}
