#!/usr/bin/env node
/**
 * Nèrvi SRE watcher — entrypoint.
 *
 * Wires the validated config to a live Kubernetes PodSource and the N-6
 * ClassifyingSink (classification rules + alert publish to
 * occitan.ops.sre.alerts over NATS/JetStream), then runs the watcher until
 * SIGTERM/SIGINT. The watcher logic lives in watcher.ts; this file only
 * assembles the real dependencies and handles process lifecycle.
 */
import { parseConfig } from './core.js';
import { K8sPodSource, loadKubeConfig } from './k8s.js';
import { NatsAlertPublisher } from './publisher.js';
import { ClassifyingSink } from './sinks.js';
import { LogWatcher, type Logger } from './watcher.js';

/** Structured logger to stderr — stdout is reserved for the NDJSON data plane. */
const stderrLogger: Logger = {
  info: (msg, meta) => log('info', msg, meta),
  warn: (msg, meta) => log('warn', msg, meta),
  error: (msg, meta) => log('error', msg, meta),
};

function log(level: string, msg: string, meta?: Record<string, unknown>): void {
  const entry = { level, msg, ...meta, ts: new Date().toISOString() };
  process.stderr.write(`${JSON.stringify(entry)}\n`);
}

async function main(): Promise<void> {
  const config = parseConfig();
  const kc = loadKubeConfig();
  const source = new K8sPodSource(kc, config);
  const publisher = await NatsAlertPublisher.connect();
  const sink = new ClassifyingSink(publisher, stderrLogger);
  const watcher = new LogWatcher({ source, sink, config, logger: stderrLogger });

  const controller = new AbortController();
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.on(signal, () => {
      stderrLogger.info('shutdown signal received', { signal });
      controller.abort();
    });
  }

  try {
    await watcher.run(controller.signal);
  } finally {
    await publisher.close();
  }
}

main().catch((err) => {
  log('error', 'watcher failed to start', { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
