/**
 * Nèrvi SRE watcher (N-5) — orchestration.
 *
 * Ties the Kubernetes seam (PodSource) to the classification seam
 * (ClassificationSink). The watcher:
 *   - periodically reconciles the set of watched pods (picking up new pods and
 *     dropping rotated-away ones),
 *   - holds one streaming task per pod/container,
 *   - follows each log stream and emits every line to the sink,
 *   - reconnects with exponential backoff when a stream ends, resuming from the
 *     last seen timestamp so no lines are lost or duplicated across the gap.
 *
 * It imports nothing Kubernetes-specific — only the seams in core.ts — so the
 * resume / reconnect / rotation behaviour is fully unit-testable.
 */
import {
  podKey,
  type ClassificationSink,
  type LogLine,
  type PodRef,
  type PodSource,
  type StreamLogsOptions,
  type WatchConfig,
} from './core.js';

export interface Logger {
  info(msg: string, meta?: Record<string, unknown>): void;
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
}

const NOOP_LOGGER: Logger = { info() {}, warn() {}, error() {} };

/** Default abortable sleep, backed by real timers. */
function defaultSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export interface WatcherDeps {
  source: PodSource;
  sink: ClassificationSink;
  config: WatchConfig;
  logger?: Logger;
  /** Injectable abortable sleep — tests substitute a non-blocking version. */
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  /** Injectable observation clock for the no-timestamp fallback. */
  now?: () => string;
}

/** A single in-flight pod streaming task. */
interface PodTask {
  controller: AbortController;
  done: Promise<void>;
}

export class LogWatcher {
  private readonly source: PodSource;
  private readonly sink: ClassificationSink;
  private readonly config: WatchConfig;
  private readonly logger: Logger;
  private readonly sleep: (ms: number, signal: AbortSignal) => Promise<void>;
  private readonly now: () => string;

  /** One streaming task per tracked pod/container, keyed by podKey. */
  private readonly tasks = new Map<string, PodTask>();

  constructor(deps: WatcherDeps) {
    this.source = deps.source;
    this.sink = deps.sink;
    this.config = deps.config;
    this.logger = deps.logger ?? NOOP_LOGGER;
    this.sleep = deps.sleep ?? defaultSleep;
    this.now = deps.now ?? (() => new Date().toISOString());
  }

  /** The pods currently being tracked (test/observability hook). */
  trackedKeys(): IterableIterator<string> {
    return this.tasks.keys();
  }

  /** Run the reconcile loop until `signal` aborts, then stop all streams. */
  async run(signal: AbortSignal): Promise<void> {
    this.logger.info('watcher starting', {
      namespaces: this.config.namespaces.length ? this.config.namespaces : ['<all>'],
      labelSelector: this.config.labelSelector ?? '<none>',
      container: this.config.container ?? '<all>',
    });
    while (!signal.aborted) {
      try {
        await this.reconcile(signal);
      } catch (err) {
        this.logger.error('reconcile failed', { error: errMessage(err) });
      }
      await this.sleep(this.config.pollIntervalMs, signal);
    }
    await this.drain();
    this.logger.info('watcher stopped');
  }

  /**
   * Diff the live target set against the tracked set: stop tasks for pods that
   * have gone (rotation / deletion) and start tasks for newly-seen pods.
   */
  async reconcile(parentSignal: AbortSignal): Promise<void> {
    const targets = await this.source.listTargets();
    const live = new Set(targets.map(podKey));

    for (const [key, task] of this.tasks) {
      if (!live.has(key)) {
        this.logger.info('pod gone — stopping stream', { pod: key });
        task.controller.abort();
        this.tasks.delete(key);
      }
    }

    for (const ref of targets) {
      const key = podKey(ref);
      if (this.tasks.has(key)) continue;
      this.logger.info('pod found — starting stream', { pod: key });
      const controller = new AbortController();
      const onParentAbort = () => controller.abort();
      parentSignal.addEventListener('abort', onParentAbort, { once: true });
      const done = this.streamPod(ref, controller.signal)
        .catch((err) => this.logger.error('pod stream crashed', { pod: key, error: errMessage(err) }))
        .finally(() => parentSignal.removeEventListener('abort', onParentAbort));
      this.tasks.set(key, { controller, done });
    }
  }

  /** Abort every tracked stream and wait for the tasks to settle. */
  async drain(): Promise<void> {
    const tasks = [...this.tasks.values()];
    for (const task of tasks) task.controller.abort();
    await Promise.allSettled(tasks.map((t) => t.done));
    this.tasks.clear();
  }

  /**
   * Follow one pod/container's logs until aborted. On each stream end we
   * reconnect with exponential backoff, resuming from the last seen timestamp
   * via `sinceTime`. Lines at or before the cursor are dropped so the boundary
   * line redelivered on reconnect is not emitted twice.
   *
   * Public for direct unit testing; `reconcile` is the normal entry point.
   */
  async streamPod(ref: PodRef, signal: AbortSignal): Promise<void> {
    const key = podKey(ref);
    let cursor = ''; // last emitted RFC3339 timestamp; '' until the first line
    let backoff = this.config.reconnectBackoffMs;

    while (!signal.aborted) {
      const opts: StreamLogsOptions = cursor
        ? { sinceTime: cursor, signal }
        : { sinceSeconds: this.config.sinceSeconds, signal };

      try {
        for await (const raw of this.source.streamLogs(ref, opts)) {
          if (signal.aborted) break;
          // Drop the boundary line redelivered by sinceTime on reconnect.
          if (cursor && raw.timestamp && raw.timestamp <= cursor) continue;

          const line: LogLine = {
            namespace: ref.namespace,
            pod: ref.pod,
            container: ref.container,
            message: raw.message,
            timestamp: raw.timestamp || this.now(),
          };
          await this.sink.emit(line);

          if (raw.timestamp) cursor = raw.timestamp;
          backoff = this.config.reconnectBackoffMs; // progress → reset backoff
        }
      } catch (err) {
        this.logger.warn('log stream error', { pod: key, error: errMessage(err) });
      }

      if (signal.aborted) break;
      this.logger.info('log stream ended — reconnecting', { pod: key, sinceTime: cursor || null });
      await this.sleep(backoff, signal);
      backoff = Math.min(backoff * 2, this.config.maxReconnectBackoffMs);
    }
  }
}
