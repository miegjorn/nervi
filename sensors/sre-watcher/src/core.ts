/**
 * Nèrvi SRE watcher (N-5) — core domain logic.
 *
 * Pure, Kubernetes-independent building blocks: the data shapes that travel
 * through the sensor, the two seams the watcher depends on (PodSource toward
 * the cluster, ClassificationSink toward N-6), config parsing, and log-line
 * parsing. Keeping this layer free of any `@kubernetes/client-node` import is
 * what lets the watcher logic be unit-tested without a cluster — the same
 * discipline the MCP server uses with its SignalBus seam.
 */

/** Identifies a single pod/container log stream. */
export interface PodRef {
  namespace: string;
  pod: string;
  container: string;
}

/** A log line as it comes off the Kubernetes log API (timestamps:true). */
export interface RawLogLine {
  /** RFC3339(Nano) timestamp parsed from the line, or '' when none is present. */
  timestamp: string;
  /** The log text with any leading Kubernetes timestamp stripped. */
  message: string;
}

/**
 * A log line enriched with its pod identity — the unit handed to the
 * classification stage. This is the N-6 interface contract: N-6 consumes
 * LogLine, applies its rules, and (N-7) writes the resulting Farga signal.
 */
export interface LogLine {
  namespace: string;
  pod: string;
  container: string;
  message: string;
  /** The k8s log timestamp when present, otherwise the observation time. */
  timestamp: string;
}

/**
 * The classification-stage seam (N-6). The watcher emits every log line here
 * and nothing more — classification, alerting, and the Farga signal write all
 * live behind this interface. Until N-6 ships, ConsoleSink stands in. Designing
 * the watcher against this seam is what lets N-6 plug in without touching N-5.
 */
export interface ClassificationSink {
  emit(line: LogLine): Promise<void> | void;
}

/** Options controlling a single log-stream attachment. */
export interface StreamLogsOptions {
  /** Resume cursor: only return lines strictly after this RFC3339 timestamp. */
  sinceTime?: string;
  /** Lookback (seconds) used on a first attach, when there is no cursor yet. */
  sinceSeconds?: number;
  /** Aborts the stream (pod gone, shutdown). */
  signal: AbortSignal;
}

/**
 * The Kubernetes seam. K8sPodSource implements it over the cluster API; tests
 * substitute a fake. The watcher depends only on this — so the reconnect,
 * resume, and rotation logic can be exercised without a live API server.
 */
export interface PodSource {
  /** List the pod/container targets currently matching the watch config. */
  listTargets(): Promise<PodRef[]>;
  /**
   * Stream log lines from one pod/container, following until the stream ends
   * (pod EOF) or the abort signal fires. The async iterable completing is the
   * signal that the stream ended and the watcher should reconnect.
   */
  streamLogs(ref: PodRef, opts: StreamLogsOptions): AsyncIterable<RawLogLine>;
}

/** Raised when watcher configuration fails validation. */
export class ConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConfigError';
  }
}

/** Validated runtime configuration for the watcher. */
export interface WatchConfig {
  /** Namespaces to watch. Empty means cluster-wide (all namespaces). */
  namespaces: string[];
  /** Optional Kubernetes label selector narrowing which pods are watched. */
  labelSelector?: string;
  /** Optional container name; when set, only that container is streamed. */
  container?: string;
  /** Lookback applied when first attaching to a pod that has no resume cursor. */
  sinceSeconds: number;
  /** How often to re-list targets to pick up new / rotated pods (ms). */
  pollIntervalMs: number;
  /** Base reconnect backoff after a stream ends or errors (ms). */
  reconnectBackoffMs: number;
  /** Upper bound on the reconnect backoff (ms). */
  maxReconnectBackoffMs: number;
}

/** Stable identity for a pod/container stream, used as a tracking-map key. */
export function podKey(ref: PodRef): string {
  return `${ref.namespace}/${ref.pod}/${ref.container}`;
}

/** True when `s` begins with an RFC3339 instant and parses as a real date. */
function looksLikeRfc3339(s: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(s)) return false;
  return !Number.isNaN(Date.parse(s));
}

/**
 * Parse one line emitted by the Kubernetes log API with `timestamps: true`.
 * The format is `<RFC3339Nano> <message>`. When the first token is not a
 * timestamp, the whole line is the message and the timestamp is ''.
 */
export function parseLogLine(raw: string): RawLogLine {
  const sep = raw.indexOf(' ');
  if (sep > 0) {
    const head = raw.slice(0, sep);
    if (looksLikeRfc3339(head)) {
      return { timestamp: head, message: raw.slice(sep + 1) };
    }
  }
  return { timestamp: '', message: raw };
}

/** Parse a non-negative integer environment value, or fall back to `def`. */
function intEnv(env: NodeJS.ProcessEnv, key: string, def: number): number {
  const raw = env[key];
  if (raw === undefined || raw.trim() === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) {
    throw new ConfigError(`${key} must be a non-negative integer, got ${JSON.stringify(raw)}`);
  }
  return n;
}

/**
 * Build a validated WatchConfig from the environment. All knobs have sane
 * defaults so the watcher runs cluster-wide out of the box; narrow it with
 * WATCH_NAMESPACES / WATCH_LABEL_SELECTOR / WATCH_CONTAINER.
 */
export function parseConfig(env: NodeJS.ProcessEnv = process.env): WatchConfig {
  const namespaces = (env.WATCH_NAMESPACES ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  const labelSelector = env.WATCH_LABEL_SELECTOR?.trim() || undefined;
  const container = env.WATCH_CONTAINER?.trim() || undefined;

  const sinceSeconds = intEnv(env, 'WATCH_SINCE_SECONDS', 10);
  const pollIntervalMs = intEnv(env, 'WATCH_POLL_INTERVAL_MS', 15_000);
  const reconnectBackoffMs = intEnv(env, 'WATCH_RECONNECT_BACKOFF_MS', 1_000);
  const maxReconnectBackoffMs = intEnv(env, 'WATCH_MAX_RECONNECT_BACKOFF_MS', 30_000);

  if (maxReconnectBackoffMs < reconnectBackoffMs) {
    throw new ConfigError(
      'WATCH_MAX_RECONNECT_BACKOFF_MS must be >= WATCH_RECONNECT_BACKOFF_MS',
    );
  }

  return {
    namespaces,
    labelSelector,
    container,
    sinceSeconds,
    pollIntervalMs,
    reconnectBackoffMs,
    maxReconnectBackoffMs,
  };
}
