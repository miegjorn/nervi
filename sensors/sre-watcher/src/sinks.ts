/**
 * Nèrvi SRE watcher — classification sinks.
 *
 * The watcher emits every log line to a ClassificationSink. ConsoleSink is the
 * N-5 stand-in (NDJSON on stdout). ClassifyingSink is N-6: it applies the
 * classification rules from core.ts and publishes a structured alert to
 * occitan.ops.sre.alerts for every line that trips a rule.
 */
import {
  ALERT_SUBJECT,
  classify,
  type AlertPublisher,
  type ClassificationSink,
  type LogLine,
} from './core.js';

/**
 * Writes each log line as a single-line JSON object to stdout. Logs (lifecycle,
 * warnings) go to stderr in main.ts, so stdout stays a clean NDJSON data plane.
 */
export class ConsoleSink implements ClassificationSink {
  emit(line: LogLine): void {
    process.stdout.write(`${JSON.stringify(line)}\n`);
  }
}

/** Minimal logger seam so the sink stays decoupled from the watcher's Logger. */
export interface SinkLogger {
  warn(msg: string, meta?: Record<string, unknown>): void;
}

/**
 * N-6 sink: classify each log line and publish a structured alert to
 * occitan.ops.sre.alerts for every line that trips a rule. Non-matching lines
 * are dropped silently. A publish failure is logged but never propagated — one
 * bad publish must not tear down the pod's log stream.
 */
export class ClassifyingSink implements ClassificationSink {
  constructor(
    private readonly publisher: AlertPublisher,
    private readonly logger?: SinkLogger,
  ) {}

  async emit(line: LogLine): Promise<void> {
    const alert = classify(line);
    if (!alert) return;
    try {
      await this.publisher.publish(ALERT_SUBJECT, JSON.stringify(alert));
    } catch (err) {
      this.logger?.warn('alert publish failed', {
        pod: `${alert.namespace}/${alert.pod}`,
        classification: alert.classification,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
}
