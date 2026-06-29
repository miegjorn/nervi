/**
 * Nèrvi SRE watcher (N-5) — classification sinks.
 *
 * The watcher emits every log line to a ClassificationSink. N-6 (the alert
 * classification rules) will be a sink implementation, and N-7 (the Farga
 * signal write) sits behind it. Until N-6 ships, ConsoleSink stands in: it
 * makes the sensor's output observable as NDJSON on stdout, which is exactly
 * the line-delimited stream N-6 will consume.
 */
import type { ClassificationSink, LogLine } from './core.js';

/**
 * Writes each log line as a single-line JSON object to stdout. Logs (lifecycle,
 * warnings) go to stderr in main.ts, so stdout stays a clean NDJSON data plane.
 */
export class ConsoleSink implements ClassificationSink {
  emit(line: LogLine): void {
    process.stdout.write(`${JSON.stringify(line)}\n`);
  }
}
