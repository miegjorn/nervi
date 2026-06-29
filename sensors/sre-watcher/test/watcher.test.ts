import { describe, expect, it } from 'vitest';
import {
  podKey,
  type LogLine,
  type PodRef,
  type PodSource,
  type RawLogLine,
  type StreamLogsOptions,
  type WatchConfig,
} from '../src/core.js';
import { LogWatcher } from '../src/watcher.js';

const CONFIG: WatchConfig = {
  namespaces: ['sre'],
  sinceSeconds: 10,
  pollIntervalMs: 1,
  reconnectBackoffMs: 1,
  maxReconnectBackoffMs: 4,
};

/** A sleep that always yields to the event loop and never blocks on real time. */
const fastSleep = (_ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const t = setTimeout(resolve, 0);
    signal.addEventListener('abort', () => { clearTimeout(t); resolve(); }, { once: true });
  });

/**
 * Scriptable PodSource. `targetScript` supplies successive listTargets results
 * (the last entry repeats). `episodes` maps a podKey to a queue of log batches;
 * each batch is yielded then the stream ends — modelling a follow stream that
 * closed and must be reconnected.
 */
class FakePodSource implements PodSource {
  targetScript: PodRef[][];
  episodes = new Map<string, RawLogLine[][]>();
  streamCalls: Array<{ ref: PodRef; opts: StreamLogsOptions }> = [];

  constructor(targets: PodRef[][]) {
    this.targetScript = targets;
  }

  async listTargets(): Promise<PodRef[]> {
    return this.targetScript.length > 1 ? this.targetScript.shift()! : this.targetScript[0] ?? [];
  }

  async *streamLogs(ref: PodRef, opts: StreamLogsOptions): AsyncIterable<RawLogLine> {
    this.streamCalls.push({ ref, opts: { ...opts } });
    const batch = this.episodes.get(podKey(ref))?.shift();
    if (batch) {
      for (const line of batch) {
        if (opts.signal.aborted) return;
        yield line;
      }
    }
    // batch (or absence of one) exhausted → generator returns → stream "ended".
  }
}

class RecordingSink {
  lines: LogLine[] = [];
  constructor(private readonly onEmit?: (line: LogLine) => void) {}
  emit(line: LogLine): void {
    this.lines.push(line);
    this.onEmit?.(line);
  }
}

const POD: PodRef = { namespace: 'sre', pod: 'api-7d', container: 'app' };

describe('LogWatcher.streamPod', () => {
  it('emits each streamed line enriched with pod identity and timestamp', async () => {
    const source = new FakePodSource([[POD]]);
    source.episodes.set(podKey(POD), [
      [
        { timestamp: 't1', message: 'hello' },
        { timestamp: 't2', message: 'world' },
      ],
    ]);
    const controller = new AbortController();
    const sink = new RecordingSink((l) => {
      if (l.message === 'world') controller.abort();
    });

    const watcher = new LogWatcher({ source, sink, config: CONFIG, sleep: fastSleep });
    await watcher.streamPod(POD, controller.signal);

    expect(sink.lines).toEqual([
      { namespace: 'sre', pod: 'api-7d', container: 'app', message: 'hello', timestamp: 't1' },
      { namespace: 'sre', pod: 'api-7d', container: 'app', message: 'world', timestamp: 't2' },
    ]);
  });

  it('falls back to the observation clock when a line has no timestamp', async () => {
    const source = new FakePodSource([[POD]]);
    source.episodes.set(podKey(POD), [[{ timestamp: '', message: 'no-ts' }]]);
    const controller = new AbortController();
    const sink = new RecordingSink(() => controller.abort());

    const watcher = new LogWatcher({
      source,
      sink,
      config: CONFIG,
      sleep: fastSleep,
      now: () => 'CLOCK',
    });
    await watcher.streamPod(POD, controller.signal);

    expect(sink.lines[0].timestamp).toBe('CLOCK');
  });

  it('resumes after a dropped stream using sinceTime and dedups the boundary line', async () => {
    const source = new FakePodSource([[POD]]);
    source.episodes.set(podKey(POD), [
      // first attach: two lines, then the follow stream drops
      [
        { timestamp: 't1', message: 'a' },
        { timestamp: 't2', message: 'b' },
      ],
      // reconnect redelivers the boundary line t2 then a fresh line t3
      [
        { timestamp: 't2', message: 'b' },
        { timestamp: 't3', message: 'c' },
      ],
    ]);
    const controller = new AbortController();
    const sink = new RecordingSink((l) => {
      if (l.message === 'c') controller.abort();
    });

    const watcher = new LogWatcher({ source, sink, config: CONFIG, sleep: fastSleep });
    await watcher.streamPod(POD, controller.signal);

    // b appears exactly once — the redelivered boundary copy is dropped.
    expect(sink.lines.map((l) => l.message)).toEqual(['a', 'b', 'c']);
    // first attach has no cursor; the reconnect resumes from the last timestamp.
    expect(source.streamCalls[0].opts.sinceTime).toBeUndefined();
    expect(source.streamCalls[0].opts.sinceSeconds).toBe(10);
    expect(source.streamCalls[1].opts.sinceTime).toBe('t2');
  });
});

describe('LogWatcher.reconcile', () => {
  it('starts tracking newly-seen pods', async () => {
    const source = new FakePodSource([[POD]]);
    const sink = new RecordingSink();
    const parent = new AbortController();

    const watcher = new LogWatcher({ source, sink, config: CONFIG, sleep: fastSleep });
    await watcher.reconcile(parent.signal);

    expect([...watcher.trackedKeys()]).toEqual([podKey(POD)]);

    parent.abort();
    await watcher.drain();
  });

  it('drops a rotated-away pod and picks up its replacement', async () => {
    const oldPod: PodRef = { namespace: 'sre', pod: 'api-OLD', container: 'app' };
    const newPod: PodRef = { namespace: 'sre', pod: 'api-NEW', container: 'app' };
    // first list returns the old pod, every subsequent list returns the new one.
    const source = new FakePodSource([[oldPod], [newPod]]);
    const sink = new RecordingSink();
    const parent = new AbortController();

    const watcher = new LogWatcher({ source, sink, config: CONFIG, sleep: fastSleep });

    await watcher.reconcile(parent.signal);
    expect([...watcher.trackedKeys()]).toEqual([podKey(oldPod)]);

    await watcher.reconcile(parent.signal);
    expect([...watcher.trackedKeys()]).toEqual([podKey(newPod)]);

    parent.abort();
    await watcher.drain();
  });
});
