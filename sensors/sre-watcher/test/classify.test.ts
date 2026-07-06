import { describe, expect, it } from 'vitest';
import { ALERT_SUBJECT, EXCERPT_MAX_CHARS, classify, type LogLine } from '../src/core.js';

function line(message: string, over: Partial<LogLine> = {}): LogLine {
  return {
    namespace: 'sre',
    pod: 'api-7d',
    container: 'app',
    message,
    timestamp: '2026-07-06T00:00:00.000Z',
    ...over,
  };
}

describe('classify — rule matching & severity', () => {
  it('classifies ERROR as error_level / error', () => {
    expect(classify(line('ERROR failed to reach upstream'))).toMatchObject({
      classification: 'error_level',
      severity: 'error',
    });
  });

  it('classifies FATAL as error_level / critical', () => {
    expect(classify(line('FATAL cannot bind port'))).toMatchObject({
      classification: 'error_level',
      severity: 'critical',
    });
  });

  it('classifies CRITICAL as error_level / critical', () => {
    expect(classify(line('CRITICAL disk failure'))).toMatchObject({
      classification: 'error_level',
      severity: 'critical',
    });
  });

  it('is case-insensitive on the level keyword', () => {
    expect(classify(line('error: lowercase still counts'))).toMatchObject({
      classification: 'error_level',
      severity: 'error',
    });
  });

  it.each(['OOMKilled', 'Out of memory: Killed process 1', 'memory limit exceeded'])(
    'classifies OOM pattern "%s" as oom / critical',
    (msg) => {
      expect(classify(line(msg))).toMatchObject({ classification: 'oom', severity: 'critical' });
    },
  );

  it.each(['CrashLoopBackOff', 'back-off restarting failed container app'])(
    'classifies crash pattern "%s" as crash_loop / critical',
    (msg) => {
      expect(classify(line(msg))).toMatchObject({ classification: 'crash_loop', severity: 'critical' });
    },
  );

  it.each(['restarting container app', 'container restarted after failure'])(
    'classifies restart pattern "%s" as restart_loop / warning',
    (msg) => {
      expect(classify(line(msg))).toMatchObject({ classification: 'restart_loop', severity: 'warning' });
    },
  );

  it.each([
    'Traceback (most recent call last):',
    'panic: runtime error: invalid memory address',
    'SIGSEGV: segmentation violation',
    'segfault at 0x0 ip 00007f',
  ])('classifies traceback pattern "%s" as traceback / critical', (msg) => {
    expect(classify(line(msg))).toMatchObject({ classification: 'traceback', severity: 'critical' });
  });

  it('returns null for a benign line', () => {
    expect(classify(line('GET /healthz 200 in 3ms'))).toBeNull();
  });
});

describe('classify — payload shape', () => {
  it('carries pod, namespace and timestamp into the alert', () => {
    expect(
      classify(line('ERROR boom', { pod: 'p1', namespace: 'ns1', timestamp: '2026-07-06T01:02:03Z' })),
    ).toMatchObject({ pod: 'p1', namespace: 'ns1', timestamp: '2026-07-06T01:02:03Z' });
  });

  it('uses the full message as the excerpt when short', () => {
    expect(classify(line('ERROR short'))?.excerpt).toBe('ERROR short');
  });

  it('truncates the excerpt to EXCERPT_MAX_CHARS', () => {
    const long = `ERROR ${'x'.repeat(1000)}`;
    const a = classify(line(long));
    expect(a?.excerpt.length).toBe(EXCERPT_MAX_CHARS);
    expect(a?.excerpt).toBe(long.slice(0, EXCERPT_MAX_CHARS));
  });
});

describe('classify — precedence', () => {
  it('prefers the more severe oom over a bare error level', () => {
    expect(classify(line('ERROR container OOMKilled'))).toMatchObject({
      classification: 'oom',
      severity: 'critical',
    });
  });

  it('prefers crash_loop over a bare error level', () => {
    expect(classify(line('ERROR CrashLoopBackOff'))).toMatchObject({ classification: 'crash_loop' });
  });

  it('publishes on the agreed subject constant', () => {
    expect(ALERT_SUBJECT).toBe('occitan.ops.sre.alerts');
  });
});
