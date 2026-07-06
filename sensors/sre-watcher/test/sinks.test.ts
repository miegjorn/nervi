import { describe, expect, it, vi } from 'vitest';
import { ALERT_SUBJECT, type AlertPublisher, type LogLine } from '../src/core.js';
import { ClassifyingSink } from '../src/sinks.js';

function line(message: string): LogLine {
  return {
    namespace: 'sre',
    pod: 'api-7d',
    container: 'app',
    message,
    timestamp: '2026-07-06T00:00:00.000Z',
  };
}

function recorder() {
  const calls: Array<{ subject: string; payload: string }> = [];
  const publisher: AlertPublisher = {
    publish: (subject, payload) => {
      calls.push({ subject, payload });
      return Promise.resolve();
    },
  };
  return { publisher, calls };
}

describe('ClassifyingSink', () => {
  it('publishes one alert to occitan.ops.sre.alerts for a matching line', async () => {
    const { publisher, calls } = recorder();
    await new ClassifyingSink(publisher).emit(line('ERROR upstream down'));
    expect(calls).toHaveLength(1);
    expect(calls[0].subject).toBe(ALERT_SUBJECT);
    expect(JSON.parse(calls[0].payload)).toMatchObject({
      pod: 'api-7d',
      namespace: 'sre',
      classification: 'error_level',
      severity: 'error',
      excerpt: 'ERROR upstream down',
      timestamp: '2026-07-06T00:00:00.000Z',
    });
  });

  it('publishes nothing for a non-matching line', async () => {
    const { publisher, calls } = recorder();
    await new ClassifyingSink(publisher).emit(line('GET /healthz 200'));
    expect(calls).toHaveLength(0);
  });

  it('does not throw and logs a warning when the publisher fails', async () => {
    const publisher: AlertPublisher = { publish: () => Promise.reject(new Error('nats down')) };
    const warn = vi.fn();
    await expect(
      new ClassifyingSink(publisher, { warn }).emit(line('FATAL boom')),
    ).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledOnce();
  });
});
