import { describe, expect, it } from 'vitest';
import {
  ConfigError,
  parseConfig,
  parseLogLine,
  podKey,
} from '../src/core.js';

describe('podKey', () => {
  it('joins namespace / pod / container into a stable key', () => {
    expect(podKey({ namespace: 'sre', pod: 'api-7d', container: 'app' })).toBe('sre/api-7d/app');
  });
});

describe('parseLogLine', () => {
  it('splits a k8s timestamped line into timestamp + message', () => {
    const raw = '2026-06-29T12:00:00.123456789Z disk full on node-3';
    expect(parseLogLine(raw)).toEqual({
      timestamp: '2026-06-29T12:00:00.123456789Z',
      message: 'disk full on node-3',
    });
  });

  it('treats a line without a leading timestamp as a bare message', () => {
    expect(parseLogLine('plain log with no timestamp')).toEqual({
      timestamp: '',
      message: 'plain log with no timestamp',
    });
  });

  it('does not misread a leading word that merely looks numeric', () => {
    expect(parseLogLine('2026 was a good year')).toEqual({
      timestamp: '',
      message: '2026 was a good year',
    });
  });

  it('preserves empty messages after the timestamp', () => {
    expect(parseLogLine('2026-06-29T12:00:00Z ')).toEqual({
      timestamp: '2026-06-29T12:00:00Z',
      message: '',
    });
  });
});

describe('parseConfig', () => {
  it('applies defaults on an empty environment', () => {
    const cfg = parseConfig({});
    expect(cfg).toEqual({
      namespaces: [],
      labelSelector: undefined,
      container: undefined,
      sinceSeconds: 10,
      pollIntervalMs: 15000,
      reconnectBackoffMs: 1000,
      maxReconnectBackoffMs: 30000,
    });
  });

  it('parses a comma-separated namespace list and trims blanks', () => {
    expect(parseConfig({ WATCH_NAMESPACES: 'sre, ops ,, default' }).namespaces).toEqual([
      'sre',
      'ops',
      'default',
    ]);
  });

  it('carries label selector and container filter through', () => {
    const cfg = parseConfig({ WATCH_LABEL_SELECTOR: 'app=api', WATCH_CONTAINER: 'app' });
    expect(cfg.labelSelector).toBe('app=api');
    expect(cfg.container).toBe('app');
  });

  it('parses numeric tunables', () => {
    const cfg = parseConfig({
      WATCH_SINCE_SECONDS: '30',
      WATCH_POLL_INTERVAL_MS: '5000',
      WATCH_RECONNECT_BACKOFF_MS: '500',
      WATCH_MAX_RECONNECT_BACKOFF_MS: '60000',
    });
    expect(cfg.sinceSeconds).toBe(30);
    expect(cfg.pollIntervalMs).toBe(5000);
    expect(cfg.reconnectBackoffMs).toBe(500);
    expect(cfg.maxReconnectBackoffMs).toBe(60000);
  });

  it('rejects non-integer tunables', () => {
    expect(() => parseConfig({ WATCH_SINCE_SECONDS: 'soon' })).toThrow(ConfigError);
    expect(() => parseConfig({ WATCH_POLL_INTERVAL_MS: '-1' })).toThrow(ConfigError);
  });

  it('rejects a max backoff below the base backoff', () => {
    expect(() =>
      parseConfig({ WATCH_RECONNECT_BACKOFF_MS: '5000', WATCH_MAX_RECONNECT_BACKOFF_MS: '1000' }),
    ).toThrow(ConfigError);
  });
});
