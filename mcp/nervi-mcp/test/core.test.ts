import { describe, expect, it } from 'vitest';
import {
  MSG_ID_HEADER,
  QUALIFIER_HEADER,
  QUALIFIERS,
  STREAM_NAME,
  STREAM_SUBJECTS,
  TIMESTAMP_HEADER,
  ValidationError,
  assertConsumerName,
  assertMaxMessages,
  assertMsgId,
  assertQualifier,
  assertSubject,
  buildHeaders,
  isQualifier,
  normalizePayload,
} from '../src/core.js';

describe('qualifier vocabulary', () => {
  it('locks the vocabulary to info | cross-project | data', () => {
    expect([...QUALIFIERS]).toEqual(['info', 'cross-project', 'data']);
  });

  it('accepts valid qualifiers', () => {
    for (const q of QUALIFIERS) {
      expect(isQualifier(q)).toBe(true);
      expect(assertQualifier(q)).toBe(q);
    }
  });

  it('rejects unknown qualifiers', () => {
    expect(() => assertQualifier('urgent')).toThrow(ValidationError);
    expect(() => assertQualifier(undefined)).toThrow(ValidationError);
    expect(isQualifier('urgent')).toBe(false);
  });
});

describe('assertSubject', () => {
  it('accepts concrete ops subjects', () => {
    expect(assertSubject('occitan.ops.sre.alerts')).toBe('occitan.ops.sre.alerts');
  });

  it('rejects empty, whitespace, wildcard, and non-ops subjects', () => {
    expect(() => assertSubject('')).toThrow(ValidationError);
    expect(() => assertSubject('ops.sre alerts')).toThrow(ValidationError);
    expect(() => assertSubject('ops.>')).toThrow(ValidationError);
    expect(() => assertSubject('ops.*')).toThrow(ValidationError);
    expect(() => assertSubject('dev.sre.alerts')).toThrow(ValidationError);
    expect(() => assertSubject('ops.sre.alerts')).toThrow(ValidationError);
    expect(() => assertSubject(42)).toThrow(ValidationError);
  });
});

describe('ADR-N-001 namespace contract', () => {
  // These assertions deliberately hardcode the occitan.* contract instead of
  // deriving it from core.ts. bus.integration.test.ts imports STREAM_SUBJECTS,
  // so if STREAM_SUBJECTS and assertSubject drift together to a wrong namespace
  // (as happened between PR #15 and PR #16), the integration test stays green
  // while production breaks silently. This block is the independent witness that pins
  // the contract to literals; it goes red the moment core.ts strays.
  it('STREAM_SUBJECTS matches occitan namespace', () => {
    expect(STREAM_SUBJECTS).toEqual(['occitan.>']);
  });
  it('assertSubject accepts occitan.* subjects', () => {
    expect(() => assertSubject('occitan.ops.sre.alerts')).not.toThrow();
    expect(() => assertSubject('occitan.foo.bar')).not.toThrow();
  });
  it('assertSubject rejects non-occitan subjects', () => {
    expect(() => assertSubject('ops.sre.alerts')).toThrow();
    expect(() => assertSubject('ops.>')).toThrow();
    expect(() => assertSubject('sre.alerts')).toThrow();
  });
});

describe('normalizePayload', () => {
  it('passes strings through unchanged', () => {
    expect(normalizePayload('hello')).toBe('hello');
  });

  it('JSON-encodes objects', () => {
    expect(normalizePayload({ a: 1 })).toBe('{"a":1}');
  });

  it('rejects null/undefined payloads', () => {
    expect(() => normalizePayload(undefined)).toThrow(ValidationError);
    expect(() => normalizePayload(null)).toThrow(ValidationError);
  });
});

describe('assertMaxMessages', () => {
  it('defaults to 10', () => {
    expect(assertMaxMessages(undefined)).toBe(10);
  });

  it('accepts integers in [1, 1000]', () => {
    expect(assertMaxMessages(1)).toBe(1);
    expect(assertMaxMessages(1000)).toBe(1000);
  });

  it('rejects out-of-range and non-integers', () => {
    expect(() => assertMaxMessages(0)).toThrow(ValidationError);
    expect(() => assertMaxMessages(1001)).toThrow(ValidationError);
    expect(() => assertMaxMessages(2.5)).toThrow(ValidationError);
  });
});

describe('assertConsumerName', () => {
  it('accepts durable-safe names', () => {
    expect(assertConsumerName('developer-consumer')).toBe('developer-consumer');
  });

  it('rejects names with NATS-forbidden characters', () => {
    expect(() => assertConsumerName('')).toThrow(ValidationError);
    expect(() => assertConsumerName('a.b')).toThrow(ValidationError);
    expect(() => assertConsumerName('a b')).toThrow(ValidationError);
    expect(() => assertConsumerName('a*')).toThrow(ValidationError);
  });
});

describe('assertMsgId', () => {
  it('returns undefined when value is undefined', () => {
    expect(assertMsgId(undefined)).toBeUndefined();
  });

  it('passes valid non-empty strings', () => {
    expect(assertMsgId('dispatch-caissa-43-20260630')).toBe('dispatch-caissa-43-20260630');
    expect(assertMsgId('dispatch-fondament-7-20260630')).toBe('dispatch-fondament-7-20260630');
  });

  it('rejects empty strings', () => {
    expect(() => assertMsgId('')).toThrow(ValidationError);
  });

  it('rejects strings with whitespace', () => {
    expect(() => assertMsgId('dispatch caissa')).toThrow(ValidationError);
    expect(() => assertMsgId('dispatch\tcaissa')).toThrow(ValidationError);
  });

  it('rejects non-string values when provided', () => {
    expect(() => assertMsgId(42)).toThrow(ValidationError);
    expect(() => assertMsgId(null)).toThrow(ValidationError);
  });
});

describe('buildHeaders', () => {
  it('embeds the qualifier and timestamp headers', () => {
    const ts = '2026-06-29T12:00:00.000Z';
    const headers = buildHeaders('cross-project', ts);
    expect(headers[QUALIFIER_HEADER]).toBe('cross-project');
    expect(headers[TIMESTAMP_HEADER]).toBe(ts);
  });

  it('omits Nats-Msg-Id when msgId is not provided', () => {
    const ts = '2026-06-29T12:00:00.000Z';
    const headers = buildHeaders('info', ts);
    expect(MSG_ID_HEADER in headers).toBe(false);
  });

  it('includes Nats-Msg-Id when msgId is provided', () => {
    const ts = '2026-06-29T12:00:00.000Z';
    const msgId = 'dispatch-caissa-43-20260630';
    const headers = buildHeaders('info', ts, msgId);
    expect(headers[MSG_ID_HEADER]).toBe(msgId);
    expect(headers[QUALIFIER_HEADER]).toBe('info');
    expect(headers[TIMESTAMP_HEADER]).toBe(ts);
  });
});

describe('MSG_ID_HEADER constant', () => {
  it('is the standard NATS deduplication header name', () => {
    expect(MSG_ID_HEADER).toBe('Nats-Msg-Id');
  });
});

describe('stream constants', () => {
  it('targets the OCCITAN stream over occitan.>', () => {
    expect(STREAM_NAME).toBe('OCCITAN');
    expect(STREAM_SUBJECTS).toEqual(['occitan.>']);
  });
});
