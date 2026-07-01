/**
 * Nèrvi MCP — core domain logic.
 *
 * Pure, NATS-independent building blocks shared by the publish/subscribe
 * handlers and the NATS bus adapter. Keeping this layer free of any NATS
 * import is what lets the handler logic be unit-tested against a fake bus.
 */

/** Qualifier vocabulary — maps directly onto Farga node types. */
export const QUALIFIERS = ['info', 'cross-project', 'data'] as const;
export type Qualifier = (typeof QUALIFIERS)[number];

/** Header carrying the qualifier on every Nèrvi message. */
export const QUALIFIER_HEADER = 'Nervi-Qualifier';
/** Header carrying the publish-time ISO-8601 timestamp. */
export const TIMESTAMP_HEADER = 'Nervi-Timestamp';
/**
 * Standard NATS JetStream deduplication header. When present on a publish,
 * the broker silently drops any subsequent publish with the same value that
 * arrives within the stream's duplicate_window (currently 1 hour).
 *
 * Convention (see ADR-N-002): dispatch messages use the format
 *   dispatch-<component>-<issue-number>-<date>
 * e.g. `dispatch-fondament-7-20260630`. This makes duplicate dispatches of the
 * same issue on the same calendar day idempotent at the broker layer.
 */
export const MSG_ID_HEADER = 'Nats-Msg-Id';

/** The single durable JetStream stream backing all operational topics. */
export const STREAM_NAME = 'OCCITAN';
/** Subjects captured by the OCCITAN stream (covers occitan.ops.sre.alerts, etc.). */
export const STREAM_SUBJECTS = ['occitan.>'];

/** Raised when tool input fails validation. Carries no NATS state. */
export class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export function isQualifier(value: unknown): value is Qualifier {
  return typeof value === 'string' && (QUALIFIERS as readonly string[]).includes(value);
}

export function assertQualifier(value: unknown): Qualifier {
  if (!isQualifier(value)) {
    throw new ValidationError(
      `qualifier must be one of ${QUALIFIERS.join(' | ')}, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

/**
 * A subject must be non-empty, whitespace-free, and live under a configured
 * stream subject prefix. We only accept `occitan.*` because that is what the
 * OCCITAN stream captures — publishing elsewhere would silently drop.
 */
export function assertSubject(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new ValidationError('subject must be a non-empty string');
  }
  if (/\s/.test(value)) {
    throw new ValidationError(`subject must not contain whitespace, got ${JSON.stringify(value)}`);
  }
  if (value.includes('>') || value.includes('*')) {
    throw new ValidationError('subject must be a concrete subject, not a wildcard');
  }
  if (!value.startsWith('occitan.')) {
    throw new ValidationError(
      `subject must live under an OCCITAN stream prefix (${STREAM_SUBJECTS.join(', ')}), got ${value}`,
    );
  }
  return value;
}

/**
 * Normalize a payload to the wire string. Strings pass through unchanged;
 * anything else is JSON-encoded so structured payloads round-trip.
 */
export function normalizePayload(payload: unknown): string {
  if (typeof payload === 'string') return payload;
  if (payload === undefined || payload === null) {
    throw new ValidationError('payload is required');
  }
  return JSON.stringify(payload);
}

export function assertMaxMessages(value: unknown): number {
  const n = value === undefined ? 10 : value;
  if (typeof n !== 'number' || !Number.isInteger(n) || n < 1 || n > 1000) {
    throw new ValidationError('max_messages must be an integer between 1 and 1000');
  }
  return n;
}

export function assertConsumerName(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new ValidationError('consumer_name must be a non-empty string');
  }
  // NATS durable names forbid these characters.
  if (/[.*>\s/\\]/.test(value)) {
    throw new ValidationError(
      `consumer_name must not contain whitespace or any of . * > / \\, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

/**
 * Validate a Nats-Msg-Id value. Must be a non-empty string without whitespace.
 * Returns the value unchanged on success, or undefined if value is undefined.
 */
export function assertMsgId(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || value.length === 0) {
    throw new ValidationError('msg_id must be a non-empty string when provided');
  }
  if (/\s/.test(value)) {
    throw new ValidationError(`msg_id must not contain whitespace, got ${JSON.stringify(value)}`);
  }
  return value;
}

/** Build the header map embedded on a published message. */
export function buildHeaders(
  qualifier: Qualifier,
  timestamp: string,
  msgId?: string,
): Record<string, string> {
  const headers: Record<string, string> = {
    [QUALIFIER_HEADER]: qualifier,
    [TIMESTAMP_HEADER]: timestamp,
  };
  if (msgId !== undefined) {
    headers[MSG_ID_HEADER] = msgId;
  }
  return headers;
}

/** Result of a successful publish, as surfaced to the MCP caller. */
export interface PublishResult {
  stream: string;
  seq: number;
}

/** A message fetched from the bus, in the shape returned to the MCP caller. */
export interface ReceivedMessage {
  sequence: number;
  subject: string;
  qualifier: string | null;
  payload: string;
  timestamp: string;
}

/**
 * The signal bus seam. The real implementation (NatsBus) wraps NATS
 * JetStream; tests substitute a fake. Handlers depend only on this.
 */
export interface SignalBus {
  publish(subject: string, payload: string, qualifier: Qualifier, msgId?: string): Promise<PublishResult>;
  fetch(subject: string, consumerName: string, maxMessages: number): Promise<ReceivedMessage[]>;
}
