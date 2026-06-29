/**
 * Nèrvi MCP — tool handlers.
 *
 * These translate raw MCP tool arguments into validated bus operations and
 * format the bus result back into an MCP CallToolResult. They depend only on
 * the SignalBus seam, so they are fully unit-testable with a fake bus.
 */
import {
  assertConsumerName,
  assertMaxMessages,
  assertQualifier,
  assertSubject,
  normalizePayload,
  type ReceivedMessage,
  type SignalBus,
} from './core.js';

/** Minimal MCP CallToolResult shape we emit. */
export interface ToolResult {
  content: Array<{ type: 'text'; text: string }>;
  isError?: boolean;
  structuredContent?: Record<string, unknown>;
}

function jsonResult(value: unknown): ToolResult {
  const structuredContent = value as Record<string, unknown>;
  return {
    content: [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    structuredContent,
  };
}

export interface PublishArgs {
  subject?: unknown;
  payload?: unknown;
  qualifier?: unknown;
}

/** N-2: publish a message to an OCCITAN subject with an embedded qualifier. */
export async function handlePublish(bus: SignalBus, args: PublishArgs): Promise<ToolResult> {
  const subject = assertSubject(args.subject);
  const qualifier = assertQualifier(args.qualifier);
  const payload = normalizePayload(args.payload);

  const result = await bus.publish(subject, payload, qualifier);
  return jsonResult({
    published: true,
    subject,
    qualifier,
    stream: result.stream,
    seq: result.seq,
  });
}

export interface SubscribeArgs {
  subject?: unknown;
  consumer_name?: unknown;
  max_messages?: unknown;
}

/** N-3: fetch pending messages from an OCCITAN subject via a durable consumer. */
export async function handleSubscribe(bus: SignalBus, args: SubscribeArgs): Promise<ToolResult> {
  const subject = assertSubject(args.subject);
  const consumerName = assertConsumerName(args.consumer_name);
  const maxMessages = assertMaxMessages(args.max_messages);

  const messages: ReceivedMessage[] = await bus.fetch(subject, consumerName, maxMessages);
  return jsonResult({
    subject,
    consumer_name: consumerName,
    count: messages.length,
    messages,
  });
}
