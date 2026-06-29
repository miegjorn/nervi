import { describe, expect, it } from 'vitest';
import { ValidationError, type Qualifier, type ReceivedMessage, type SignalBus } from '../src/core.js';
import { handlePublish, handleSubscribe } from '../src/handlers.js';

/** Records every publish and replays canned fetch results. */
class FakeBus implements SignalBus {
  published: Array<{ subject: string; payload: string; qualifier: Qualifier }> = [];
  fetched: Array<{ subject: string; consumerName: string; maxMessages: number }> = [];
  fetchResult: ReceivedMessage[] = [];

  async publish(subject: string, payload: string, qualifier: Qualifier) {
    this.published.push({ subject, payload, qualifier });
    return { stream: 'OCCITAN', seq: this.published.length };
  }

  async fetch(subject: string, consumerName: string, maxMessages: number) {
    this.fetched.push({ subject, consumerName, maxMessages });
    return this.fetchResult;
  }
}

function parse(result: { content: Array<{ text: string }> }) {
  return JSON.parse(result.content[0].text);
}

describe('handlePublish', () => {
  it('publishes a string payload with the qualifier carried through', async () => {
    const bus = new FakeBus();
    const result = await handlePublish(bus, {
      subject: 'ops.sre.alerts',
      payload: 'disk full on node-3',
      qualifier: 'info',
    });

    expect(bus.published).toEqual([
      { subject: 'ops.sre.alerts', payload: 'disk full on node-3', qualifier: 'info' },
    ]);
    const body = parse(result);
    expect(body).toMatchObject({ published: true, subject: 'ops.sre.alerts', qualifier: 'info', stream: 'OCCITAN', seq: 1 });
  });

  it('JSON-encodes object payloads before handing them to the bus', async () => {
    const bus = new FakeBus();
    await handlePublish(bus, {
      subject: 'ops.sre.alerts',
      payload: { level: 'critical', node: 'node-3' },
      qualifier: 'data',
    });
    expect(bus.published[0].payload).toBe('{"level":"critical","node":"node-3"}');
  });

  it('rejects bad input before touching the bus', async () => {
    const bus = new FakeBus();
    await expect(handlePublish(bus, { subject: 'ops.x', payload: 'p', qualifier: 'nope' })).rejects.toThrow(
      ValidationError,
    );
    await expect(handlePublish(bus, { subject: 'bad.subject', payload: 'p', qualifier: 'info' })).rejects.toThrow(
      ValidationError,
    );
    expect(bus.published).toHaveLength(0);
  });
});

describe('handleSubscribe', () => {
  it('fetches via a durable consumer and returns formatted messages', async () => {
    const bus = new FakeBus();
    bus.fetchResult = [
      {
        sequence: 7,
        subject: 'ops.sre.alerts',
        qualifier: 'info',
        payload: 'disk full',
        timestamp: '2026-06-29T12:00:00.000Z',
      },
    ];

    const result = await handleSubscribe(bus, {
      subject: 'ops.sre.alerts',
      consumer_name: 'developer-consumer',
      max_messages: 5,
    });

    expect(bus.fetched).toEqual([
      { subject: 'ops.sre.alerts', consumerName: 'developer-consumer', maxMessages: 5 },
    ]);
    const body = parse(result);
    expect(body.count).toBe(1);
    expect(body.messages[0]).toMatchObject({ sequence: 7, qualifier: 'info', payload: 'disk full' });
  });

  it('defaults max_messages to 10', async () => {
    const bus = new FakeBus();
    await handleSubscribe(bus, { subject: 'ops.sre.alerts', consumer_name: 'dev' });
    expect(bus.fetched[0].maxMessages).toBe(10);
  });

  it('rejects bad consumer names before touching the bus', async () => {
    const bus = new FakeBus();
    await expect(
      handleSubscribe(bus, { subject: 'ops.sre.alerts', consumer_name: 'bad.name' }),
    ).rejects.toThrow(ValidationError);
    expect(bus.fetched).toHaveLength(0);
  });
});
