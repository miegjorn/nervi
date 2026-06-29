# N-4 — Integration test plan: SRE sensor → `ops.sre.alerts` → consumer

**Status:** planned (not yet implemented). Closes nervi#6 when realized.

N-4 is the first end-to-end proof that the Signal Bus Core (N-1/N-2/N-3) carries a real
operational signal from a producer to a consumer through the OCCITAN stream. It depends
on N-1 being deployed and the `nervi-mcp` server reachable in-cluster.

This document is the plan the implementation will follow — it is not a runnable test
yet. Epic 2 (N-5 watcher, N-6 classification, N-7 Farga write) provides the *real* SRE
sensor; N-4 may use a stand-in producer so the fabric can be validated before the sensor
lands.

## Goal

Demonstrate, against a live NATS JetStream + `nervi-mcp`:

> A signal published to `ops.sre.alerts` by an SRE-style producer is durably stored on
> the OCCITAN stream and later fetched, intact and correctly qualified, by an
> independent durable consumer — even when the consumer reads *after* the producer has
> finished.

## Preconditions

1. `helm upgrade --install nervi ./helm/nervi -n nervi` succeeded.
2. The `nervi-stream-provision` hook Job completed (OCCITAN stream exists).
   Verify: `nats -s nats://nervi-nats.nervi:4222 stream info OCCITAN`.
3. `nervi-mcp` Deployment is Ready (readiness probe `/health` green).

## Test environment

Two viable harnesses; pick one when implementing:

- **In-cluster (preferred, true E2E):** a `Job` in namespace `nervi` running an MCP
  client that talks to `nervi-mcp` over Streamable HTTP, exercising the real tools.
- **Ephemeral NATS (CI-friendly):** spin up `nats:2.10-alpine` with `-js`, provision the
  OCCITAN stream, and run the `nervi-mcp` server (stdio) against it. No k8s required;
  suitable for the `nervi-mcp` package's own integration suite.

## Scenario

| Step | Actor | Action | Expectation |
|------|-------|--------|-------------|
| 1 | Producer | `nervi_publish` `{subject: "ops.sre.alerts", qualifier: "info", payload: {level:"critical", node:"node-3", msg:"disk 95% full"}}` | Returns `{published:true, stream:"OCCITAN", seq:N}` |
| 2 | Producer | Publish a second, distinct alert (`qualifier: "cross-project"`) | `seq` increments |
| 3 | — | (consumer is created only now — proves async/durable delivery) | — |
| 4 | Consumer | `nervi_subscribe` `{subject:"ops.sre.alerts", consumer_name:"developer-consumer", max_messages:10}` | Returns both messages |
| 5 | Consumer | Inspect returned messages | `qualifier` matches what was published; `payload` round-trips byte-for-byte; `sequence` ascending; `timestamp` present and ISO-8601 |
| 6 | Consumer | Call `nervi_subscribe` again, same `consumer_name` | Returns 0 messages (previous fetch acked them — durable cursor advanced) |
| 7 | Consumer | Call with a *new* `consumer_name` | Returns both messages again (independent cursor) |

## Assertions

- **Durability:** consumer created after producer exits still receives all messages
  (steps 3–4).
- **Qualifier integrity:** `Nervi-Qualifier` header survives the round-trip (step 5).
- **Payload integrity:** JSON payloads round-trip without mangling (step 5).
- **Consumer durability / ack semantics:** acked messages are not redelivered to the same
  durable consumer; a fresh consumer name replays from the stream (steps 6–7).
- **Subject filtering:** a message published to a *different* `ops.*` subject is not
  delivered to the `ops.sre.alerts` consumer.

## Negative / edge cases

- Publish to a non-`ops.*` subject → tool returns an input-validation error, nothing
  stored.
- `nervi_subscribe` with `max_messages` larger than the pending count returns only what
  is pending (no hang beyond the fetch expiry).
- Oversized payload beyond stream `max_msg_size` (if configured) → publish error
  surfaced to the caller.

## Teardown

Delete the durable consumers created during the test (`developer-consumer` and any
fresh-cursor names) so reruns start clean; optionally `nats stream purge OCCITAN`.

## Exit criteria

All assertions pass against a live broker. At that point nervi#6 closes and Epic 1 is
end-to-end proven, unblocking Epic 2's real SRE sensor.
