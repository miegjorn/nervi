# N-4 — Integration test: SRE sensor → `occitan.ops.sre.alerts` → consumer

**Status:** implemented at two layers. Closes nervi#6.

**TypeScript layer (`nervi-mcp`):** test at
[`mcp/nervi-mcp/test/bus.integration.test.ts`](../mcp/nervi-mcp/test/bus.integration.test.ts).
Run with `npm run test:integration` (see the repo README). Drives the real `NatsBus` (no
bus mock) against a live `nats:2.10-alpine -js` broker; provisions the `OCCITAN` stream
in setup. CI runs this in a dedicated `integration` job.

**Rust layer (`nervi-core`):** test at
[`nervi-core/tests/integration.rs`](../nervi-core/tests/integration.rs).
Runs in CI (`.github/workflows/ci.yml`, job `nervi-core`) against a real
`nats:2.10-alpine -js` broker — no mocks. Exercises the Rust `NerviClient`
(`publish` / `subscribe`) directly.

**What the Rust test asserts** (a focused subset of the full scenario below,
matching nervi#6's definition of done):

- **Late, durable subscription:** the consumer is created *after* the producer has
  published and exited, and still receives the alert from the file-backed stream.
- **Payload integrity:** the JSON alert round-trips byte-for-byte and decodes back
  into the same struct.
- **Subject filtering:** noise published to a different `occitan.*` subject
  (`occitan.ops.metrics.cpu`) is not delivered to the `occitan.ops.sre.alerts` consumer.

**Deferred to follow-up work** (see Farga TODO `nervi/n4-integration`):

- Durable-cursor ack semantics (plan steps 6–7): the Rust `nervi_subscribe` (in
  `nervi-core`) uses stateless ephemeral consumers, so re-fetch replays rather than
  advancing a cursor. The deployed TypeScript `nervi_subscribe` uses durable consumers
  with an explicit `consumer_name`, so it does advance the cursor correctly.
- `Nervi-Qualifier` header round-trip (the Rust client carries no qualifier yet).
- The true in-cluster E2E harness driving `nervi-mcp` over Streamable HTTP.

The remainder of this document is the original plan, retained as the target the
full E2E harness (in-cluster, MCP tools, qualifiers) will grow toward.

N-4 is the first end-to-end proof that the Signal Bus Core (N-1/N-2/N-3) carries a real
operational signal from a producer to a consumer through the OCCITAN stream.

The rest of this document is the original plan the implementation followed.

## Goal

Demonstrate, against a live NATS JetStream + `nervi-mcp`:

> A signal published to `occitan.ops.sre.alerts` by an SRE-style producer is durably
> stored on the OCCITAN stream and later fetched, intact and correctly qualified, by an
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
| 1 | Producer | `nervi_publish` `{subject: "occitan.ops.sre.alerts", qualifier: "info", payload: {level:"critical", node:"node-3", msg:"disk 95% full"}}` | Returns `{published:true, stream:"OCCITAN", seq:N}` |
| 2 | Producer | Publish a second, distinct alert (`qualifier: "cross-project"`) | `seq` increments |
| 3 | — | (consumer is created only now — proves async/durable delivery) | — |
| 4 | Consumer | `nervi_subscribe` `{subject:"occitan.ops.sre.alerts", consumer_name:"developer-consumer", max_messages:10}` | Returns both messages |
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
- **Subject filtering:** a message published to a *different* `occitan.*` subject is not
  delivered to the `occitan.ops.sre.alerts` consumer.

## Negative / edge cases

- Publish to a non-`occitan.*` subject → tool returns an input-validation error, nothing
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

## Deferred work

The implemented test covers the full signal-bus chain below the MCP transport. Two items
from this plan are intentionally deferred (logged in Farga, project `nervi`, component
`n4`):

- **In-cluster true E2E:** the `Job`-in-namespace harness that talks to `nervi-mcp` over
  Streamable HTTP. The CI-friendly ephemeral broker is sufficient to prove Epic 1; the
  in-cluster smoke test belongs with deployment verification.
- **Oversized-payload edge case:** the `OCCITAN` stream sets no `max_msg_size`, so there
  is nothing to assert yet. Revisit if a size limit is configured.
