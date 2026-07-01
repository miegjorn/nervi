# Nèrvi — architecture notes

Founding architecture, Epic 1. This captures the decisions locked during the
2026-06-28/29 design and realized in this repository. The wider, evolving design
lives in Farga (project `nervi`); this document records only what Epic 1 builds.

## What Nèrvi is

The async subscription fabric that engulfs the synchronous session model. Sessions
become one bounded topology within a larger subscription fabric. Producers publish
signals; subscribers (agents, sensors, consumers) read them asynchronously. Nèrvi is
the machine-readable nervous system; Charradissa remains the human-readable voice.

## Substrate: NATS JetStream

NATS JetStream was chosen over Redis: durable, file-backed streams; native pull
consumers with explicit ack; subject hierarchies that map cleanly onto the
`<domain>.<source>.<kind>` topic model; and a small operational footprint suited to the
Occitan k8s cluster.

### The OCCITAN stream

A single durable stream captures all operational topics under `occitan.>`:

- **Subjects** `occitan.>` — e.g. `occitan.ops.sre.alerts`, future `occitan.ops.*.*`.
- **Storage** file, 1 GiB max, 7-day max age, `discard: old` — bounded, self-trimming.
- **Replicas** 1 to start (single-writer; revisit when the cluster grows).
- **Exposure** ClusterIP only. Nèrvi is intra-cluster infrastructure, never public.

One stream (not one-per-topic) keeps operations simple; subjects partition the space,
and durable consumers with `filter_subject` give each reader its own cursor.

## Qualifier vocabulary

Every message carries a qualifier in the `Nervi-Qualifier` header:

| Qualifier | Meaning | Farga node type |
|-----------|---------|-----------------|
| `info` | An observation / event | info node |
| `cross-project` | A signal relevant beyond its origin | cross-project node |
| `data` | A structured datum | data node |

The vocabulary is deliberately small and maps directly onto Farga node types — the
queue manifest is an agent-graph node, and the qualifier *is* the node type. Vocabulary
extensibility (the triager rule schema) is deferred to a later epic.

## MCP surface (N-2 / N-3)

Two stateless tools, the minimum to prove the fabric:

- **`nervi_publish`** validates the subject (concrete, under `occitan.*`), normalizes the
  payload (string passthrough or JSON encode), embeds the qualifier + an ISO-8601
  publish timestamp as headers, and JetStream-publishes. Returns the stream + sequence.
- **`nervi_subscribe`** creates (idempotently) a durable pull consumer filtered to the
  subject, fetches up to `max_messages`, acks them, and returns each message's
  `sequence`, `subject`, `qualifier`, `payload`, `timestamp`. Stateless pull — no
  long-running subscription, so it composes cleanly with request/response agents.

### Why a publish-time timestamp header

The fetched JetStream message exposes a stream timestamp (`info.timestampNanos`), used as
a fallback. But publishers also stamp `Nervi-Timestamp` at publish time so the
*semantic* event time travels with the message regardless of how/when it is consumed.

## Layering (testability)

```
core.ts      pure domain: vocabulary, validation, header build — no NATS import
handlers.ts  tool logic over the SignalBus seam — unit-tested with a fake bus
bus.ts       the only NATS-aware module — NatsBus implements SignalBus
server.ts    MCP wiring (stdio locally / Streamable HTTP in-cluster)
```

The `SignalBus` seam is what lets N-2/N-3 logic be unit-tested without a broker. Live
broker behavior is covered by the N-4 integration test (implemented and CI-green; see
`mcp/nervi-mcp/test/bus.integration.test.ts` and `nervi-core/tests/integration.rs`).

## Two MCP server implementations

There are two MCP server implementations in the repo. **Only the TypeScript one is deployed.**

### TypeScript (`mcp/nervi-mcp/`) — deployed in-cluster

Built as a Node.js process, packaged as `ghcr.io/miegjorn/nervi-mcp`, and deployed by the
Helm chart (`mcp.enabled: true`). Runs over Streamable HTTP (`NERVI_MCP_TRANSPORT=http`)
in-cluster on port 8080; falls back to stdio locally. It is the live implementation that
agents call through the Helm-provisioned ClusterIP Service.

Layering: `core.ts` (pure domain) → `handlers.ts` (tool logic over `SignalBus`) →
`bus.ts` (NATS adapter, `NatsBus`) → `server.ts` (MCP wiring).

Subscribe uses **durable** pull consumers: `consumer_name` is a required argument that
identifies the consumer across calls. A new consumer name starts from the stream head;
the same name resumes from where it last acked.

### Rust (`nervi-server/`) — built and CI-tested, not deployed

A Rust implementation (`axum`-based HTTP server, JSON-RPC 2.0 over `POST /mcp`) that
wraps `nervi-core`. It exposes `nervi_publish` and `nervi_subscribe` using the same tool
names as the TypeScript server. Built and linted by `cargo clippy` in CI as part of the
workspace (`cargo test --workspace`), but there is no Docker image job for it in CI and
it is not referenced in the Helm chart.

Subscribe in `nervi-core` uses **ephemeral** consumers (fresh consumer per call, no
durable cursor). This is the primary behavioural difference from the deployed TypeScript
implementation: ephemerals see only messages published after the call; durable consumers
maintain a cursor.

`nervi-server` is available for future use if a Rust-native deployment path is wanted,
but the canonical in-cluster server is `nervi-mcp` (TypeScript).

## The SRE log watcher (N-5)

Epic 2's first sensor. A long-running watcher (`sensors/sre-watcher/`, a k8s Deployment)
that follows pod logs and feeds the classification stage. It mirrors the MCP server's
seam-based layering so its policy logic is unit-testable without a cluster:

```
core.ts      pure domain: PodRef / RawLogLine / LogLine, config parse, log-line parse,
             and the two seams — no @kubernetes import
k8s.ts       the only Kubernetes-aware module — K8sPodSource implements PodSource
watcher.ts   reconcile / stream / resume / reconnect policy over the seams
sinks.ts     ConsoleSink — the N-6 stand-in
main.ts      entrypoint: config → live PodSource + ConsoleSink → run, with SIGTERM
```

### Two seams

- **`PodSource`** (toward Kubernetes): `listTargets()` returns the running pod/container
  targets matching the watch config; `streamLogs(ref, opts)` follows one pod's logs as an
  async iterable of `RawLogLine`, completing when the stream ends. `K8sPodSource` reads
  pods via `CoreV1Api` and logs via `Log` (`follow`, `timestamps`), bridging the log
  stream through `readline`.
- **`ClassificationSink`** (toward N-6): `emit(line: LogLine)`. This is the N-6 interface
  contract. The watcher emits every line and nothing more — N-6 (classification rules)
  is a sink implementation, and N-7 (the Farga signal write) sits behind it. The watcher
  needs no change when N-6 lands; it is constructed with whichever sink is wired in.

### Log tracking — reconnection and resumption

- **New / rotated pods.** A reconcile loop re-lists targets every `pollIntervalMs`. Newly
  seen pods get a streaming task; pods that have left the set (rollout, deletion) have
  their task aborted. A rollout therefore "resumes cleanly": the old pod's stream stops,
  the replacement pod is picked up on the next reconcile.
- **Dropped streams.** When a follow stream ends, the watcher reconnects with exponential
  backoff (`reconnectBackoffMs` → `maxReconnectBackoffMs`, reset on progress), resuming
  with `sinceTime` set to the last emitted timestamp. The boundary line redelivered by
  `sinceTime` is dropped (timestamp ≤ cursor), so no line is emitted twice. This relies
  on the Kubernetes log timestamps being consistently formatted RFC3339Nano in UTC, so a
  lexicographic comparison matches chronological order.
- **First attach.** With no cursor yet, the watcher attaches with `sinceSeconds` lookback
  rather than replaying the pod's full history.

## Deferred (recorded, not built here)

- Triager rule engine and qualifier-vocabulary extensibility.
- Subscriber weighting / reversal recognition (depends on Cor dream-introspection).
- Endorsement protocol and scope-authority layers.
- N-6 alert classification rules and N-7 Farga signal write — they plug into the N-5
  watcher's `ClassificationSink` seam.
