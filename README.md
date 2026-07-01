# Nèrvi

**The async subscription fabric of the Occitan stack — the nervous system.**

Grok support: full inter-mingling via per-agent model in Fondament (grok* models supported in component agents that use nervi MCP).

Nèrvi is not a queue alongside [Amassada](https://github.com/miegjorn/Occitan) — it is
Amassada *inside* the queue. Agents subscribe to topics; sensors publish signals; the
full agent-to-agent signal flow is captured and routed asynchronously. Where
Charradissa is the human-readable *voice* of the stack, Nèrvi is its machine-readable
*nervous system*.

The substrate is **NATS JetStream** (durable, file-backed, intra-cluster). Signals are
qualified (`info` | `cross-project` | `data`) and the qualifier maps directly onto
Farga node types.

## Epic 1 — Signal Bus Core (this repository)

The first proof-of-value: **SRE logs → `occitan.ops.sre.alerts` subject → developer consumer.**

| Story | What |
|-------|------|
| N-1 (#3) | Deploy NATS JetStream via Helm on the Occitan k8s cluster |
| N-2 (#4) | MCP tool `nervi_publish` — publish to a subject |
| N-3 (#5) | MCP tool `nervi_subscribe` — fetch pending messages from a subject |
| N-4 (#6) | Integration test: SRE sensor → `occitan.ops.sre.alerts` → consumer — TypeScript layer (`nervi-mcp`) and Rust layer (`nervi-core`) both green (see [docs/integration-test-n4.md](docs/integration-test-n4.md)) |

## Layout

```
helm/nervi/           Helm chart: NATS JetStream (OCCITAN stream) + nervi-mcp + sre-watcher
mcp/nervi-mcp/        TypeScript MCP server exposing nervi_publish / nervi_subscribe  ← deployed
nervi-core/           Rust client library (NerviClient) — used by nervi-server and integration tests
nervi-server/         Rust MCP server (wraps nervi-core) — built and CI-tested, NOT deployed
sensors/sre-watcher/  TypeScript SRE log watcher (N-5) — streams pod logs to the N-6 stage
docs/                 Architecture notes + the N-4 integration test plan
```

## The OCCITAN stream

A single durable JetStream stream backs all operational topics:

| Property | Value |
|----------|-------|
| Name | `OCCITAN` |
| Subjects | `occitan.>` (covers `occitan.ops.sre.alerts`, etc.) |
| Storage | file (persistent, 1 GiB max) |
| Retention | limits, 7-day max age |
| Replicas | 1 |
| Exposure | ClusterIP only — never public |

## Deploy

```sh
# From the cluster context (namespace and release name must both be "nervi"
# so the NATS service DNS matches the MCP default).
helm dependency build helm/nervi
helm upgrade --install nervi ./helm/nervi -n nervi --create-namespace
```

A post-install hook idempotently reconciles the `OCCITAN` stream against
`helm/nervi/values.yaml`.

## The MCP server

`mcp/nervi-mcp/` is a small [`@modelcontextprotocol/sdk`](https://github.com/modelcontextprotocol)
server. It connects to NATS via `NATS_URL`
(default `nats://nervi-nats.nervi.svc.cluster.local:4222`) and exposes two tools:

- **`nervi_publish`** — publish to an `occitan.*` subject. Inputs: `subject`, `payload`
  (string or JSON), `qualifier` (`info` | `cross-project` | `data`). The qualifier is
  embedded as the `Nervi-Qualifier` message header.
- **`nervi_subscribe`** — fetch pending messages via a durable pull consumer (stateless,
  no long-running subscription). Inputs: `subject`, `consumer_name`, `max_messages`
  (default 10). Returns `sequence`, `subject`, `qualifier`, `payload`, `timestamp`.

In-cluster the server runs over Streamable HTTP (`NERVI_MCP_TRANSPORT=http`); locally it
defaults to stdio.

```sh
cd mcp/nervi-mcp
npm install
npm test                 # unit tests (mocked bus) — fast, no broker needed
npm run build
```

### Integration test (N-4, TypeScript)

`npm run test:integration` runs the end-to-end test that proves Epic 1: a producer
publishes to `occitan.ops.sre.alerts` and an independent consumer receives the alert —
payload byte-for-byte and `Nervi-Qualifier` intact, including when the consumer subscribes
*after* publication. It drives the real `NatsBus` (no bus mock) against a real NATS
server, provisioning the `OCCITAN` stream itself.

It needs a live JetStream broker, addressed by `NATS_URL` (default
`nats://localhost:4222`). Locally, start one with Docker:

```sh
docker run --rm -p 4222:4222 nats:2.10-alpine -js
NATS_URL=nats://localhost:4222 npm run test:integration
```

CI starts `nats:2.10-alpine -js` in a dedicated `integration` job and runs it there.
Integration tests live in `test/**/*.integration.test.ts` (config
`vitest.integration.config.ts`), so they are excluded from the default `npm test`.

## The Rust bus client (`nervi-core`)

`nervi-core` is the Rust client the in-cluster server and future sensors build
on. `NerviClient` exposes `publish`, `subscribe`, and `ensure_stream` (idempotent
stream provisioning for local / test brokers — in-cluster the Helm hook owns it).

### Testing

Unit tests are pure and need nothing external:

```sh
cargo test -p nervi-core --lib
```

The **N-4 integration test** (`nervi-core/tests/integration.rs`) is the
end-to-end proof of the Signal Bus Core at the Rust layer: an SRE-style producer
publishes on `occitan.ops.sre.alerts`, and a developer consumer fetches the alert back —
payload intact, *after* the producer has finished (durable, late subscription). It runs
against a **real** NATS JetStream broker (no mocks):

```sh
nats-server -js &      # JetStream-enabled broker on :4222 (any real NATS works)
cargo test --workspace # includes tests/integration.rs
```

The broker URL is read from `NERVI_TEST_NATS_URL` (default
`nats://127.0.0.1:4222`). CI starts `nats:2.10-alpine -js` and runs this test on
every push and pull request (see `.github/workflows/ci.yml`).

## The Rust MCP server (`nervi-server/`)

`nervi-server/` is a Rust MCP server that wraps `nervi-core` over an `axum`-based HTTP
listener (`POST /mcp`, JSON-RPC 2.0). It exposes the same `nervi_publish` /
`nervi_subscribe` tool names as the TypeScript server, using `nervi-core`'s `NerviClient`
underneath. It is compiled and linted by CI as part of the Cargo workspace but **has no
Docker image and is not referenced in the Helm chart** — the deployed in-cluster server is
the TypeScript `nervi-mcp`. `nervi-server` is available as a Rust-native alternative if a
different deployment path is chosen later.

Note: `nervi-core`'s `subscribe` uses ephemeral pull consumers (fresh consumer per call,
no durable cursor), which differs from the TypeScript server's durable-consumer model.

## The SRE log watcher (N-5)

`sensors/sre-watcher/` is the first sensor (Epic 2). It connects to the Kubernetes API
with a **read-only ServiceAccount** (`get`/`list`/`watch` on `pods`, `get` on
`pods/log`), follows the logs of the pods it targets, and emits every log line to the
**classification stage (N-6)**. It tracks pods continuously: new and rotated pods are
picked up on each reconcile, and a dropped log stream reconnects with exponential
backoff, resuming from the last seen timestamp so no line is lost or duplicated across
the gap.

The watcher depends only on two seams (see `src/core.ts`):

- **`PodSource`** — the Kubernetes side (`K8sPodSource` in `src/k8s.ts`).
- **`ClassificationSink`** — the N-6 side. The watcher calls `sink.emit(line)` for every
  `LogLine` and does nothing else; classification, alerting, and the Farga signal write
  (N-7) all live behind this interface. Until N-6 ships, `ConsoleSink` stands in, emitting
  each line as NDJSON on stdout — the same line-delimited stream N-6 will consume.

Configuration (env vars, surfaced as `watcher.*` Helm values):

| Env var | Helm value | Default | Meaning |
|---------|-----------|---------|---------|
| `WATCH_NAMESPACES` | `watcher.namespaces` (list) | _all_ | Comma-separated namespaces; empty = cluster-wide |
| `WATCH_LABEL_SELECTOR` | `watcher.labelSelector` | _none_ | k8s label selector narrowing pods |
| `WATCH_CONTAINER` | `watcher.container` | _all_ | Restrict to a single container name |
| `WATCH_SINCE_SECONDS` | `watcher.sinceSeconds` | `10` | Lookback when first attaching to a pod |
| `WATCH_POLL_INTERVAL_MS` | `watcher.pollIntervalMs` | `15000` | How often to re-list pods |
| `WATCH_RECONNECT_BACKOFF_MS` | `watcher.reconnectBackoffMs` | `1000` | Base reconnect backoff |
| `WATCH_MAX_RECONNECT_BACKOFF_MS` | `watcher.maxReconnectBackoffMs` | `30000` | Backoff cap |

The watcher ships **disabled by default** (`watcher.enabled=false`): until a
classification consumer (N-6) is in place it only emits to its own stdout. Enable it with
`--set watcher.enabled=true`.

```sh
cd sensors/sre-watcher
npm install
npm test         # unit tests (fake PodSource + sink — no cluster needed)
npm run build
```

## Status

Founding implementation of Epic 1 (N-1/N-2/N-3) plus N-4 end-to-end integration tests
(TypeScript and Rust layers) and the first Epic 2 sensor (N-5, the SRE log watcher).
N-6 (classification rules) and N-7 (Farga signal on alert) plug into the watcher's
`ClassificationSink` seam. See Farga (project `nervi`) for the running development
narrative.
