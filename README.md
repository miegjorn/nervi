# Nèrvi

**The async subscription fabric of the Occitan stack — the nervous system.**

Nèrvi is not a queue alongside [Amassada](https://github.com/miegjorn/Occitan) — it is
Amassada *inside* the queue. Agents subscribe to topics; sensors publish signals; the
full agent-to-agent signal flow is captured and routed asynchronously. Where
Charradissa is the human-readable *voice* of the stack, Nèrvi is its machine-readable
*nervous system*.

The substrate is **NATS JetStream** (durable, file-backed, intra-cluster). Signals are
qualified (`info` | `cross-project` | `data`) and the qualifier maps directly onto
Farga node types.

## Epic 1 — Signal Bus Core (this repository)

The first proof-of-value: **SRE logs → `ops.sre.alerts` subject → developer consumer.**

| Story | What |
|-------|------|
| N-1 (#3) | Deploy NATS JetStream via Helm on the Occitan k8s cluster |
| N-2 (#4) | MCP tool `nervi_publish` — publish to a subject |
| N-3 (#5) | MCP tool `nervi_subscribe` — fetch pending messages from a subject |
| N-4 (#6) | Integration test: SRE sensor → `ops.sre.alerts` → consumer (done — see [docs/integration-test-n4.md](docs/integration-test-n4.md)) |

## Layout

```
helm/nervi/          Helm chart: NATS JetStream (OCCITAN stream) + nervi-mcp deployment
mcp/nervi-mcp/        TypeScript MCP server exposing nervi_publish / nervi_subscribe
docs/                 Architecture notes + the N-4 integration test plan
```

## The OCCITAN stream

A single durable JetStream stream backs all operational topics:

| Property | Value |
|----------|-------|
| Name | `OCCITAN` |
| Subjects | `ops.>` (covers `ops.sre.alerts`, etc.) |
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

- **`nervi_publish`** — publish to an `ops.*` subject. Inputs: `subject`, `payload`
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

### Integration test (N-4)

`npm run test:integration` runs the end-to-end test that proves Epic 1: a producer
publishes to `ops.sre.alerts` and an independent consumer receives the alert — payload
byte-for-byte and `Nervi-Qualifier` intact, including when the consumer subscribes
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

## Status

Founding implementation of Epic 1 (N-1/N-2/N-3) plus the N-4 end-to-end integration test
that proves the signal bus carries a real SRE alert producer → consumer. Epic 2 (the SRE
log sensor, N-5…N-7) builds on this. See Farga (project `nervi`) for the running
development narrative.
