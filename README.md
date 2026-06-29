# Nèrvi

Nèrvi is the async subscription fabric for the Occitan stack. It deploys NATS
JetStream on the cluster and exposes `nervi_publish` and `nervi_subscribe` MCP
tools so agents can exchange machine-readable signals without synchronous
coordination or a Matrix room.

In Occitan, *nèrvi* means nerve — sinew, impulse, the thread that carries
sensation. Nèrvi carries intra-stack signals between components.

## What it is not

Nèrvi is not Charradissa (the human-chat layer) and is not a general
application message queue. It carries intra-stack agent signals only. The first
sensor is an SRE log monitor that publishes anomalies to `ops.sre.alerts`.

## Architecture

```
nervi-core/     — NATS JetStream client + shared types
nervi-server/   — MCP HTTP server (POST /mcp) exposing nervi_publish, nervi_subscribe
deploy/         — Helm chart: NATS JetStream sub-chart + nervi-server Deployment
```

## MCP tools

| Tool | Description |
|---|---|
| `nervi_publish` | Publish a message to a NATS subject (e.g. `ops.sre.alerts`) |
| `nervi_subscribe` | Consume pending messages from a NATS subject (ephemeral pull consumer) |

MCP endpoint: `http://nervi.occitan-system.svc.cluster.local:8080/mcp`

## Streams

| Stream | Subjects | Retention |
|---|---|---|
| `OPS` | `ops.>` | 10 000 msgs / 24 h |

## Development

```bash
cargo build --all
cargo test --all
```

## Deployment

The chart lives in `deploy/` and is deployed via ArgoCD from
`miegjorn/Caissa/deploy/argocd/apps/nervi.yaml` into the `occitan-system`
namespace.
