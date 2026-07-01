# ADR-N-002: Dispatch Deduplication via Nats-Msg-Id

**Status:** Accepted  
**Date:** 2026-07-01  
**Closes:** [Occitan#37](https://github.com/miegjorn/Occitan/issues/37)  
**References:** Adversarial challenge `98b0d648`, Fondament#22 incident

---

## Context

Fondament#22 documented a confirmed incident: two Guilhem sessions both picked up
Fondament#7 (F-3 CI lint) within the same window and each dispatched an agent against
it, producing competing PRs (#19 and #20) with no coordination signal. PR #20 was
merged; #19 remained as a stale duplicate.

The root cause was the absence of a dispatch deduplication mechanism: when Guilhem
publishes a task to `occitan.dispatch.<component>`, nothing prevents a second session
from publishing an identical task before the first is consumed.

Three approaches were evaluated (per Occitan#37):

| Approach | Mechanism | Verdict |
|---|---|---|
| A — GitHub label | `state:in-flight` label, agent removes on completion | Pragmatic but agent-cooperative; stale on crash |
| B — NATS claim subject | `occitan.dispatch.claimed.<ref>` publish before dispatch | Requires agent subscription pattern on Guilhem side |
| C — Farga context node | Write `[dispatch:in-flight]` node before dispatch | Context nodes are not atomic; race condition window |

**Selected approach: NATS JetStream native deduplication** (A-2: substrate over governors).

---

## Decision

### 1. Stream configuration: `discard_new_per_subject` + `duplicate_window`

The OCCITAN stream is configured with two changes (see `helm/nervi/values.yaml`):

```yaml
stream:
  discardNewPerSubject: true      # discard new publishes to a full subject
  maxMsgsPerSubject: 1000         # per-subject ceiling (enables discard_new_per_subject)
  duplicateWindowSeconds: 3600    # broker-side dedup window: 1 hour (was 2 minutes)
```

**`discard_new_per_subject: true`** — When a subject already holds
`max_msgs_per_subject` messages, new publishes are rejected rather than evicting
the oldest message. This prevents a dispatch subject from silently growing unbounded
under a dispatch storm and provides back-pressure to the publisher rather than data
loss. It requires `max_msgs_per_subject > 0`.

**`duplicate_window: 3600s`** — The broker's built-in deduplication window for
`Nats-Msg-Id` headers. Any publish carrying the same `Nats-Msg-Id` as a prior
publish within this window is silently dropped at the broker — the publisher receives
a normal ack (the broker returns the sequence of the original). A 1-hour window covers
the typical agent session duration.

### 2. Publisher convention: `Nats-Msg-Id` header

When Guilhem dispatches to `occitan.dispatch.<component>`, the `nervi_publish` call
**must** include the `msg_id` argument. The broker uses this value as the deduplication
key within the `duplicate_window`.

**`msg_id` format:**

```
dispatch-<component>-<issue-number>-<date>
```

Examples:
- `dispatch-caissa-43-20260630`
- `dispatch-fondament-7-20260630`
- `dispatch-nervi-37-20260701`

**Rules:**
- `<component>` is the lower-case component name
- `<issue-number>` is the GitHub issue number (without `#`)
- `<date>` is the UTC calendar date in `YYYYMMDD` format on which the dispatch was published
- No spaces or dots — NATS header values must be printable ASCII

**Effect:** Two Guilhem sessions dispatching the same issue on the same calendar day
with `msg_id: dispatch-fondament-7-20260630` will result in exactly one message in
the stream. The second publish is dropped at the broker layer without any agent-side
logic.

**When `msg_id` should be omitted:** Operational signals (SRE alerts, metrics,
issue queue publishes) are not dispatch messages and do not need deduplication — they
carry independent events. Omit `msg_id` for those.

### 3. MCP tool change

`nervi_publish` now accepts an optional `msg_id` parameter (string, no whitespace).
When provided, it is set as the `Nats-Msg-Id` header on the NATS publish. When absent,
no deduplication header is sent.

```
nervi_publish(
  subject = "occitan.dispatch.caissa",
  payload = { ... },
  qualifier = "info",
  msg_id = "dispatch-caissa-43-20260630"   ← new optional field
)
```

The returned result includes `msg_id` when it was provided, for auditability.

---

## Consequence

- **Duplicate dispatches within 1 hour** with the same `msg_id` are idempotent at
  the broker layer. Guilhem session restarts or parallel sessions publishing the same
  dispatch land only once in the stream.

- **Different calendar days** produce different `msg_id` values, so a re-dispatch of
  the same issue on a different day is not suppressed. This is intentional: a stale
  dispatch re-issued on a new day reflects a genuine re-decision by Guilhem.

- **Crashes before consume** leave the message in the stream. Durable pull consumers
  (N-3) retain the message until it is fetched and acked. This is correct: deduplication
  applies to publishes, not to consume events.

- **Guilhem dispatch protocol** must be updated to include `msg_id` when calling
  `nervi_publish` for `occitan.dispatch.*` subjects. The format is specified above.
  GitHub label `state:in-flight` (Option A from Occitan#37) remains orthogonal — it
  is useful as human observability in the GitHub UI but is **not** the deduplication
  mechanism.

---

## Migration

The stream configuration change requires a `nats stream edit` (or Helm upgrade, which
runs the provisioning Job). Fields affected: `duplicate_window`, `max_msgs_per_subject`,
`discard_new_per_subject`.

**Note:** `discard_new_per_subject` is not updatable via `nats stream update` in all
NATS versions. If the update is rejected, delete and recreate the OCCITAN stream:

```sh
nats stream rm OCCITAN
# Helm post-upgrade hook recreates it with the new config
helm upgrade nervi ./helm/nervi -n nervi
```

This loses any undelivered messages in the stream. Plan the migration during a
quiet dispatch window (no active agent sessions consuming from `occitan.dispatch.*`).

---

## Invariants preserved

- **A-1 (no persistent process state):** Deduplication state lives in NATS
  (the broker), not in agent process memory.
- **A-2 (substrate over governors):** The broker enforces deduplication — no
  agent-side bookkeeping, no bolted-on Farga node, no GitHub label dependency.
- **ADR-N-001 (occitan.* namespace):** Unchanged. All dispatch subjects remain
  under `occitan.dispatch.<component>`.
