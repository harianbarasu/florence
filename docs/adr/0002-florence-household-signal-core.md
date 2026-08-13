# ADR 0002: Florence owns one household signal core

Status: accepted
Date: 2026-08-12
Supersedes: ADR 0001 where it names the former Life OS/Codex runtime

## Context

Florence is an iMessage-first family Chief of Staff with a supporting mobile setup and control surface. The
previous repository centered a personal project dashboard and generated Codex app-server bindings. Those
types and runtime protocols did not represent household identity, private/group authority, family memory,
open outcomes, timers, or provider effects.

## Decision

Every authenticated message, onboarding change, timer, and provider receipt enters through the sole
external command/signal interface, `HouseholdChiefOfStaff.accept(HouseholdIngress)`. PostgreSQL owns the
ordered household event stream, accepted signals, immutable deliberation results, timers, and outbox
effects. `processNext()` is an internal continuation over an already accepted signal, not another ingress.
The application—not the model or connector—reopens current household and conversation authority before
committing state or delivering an effect.

Unknown groups are silent. The single pilot family group becomes interactive only after both adults
independently verify and a live Linq observation proves exactly those two identities for exactly one
household. A participant change or uncertain observation invalidates the binding before any content is
accepted or emitted.

Model inference remains replaceable behind `WorkerRuntime` and `ModelGateway`. It receives an
audience-safe snapshot and returns bounded proposals; it never receives connector credentials or direct
mutation tools. A separate app-owned background dispatcher may claim authorized timers and outbox effects,
invoke deep Linq/Google adapters, and return provider receipts through `accept()`. Connector implementations
hold credentials; model requests, proposal workers, and runtime state never do. The React application is
retained only as the household onboarding/control surface.

OAuth challenges, encrypted refresh credentials, provider cursors, leases, and connection status remain
operational provider state behind the deep connector boundary, not household event truth. Authenticated
connector routes reopen the caller's current household authority before changing that state. Gmail
observations, Calendar approvals, provider actions, and receipts still cross the household boundary only
through `accept()`; credentials and OAuth state never do.

## Consequences

- The generated Codex protocol runtime and personal project/task product are deleted.
- API, worker, dashboard, and future Linq/Gmail/Calendar adapters share one product state machine instead
  of feature-specific workflow engines.
- Private information cannot become household memory or work without an explicit app-owned promotion.
- Provider retries use durable idempotency keys, and model retries reuse the signal's persisted result.
- New tables or public commands must justify a distinct durable product truth rather than infrastructure
  convenience.
