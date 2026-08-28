Label: wayfinder:task
Type: task
Status: open
Blocked by: 01, 02, 04

# Port durable family work

## Question

How can Florence durably accept genuinely long parent work, keep it alive across turns and process loss, support steering and cancellation, persist progress and results, deliver completion once, and report an honest `unknown` outcome when a consequential effect cannot yet be reconciled?

Adapt Pi's intent/effect/settlement specification in `packages/agent/docs/harness.md`, event and steering contracts in `packages/agent/src/agent-loop.ts`, and test vocabulary under `packages/agent/test/harness/`. Port Hermes's accepted/running/succeeded/failed/unknown/cancelled lifecycle, completion ledger, and recovery semantics from `tools/async_delegation.py`, `agent/replay_cleanup.py`, `tests/gateway/test_async_delegation_session_binding.py`, `tests/gateway/test_async_delivery_capability.py`, and `tests/cron/test_execution_ledger.py`. Florence's PostgreSQL store, live authority checks, idempotency, and provider reconciliation remain authoritative.
