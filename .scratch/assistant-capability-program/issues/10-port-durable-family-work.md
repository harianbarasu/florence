Label: wayfinder:task
Type: task
Status: open
Blocked by: 04, 07, 08, 09

# Keep multi-step family work going and show real progress

## Question

How can Florence take one real multi-source comparison using the maps, travel, page, or reminder tools, keep it alive across turns and process loss, accept natural steering or cancellation, and acknowledge, report meaningful progress, and deliver the terminal result while the family keeps chatting?

Adapt only the useful work states, steering, cancellation, and recovery behavior from Pi's `packages/agent/src/agent-loop.ts` and `packages/agent/docs/harness.md`, plus Hermes's `tools/async_delegation.py` and `agent/replay_cleanup.py`. Reuse Florence's existing PostgreSQL due-work seam, reactions, typing, and Linq outbox; do not add another runtime, queue, or progress subsystem.
