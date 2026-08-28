Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 04, 07, 08, 09

# Keep multi-step family work going and show real progress

## Question

How can Florence take one real multi-source comparison using the maps, travel, page, or reminder tools, keep it alive across turns and process loss, accept natural steering or cancellation, and acknowledge, report meaningful progress, and deliver the terminal result while the family keeps chatting?

Adapt only the useful work states, steering, cancellation, and recovery behavior from Pi's `packages/agent/src/agent-loop.ts` and `packages/agent/docs/harness.md`, plus Hermes's `tools/async_delegation.py` and `agent/replay_cleanup.py`. Reuse Florence's existing PostgreSQL due-work seam, reactions, typing, and Linq outbox; do not add another runtime, queue, or progress subsystem.

## Answer

Florence can now accept one real multi-step family task in ordinary conversation, acknowledge it immediately, keep it running without blocking normal family chat, report a useful intermediate result, accept natural steering or cancellation, survive process loss at a persisted tool checkpoint, and deliver exactly one terminal result. The concrete controlling scenario resolves delayed `DL 747`, preserves the original Delta preference when JetBlue and a 7 PM constraint are added, searches two current nonstop alternatives, and returns both options and booking URLs.

The implementation adds a versioned `family_task` state to the existing `proactive_work` row and uses the existing due-work scheduler and Linq outbox. Each model-planned read is persisted before execution. A two-minute lease permits takeover after process loss; generation plus claim ID rejects a late worker. Steering advances the generation, removes only an abandoned unmatched tool call, preserves completed evidence, and replans from the new constraint. Cancellation invalidates unsent work output. Progress, waiting questions, and terminal results are staged transactionally with their checkpoint, and terminal work remains `delivering` until Linq confirms the message receipt. Oversized task history becomes one compact honest terminal failure before it can exceed the database bound and retry forever.

No new worker runtime, scheduler, queue, table, registry, policy layer, or generic progress subsystem was added. The unused generic progress/event-history machinery and unused family-task fields were removed. This slice can use Florence's current public research, maps, weather, and flight capabilities; the active roadmap continues with full Google Workspace, authenticated browser/computer use, live voice, calls/texts, bookings, and errands.

### Upstream reuse

- Pi `4e494929998d6bc4fccf75e0a233f727db4b70ee`, `packages/agent/src/agent-loop.ts` — adapted the bounded tool-turn loop and the distinction between planning a call, receiving its result, and choosing the next step or terminal output.
- Pi at the same commit, `packages/agent/docs/harness.md` — used only as contract guidance for intent/effect/settlement, generation/CAS, recovery, cancellation, and terminal delivery because the documented durable harness implementation is not shipped.
- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `tools/async_delegation.py` and `agent/replay_cleanup.py` — adapted nonblocking dispatch, work-versus-delivery state, stale-claim takeover, steering/stop behavior, and one final delivery. Hermes's worker is process-local and cannot resume computation after process loss, so Florence's existing PostgreSQL due-work state supplies the actual recovery rather than porting that runtime.

### Verification

- `pnpm check` passes: lint, all package/app typechecks, 31 default tests with database-gated journeys skipped, and all builds.
- `pnpm --filter @florence/api exec vitest run src/capability-lifecycle.test.ts src/reasoner-tool-loops.test.ts` passes 15 focused tests.
- A clean PostgreSQL 17 run of `src/florence.integration.test.ts -t "recovers one persisted task and completes only after its terminal receipt"` passes and covers reminder-before-task scheduler order, persisted `tool_pending` restart, expired-lease takeover, stale settlement rejection, transactional progress/final outboxes, `delivering` before receipt, `completed` after receipt, and no duplicate outbound.
- A second direct clean-database probe ends with `completed:terminal:2` and exactly two sent family-work messages (`sent,sent`).

The three older database-gated parent-journey tests remain outside this ticket's claim; when run wholesale against the local empty database they still expose their existing reminder/setup/Calendar expectation failures. This ticket adds and passes the focused durability regression rather than weakening or expanding those unrelated journeys.
