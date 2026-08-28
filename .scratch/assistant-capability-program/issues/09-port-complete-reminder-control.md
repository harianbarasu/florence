Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 01, 04

# Port complete reminder control

## Question

The family can now create, list, change, cancel, pause, resume, run, and recur reminders in ordinary iMessage language. How should Florence deepen its existing PostgreSQL/outbox reminder path to do that without adding another scheduler?

Adapt Hermes's action-oriented contract and state transitions from `tools/cronjob_tools.py`, `cron/jobs.py`, and the focused schema, stale-claim, recurring, one-shot, completion, and rearm tests under `tests/cron/`. Keep storage in Florence's PostgreSQL due-work seam and delivery in Florence's outbox; do not import Hermes's scheduler, local files, session runtime, or coding cron jobs.

## Answer

Florence now keeps reminders as durable, conversation-scoped PostgreSQL work instead of reducing them to anonymous future messages. Ordinary iMessage language can create, list, change, pause, resume, run, cancel, and recur private or household reminders. Supported schedules cover one-time, interval, daily, weekly, monthly, and yearly wall-clock recurrence in the household time zone.

The existing due-work scan stages each occurrence through the existing Linq outbox with a stable occurrence identity. Recurring reminders collapse missed slots to one catch-up and advance to the next future occurrence. One-time reminders remain addressable while queued or retrying and become completed only after Linq accepts the message; cancellation and changes can still suppress or replace a pending occurrence. Run-now reuses an already queued due occurrence instead of duplicating it and leaves recurring cadence intact.

The product contract stays conversational rather than policy-driven: listing is read-only, reminder operations do not depend on the generic retention/scheduling switch or model-supplied provenance, a parent may refer naturally to replies, voice/context, Gmail, Calendar, memory, attachments, and tool results, and run-now may use a natural Tapback without a redundant acknowledgement bubble.

### Upstream reuse

- Hermes Agent commit `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `tools/cronjob_tools.py`: directly adapted the unified create/list/update/pause/resume/remove/run contract, patch-only updates, and never-guess-an-ID behavior into Florence's reasoner contract.
- Same commit, `cron/jobs.py`: directly adapted tagged schedule types, pause/resume/run state transitions, one-shot completion, recurring advancement, catch-up collapse, and rearm semantics into Florence's existing PostgreSQL due-work/outbox path.
- Same commit, focused tests under `tests/cron/` including `test_cronjob_schema.py`, `test_jobs.py`, `test_claim_job_for_fire.py`, `test_oneshot_dispatch_failure_run_claim.py`, `test_terminal_job_rearm.py`, and `test_persisted_error_rearm_legality.py`: adapted the concrete retry, stale-claim, terminal-state, and recurrence invariants into the existing family journey.
- Florence intentionally did not import Hermes's Python/JSON scheduler, session runtime, coding jobs, filesystem scanners, or another queue/process.

### Verification

- `pnpm check` passes: lint, all workspace typechecks, 30 tests, and all builds.
- The existing family integration journey now covers durable one-shot retry/completion plus create/list/change/pause/resume/run/catch-up/partner-cancel recurrence, optional reaction, and the due/run-now deduplication race.
- Three database-backed tests, including that journey, are skipped on this machine because `TEST_DATABASE_URL` is unavailable and the local Docker daemon is not running; their TypeScript and SQL assertions compile, but they were not claimed as locally executed.
