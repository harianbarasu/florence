# Hermes Boundary

Florence should use Hermes for agentic reasoning, but Hermes should not become
the application database, transport, scheduler, or approval authority.

## Division Of Responsibility

Hermes owns:

- Open-ended reasoning.
- Natural language planning.
- Deciding whether a parent-facing reply should include a proposed follow-up.
- Suggesting durable household memory when the parent plainly states a stable
  family fact, preference, routine, or constraint.
- Suggesting household source rules when the parent plainly states what Florence
  should always surface or mute from connected sources.

Florence owns:

- Linq webhook verification and outbound idempotency.
- Household/member identity and SaaS tenant isolation.
- Timezone-aware reminder parsing and stale reminder suppression.
- Need-to-Know source triage.
- Parent approval, action expiry, action execution, and audit logging.
- Memory storage, privacy controls, export, deletion, and pause/resume behavior.
- External tool access and connected integrations.

Hermes toolsets are disabled in Florence's SaaS pilot configuration. Florence
injects the active household memory into the prompt and accepts structured
memory proposals back from Hermes, but durable memory writes stay inside
Florence's household-scoped store. Florence also owns connected sources, web
source ingestion, external actions, retry behavior, and audit trails. The
runtime adapter fails closed before loading Hermes if any Hermes toolset is
configured, so this boundary does not depend only on the pilot checklist.
For Postgres-backed SaaS traffic, the adapter also fails closed before importing
`run_agent.py` unless `FLORENCE_HERMES_STRICT=1`,
`FLORENCE_HERMES_AGENT_PATH` is set, `HERMES_AGENT_REF` is a full pinned Git
commit SHA, the checkout marker or git `HEAD` exactly matches that ref, and
`FLORENCE_HERMES_PROVIDER` plus `FLORENCE_HERMES_MODEL` are explicit. The
operator preflight reports the same contract, but the runtime enforces it too so
a web or worker process cannot silently fall back to a floating local Hermes
profile or deterministic fallback reply.
Florence also calls Hermes with `skip_memory=True`, `save_trajectories=False`,
and a fresh `florence-turn-*` session id for each turn. That prevents Hermes's
own session database or trajectory logs from becoming the durable household
memory layer. Florence also sets `HERMES_HOME` to
fresh per-turn directories under `FLORENCE_HERMES_RUNTIME_HOME` before importing
Hermes, then removes those directories after each call, including failed strict
Hermes turns. Any Hermes default runtime files therefore stay out of a shared
user `~/.hermes` profile and out of a cross-family scratch bucket. Operator
preflight/status imports use fresh
`florence-preflight-*` runtime directories under the same base, so multiple app
workers do not share one fixed preflight scratch home. Florence serializes every
Hermes runtime context because `HERMES_HOME` is process-global: an in-process
thread lock protects threads inside one worker, and an interprocess file lock
under `FLORENCE_HERMES_RUNTIME_HOME` protects web and background workers that
share the same filesystem. That lock covers operator preflight/status imports
and live parent turns.
Florence also adds the configured Hermes checkout to Python's import path only
while importing or running Hermes, then removes it afterward. Same-named
pre-existing modules from the checkout root are temporarily shadowed during the
call, and newly imported Python modules loaded from that checkout are cleared
from `sys.modules` after each preflight or parent turn, even when Hermes raises.
Module globals initialized under one runtime home cannot leak into another
family turn.
Florence supplies the recent transcript window explicitly.

Florence also injects read-only household policy context into Hermes:

- Current local time and household timezone.
- Current sender and household members.
- Setup/readiness gaps.
- Privacy state, including whether durable memory is paused.
- Household source rules such as `always_surface` and `mute`.
- Upcoming reminders and commitments.

Hermes can reason over this context, but it cannot mutate it directly. The
normal recent household context window is used for parent turns. For helper
turns, Florence omits parent-created memories and source rules and limits
transcript history to messages at or after that helper first appeared in the
household. Upcoming reminder context follows the same boundary: helper turns omit
reminders created before that helper first appeared.
The prompt labels unnamed household members by role, such as `unnamed parent`,
instead of sending their phone numbers. Florence also redacts phone-shaped values
and email addresses from display names, the current user message, recent
conversation history, upcoming reminder titles, source-rule phrases, and memory
text before calling Hermes. The system message tells Hermes that external tools
are unavailable in the SaaS pilot and that Florence owns connected sources,
lookups, reminders, actions, memory, and outbound delivery.

## Proposal Protocol

Hermes may append one hidden fenced block to its final reply:

~~~text
Parent-facing reply goes here.

```florence
{
  "actions": [
    {
      "type": "create_reminder",
      "summary": "Add reminder: Pack cleats",
      "payload": {
        "title": "Pack cleats",
        "due_at_utc": "2026-06-06T15:00:00+00:00"
      }
    }
  ],
  "memories": [
    {
      "kind": "preference",
      "subject": "Maya",
      "text": "Maya likes pasta.",
      "confidence": 0.6
    }
  ],
  "source_preferences": [
    {
      "preference": "always_surface",
      "phrase": "permission slips"
    }
  ]
}
```
~~~

Florence strips this block before texting the household. The block is not a
command surface. It is a proposal surface. Florence also checks the visible
parent-facing reply against the proposal outcome: if Hermes claims it saved,
remembered, scheduled, muted, or set something that Florence only queued for
approval or ignored, Florence replaces that visible line with a truthful guard
message before texting the household.

## Validation

Florence validates proposals before state changes:

- Unsupported actions are ignored.
- Reminder proposals with missing or past `due_at_utc` are ignored.
- Reminder proposals whose title contains phone-shaped values, email addresses,
  or Florence's `[phone number]` redaction marker are ignored.
- Reminder proposals become `pending_actions`; they do not execute until a parent
  replies with `approve <code>`, and the approval expires no later than
  `due_at_utc`.
- Memory proposals are ignored when household memory is paused or when the
  current sender is not a parent.
- Memory proposals containing phone-shaped values, email addresses, or
  Florence's `[phone number]` redaction marker are ignored rather than persisted.
- Memory proposals remain household-scoped and keep the source message and member
  provenance.
- Source preference proposals are limited to `always_surface` and `mute`, then
  normalized and stored as household-scoped source rules only when the current
  sender is a parent.
- Source preference proposals containing phone-shaped values or email addresses
  are ignored; source tuning from concrete senders/domains stays in Florence's
  local parent-controlled feedback paths.
- Parent-facing text is guarded after validation, so Hermes cannot tell a family
  that a reminder, memory, or source rule was applied when Florence did not apply
  it or still needs parent approval.

This gives Hermes room to be useful without letting it bypass Florence's safety
rails.

## Verification

The app-level regression
`tests/test_app.py::test_linq_webhook_agent_proposal_stays_bounded_by_approval_and_worker`
proves the public Linq path through this boundary. The test injects a fake
agent backend, sends a signed Linq message, verifies Florence strips the hidden
proposal block before texting, creates a household-scoped pending reminder
action, requires parent approval, executes the approved action in the worker,
and only then sends the due reminder.

Production uses the same `AgentBackend` boundary with `HermesBackend`. Florence
does not call a separate hosted SaaS version of Hermes and does not fork Hermes
into this repository. Instead, `FLORENCE_HERMES_AGENT_PATH` points at a Hermes
Agent checkout on disk, and Florence imports `run_agent.AIAgent` from that exact
checkout before calling `AIAgent.run_conversation(...)`. Pilot preflight checks
that the configured checkout exposes the constructor kwargs and
`run_conversation(...)` parameters Florence uses, without making a live model
call. The operator-only `POST /dev/hermes-smoke/{chat_id}` endpoint then runs the
same boundary against an existing household without saving a message or texting
the family. It reports whether a non-fallback response was produced but excludes
the model response body from the operator payload.

In Docker deploys, the intended path is `/opt/hermes-agent`. Set
`INSTALL_HERMES_AGENT=1` and pin `HERMES_AGENT_REF` to an upstream Hermes commit
SHA so the image build clones and installs that checkout. The image build and
pilot preflight both require that pinned ref and reject floating branch names.
The build records the actual checkout commit in
`/opt/hermes-agent/.florence-hermes-ref`, and pilot preflight blocks if that
recorded ref does not match `HERMES_AGENT_REF`.
Local development can point `FLORENCE_HERMES_AGENT_PATH` at any local Hermes
clone. Ambient Python `run_agent` imports are local SQLite development only;
Postgres-backed SaaS traffic requires `FLORENCE_HERMES_AGENT_PATH` so deployed
turns cannot bypass the pinned checkout contract.
Postgres-backed SaaS traffic also requires POSIX `fcntl` interprocess file
locking around Hermes runtime state. Local SQLite development can tolerate the
thread-only fallback, but `/dev/hermes-status`, `/dev/deployment-check`, and the
runtime adapter block a Postgres pilot when the reported runtime lock is
`thread_lock_only_no_interprocess_lock` instead of
`thread_lock_plus_interprocess_file_lock`.

That means Florence, not Hermes, is the SaaS layer. Florence resolves the active
household, builds the household-scoped prompt, passes only that household's
context to Hermes, disables Hermes toolsets, native memory, and trajectory
saving, points `HERMES_HOME` at a Florence per-turn scratch runtime directory,
uses an ephemeral Hermes turn session, validates any structured proposals, and
writes or sends state changes through Florence's own household-scoped stores.
