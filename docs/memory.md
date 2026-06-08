# SaaS Memory Model

Florence memory is household-scoped. A memory can never be read without a
`household_id`, and the store does not expose global semantic recall.

## Layers

- Transcript: recent messages used for immediate conversational continuity.
- Durable household memory: explicit facts, preferences, routines, and
  constraints that should survive across turns. Each record tracks which
  household member asserted it when that provenance is available.
- Source memory: emails, calendar items, flyers, and other imported signals.
  These are stored with provenance but only surfaced through Need-to-Know.
- Agent memory: Hermes native memory, stable Hermes household sessions, and
  trajectory saving are disabled for SaaS turns. Florence passes only the
  current household's memories and recent transcript window into Hermes context.
  Hermes runtime defaults are pointed at fresh per-turn directories under
  `FLORENCE_HERMES_RUNTIME_HOME`, then removed after each call. Same-named
  pre-existing modules from the checkout root are temporarily shadowed during the
  call, and newly imported modules from the configured Hermes checkout are
  cleared from `sys.modules` after each preflight or parent turn.

## Hermes Boundary

Hermes is the reasoning backend, not the source of truth for SaaS memory.
Florence disables Hermes toolsets for SaaS pilots, calls
`AIAgent(..., skip_memory=True, save_trajectories=False, session_id="florence-turn-*")`,
sets `HERMES_HOME` to a per-turn scratch directory, and passes the active
household's vetted memories into the prompt instead. Because `HERMES_HOME` is
process-global, Florence serializes every Hermes runtime context with an
in-process thread lock plus an interprocess file lock under the configured
runtime base.
Phone-shaped values inside memory text are redacted before that prompt is sent
to Hermes. Do not opt into Hermes toolsets for the pilot; any future Hermes
toolset must prove tenant isolation, export, deletion, provenance, and external
data-sharing boundaries before production use.

## Rules

- Default scope is `household`.
- Future member-level memory should still belong to a household and include a
  `subject`, not a free-floating user profile.
- The first sender in a household is the founding parent. Later unknown senders
  are helpers unless a parent invites or confirms the partner as the second
  parent.
- Every memory keeps provenance through `source_message_id` when available.
- Parent-asserted memories also keep `asserted_by_member_id`, which always points
  back to a member inside the same household. The store rejects memory writes
  with an `asserted_by_member_id` from another household.
- The store enforces one durable memory row per household, kind, subject, and
  text. Missing subjects are treated as a real "no subject" value for uniqueness
  so Postgres cannot create duplicate household memories just because `subject`
  is null.
- Parents can correct memory by saying "remember..." and remove it by saying
  "forget...".
- Parent-written durable facts must be short. Florence asks for a shorter
  version instead of storing oversized memory text.
- Parents can clear every active durable memory by saying
  `clear household memory`.
- Helpers can participate in the shared thread, but they cannot view, write, or
  delete durable household memory.
- Hermes memory proposals are accepted only from parent turns and only when
  household memory is enabled.
- Hermes memory proposals over 240 characters are ignored rather than truncated,
  so Florence never stores partial or cut-off durable facts.
- Hermes memory proposals containing phone-shaped values, email addresses, or
  Florence's `[phone number]` redaction marker are ignored. Parents can still
  explicitly manage household memory through local commands, but Hermes does not
  get to persist contact details from a model proposal.
- Parents can ask "what do you remember?" to see durable household memory.
  Durable memory is injected into Hermes only on parent turns; helper turns omit
  it even when household memory is enabled.
- Parents can say "pause memory" to stop new durable memory writes and exclude
  existing memories from Hermes context. Existing records stay available for
  export and deletion.
- Parents can say "resume memory" to re-enable household memory.
- Memories can expire; this matters for temporary schedules, phases, and
  short-term constraints.
- Delete/export operations must be scoped by `chat_id` or `household_id`; never
  expose global memory lookup by `memory_id` alone.
- `data summary` gives parents a count-level household inventory without raw
  message, email, or durable-memory text.
- Whole-household deletion is parent-only and two-step in the shared thread:
  `delete my data` followed by `confirm delete household data` within
  `FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES`.
- Cross-family learning must use aggregate, de-identified product analytics,
  never raw memory records.
- Product analytics are off by default and can only be opted into by a parent.

## Why This Matters

The assistant should feel like it knows the family, but SaaS deployment means
the most important feature is isolation. A brilliant memory system that leaks
between households is worse than no memory system.

## Current Control Plane

- `GET /dev/memory/{chat_id}` exports active durable memory for that household.
- `GET /dev/privacy/{chat_id}` exports the effective household privacy snapshot.
- `DELETE /dev/memory/{chat_id}/{memory_id}` soft-deletes a memory only if the
  memory belongs to that household.
- `what do you remember?` gives parents a text-native view of the same active
  memory set. Helpers are blocked from this export-like view.
- `forget...` and `clear household memory` soft-delete active memories inside the
  household memory set and do not touch other households.
- `privacy status`, `pause memory`, and `resume memory` expose the memory control
  plane through the shared thread.
- `data summary` shows count-level household inventory across messages,
  reminders, source items, connected accounts, source rules, pending approvals,
  and durable memories without raw content.
- `delete my data` starts the broader household deletion flow, and
  `confirm delete household data` removes messages, reminders, connected-source
  records and encrypted tokens, source preferences and feedback, pending actions,
  action execution audit rows, durable memories, members, chat aliases, OAuth
  states for that chat, and the household row for that household. The
  confirmation is accepted only after a recent parent request.

Development control-plane reads and deletes require an existing household chat.
They return `household_not_found` instead of silently creating a new tenant row
for an unknown chat id.

These endpoints are development-only until there is a real authenticated admin
surface. Production auth should bind a parent identity to one or more household
members, then resolve every memory operation through that household membership.
