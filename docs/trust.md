# Trust and Approval Model

Florence can reason with Hermes, but Florence owns execution safety. Any action
that changes the outside world should become a household-scoped pending action
before it runs.

## Rules

- Pending actions always belong to one household and one iMessage chat.
- Actions start as `pending`, then move to `approved`, `canceled`, or `expired`.
- Approval lookup is scoped by household; a code from one family is invisible in
  another family.
- The first sender in a household is the founding parent. Later unknown senders
  are helpers until a parent invites or confirms the second parent.
- Only household members with the `parent` role can approve or cancel actions.
- Helpers can participate in the thread, but they cannot approve external
  actions.
- Helpers can identify themselves, but they cannot change durable household
  setup details such as child profiles or timezone.
- Partner invites, source connections, source rules, privacy settings, durable
  memory inspection/writes/deletes, reminder completion/cancellation, and
  household handoff, household stop/restart, household data summary, household
  data deletion, and analytics consent are parent-only.
- Pending actions expire after `FLORENCE_PENDING_ACTION_TTL_MINUTES`.
- Household data deletion confirmations expire after
  `FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES` and require a recent
  parent deletion request first.
- Reminder actions expire no later than their `due_at_utc`, so a parent cannot
  approve a reminder after the moment it was supposed to remind them.
- Stopped households are skipped by background reminder, briefing,
  connected-source polling, and approved-action execution paths until a parent
  restarts Florence.
- External source automation enters through typed source ingestion with its own
  API key and required source `external_id`; arbitrary JSON is not forwarded to
  Hermes as agent context. Oversized source fields are rejected instead of being
  treated as trusted instructions or broad family context.
- Hermes should propose or prepare actions; Florence should enqueue and resolve
  them.
- Hermes proposals use a stripped `florence` JSON block in the final reply.
  Florence validates the block and only supports narrow, typed proposals.
- The worker executes only actions that are already `approved` and belong to an
  active household; pending, canceled, expired, executed, failed, and
  stopped-household actions are skipped.
- Every execution attempt writes one audit row. A given action can only have one
  execution row, which prevents accidental duplicate sends.

## Current Control Plane

- `POST /dev/actions` creates a pending action and texts the household with an
  approval code.
- `POST /dev/actions/tick` executes approved actions.
- `GET /dev/actions/{chat_id}` lists active pending actions for that household.
- `GET /dev/actions/{chat_id}/executions` lists that household's action audit
  rows.
- Action read routes require an existing household chat; they do not create
  households as a side effect of support inspection.
- Parents approve through iMessage with `approve <code>`.
- Parents cancel through iMessage with `cancel <code>`.
- Parents can text `handoff` or `what's open?` to see active approval codes and
  upcoming reminders for the household.

## Current Executors

- `send_message`: sends `payload.text` to the household chat through Linq using
  an action-scoped idempotency key.
- `create_reminder`: adds a household reminder only after a parent approves the
  proposed reminder action. Execution refuses stale `due_at_utc` values even if
  a malformed or operator-created action reaches the runner.

## Hermes Proposal Boundary

Hermes can currently propose:

- `create_reminder` pending actions with a future `due_at_utc`.
- Durable household memory candidates when memory is enabled.

Florence ignores unsupported proposals, stale reminder proposals, and all memory
proposals while household memory is paused. See `docs/hermes-boundary.md`.
Florence also guards the visible Hermes reply after validation: if the reply says
a reminder, memory, or source rule was saved or scheduled when Florence only
queued approval or ignored the proposal, Florence replaces that line with a
truthful parent-facing guard message.

Unsupported action types fail closed and are audited as failed. The next
production step is to add narrowly typed executors for real integrations, such
as email replies, calendar changes, or partner reminders.
