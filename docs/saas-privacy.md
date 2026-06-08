# SaaS Privacy And Tenancy

Florence is built as a multi-family SaaS system, so tenant isolation is a
product requirement, not just an implementation detail.

## Default Posture

- Privacy mode is `maximum` for every household.
- Durable memory is enabled only inside the household namespace.
- Product analytics are off unless a parent opts in.
- Cross-family memory sharing is always off.
- Hermes receives only the active household's transcript window, members,
  reminders, setup/readiness status, source rules, privacy status, and durable
  memory when memory is enabled.
- Hermes prompt labels use parent-provided display names or role labels for
  unnamed members; phone-shaped values inside display names are redacted, and
  raw member phone numbers are not needed for reasoning or included in the
  system prompt.
- Florence redacts phone-shaped values from Hermes-bound user text, recent
  transcript history, upcoming reminder titles, source-rule phrases, and memory
  text. Local command handlers can still use phone numbers for setup and Linq
  invite flows before the Hermes boundary.
- Hermes native memory is disabled by default; Florence injects tenant-scoped
  memory into the agent context only for parent turns and keeps durable writes in
  its own store.
- Hermes turns use fresh ephemeral session ids and `save_trajectories=False`;
  stable per-household transcript and memory live in Florence/Postgres, not in
  Hermes's native session state.
- Florence sets `HERMES_HOME` to a fresh per-turn directory under
  `FLORENCE_HERMES_RUNTIME_HOME`, removes that directory after the call, and
  serializes every Hermes runtime context with an in-process thread lock plus an
  interprocess file lock because `HERMES_HOME` is process-global.
  The configured base should be a writable non-durable path in SaaS containers,
  not a shared `~/.hermes` profile.
- Florence adds the configured Hermes checkout to Python's import path only while
  importing or running Hermes, then removes it afterward. Same-named
  pre-existing modules from the checkout root are temporarily shadowed during the
  call, and newly imported modules loaded from that checkout are cleared from
  `sys.modules` after each preflight or parent turn.
- Hermes toolsets are empty by default for SaaS pilots. Florence owns connected
  sources, web/source ingestion, external actions, retry behavior, and audit
  trails.

This is intentionally close to the trust posture Poke advertises: a
message-native assistant with strong default privacy, integrations, automation,
and memory used at the right time.

## Tenant Boundary

Every durable table uses `household_id` or `chat_id` as the boundary:

- `households`
- `household_members`
- `messages`
- `memories`
- `source_items`
- `source_preferences`
- `connected_accounts`
- `connected_account_tokens`
- `outbound_deliveries`
- `reminders`

Parent-created `memories` and `source_preferences` are injected into Hermes only
on parent turns. Helper turns can get ordinary replies in the shared thread, but
they do not receive those durable parent-controlled context records.
Helper-turn transcript context is also bounded to messages at or after that
helper first appeared in the household, so Hermes does not use older parent-only
thread history when replying to a helper. Upcoming reminder context follows the
same boundary: helper turns omit reminders created before that helper first
appeared.
- `pending_actions`
- `action_executions`

There is no global semantic recall path. Store APIs that read memory require a
household identifier, and delete/export operations are scoped by `chat_id` or
`household_id`.

The default `FLORENCE_HERMES_TOOLSETS` value is empty. That keeps Hermes from
calling external tools with household context outside Florence's tenant-scoped
control plane. If an operator enables a Hermes toolset later, it must be treated
like a new production integration and audited against the same tenant boundary.
The pilot preflight endpoint blocks readiness when any toolset appears in
`FLORENCE_HERMES_TOOLSETS`.

Connected-source tokens are scoped through `connected_accounts` and encrypted at
rest with `FLORENCE_TOKEN_ENCRYPTION_KEY`. They are not exposed through review
endpoints and are not passed into Hermes. Raw email/calendar payloads are also
not added to the ordinary Hermes prompt; Florence stores and triages those
through Need-to-Know first.

## Text Controls

Parents can manage privacy directly in the household thread:

- `privacy status`
- `data summary`
- `pause memory`
- `resume memory`
- `clear household memory`
- `delete my data`
- `confirm delete household data`
- `opt in to product analytics`
- `opt out of product analytics`
- `stop`
- `start`
- `support`

Parents can use `data summary` to see count-level inventory for messages,
members, reminders, connected sources, source rules, durable memory, and pending
approvals without raw message or email bodies in the reply.
Whole-household deletion is parent-only and requires `delete my data` followed by
`confirm delete household data` before
`FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES` expires.
Pending OAuth states for that household are removed, and stale provider
callbacks do not recreate the household.
Florence keeps short-lived hashed tombstones for the deleted household's inbound
Linq message ids so retries cannot recreate the just-deleted household. The
tombstones do not include message text, sender phone, members, memories, or
source data.

Helpers can ask for privacy status, but cannot view the data summary or change privacy controls.
Helpers also cannot view, write, or delete durable household memory. They cannot
delete household data, or stop or restart Florence for the whole household.
Anyone in the thread can ask for support contact information.

## Stopped Household Behavior

When a parent texts `stop`, Florence treats the household as quiet:

- Ordinary replies are suppressed.
- Due reminders, daily briefings, connected-source polling, and approved-action
  execution are skipped.
- Google OAuth callbacks can save a parent-started connection, but they do not
  text the paused iMessage thread.
- Public Google OAuth callback failures use generic browser errors and do not
  echo provider error text, tokens, email addresses, phone numbers, or configured
  secrets.
- Pending reminders and source/account records remain household-scoped in the
  database.
- `help`, `support`, parent-only `start`, parent-only `data summary`, and
  parent-only household data deletion still work in the thread.

This is separate from `pause memory`, which only controls durable family context.

## Paused Memory Behavior

When memory is paused:

- New `remember...` commands are rejected.
- Existing memories remain available for export and deletion.
- `clear household memory` can soft-delete every active memory in that household.
- Existing memories are not passed into Hermes for ordinary replies.
- Source ingestion and reminder delivery continue, but remain household-scoped.

This makes "pause memory" reversible while honoring the parent's expectation
that Florence should stop using durable family context.

## Analytics

Product analytics must stay aggregate and de-identified. Valid examples:

- Count of surfaced vs. stored-only source items.
- Reminder delivery latency buckets.
- Action approval and cancellation rates.

Invalid examples:

- Raw memory text.
- Raw email/calendar/source content.
- Cross-household embeddings or semantic memory.
- Any household-specific export outside an authenticated household admin path.
- Any external automation path that forwards arbitrary JSON directly to Hermes
  instead of validating a typed household source item first.

## Poke-Informed Product Notes

Poke's public docs emphasize Apple Messages support, connected integrations,
recipes, MCP-based extensibility, proactive timing, and a privacy-first default:

- [Poke homepage](https://poke.com/)
- [Poke docs](https://poke.com/docs)
- [Poke managing integrations](https://poke.com/docs/managing-integrations)
- [Poke recipes](https://poke.com/docs/creating-recipes)
- [Poke MCP servers](https://poke.com/docs/mcp-servers)
- [Poke FAQ](https://poke.com/faq)

Florence should copy the durable ideas, not the exact product surface:

- Stay message-native.
- Treat integrations as typed capabilities.
- Use memory for timely help, not for broad surveillance.
- Prefer parent-approved actions until trust is earned.
- Keep privacy controls in the same thread parents already use.
