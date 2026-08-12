# Florence Engineering Rules

- Do not preserve backward compatibility. Remove obsolete paths instead of adding compatibility layers, fallbacks, or migrations.
- Choose the simplest implementation that fully meets the current requirements. Avoid speculative abstractions, configuration, and indirection.
- Grow the system in layers. Start from the smallest version that works end to end, and add each new capability on top of a product that already works. Never trade a working product for unfinished complexity.
- Keep modules modular and concerns clearly separated.
- Prefer established, well-maintained libraries when they reduce overall complexity or improve reliability. Do not reimplement common functionality without a clear reason.
- Lean on the dependencies already in the project before writing your own implementation or adding packages. Do not assume a library lacks a capability without checking its documentation and types.
- Make architectural decisions for the long term. Do not accept a stopgap that only works for now and is meant to be replaced later.

## Product invariants

- Florence is one persistent family Chief of Staff. Model calls and specialist workers are ephemeral.
- One global person identity may participate in multiple relationships and conversations. Roles, context, and authority are relationship-local.
- PostgreSQL and app-owned normalized domain records are canonical. Chat history, model context, framework state, traces, and scratch files are not.
- `HouseholdChiefOfStaff.accept()` is the only external command or signal ingress into authoritative household state. It reopens current authority and atomically accepts the signal. `processNext()` is an internal continuation over an already accepted signal; it reconciles bounded proposals and commits events, timers, and effects rather than creating a second ingress.
- Model workers may propose. They may not message people, mutate accepted state, approve actions, widen disclosure, change people or chat policy, schedule future work, or canonize learning. An app-owned background dispatcher may claim authorized timers and outbox effects, invoke deep connector adapters, and return provider receipts through `accept()`; it is not a model worker.
- An unknown or non-exact group is silent: Florence retains no family meaning and emits no message, reaction, typing indicator, enrollment notice, or error. For the private two-adult pilot, the one exact family group becomes interactive only after both adults independently verify and a live provider read proves exactly those two current identities for exactly one household. Any participant change or uncertain provider read invalidates authority and returns to silence.
- Group invocation and group write authority are separate. Once that exact pilot group is bound, ordinary family coordination may invoke Florence as described in `PLAN.md`; every outbound message still reopens the exact current audience. No other group gains ambient interaction from being added.
- Private person-owned data is private by default. Promotion to a household or chat requires an exact approval or a previously approved applicable bridge rule.
- Shared output uses the intersection of the live participant epoch, every participant's applicable policy, source visibility, purpose, and bridge rules.
- Consequential external communication, submission, booking, purchasing, payment, cancellation, or account mutation requires an app-owned action intent, exact approval, and reconciled receipt.
- Group reminders are neutral and factual. Florence never assigns blame.
- Timers trigger reevaluation. They are never authority to send stale reminders.
- A provider or framework type must never cross the `ModelGateway` or `WorkerRuntime` seam into domain code.
- The mobile web app is an authority and exception plane for onboarding, integrations, identity, chats, memory, privacy, and data controls. It is not a parallel chat, task dashboard, calendar, or planner.
- The first complete release closes one family episode end to end. Do not widen the implementation into generic Life OS modules before the production acceptance flow in `PLAN.md` passes.

## Security and operations

- Never commit, print, log, fixture, snapshot, or place a secret in model context. Secrets belong only in ignored local configuration or Railway variables.
- Treat email bodies, attachments, child data, calendars, OAuth tokens, and derived private context as sensitive.
- Verify webhook authenticity before parsing business meaning. Deduplicate every inbound signal and outbound effect.
- Fail closed on privacy, authorization, ambiguous approvals, invalid model output, and uncertain external action results.
- Keep provider credentials and connector clients outside model workers and `WorkerRuntime`. The app-owned background dispatcher composes connector adapters whose implementations hold credentials; credentials never enter model requests, proposal workers, or runtime state.
- OAuth challenges, encrypted refresh credentials, provider cursors, leases, and connector status are operational provider state, not household event truth. Authenticated connector routes must reopen the caller's current household authority before changing that state; every resulting provider observation, action approval, and receipt still enters household truth through `HouseholdChiefOfStaff.accept()`.
- Use exact dependency versions and commit the lockfile.
- Keep tests deliberately lean. Protect durable privacy, authorization, idempotency, recovery, connector, timing, and end-to-end product boundaries; do not add broad matrices or implementation-detail tests for every behavior change.

## Required verification

Run these before committing or deploying:

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm build
```

Run PostgreSQL integration and live connector checks when their required environment is available. Never weaken a test merely to make a release pass.
