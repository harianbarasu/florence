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
- `FlorenceApplication.process()` is the only mutating ingress into authoritative product state. It reconciles proposals and atomically commits state, audit, jobs, timers, and effects.
- Workers may propose. They may not message people, mutate accepted state, approve actions, widen disclosure, change people or chat policy, schedule future work, or canonize learning.
- A group is content-disabled while any current participant lacks verified global registration and consent. Every participant change starts a new immutable epoch, invalidates proactive rules, and requires an exact new-audience rule before proactive writing.
- Private person-owned data is private by default. Promotion to a household or chat requires an exact approval or a previously approved applicable bridge rule.
- Shared output uses the intersection of the live participant epoch, every participant's applicable policy, source visibility, purpose, and bridge rules.
- Consequential external communication, submission, booking, purchasing, payment, cancellation, or account mutation requires an app-owned action intent, exact approval, and reconciled receipt.
- Group reminders are neutral and factual. Florence never assigns blame.
- Timers trigger reevaluation. They are never authority to send stale reminders.
- A provider or framework type must never cross the `ModelGateway` or `WorkerRuntime` seam into domain code.
- The mobile web app is an authority and exception plane for onboarding, integrations, identity, chats, memory, privacy, and data controls. It is not a parallel chat, task dashboard, calendar, or planner.
- The first complete release closes coverage loops. Do not widen the implementation into deferred Life OS modules before the production acceptance flow in `PLAN.md` passes.

## Security and operations

- Never commit, print, log, fixture, snapshot, or place a secret in model context. Secrets belong only in ignored local configuration or Railway variables.
- Treat email bodies, attachments, child data, calendars, OAuth tokens, and derived private context as sensitive.
- Verify webhook authenticity before parsing business meaning. Deduplicate every inbound signal and outbound effect.
- Fail closed on privacy, authorization, ambiguous approvals, invalid model output, and uncertain external action results.
- Keep provider credentials and connector clients outside workers. Tools close over short-lived, server-created capability grants.
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
