# Florence Engineering Rules

- Do not preserve backward compatibility. Remove obsolete paths instead of adding compatibility layers, fallbacks, or migrations.
- Choose the simplest implementation that fully meets the current requirements. Avoid speculative abstractions, configuration, and indirection.
- Grow the system in layers. Start from the smallest version that works end to end, and add each new capability on top of a product that already works. Never trade a working product for unfinished complexity.
- Keep modules modular and concerns clearly separated.
- Prefer established, well-maintained libraries when they reduce overall complexity or improve reliability. Do not reimplement common functionality without a clear reason.
- Lean on the dependencies already in the project before writing your own implementation or adding packages. Do not assume a library lacks a capability without checking its documentation and types.
- Make architectural decisions for the long term. Do not accept a stopgap that only works for now and is meant to be replaced later.

## Product invariants

- Florence is one persistent household Chief of Staff. Model calls and specialist workers are ephemeral.
- PostgreSQL and app-owned domain records are canonical. Chat history, model context, framework state, traces, and scratch files are not.
- `HouseholdChiefOfStaff.accept()` is the only mutating ingress into household truth. One household writer reconciles proposals and commits changes.
- Workers may propose. They may not message people, mutate household state, approve actions, widen disclosure, or canonize learning.
- Private adult data is personal by default. Household promotion requires explicit approval or a previously approved, applicable rule.
- External communication, submission, booking, purchasing, payment, cancellation, or account mutation requires app-owned approval.
- Group reminders are neutral and factual. Florence never assigns blame.
- Timers trigger reevaluation. They are never authority to send stale reminders.
- A provider or framework type must never cross the `ModelGateway` or `WorkerRuntime` seam into domain code.
- No customer dashboard in v1. Browser pages are limited to secure OAuth and consent handoffs.

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
