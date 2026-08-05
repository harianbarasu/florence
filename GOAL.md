# Florence v1 Delivery Goal

## Outcome

Ship a complete parent-first Florence v1 to the canonical GitHub repository and existing Railway project. Hari must be able to test the real product through iMessage and its secure Google connection, not a disposable demo or mock-only checkpoint.

## Product acceptance

Florence must:

1. Onboard verified adults conversationally through private iMessage DMs and one household group.
2. Accept family obligations from group messages, screenshots, PDFs, Gmail, and Calendar.
3. Keep each adult's inbox private, promote only the minimum approved family meaning, and remember approved routing rules.
4. Turn accepted meaning into a proof-carrying Family Episode with an outcome, adult owner, useful temporal plan, neutral reminders, and verified closure.
5. Understand household time: time zones, DST, routines, preparation windows, conflicts, last responsible moments, and stale-plan invalidation.
6. Provide one concise daily household brief and interrupt privately for urgent, high-confidence findings.
7. Handle request-led family research and meal-plan-to-grocery-list work through invisible, ephemeral specialists.
8. Learn low-risk preferences and noise suppression while requiring approval before broadening disclosure or authority.
9. Expose correction, explanation, rule revocation, connection management, export, and deletion through private DMs or narrow secure handoffs.

## Technical acceptance

- TypeScript modular monolith with Fastify web and worker entry points.
- PostgreSQL owns identities, signals, household sequence, episodes, evidence, memory, rules, approvals, timers, jobs, outbox, effects, and audit.
- Idempotent authenticated ingress, one household writer, transactional outbox, leased jobs, bounded retries, dead letters, crash recovery, and replay.
- Provider-neutral `ModelGateway` for OpenAI, Anthropic, and OpenAI-compatible open-weight or self-hosted endpoints.
- Replaceable `WorkerRuntime` using pinned MIT LangGraph.js and Deep Agents.js with synchronous, isolated, non-durable subagents.
- Per-adult Google OAuth, Gmail push/history/backfill, Calendar sync, encryption, revocation, and strict private-to-household promotion.
- Linq v3 group/DM messaging, signatures, attachments, replies, reactions, STOP handling, dedupe, and idempotent sends.
- Deterministic domain tests, PostgreSQL integration tests, connector fixtures, runtime contract tests, privacy/authorization adversarial tests, and end-to-end founding-family scenarios.
- Railway web, worker, and PostgreSQL services deployed, migrated, healthy, and exercised against live integrations where provider state permits.

## Definition of done

The goal is complete only when:

- canonical code is pushed to `harianbarasu/florence`;
- required checks and v1 acceptance scenarios pass;
- Railway web and worker services are healthy on the migrated database;
- live model and connector paths are tested or an unavoidable external provider gate is documented with an exact recovery action;
- Hari has concise morning test instructions;
- supplied credentials remain outside Git and logs, and immediate rotation steps are documented.

Scaffolding, a fake-only vertical slice, a partially deployed build, or a list of unfinished tranches is not completion.
