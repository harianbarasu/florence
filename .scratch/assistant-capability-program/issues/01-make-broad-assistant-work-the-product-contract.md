Label: wayfinder:task
Type: task
Status: resolved
Blocked by:

# Make broad assistant work the product contract

## Question

How must `PLAN.md` and `AGENTS.md` change so the user's approved noncoding-assistant breadth is the controlling Florence goal—including reminder management, durable work, broader Google reads, isolated browsing, exact-approval external actions, and opt-in connectors—while preserving the two-adult household, application-owned delivery, private-source minimal crossing, and the exclusion of coding, shell, and arbitrary-filesystem tools?

Use `docs/research/pi-hermes-assistant-tool-gap-2026-08-27.md` as the upstream adoption record. This is a contract change, not an architecture rewrite.

## Answer

[`PLAN.md`](../../../PLAN.md) and [`AGENTS.md`](../../../AGENTS.md) now make the complete noncoding assistant-capability program the controlling goal instead of treating the two-parent household loop as the finish line.

The resolved contract:

- makes direct Pi/Hermes reuse mandatory at the pinned commits, requires exact upstream file/test provenance and a declared reuse mode, and requires a concrete reason for any Florence-owned replacement;
- admits safe pages, maps/weather/travel, complete Gmail/Calendar and owner-consented Workspace reads, reminder CRUD/recurrence, durable work, docket/document workflows, an isolated browser, selected connectors, progressive discovery, and exact-approved external actions;
- preserves the uncapped 90-day Gmail/all-personal-calendar review, separately reconciled Family Calendar, parental-unit facts with owner-private raw provenance, and the rule that a one-adult personal Calendar event needs that owner's direction before becoming household truth;
- assigns harmless reversible work to current consent, provider-backed private drafts to incremental owner scope and direction, family Calendar changes to the narrow standing rule, and every consequential action to exact owning-adult approval, deterministic identity, provider-observed settlement, and `unknown` reconciliation;
- authorizes one deep Florence capability module, one PostgreSQL durable-work seam, true-external provider adapters, and an ephemeral isolated browser without authorizing a second runtime or control plane; and
- replaces the old three-test ceiling with four product narratives, focused upstream-derived boundary tests, staged tranche gates, and a 20-item real-world completion rehearsal that covers every selected connector and every admitted consequence class.

No upstream runtime code was copied in this contract-only ticket. The source-level adoption decisions come from [`Pi and Hermes assistant-tool gap for Florence`](../../../docs/research/pi-hermes-assistant-tool-gap-2026-08-27.md); implementation tickets must use `dependency`, `direct port`, `adapted port`, or `workflow copy` as specified there and in the map.

Verification: `pnpm check` passed on Node 24 and pnpm 10.10. Lint, typecheck, and build passed; the existing three database integration cases were skipped because `TEST_DATABASE_URL` is not configured. Independent contract reviews found no remaining scope, privacy, authority, idempotency, or completion-gate contradiction.
