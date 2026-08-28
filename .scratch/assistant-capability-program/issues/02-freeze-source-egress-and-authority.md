Label: wayfinder:prototype
Type: prototype
Status: resolved
Blocked by: 01

# Freeze source egress and authority

## Question

What single source-egress and authority matrix covers each source owner, permitted query fields, provider, result audience, retention/deletion rule, consequence class, approval owner, and disconnect behavior for the newly approved capability breadth?

Adapt Pi's policy-hook shape from `packages/agent/src/types.ts` and `packages/agent/src/agent-loop.ts`, plus Hermes's capability metadata in `tools/registry.py` and `toolsets.py`. Florence must own the household-specific policy values and live audience rechecks.

## Answer

[`Florence assistant capability authority matrix`](../../../docs/design/assistant-capability-authority-matrix.md) now freezes one policy-complete record and one source-egress/authority matrix for every admitted source, provider read, internal mutation, family delivery, provider-backed draft, standing household effect, and exact external effect.

The resolved boundary:

- evaluates the same application-owned policy at catalog, admission, result, and delivery/effect checkpoints, with live authority rechecks and no model-selected adult, provider account, credential, connection, visibility, audience, retention, consequence, approval, or disconnect behavior;
- makes each record declare schemas, source ownership/visibility, conjunctive grants, positive query-field egress, model context, result audience, retention/deletion, consequence, every required approval owner, injection treatment, settlement, progress, terminal outcomes, timeout, bounds, and cancellation;
- preserves the complete uncapped 90-day Gmail and all-readable-personal-Calendar review, parental-unit family meaning with owner-private raw supports, explicit ask-first promotion of personal Calendar dates, and the owner's revocable private-conflict-sharing permission;
- separates idempotent Linq messages from desired-state-reconciled reactions and best-effort private typing, so presentation never impersonates work or completion;
- requires both the provider-account owner and every distinct private-source owner to approve an exact external disclosure, binds approvals to the exact payload/source version, and retains honest `unknown` settlement; and
- distinguishes disconnect, deletion, and irretractable external history while keeping authorization live and uncached.

Pi reuse is an adapted policy/lifecycle port at `4e494929998d6bc4fccf75e0a233f727db4b70ee`: the lifecycle/result/abort/progress contracts and applicable loop tests are direct ports, while Pi's tested post-validation argument mutation is deliberately inverted into an immutability regression because Florence's egress, approval digest, idempotency, and settlement must share one canonical value.

Hermes reuse is an adapted registry/toolset/discovery port at `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`: stable snapshots, collision handling, composable toolsets, scoped progressive discovery, bounded results, connector manifests, staged payloads, approval queue/digest/coalescing, and unknown-effect recovery are pinned to exact source ranges. Its last-good cache is reused only for provider health, never authority. Florence-owned metadata is limited to dimensions the upstreams do not represent: two-adult identity, source visibility, multi-owner disclosure approval, live Linq audience, positive per-field egress, source-aware deletion, narrow household standing rules, and provider-observed settlement.

Verification: `pnpm check` passed on Node 24 and pnpm 10.10. Lint, typecheck, and build passed; the existing three database integration cases were skipped because `TEST_DATABASE_URL` is not configured. Independent reviews found and closed the grant-predicate, reaction/typing settlement, multi-owner approval, metadata-completeness, and provenance gaps; no P0–P2 issue remains.
