Label: wayfinder:prototype
Type: prototype
Status: resolved
Blocked by: 01, 02

# Choose one Florence capability interface

## Question

What 1–3-entry-point deep module can replace the current scattered model-tool wiring while keeping callers ignorant of schemas, availability, privacy, timeouts, cancellation, progress, output bounds, and terminal outcomes?

Design it twice from Pi's `AgentTool` and lifecycle types in `packages/agent/src/types.ts` and `packages/agent/src/agent-loop.ts`, and Hermes's registration/discovery implementations in `tools/registry.py`, `toolsets.py`, and `tools/tool_search.py`. The chosen interface must deepen the current reasoner rather than introduce a parallel runtime or a speculative connector framework.

## Answer

[`Florence capability interface`](../../../docs/design/florence-capability-interface.md) chooses one whole-run entry point:

```ts
interface FlorenceCapabilities {
  respond(turn: EnrolledTurnRef, signal: AbortSignal): Promise<RespondReceipt>;
}
```

`EnrolledTurnRef` is a nominal locator minted only by `@florence/database`; it carries no household policy or source content and grants nothing. `respond` reloads the verified turn and owns the immutable authorized catalog, existing strict Responses reasoner loop, every tool lifecycle, source-bearing result envelope, final decision validation, and shielded PostgreSQL commit. Callers no longer receive schemas, connection IDs, provider selection, availability, privacy metadata, per-tool timeouts/cancellation/progress, result bounds, or terminal outcomes.

Three independent alternatives were compared: a public Hermes-shaped registry/dispatcher, a Pi-shaped scoped turn session, and the chosen whole-run façade. The registry was rejected because it leaves the model loop and ordering with the caller; the session remains a useful private object but exposes tools/invocation/sealing publicly. The façade has the deepest interface, one useful default, one catalog/dispatch predicate, and no reorderable policy phases.

The current interface deliberately does not speculate about a durable-work lease. The accepted-work ticket may add one second typed entry point only after its actual PostgreSQL schema/claim contract exists, reusing the same private engine. Linq delivery and provider effects stay application-owned. The cutover must deepen delivery with live source/grant plus participant checks, and it must install safe persist-before-dispatch/read-back/`unknown` settlement for currently reachable Calendar, invitation, group, and activation effects before `respond` may stage those intents.

Pi reuse at `4e494929998d6bc4fccf75e0a233f727db4b70ee` is an adapted source port of `AgentTool`, results/updates, execution modes, lifecycle events, batching, validation, cancellation, progress suppression, and source-order transcript assembly from `packages/agent/src/types.ts:360-443` and `packages/agent/src/agent-loop.ts:376-545,575-790`. Unchanged truncated-call, blocked-call, ordering, and late-progress assertions are direct test ports; cancellation/persistence assertions are adapted. Florence deliberately replaces Pi's mutable post-validation arguments and pre-policy start event with frozen canonical values plus `requested`/`admitted`/`running`.

Hermes reuse at `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` is an adapted TypeScript port of `ToolEntry`, stable generations, collisions, availability, dispatch errors, maximum-result metadata, and toolset composition from `tools/registry.py` and `toolsets.py`. Hermes discovery is reserved as a later adapted port from `tools/tool_search.py`, scoped only to the already-authorized frozen catalog. Florence owns universal typed output bounds, exact matrix-row/schema binding, cycle rejection, household authority, source egress, audience, retention, delivery, and effect settlement because the upstreams do not represent those dimensions.

The replace-not-layer handoff deletes `FlorenceReadTools`, `FlorenceDecisionHooks`, all five hard-coded read tool declarations, `runReadTool`, model-authored Google connection IDs, caller evidence plumbing, and the premature work-start name switch. Every current function-call path, including private Gmail attachment reads, moves to the one private engine in the same tranche; there is no Pi/Hermes runtime, compatibility registry, or dual path.

Verification: `pnpm check` passed on Node 24 and pnpm 10.33. Lint, typecheck, tests, and build passed; the existing three database integration cases were skipped because `TEST_DATABASE_URL` is not configured. Three independent final reviews found no remaining P0–P2 interface, provenance, authority, effect-settlement, or commit issue.
