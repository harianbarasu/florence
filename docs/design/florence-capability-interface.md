# Florence capability interface

The family can now ask Florence to do ordinary assistant work through one policy-complete turn path, and Florence either commits a useful response or accepted work without callers assembling tools, accounts, privacy rules, or lifecycle state.

Status: capability-interface prototype. This chooses the module boundary and the replace-not-layer migration. It does not yet port the runtime or add a capability.

## Decision

Deepen the existing Florence reasoner behind one `FlorenceCapabilities` entry point:

```ts
// packages/database/src/store.ts
declare const enrolledTurnRefBrand: unique symbol;

export type EnrolledTurnRef = Readonly<{
  sourceId: string;
  [enrolledTurnRefBrand]: true;
}>;

function enrolledTurnRef(sourceId: string): EnrolledTurnRef {
  // This is the sole nominal-construction assertion and remains private to the store.
  return { sourceId } as EnrolledTurnRef;
}

// apps/api/src/capabilities.ts
import type { EnrolledTurnRef } from "@florence/database";

export interface FlorenceCapabilities {
  /** Resolve one already-enrolled parent turn and commit its durable outcome. */
  respond(turn: EnrolledTurnRef, signal: AbortSignal): Promise<RespondReceipt>;
}

export type RespondReceipt = Readonly<{
  sourceId: string;
  disposition: "committed" | "superseded" | "authority_lost" | "retry_scheduled";
}>;

```

`respond` owns the enrolled-turn path from an opaque store-produced reference through a freshly loaded verified `InboundTurn`, a live immutable capability catalog, the existing OpenAI Responses reasoning loop, every tool lifecycle, final-decision validation, and the PostgreSQL turn commit. The reference is only a locator: it contains no adult, household, audience, source content, connection, credential, policy, approval, or provider data, and possession grants nothing.

The current interface deliberately does not freeze a durable-work lease before the accepted-work ticket defines its PostgreSQL schema and claim semantics. That ticket may add a second typed entry point which reuses the same private execution engine. It may not generalize this method into `run(any)`. There is no public `register`, `catalog`, `toolset`, `describe`, `invoke`, `approve`, `emitProgress`, `settle`, or provider-selection method.

The passed `AbortSignal` is only the outer run lifetime used by the existing scheduler when a newer inbound message supersedes a turn or the process shuts down. The module privately composes it with per-capability timeouts and effect-aware cancellation. The caller never decides whether an individual call timed out, may be retried, must be reconciled, or has reached a terminal outcome.

Expected tool, model, provider, policy, timeout, and result failures do not escape as tool-shaped errors. `respond` converts them into bounded model evidence and then a truthful committed response or a persisted retry. Only configuration failure, unavailable persistence that prevents an honest transition, or an impossible invariant may throw.

`FlorenceCapabilities` may perform harmless reads and durably stage assistant state, outbox messages, reversible provider-write drafts, standing-effect intents, and exact-effect intents. It does not deliver through Linq and does not execute a provider write. The application-owned outbox path rechecks the live Linq participants **and** every retained source support/provider grant immediately before delivery; revoked source-dependent output is suppressed, never redirected. The application-owned provider-effect settlement seam covers reversible owner writes, standing household effects, and exact external effects; it owns dispatch, read-back, `unknown`, and reconciliation and is not made into a generic model capability. `respond` cannot stage an intent until that consequence subtype has a safe settler.

## Default callers

The enrolled portion of `Florence.#handleInbound` becomes one call:

```ts
await this.#capabilities.respond(enrolledTurnRef, controller.signal);
```

`@florence/database` owns the branded `EnrolledTurnRef` type and its private constructor; `PostgresFlorenceStore` is the only producer after classifying a current inbound as an enrolled-assistant turn. The API imports only that type, so there is no database-to-API dependency. `respond` still reloads the turn and rechecks live authority; a stale reference can only lose access. Setup remains on its separate no-capability branch.

Presentation uses injected, non-authoritative subtype ports, not another run entry point. The capability module—not the adapters—maps lifecycle into a concrete intent already bound to the inbound source and permitted audience. Message/progress intents enter the durable outbox; reaction desired state is retained only until provider reconciliation; private typing uses an ephemeral in-memory/live adapter with a bounded lease and mandatory stop, never persistence. The adapters cannot interpret raw `requested`/`admitted`/`running` events or choose copy, target, timing, or policy. Adapter failure cannot change admission, execution, result, commit, or retry behavior. A raw model tool request never produces a user-visible work cue.

Outer cancellation is also accounted for. Supersession returns `superseded` only after the newer inbound is durably authoritative. A revoked/stopped channel returns `authority_lost` only after PostgreSQL marks the inbound handled and suppresses its output. An exact-family-group participant mismatch returns `authority_lost` only after one transaction retires the group, suppresses its pending delivery, stages private notices to both adults, and enters the fixed replacement-group path. Silence in the mismatched group is correct because delivery there is no longer authorized; recovery is not optional. Shutdown or another recoverable abort persists a retry and returns `retry_scheduled`. A normal enrolled message cannot disappear through a generic `cancelled` receipt.

Setup, provider OAuth, voice transcription, webhook acceptance, Linq delivery, and provider-effect settlement remain application seams. They either intentionally have no assistant catalog or own identity/irreversibility that model-tool registration must not blur.

## Why the current seam is too shallow

The current apparent interface is `FlorenceReasoner.decide(input, reads, signal, hooks)`, but its caller must know almost the entire tool system:

- `apps/api/src/florence.ts:1578-1892` constructs a model-safe projection, selects Google accounts, creates five read closures, tracks evidence, and exposes connection IDs.
- `apps/api/src/reasoner.ts:1065-1092` exports the provider-shaped `FlorenceReadTools` and the presentation-shaped `FlorenceDecisionHooks`.
- `apps/api/src/reasoner.ts:1310-1387` separately declares hard-coded model schemas.
- `apps/api/src/reasoner.ts:2044-2051` separately decides availability from move type, authored text, audience, and connection count.
- `apps/api/src/reasoner.ts:2094-2106` treats streamed model emission as work starting before argument validation or policy admission.
- `apps/api/src/reasoner.ts:2141-2186` separately recognizes and dispatches the same names.
- `apps/api/src/reasoner.ts:2969-3050` parses arguments again, trusts model-visible global connection IDs, checks privacy, executes adapters, expands source reachability, and bounds output.
- `apps/api/src/florence.ts:1402-1457` then applies more policy, threads Google evidence back into `decisionCommit`, and commits.

A new capability currently requires coordinated edits to schemas, availability, dispatch, source tracking, policy, evidence, progress, and tests. The caller can forget a step, and the same predicate can disagree between catalog construction and execution. That is exactly the shallow-module failure this boundary removes.

## Dependency classification

The boundary is deliberately Florence-shaped rather than an abstract connector framework.

### In-process and deterministic

These belong inside the module and are tested through `respond`:

- curated capability definitions and the completed authority-matrix metadata;
- stable registry generations, collision checks, and toolset composition;
- model schema projection and later authorized progressive discovery;
- raw-argument capture, new-value canonicalization, schema validation, deep freezing, and digesting;
- live policy admission, source reachability, minimized query egress, result audience, and consequence classification;
- Pi-shaped execution, progress suppression, error normalization, result bounds, transcript ordering, and terminal reduction;
- reasoner input projection, source ledger, minimum private-to-household crossing, decision validation, duplicate suppression, and commit construction.

### Concrete local durable dependency

PostgreSQL is Florence's concrete local durable authority, not a substitutable/in-memory adapter. The module uses the existing `PostgresFlorenceStore`, and integration tests require the real database selected by `TEST_DATABASE_URL`; it does not export a broad repository solely for unit tests. The encrypted attachment vault is also local application infrastructure. Focused external-provider behavior uses fakes only at the true network boundaries.

### True external adapters

OpenAI, Google, public-page/search, maps, weather, and travel each receive a concrete internal port with a production adapter and focused fake. Provider credentials and global account/connection IDs stay behind these adapters. The model receives an opaque run-local handle only when a task genuinely has multiple currently eligible resources.

### Remote-owned adapter

The later isolated browser worker is one bounded remote port with an in-process fake. Page content remains untrusted evidence and cannot change catalog, grants, audience, retention, approval, or settlement.

### Explicit non-dependencies

There is no universal MCP/connector/provider super-interface, arbitrary runtime registration, shell, coding tool, filesystem tool, cross-channel send tool, or second Pi/Hermes agent loop. Curated optional connectors become concrete definitions only after they have a complete Florence authority-matrix row.

## Design it three ways

The alternatives below were designed independently and then compared in caller order.

### Alternative A — public registry and dispatcher

```ts
interface FlorenceCapabilityRegistry {
  catalog(turn: InboundTurn): Promise<ReadonlyArray<ModelToolDefinition>>;
  invoke(turn: InboundTurn, call: ModelToolCall, signal: AbortSignal): Promise<ModelToolResult>;
  finalize(turn: InboundTurn, decision: FlorenceDecision): Promise<RespondReceipt>;
}
```

The caller builds the model loop: obtain a catalog, send schemas, inspect calls, invoke each one, append results, validate the final decision, and finalize.

This is superficially close to Hermes and easy to reuse in ad hoc model calls. It is rejected because every step has one required order and no independent product meaning. The caller sees schemas and tool terminal values, can hold a stale catalog, can call `invoke` without the matching transcript, can forget evidence or finalization, and must understand cancellation and progress. It ports a framework surface rather than deepening Florence.

### Alternative B — scoped turn session

```ts
interface FlorenceCapabilitySessions {
  open(turn: InboundTurn, signal: AbortSignal): Promise<CapabilityTurn>;
}

interface CapabilityTurn {
  readonly modelInput: ResponseInput;
  readonly tools: readonly FunctionTool[];
  invoke(call: FunctionCall): Promise<FunctionCallOutput>;
  seal(decision: FlorenceDecision): Promise<RespondReceipt>;
}
```

This improves safety: `open` can freeze one authorized catalog and the session can retain evidence, known sources, and lifecycle state. It also maps cleanly to Pi's per-turn lifecycle.

It is still rejected as the public seam. The only caller is the existing reasoner loop, so exposing `modelInput`, `tools`, `invoke`, and `seal` gives the caller four ways to violate ordering. It also leaves `#reasonerContext`, model-loop errors, decision validation, and commit policy distributed across modules. This is a good private implementation object inside `respond`, not a good Florence interface.

### Alternative C — whole-run façade

```ts
interface FlorenceCapabilities {
  respond(turn: EnrolledTurnRef, signal: AbortSignal): Promise<RespondReceipt>;
}
```

The caller supplies only a store-produced enrolled-turn reference and the outer lifetime. The reference is a locator, never a grant. The module reloads the turn, rereads all live authority that can revoke access, and privately owns the session, catalog, reasoner loop, tools, evidence, decision validation, and durable transition.

This is chosen. It has the deepest interface, the highest policy locality, the fewest invalid caller states, and the most useful default. It also forces tests to exercise the same seam production uses.

| Criterion | A: registry | B: session | C: whole run |
|---|---:|---:|---:|
| Public run entry points | 3 plus a caller-owned loop | 1 factory plus 2 session operations | 1 total |
| Caller sees schemas/tool calls | Yes | Yes | No |
| Caller can reorder required phases | Yes | Yes | No |
| Catalog and dispatch predicate locality | Medium | High per session | High across the full turn transition |
| Final decision/evidence/commit locality | Low | Medium | High |
| Fits current reasoner without a second runtime | Poor | Good | Best |
| Can later reuse its private engine for durable work | Poor | Medium | High, after the work lease is concrete |
| Migration size | Smallest | Medium | Largest, but deletes the most obsolete surface |

The larger migration is accepted because the old pieces are removed in the same tranche. Keeping them as compatibility shims would erase the depth gained by the façade.

## Private capability contract

The internal registry ports Pi's execution contract and extends Hermes's entry metadata with the already-frozen Florence policy. It is intentionally not exported from the module:

```ts
type AdapterOutcome<Output> =
  | Readonly<{
      status: "succeeded";
      output: Output;
      observedAt: string;
      references: readonly ProviderReference[];
    }>
  | Readonly<{ status: "failed"; code: AdapterErrorCode; retryable: boolean }>
  | Readonly<{ status: "cancelled" }>;

type CapabilityOutcome<Output> =
  | Readonly<{ status: "succeeded"; output: Output }>
  | Readonly<{ status: "blocked"; code: string; safeMessage: string }>
  | Readonly<{ status: "failed"; retryable: boolean; safeMessage: string }>
  | Readonly<{ status: "cancelled" }>
  | Readonly<{ status: "unknown"; reconciliationRef: string; safeMessage: string }>;

type ValidatedCapabilityTerminal<Output> = Readonly<{
  capabilityId: string;
  provider: string;
  observedAt: string;
  sourceOwner: ResolvedSourceOwner;
  visibility: ResolvedVisibility;
  audience: ResolvedResultAudience;
  canonicalQueryDigest: string;
  references: readonly ProviderReference[];
  retention: ResolvedRetention;
  outputBytes: number;
  outputLimit: number;
  outcome: CapabilityOutcome<Output>;
}>;

type CapabilityDefinition<Input, Output> = Readonly<{
  id: string;
  description: string;
  inputSchema: z.ZodType<Input>;
  outputSchema: z.ZodType<Output>;
  policy: CapabilityPolicyMetadata;
  executionMode: "sequential" | "parallel_read_only";
  canonicalize(raw: unknown): Input;
  execute(
    admission: AdmittedCapability<Input>,
    signal: AbortSignal,
    progress: (update: CapabilityProgress) => void,
  ): Promise<AdapterOutcome<Output>>;
}>;
```

`CapabilityDefinition` is a local implementation type, not a provider API. A definition closes over one concrete adapter and cannot select a credential or widen an account from model arguments. `CapabilityPolicyMetadata` is private frozen metadata from `assistant-capability-authority-matrix.md`; missing metadata means the definition cannot register.

The adapter may report only typed output, observation time, concrete provider references, or a closed internal error code/retry class. It cannot author model-visible error prose. The runtime maps that code to fixed bounded safe evidence and—not the adapter, definition, or model—derives source owner, visibility, query digest, retention, audience, and byte limit from the request/admission and policy. It validates those fields, wraps **every** succeeded, blocked, failed, cancelled, or unknown outcome in `ValidatedCapabilityTerminal`, and serializes the standard bounded model result. Definitions cannot relabel or custom-project their evidence after execution. `unknown` cannot originate from a `respond` read adapter; it enters only from a separately persisted application-owned effect/work settlement that already has a reconciliation identity.

Registration binds identity and schema, not just presence. The registry generates the input/output schema hashes from the definition and requires its `id`, schema references, schema versions/hashes, provider adapter, and consequence class to match the exact frozen `CapabilityPolicyMetadata` row before admitting the generation. Any mismatch, duplicate, or incomplete row fails startup; a definition cannot borrow another capability's grants, egress, retention, audience, or output limit.

Malformed, truncated, and unknown calls use a separate frozen application protocol record, not a fabricated capability result. The attempted unknown capability remains effect-capable and unavailable; only the application's no-op rejection record is `read_only`. The runtime creates and freezes a new canonical no-egress value `{ kind: "protocol_rejection", reason, attemptedNameDigest, rawArgumentsDigest }` and hashes that whole value. Its complete fixed policy is: capability and schema pair `florence.protocol_rejection`, provider/source kind/owner `none`/application/`florence_application`, visibility `application_only`, current verified actor required, the enclosing already-authorized `adult_private` or `household_shared` model context without widening, no query/action egress, `no_delivery` result audience, ephemeral retention/deletion with the run, `read_only` consequence, no approval owners, provider-disconnect-independent behavior, `untrusted_evidence` treatment, deterministic call identity, no progress, not cancellable, zero execution timeout, a fixed small output limit, and `blocked` terminal status. References are empty and `observedAt` is the rejection time. Provider disconnect cannot disable this fail-closed result; only loss of the enclosing turn authority suppresses the run. The safe bounded rejection may guide the current reasoner, but it cannot be delivered directly, retained as family evidence, or activate another capability.

Executable capability definitions are statically limited to `read_only` adapters and safe in-process computation. They may return a validated provider-write **intent proposal** as ordinary bounded evidence; they cannot claim it is durable or dispatch it. The final structured decision may select that proposal only when its exact consequence subtype already has an application-owned safe settler, and only the shielded `commitTurn` transaction turns it into a staged immutable intent. User-visible staging confirmation is in the committed outbox, so a superseded or failed transaction cannot produce a false success. The provider-effect settler is the only component that may execute reversible owner writes, standing household effects, or exact external effects. It persists intent before dispatch, reads provider state back, has the additional `unknown` terminal state, and never collapses `unknown` into `failed`, `cancelled`, or a retry.

## Mandatory execution order

### `respond`

1. Reobserve the current Linq thread and participant authority. A stale `InboundTurn` may lose access; it may never gain a broader audience.
2. Reread provider grants, source reachability, adult preferences, and pending exact approvals from PostgreSQL/provider state.
3. Build one owner-safe reasoner context. Raw private contexts from two adults never enter one model request.
4. Take a stable registry-generation snapshot, filter every definition through the same live policy predicate used by dispatch, and freeze the resulting turn catalog.
5. Read current message attachments through their exact source-bound references and bounds.
6. Run the existing strict Responses loop with the frozen catalog. For every complete model call:
   1. record `requested` with the raw argument digest;
   2. turn a truncated or unknown call into the fixed canonical protocol-rejection envelope without execution;
   3. parse raw arguments, canonicalize into a new value, schema-validate, deep-freeze, and retain the canonical digest; malformed/schema-invalid input uses the same fixed protocol-rejection path;
   4. verify the name existed in the frozen catalog;
   5. rerun live authority, grant, owner, source, egress, consequence, and provider-health admission;
   6. record `admitted`; only now may the module stage a bounded reaction or private typing intent;
   7. compose the outer signal with the definition's private timeout, record `running`, and execute the concrete adapter;
   8. accept bounded progress only before settlement and suppress late updates;
   9. validate output type, source owner, visibility, provenance, audience projection, and byte/token bound;
   10. record exactly one terminal outcome and append the bounded model result in model-source order. A result that requests an external effect is only a validated intent proposal and cannot claim durability; staging occurs only at the final shielded commit, and this loop cannot dispatch it.
7. Validate the structured Florence decision only against sources, facts, Calendar coverage, and public URLs actually returned in this run.
8. Apply private-to-household minimum crossing and exact pending-approval interpretation in their isolated, typed paths.
9. Atomically commit facts, reminders/work, source support, staged provider intents, and the outbox. `committed` means this PostgreSQL transition succeeded; it does not mean Linq delivered or another provider effect settled.

The PostgreSQL commit is a shielded linearization point. If the outer signal aborts before the transaction starts, `respond` atomically records retry, proves supersession, or durably accounts for lost authority. Once commit begins, cancellation cannot rewrite its outcome; `respond` awaits the actual database result and derives `committed`, `superseded`, `authority_lost`, or `retry_scheduled` from that result. A process crash is recovered from the same due inbound and deterministic commit/outbox identities.

### Future accepted work

The accepted-work ticket will define its lease/version and only then add a second typed entry point. That implementation must reread and lock the lease, commit `accepted -> running` before work language, rebuild a fresh policy-complete catalog, consume steering/cancellation at safe boundaries, use this same private Pi/Hermes engine, and commit only the frozen work states `accepted`, `running`, `succeeded`, `failed`, `unknown`, or `cancelled`. A blocked tool either yields one persisted parent question, a safe release/retry, or an honest `failed` work result; it never invents a durable `blocked` state.

### Error and lifecycle rules

- Policy denial, unavailable capability, malformed model arguments, and bounded provider rejection become safe typed tool results so the reasoner can still help, ask one genuinely blocking question, or fail honestly.
- `requested` is audit-only. `admitted` may drive only a bounded reaction or private typing intent. Future-tense work language requires `running`; "I’ll continue" or follow-up language requires a committed `accepted` work record. Progress must describe an observed phase, not fabricated effort.
- A timeout before a harmless request settles is retryable only according to its definition. Capability execution never dispatches a provider write. In the separate application-owned provider-effect settler, a timeout or abort after dispatch may have started persists `unknown` and requires provider reconciliation.
- The event observer is best effort and non-authoritative. Its failure never changes execution or commit.
- Parallel tools may complete out of order, but model transcript results and durable evidence preserve model-call source order.
- An ordinary parent-authored turn never commits silence. It ends with useful bubbles, one genuinely blocking question plus partial findings, or an honest failure.

## Exact Pi reuse

Pinned upstream: `4e494929998d6bc4fccf75e0a233f727db4b70ee` in `/Users/harianbarasu/Projects/florence-upstreams/pi`.

- **Adapted port from exact source types:** start from `AgentTool`, `AgentToolResult`, `AgentToolUpdateCallback`, the `AbortSignal`/update execution signature, execution-mode vocabulary, and tool start/update/end event shapes in `packages/agent/src/types.ts:360-443`. Port the callback/lifetime mechanics into `CapabilityDefinition.execute`, but adapt result content, progress payloads, event names, and execution modes to Florence's typed provenance and read-only parallelism. Florence adds monotonic/size bounds because Pi's callback itself does not.
- **Adapted port:** before/after tool-call context and result contracts from `packages/agent/src/types.ts:55-123` and `260-292`. Florence preflight adds live household admission and Florence postflight validates source owner, visibility, audience, retention, and terminal status.
- **Adapted port:** batch selection, sequential/parallel execution, completion-order events, source-order transcript assembly, complete-call lookup, argument preparation, validation, preflight, execution, progress, postflight, error normalization, and terminal ordering from `packages/agent/src/agent-loop.ts:376-545` and `575-790`, inserted into Florence's existing Responses loop rather than importing Pi's agent/provider loop.
- **Direct/adapted test ports:** directly port the unchanged assertions that truncated calls do not execute, blocked calls settle once, parallel completion preserves source-order model history, and late progress is suppressed. Adapt Pi's abort/error assertions to Florence's persisted retry, supersession, and application-owned `unknown` settlement rules. Start from `packages/agent/test/agent-loop.test.ts:371-725` and `1253-1395`, and `packages/agent/test/agent.test.ts:159-439`.
- **Adapted inverse test:** Pi permits a pre-hook to mutate validated arguments in `packages/agent/test/agent-loop.test.ts:444-504`. Florence preserves raw arguments, creates and freezes a separate canonical value, and rejects later mutation because egress, approval, idempotency, and settlement digests depend on it.
- **Intentional semantic split:** Pi emits `tool_execution_start` before lookup/admission. Florence ports the event mechanism but splits it into `requested`, `admitted`, and `running`; only `admitted` can stage a bounded reaction/private typing intent, and work language waits for `running` or committed acceptance.

Using Pi as a package dependency would also import a parallel agent/message/provider runtime. The direct and adapted ports above reuse Pi's relevant lifecycle code and tests inside Florence's existing Responses loop, which is both more faithful to the product and less duplicated runtime.

## Exact Hermes reuse

Pinned upstream: `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` in `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.

- **Adapted port:** `ToolEntry` fields for schema, handler, availability, required environment, async execution, description, dynamic schema, and maximum result size from `tools/registry.py:204-233`. Florence adds the mandatory policy metadata, output schema, canonicalizer, provenance, and result projector.
- **Adapted port:** collision rejection, stable registry snapshots, generation invalidation, availability filtering, normalized dispatch errors, maximum-result metadata, and bounded error text from `tools/registry.py:452-534`, `763-882`, and `1044-1182`. Florence-owned typed result validation enforces the universal successful-output limit because Hermes dispatch does not.
- **Adapted port:** provider-health last-good caching from `tools/registry.py:245-417`, limited to transient health. Actor authority, consent, audience, source visibility, connection state, and revocation are always live and fail closed.
- **Adapted port:** recursive toolset composition and generation-keyed memoization from `toolsets.py:620-870`. Hermes silently skips both cycles and diamond revisits; Florence separately detects recursion and rejects cycles at startup while preserving valid diamond composition. Toolsets remain private implementation groupings and may never grant authority.
- **Later adapted port:** direct/deferrable classification, stateless bounded catalog assembly, session-scoped names, and deferred argument probing from `tools/tool_search.py:210-325`, `370-615`, `880-990`, and `1180-1305`. Discovery indexes only the already-authorized frozen catalog and cannot discover a broader capability.
- **Direct/adapted test ports:** registry collisions/generations/availability/dispatch/error bounds, toolset recursion/diamond behavior with Florence's inverse cycle assertion, and scoped deferred dispatch from Hermes's `tests/tools/test_registry.py`, `tests/test_toolsets.py`, and `tests/tools/test_tool_search.py`.

Hermes is Python and lacks Florence's two-adult identity, private-source ownership, minimum-crossing audience, per-field egress, live Linq check, deletion cascade, or exact-effect settlement. Those additions are Florence-owned for concrete policy reasons, not because equivalent upstream behavior was ignored. Hermes's arbitrary plugin loading, shell approval patterns, broad messaging, coding tools, and runtime-wide registries are deliberately excluded.

## Replace-not-layer migration

Ticket 04 implements the cutover through `respond`; it must not leave a compatibility runtime.

1. Add a private policy-complete capability definition for each of the five current read tools, starting from its existing schema/handler and the frozen matrix row.
2. Port Pi's lifecycle reducer and Hermes's registry snapshot/dispatch into private reasoner modules. Every current function-call path moves to that engine in this tranche, including the `PRIVATE_GMAIL_ATTACHMENT_TOOL` loops used by private Google classification/change assessment; the isolated OpenAI web-search pass becomes the concrete public-research adapter instead of a second caller-owned tool loop. Keep the existing strict Responses reasoning and structured `FlorenceDecision` result, not a second agent runtime.
3. Before `respond` may stage any provider-write subtype, replace the currently reachable Family Calendar, partner invitation, group creation, and activation effect executors with the application-owned settlement seam: persist a deterministic intent before dispatch, provider-read-back afterward, durable `unknown` for ambiguity, and reconcile before retry. Delete the current blind-retry paths in the same cutover.
4. Move, rather than wrap, `#reasonerContext` and its Google/source/media closures from `apps/api/src/florence.ts:1578-1892` behind `respond`.
5. Move, rather than wrap, enrolled-turn policy/approval interpretation and `decisionCommit` from `apps/api/src/florence.ts:1279-1457` and `4232-4446` behind `respond`. Preserve `PostgresFlorenceStore.commitTurn` as the transactional authority check.
6. Change the scheduler/store boundary to yield an opaque enrolled-turn reference, and replace the enrolled `#handleInbound` body with `capabilities.respond(enrolledTurnRef, controller.signal)`. `respond` reloads the full turn privately. Keep pre-enrollment/setup outside because it intentionally has no household capability catalog.
7. Delete `FlorenceReadTools`, `FlorenceDecisionHooks`, `MEMORY_TOOL`, `SOURCE_TOOL`, `PUBLIC_RESEARCH_TOOL`, `GMAIL_TOOL`, `CALENDAR_TOOL`, `runReadTool`, and the caller-owned Google evidence/connection-ID plumbing.
8. Remove `connectionId` from model-authored Gmail and Calendar arguments. Resolve the verified account owner internally; expose only a frozen run-local opaque handle when multiple resources are genuinely eligible.
9. Replace the hard-coded work-name array and premature `onWorkStarted` call with module-owned lifecycle-to-presentation intent mapping. Admission may stage only a bounded reaction/private typing intent; work language begins only after execution or committed durable acceptance.
10. Port lifecycle/registry tests through `FlorenceCapabilities.respond`; delete tests that preserve obsolete direct schema switches or context-builder contracts.
11. Add no durable-work public method now. Once the accepted-work ticket defines the real schema and lease, it may add one second typed entry point; in that tranche, move the relevant initial-intelligence/proactive orchestration behind it and delete the old workflow executors. Their tool execution already uses the shared private engine from step 2, so there is never a second capability runtime.
12. Keep Linq sending application-owned, but deepen the outbox claim/delivery guard in the same cutover: recheck every source support and provider/connector grant recorded by the capability result as well as the live channel/participants. Disconnect, deletion, or lost support suppresses pending source-dependent output before send.

The application keeps `#runCycle` as the scheduler, `#deliverOutbound` as the live-authorized Linq delivery seam, and the step-3 provider-effect settler for reversible owner writes, standing household effects, and exact external effects. The module stages those records but does not call delivery or settle an effect.

## Verification contract for the implementation ticket

The first implementation is complete only when:

- a normal parent turn, a harmless Gmail/Calendar/memory/public read, a blocked private-source read, a malformed and truncated call, a cancellation, a timeout, a late progress update, and a provider error all enter through `respond`;
- catalog filtering and dispatch demonstrate the same live predicate;
- raw/canonical argument separation and immutability are tested;
- connection/account IDs cannot appear in model-authored arguments;
- every succeeded, blocked, malformed/truncated, failed, cancelled, and persisted-unknown terminal has a runtime-constructed source-bearing bounded envelope; admission may stage only a bounded reaction/private typing intent, while work language requires running execution or committed accepted work;
- final decision validation can cite only evidence produced inside the run;
- PostgreSQL commit remains atomic and Linq delivery remains separate, with live source/grant plus participant rechecks before every send;
- a changed family-group participant set durably retires the group, suppresses its output, privately notices both adults, and enters replacement before `authority_lost` settles;
- every currently reachable provider effect persists intent before dispatch, reads provider state back, records ambiguity as `unknown`, and never blind-retries;
- the five legacy tool declarations/dispatch switch, exported read hooks, and caller evidence plumbing are gone;
- the full repository check passes, followed by the database integration suite when `TEST_DATABASE_URL` is available.

## Consequences

- Adding a Florence capability becomes one internal definition plus one concrete adapter and its matrix row. Callers do not change.
- Tests get heavier at the boundary but much less coupled to internal model-tool wiring.
- Bespoke model calls cannot bypass the run ledger. That restriction is desirable for household privacy and durable work.
- The first migration is larger than a registry wrapper, but it deletes multiple sources of policy drift instead of preserving them.
- Future progressive discovery can scale the private catalog without changing the public interface.
- Durable work reuses the same private execution machinery without creating another scheduler or agent runtime.
