Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 03

# Port the Pi tool lifecycle into Florence

## Question

How can the family receive truthful tool start, bounded progress, cancellation, terminal success/failure/unknown, and follow-up behavior through the chosen Florence capability interface without replacing Florence's transactional decision and delivery core?

Use an adapted port of Pi's `packages/agent/src/types.ts`, `packages/agent/src/agent-loop.ts`, `packages/agent/test/agent-loop.test.ts`, and relevant retry classification in `packages/coding-agent/src/core/agent-session.ts`. Preserve Florence's strict structured decision, source validation, `commitTurn()` authority recheck, and Linq-owned delivery.

## Answer

The family can now see Florence acknowledge only admitted work, remain responsive through real progress, and always finish or recover a tool-backed request truthfully in iMessage.

`FlorenceCapabilities.respond(EnrolledTurnRef, AbortSignal)` is the sole enrolled-turn application seam. Setup may promote only a store-minted inbound locator, after which `#respondEnrolled` reloads current authority and runs the capability engine exactly once. The façade no longer recursively re-enters setup, and callers cannot see tool schemas, provider identifiers, admission rules, evidence plumbing, lifecycle state, or commit construction.

The private `CapabilityRegistry` now owns one immutable, generation-bound catalog and dispatch path. Every current model function call—family-memory search, source read, public research, Gmail search, Calendar-window read, and both private Gmail-attachment loops—uses it. Arguments are parsed, canonically cloned, recursively frozen, and digested. Catalog admission and dispatch admission share one predicate, with dispatch reloading the turn, observing Linq authority, and rechecking the applicable Google grant. Only read-only capabilities enter this runtime; Calendar writes, invitations, group repair, and other effects remain in Florence's transactional application path.

Each accepted call emits `requested`, `admitted`, `running`, bounded `progress`, and exactly one terminal envelope. Truncated, malformed, duplicate, unknown, unavailable, timed-out, cancelled, oversized, and adapter-invalid calls never execute or masquerade as success. Terminal envelopes return to the model in source order, even when read-only calls finish in parallel, and include bounded source/provenance evidence. Attachment bytes and Google evidence rejoin model input or durable commit only after a successful terminal settlement. A late adapter result cannot replace timeout/cancellation or mutate committed evidence.

Presentation follows real lifecycle edges but is not authority: `requested` is inert; `admitted` may start a natural reaction/private typing indicator; `running` may produce one delayed work cue; and provider failures or hangs cannot delay the substantive commit or receipt. Florence still closes every ordinary turn with a model result, a safe failure, or a scheduled retry. A retry now invalidates already-issued turn locators until it is due, preventing a second worker from defeating backoff.

Commit and delivery remain Florence-owned. `commitTurn()` rechecks the branded enrolled locator and due state, persists only successfully settled Google evidence, and records every source, Google grant, and exact fact revision used by the response. Linq delivery rechecks current channel membership, sharing preferences, source visibility, Google ownership, and fact revisions. Authority loss and STOP atomically settle sibling received messages, pending/sending outbounds, and approved Calendar work instead of leaving orphaned rows.

### Upstream reuse

- **Pi `4e494929998d6bc4fccf75e0a233f727db4b70ee` — adapted port:** lifecycle/result/update types from `packages/agent/src/types.ts:259-292,360-408,421-443`; immutable per-turn tools, validation, parallel/sequential execution, cancellation, progress suppression, source-order result assembly, and terminal handling from `packages/agent/src/agent-loop.ts:374-405,408-580,600-795`; focused invariants from `packages/agent/test/agent-loop.test.ts:371-427,586-675,787-1028` and `packages/agent/test/agent.test.ts:306-443`.
- **Pi `4e494929998d6bc4fccf75e0a233f727db4b70ee` — direct/adapted port:** provider-error precedence from `packages/ai/src/utils/retry.ts:3-68,209-228`. Florence ports classification, not Pi's blind rerun loop; retry is a fresh durable turn after current authority is rechecked.
- **Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` — adapted port:** registry entries, collision rejection, stable catalog/dispatch lookup, availability, dispatch failures, and result bounds from `tools/registry.py:452-534,1044-1091,1097-1168`.
- Hermes global toolsets and progressive `tool_search` were inspected but deliberately not copied here. A mutable/global toolset would create a second policy plane, while progressive disclosure is already mapped to ticket 18 and must search only Florence's authorized frozen catalog.

Florence-owned differences are limited to household identity, adult/source visibility, canonical frozen arguments, live Linq and Google admission, evidence envelopes, PostgreSQL idempotency and fact revisions, approval/effect settlement, and Linq delivery—dimensions neither upstream models. The façade implementation stays as the distinct `Florence.#respondEnrolled` core behind the frozen value interface instead of adding a one-caller dependency-injection class; moving the same Florence-owned orchestration would add infrastructure without deepening the interface.

Two concrete continuations remain where the map already owns them: ticket 05 will add all-calendar coverage plus provider-event/window revision revalidation, and tickets 09/10 will add typed fact-backed reminder/work dependencies instead of pretending a fact UUID is a raw source UUID. They are not hidden inside this lifecycle runtime.

### Verification

- `pnpm check` passed on Node `24.19.0` and pnpm `10.10.0`: lint, all package/application typechecks, tests, and builds are green.
- 16 focused lifecycle ports and 4 focused reasoner-cutover tests passed. No unrelated harness or permutation suite was added.
- The existing three PostgreSQL integration cases were skipped because `TEST_DATABASE_URL` is not configured in this shell; database changes were typechecked, built, statically reviewed, and remain covered by the release-gate database rehearsal.
- `git diff --check` passed.
- Final read-only review found no P0. Its stale recursive-façade finding was superseded by the one-way `#respondEnrolled` split; its retry-locator race was fixed. Calendar-provider revision and durable fact-backed scheduling findings were routed to their existing owning tickets rather than spawning infrastructure here.
