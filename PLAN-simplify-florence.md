# Florence Simplification Plan

## Goal

Make Florence a **thin household layer on top of Hermes**, not a second orchestration stack.

Mental model:

- OpenClaw / Hermes for the household
- trusted consumer wrapper
- family / parent-group first
- Florence-specific code only where household memory, Google grounding, privacy, and transport require it

Target shape:

- Hermes owns almost all conversation behavior
- Florence tools own household state reads/writes
- transport layers own webhook parsing, sending, timers, and idempotency
- very little product logic lives outside [`florence/runtime/chat.py`](./florence/runtime/chat.py)

Principles:

1. Delete before refactor.
2. Prefer one obvious seam over multiple “service” layers.
3. Determinism should only protect invariants.
4. Anything that sounds like Florence talking should usually come from Hermes.
5. Web onboarding/setup should not survive as a second product.

## Current State

Status after the first simplification pass:

- Full web onboarding/setup path has been removed from the active runtime.
- SMS-first onboarding is the only active product path.
- Dead onboarding link modules are deleted.
- The dead `runtime/jobs.py` wrapper is deleted.
- `production.py` no longer carries the old web rendering/control-plane layer.
- `server.py` is now mostly health, Google callback, and Linq/SendBlue webhooks.
- The live runtime no longer uses ingress-owned schedule/tracking/reminder summary shortcuts; those turns now route toward Hermes instead of a deterministic query side path.
- The old query-service compatibility seam is deleted entirely from the runtime and tests.
- Web-era Google connection metadata like `web_primary` is gone.
- `florence.runtime` exports are trimmed down closer to the symbols the repo actually uses.
- `FlorenceHouseholdManagerService` is split out of [`florence/runtime/services.py`](./florence/runtime/services.py) into [`florence/runtime/household_manager.py`](./florence/runtime/household_manager.py).
- Google OAuth/connect/sync services are split out of [`florence/runtime/services.py`](./florence/runtime/services.py) into [`florence/runtime/google_services.py`](./florence/runtime/google_services.py).
- The onboarding session service is split out of [`florence/runtime/services.py`](./florence/runtime/services.py) into [`florence/runtime/onboarding_service.py`](./florence/runtime/onboarding_service.py).
- Onboarding state is reduced to:
  - parent name
  - Google connected
  - kids
  - per-child age / school / activities
- Florence still has meaningful simplification work left, but the complexity is now concentrated in a few files instead of spread across a second product surface.

Current hot spots by size and responsibility:

- [`florence/messaging/ingress.py`](./florence/messaging/ingress.py) — still too many gates and heuristics
- [`florence/runtime/production.py`](./florence/runtime/production.py) — still larger than the ideal transport shell
- [`florence/runtime/chat.py`](./florence/runtime/chat.py) — should keep growing in ownership because this is the intended product brain
- [`florence/runtime/household_manager.py`](./florence/runtime/household_manager.py) — newly split out; this is progress, not a new problem
- [`florence/runtime/google_services.py`](./florence/runtime/google_services.py) — coherent and acceptable; keep it infrastructure-focused
- [`florence/runtime/onboarding_service.py`](./florence/runtime/onboarding_service.py) — coherent and acceptable; keep it as a thin onboarding skeleton

Current line counts after the latest pass:

- [`florence/config.py`](./florence/config.py): `395`
- [`florence/runtime/entrypoints.py`](./florence/runtime/entrypoints.py): `200`
- [`florence/runtime/services.py`](./florence/runtime/services.py): `649`
- [`florence/runtime/onboarding_service.py`](./florence/runtime/onboarding_service.py): `327`
- [`florence/runtime/google_services.py`](./florence/runtime/google_services.py): `558`
- [`florence/runtime/household_manager.py`](./florence/runtime/household_manager.py): `580`
- [`florence/messaging/ingress.py`](./florence/messaging/ingress.py): `1382`
- [`florence/runtime/production.py`](./florence/runtime/production.py): `1002`
- [`florence/runtime/chat.py`](./florence/runtime/chat.py): `859`

## End-State Seams

### 1. Hermes Conversation Layer

Primary file:

- [`florence/runtime/chat.py`](./florence/runtime/chat.py)

Owns:

- normal DM replies
- group-chat replies
- review framing
- sync-status phrasing
- first-sync activation brief
- daily / weekly briefs
- capture -> handled behavior
- private -> shared promotion phrasing

### 2. Household State + Tools

Primary files:

- [`florence/contracts.py`](./florence/contracts.py)
- [`florence/state/store.py`](./florence/state/store.py)
- [`florence/state/db.py`](./florence/state/db.py)
- Florence tool implementations outside this package

Owns:

- durable state
- ids
- persistence
- invariants
- query/update APIs

### 3. Sync + Import Pipeline

Primary files:

- [`florence/google/fetch.py`](./florence/google/fetch.py)
- [`florence/google/sync.py`](./florence/google/sync.py)
- [`florence/relevance/google_candidates.py`](./florence/relevance/google_candidates.py)

Owns:

- Gmail/Calendar fetch
- normalization
- candidate creation / possible-fact extraction

### 4. Transport + Scheduling

Primary files:

- [`florence/server.py`](./florence/server.py)
- [`florence/worker.py`](./florence/worker.py)
- [`florence/runtime/entrypoints.py`](./florence/runtime/entrypoints.py)
- [`florence/runtime/production.py`](./florence/runtime/production.py)
- [`florence/runtime/scheduler.py`](./florence/runtime/scheduler.py)
- [`florence/runtime/queue.py`](./florence/runtime/queue.py)
- provider adapters/clients

Owns:

- webhook receipt
- OAuth callback receipt
- message send
- timer/debounce
- queue claim / retry
- no product phrasing beyond hard failures

## What Is Too Complicated Today

The complexity is not mainly in the domain model. It is in **behavior being split across too many layers**:

- [`florence/messaging/ingress.py`](./florence/messaging/ingress.py)
- [`florence/runtime/production.py`](./florence/runtime/production.py)
- [`florence/runtime/entrypoints.py`](./florence/runtime/entrypoints.py)
- [`florence/onboarding/flow.py`](./florence/onboarding/flow.py)
- [`florence/runtime/services.py`](./florence/runtime/services.py)

Those files currently all influence:

- what Florence says
- when Florence says it
- when onboarding advances
- how sync/review/onboarding interleave

That is the main simplification target.

## File-by-File Plan

### Keep Mostly As-Is

- [`florence/contracts.py`](./florence/contracts.py)
  - Keep. This is the shared household schema.
  - Only trim dead enums/fields after product cuts.

- [`florence/state/db.py`](./florence/state/db.py)
  - Keep. Thin DB abstraction is justified.
  - Do not add more behavior here.

- [`florence/state/store.py`](./florence/state/store.py)
  - Keep. This is the state spine.
  - Refactor only to remove dead methods after feature cuts.

- [`florence/google/types.py`](./florence/google/types.py)
  - Keep.

- [`florence/google/oauth.py`](./florence/google/oauth.py)
  - Keep.
  - This is real infrastructure complexity.

- [`florence/google/fetch.py`](./florence/google/fetch.py)
  - Keep.
  - It is large, but it is core fetch/normalization logic.

- [`florence/google/sync.py`](./florence/google/sync.py)
  - Keep.
  - This is the right place for grounding/candidate conversion.

- [`florence/relevance/common.py`](./florence/relevance/common.py)
  - Keep.

- [`florence/relevance/temporal.py`](./florence/relevance/temporal.py)
  - Keep.

- [`florence/relevance/google_candidates.py`](./florence/relevance/google_candidates.py)
  - Keep for now.
  - Later simplification option: reduce heuristic branches and rely more on LLM scoring.

- [`florence/linq/adapter.py`](./florence/linq/adapter.py)
  - Keep.

- [`florence/linq/client.py`](./florence/linq/client.py)
  - Keep.

- [`florence/sendblue/adapter.py`](./florence/sendblue/adapter.py)
  - Keep.

- [`florence/sendblue/client.py`](./florence/sendblue/client.py)
  - Keep.

- [`florence/messaging/types.py`](./florence/messaging/types.py)
  - Keep.

- [`florence/runtime/queue.py`](./florence/runtime/queue.py)
  - Keep.
  - It is already small and single-purpose.

- [`florence/runtime/scheduler.py`](./florence/runtime/scheduler.py)
  - Keep.
  - It is already tiny.

- [`florence/worker.py`](./florence/worker.py)
  - Keep.
  - Small and focused.

### Keep But Aggressively Slim

- [`florence/runtime/chat.py`](./florence/runtime/chat.py)
  - Keep and make this the primary Florence product file.
  - Move more phrasing and interpretation here.
  - Add small helpers rather than pushing logic back into ingress/production.

- [`florence/runtime/entrypoints.py`](./florence/runtime/entrypoints.py)
  - Keep, but shrink to:
    - provider payload normalization
    - identity resolution
    - call ingress
    - OAuth callback handoff
  - No product copy beyond hard failure fallback.

- [`florence/runtime/production.py`](./florence/runtime/production.py)
  - Keep, but cut heavily.
  - This file is currently too large because it owns:
    - HTTP results
    - HTML rendering
    - sync job orchestration
    - message delivery
    - callback handling
    - activation brief fallback copy
  - Biggest simplification:
    - remove web onboarding/setup rendering
    - remove large HTML helper surface
    - keep only transport/job/delivery/orchestration

- [`florence/runtime/services.py`](./florence/runtime/services.py)
  - Keep short-term, but reduce scope.
  - It is currently a “misc everything” module.
  - Target role:
    - pure state transition coordinators
    - Google/account sync services
    - household manager persistence
  - Remove conversational responsibility from here.

- [`florence/messaging/ingress.py`](./florence/messaging/ingress.py)
  - Keep, but reduce drastically.
  - Target role:
    - dedupe
    - append inbound message
    - one onboarding gate
    - one review confirmation gate
    - one reminder done/snooze gate
    - share/promote gate
    - else -> `chat.py`
  - Delete most semantic routing helpers over time.

- [`florence/onboarding/state.py`](./florence/onboarding/state.py)
  - Keep, but shrink the state model.
  - Only keep fields required for:
    - parent name
    - Google connected
    - kids
    - per-child age/school/activities
    - maybe group activation later
  - Everything else should be learned in natural conversation later.

- [`florence/onboarding/flow.py`](./florence/onboarding/flow.py)
  - Keep, but make it much smaller.
  - It should only answer:
    - what do we still need?
    - what is the next required question?
  - Remove extra onboarding branches and nonessential fields.

- [`florence/onboarding/intake.py`](./florence/onboarding/intake.py)
  - Keep.
  - This is the right place for LLM extraction of onboarding replies.
  - Make it the only place that interprets onboarding freeform text.

- [`florence/linq/media.py`](./florence/linq/media.py)
  - Keep, but treat as generic media extraction.
  - Do not let it accumulate product-level logic.

- [`florence/config.py`](./florence/config.py)
  - Keep, but aggressively reduce config surface.
  - Florence should not expose dozens of tunable Hermes knobs unless they are truly needed in production.
  - Push advanced model/tool settings down to Hermes defaults unless Florence absolutely needs them.

### Delete Or Remove From Active Product Path

- [`florence/onboarding/parsing.py`](./florence/onboarding/parsing.py)
  - Delete or reduce to tiny fallback utils.
  - The main onboarding understanding should come from the LLM intake path, not string splitting.

- [`florence/onboarding/links.py`](./florence/onboarding/links.py)
  - Delete if web onboarding is fully removed.
  - If some token helper is still needed elsewhere, move the minimal encode/decode into the owning module.

- [`florence/runtime/onboarding_links.py`](./florence/runtime/onboarding_links.py)
  - Delete with web onboarding removal.

- [`florence/runtime/jobs.py`](./florence/runtime/jobs.py)
  - Delete.
  - It is a tiny wrapper that does not buy enough abstraction.

- [`florence/source_rules.py`](./florence/source_rules.py)
  - Keep for now, but consider later merge into [`florence/runtime/services.py`](./florence/runtime/services.py) if the separate abstraction stays small.
  - Not an immediate target.

- [`florence/server.py`](./florence/server.py)
  - Keep, but delete any nonessential preflight/debug/HTML responsibilities once production flow stabilizes.
  - This should become a thin HTTP adapter, not a control plane.

- [`florence/__init__.py`](./florence/__init__.py)
  - Keep or slim if exports are unnecessary.

- [`florence/runtime/__init__.py`](./florence/runtime/__init__.py)
  - Simplify or delete if lazy exports are not buying much.
  - Direct imports are easier to reason about in a small product layer.

- [`florence/messaging/__init__.py`](./florence/messaging/__init__.py)
  - Keep, but trivial.

- [`florence/onboarding/__init__.py`](./florence/onboarding/__init__.py)
  - Simplify after onboarding cuts.
  - It currently re-exports too much because onboarding owns too much.

- [`florence/google/__init__.py`](./florence/google/__init__.py)
  - Keep trivial.

- [`florence/linq/__init__.py`](./florence/linq/__init__.py)
  - Keep trivial.

- [`florence/sendblue/__init__.py`](./florence/sendblue/__init__.py)
  - Keep trivial.

- [`florence/state/__init__.py`](./florence/state/__init__.py)
  - Keep trivial.

- [`florence/relevance/__init__.py`](./florence/relevance/__init__.py)
  - Keep trivial.

### Remove Entirely

- `florence/bluebubbles/`
  - Remove completely.
  - User confirmed Florence will use SendBlue and later Linq, not BlueBubbles.

## Tests: Simplification Rule

Test count is not the problem. Test shape should follow product seams.

Good future test buckets:

- chat behavior
- ingress invariants
- transport adapters
- sync pipeline
- state/store

As files are deleted or merged, delete corresponding tests instead of preserving old architecture in test form.

## Phase Plan

### Phase 1: Delete Dead Paths

Do first:

1. Remove `bluebubbles`.
2. Remove web onboarding link plumbing:
   - [`florence/onboarding/links.py`](./florence/onboarding/links.py)
   - [`florence/runtime/onboarding_links.py`](./florence/runtime/onboarding_links.py)
3. Remove [`florence/runtime/jobs.py`](./florence/runtime/jobs.py).
4. Remove old web setup/onboarding rendering from [`florence/runtime/production.py`](./florence/runtime/production.py) and [`florence/server.py`](./florence/server.py).

Expected result:

- smaller runtime surface
- fewer codepaths that can speak to the user

Status:

- Done.

### Phase 2: Shrink Onboarding To The Bare Minimum

Target onboarding:

1. parent name
2. Google connect
3. kid names
4. for each kid: age, school, activities

Delete from onboarding flow/state:

- household members
- household operations
- nudge preferences
- operating preferences
- any extra setup fields that can be learned later through normal conversation

Expected result:

- [`florence/onboarding/flow.py`](./florence/onboarding/flow.py) gets much smaller
- [`florence/onboarding/state.py`](./florence/onboarding/state.py) gets much smaller
- [`florence/runtime/services.py`](./florence/runtime/services.py) loses onboarding baggage

Status:

- Mostly done.
- Remaining work is not new onboarding fields; it is better separation between:
  - onboarding state transitions
  - onboarding interpretation
  - ingress orchestration

### Phase 3: Collapse Conversation Logic Into `runtime/chat.py`

Move toward this rule:

- if it is a normal conversational reply, `chat.py` owns it
- if it is a hard state transition, ingress owns it

Reduce [`florence/messaging/ingress.py`](./florence/messaging/ingress.py) to:

- dedupe
- persist inbound message
- onboarding gate
- explicit review yes/no/skip gate
- reminder done/snooze gate
- group promotion gate
- debounce timer handling
- fallback `chat.py`

Delete or replace many `_looks_like_*` routes as Hermes proves reliable.

Status:

- In progress.
- Some deterministic copy paths have already been pushed into [`florence/runtime/chat.py`](./florence/runtime/chat.py).
- The deterministic query shortcut layer is already gone from the live path.
- The main remaining work is to strip duplicated review / sync / onboarding gates out of [`florence/messaging/ingress.py`](./florence/messaging/ingress.py) and let Hermes own more of the normal turn handling.

### Phase 4: Cut `production.py` Down To Transport + Jobs

`production.py` should not own product storytelling.

Keep only:

- webhook handling
- callback receipt
- queue launch
- message send
- retry/idempotency
- sync job lifecycle

Push:

- all user-facing soft copy into [`florence/runtime/chat.py`](./florence/runtime/chat.py)
- large HTML/UI rendering out entirely

Status:

- Major progress made.
- The old web setup/UI layer is gone.
- `FlorenceEntrypointService` no longer constructs household chat.
- `FlorenceProductionService` now owns one shared Hermes household chat service and injects it downward.
- The old deterministic first-sync brief builder has been reduced to a tiny safe fallback.
- Remaining work is to keep trimming helper/fallback logic until `production.py` is mostly:
  - webhook receipt
  - callback receipt
  - queue launch
  - message delivery
  - retry / idempotency

### Phase 5: Reduce Florence-Specific Config

Audit [`florence/config.py`](./florence/config.py) and remove Florence-only knobs that should just inherit Hermes defaults.

Keep only core runtime config:

- DB
- public URL
- Google OAuth
- provider credentials
- Redis queue
- primary model

Everything else should default unless there is a production reason to override it.

Status:

- Major progress made.
- Web onboarding config was removed.
- Florence-specific Hermes override knobs were removed; the Florence Hermes runtime surface is now just:
  - model
  - max_iterations
  - provider
- Remaining work is mostly cleanup, not more configuration design.

## Next Execution Slice

The next simplification work should be:

1. Keep shrinking [`florence/messaging/ingress.py`](./florence/messaging/ingress.py):
   - remove fake optionality around Hermes household chat
   - collapse more review/sync/share branching into a smaller number of gates
   - keep pushing normal conversational handling toward [`florence/runtime/chat.py`](./florence/runtime/chat.py)
   - dedupe sync-waiting branches
   - reduce “normal chat” fallback logic to one obvious path
2. Keep shrinking [`florence/runtime/production.py`](./florence/runtime/production.py):
   - move any remaining Florence phrasing into [`florence/runtime/chat.py`](./florence/runtime/chat.py)
   - keep only delivery, queue launch, callback receipt, and idempotency
3. Re-audit [`florence/config.py`](./florence/config.py) and [`florence/runtime/__init__.py`](./florence/runtime/__init__.py) for unnecessary Florence-only surface.

What already landed from this slice:

- query shortcut removal from the live runtime path
- deletion of the query-service compatibility seam
- removal of dead helper functions in [`florence/runtime/services.py`](./florence/runtime/services.py)
- removal of web-era `web_primary` metadata
- trimming of unused exports from [`florence/runtime/__init__.py`](./florence/runtime/__init__.py)
- split of [`florence/runtime/household_manager.py`](./florence/runtime/household_manager.py) out of `services.py`
- split of [`florence/runtime/google_services.py`](./florence/runtime/google_services.py) out of `services.py`
- split of [`florence/runtime/onboarding_service.py`](./florence/runtime/onboarding_service.py) out of `services.py`

## Recommended Implementation Order

1. Continue stripping semantic routing out of [`florence/messaging/ingress.py`](./florence/messaging/ingress.py).
2. Keep shrinking [`florence/runtime/production.py`](./florence/runtime/production.py).
3. Keep pushing user-visible behavior into [`florence/runtime/chat.py`](./florence/runtime/chat.py).
4. Simplify [`florence/config.py`](./florence/config.py).
5. Simplify [`florence/runtime/__init__.py`](./florence/runtime/__init__.py).
6. Clean up tests to match the new seams.

## Success Criteria

We are done when:

- Florence feels like Hermes with household tools, not a custom wizard app
- `runtime/chat.py` is clearly the main product brain
- `ingress.py` is mostly invariants
- `production.py` is mostly transport/job orchestration
- onboarding is tiny
- web onboarding is gone
- there is only one obvious place to change how Florence talks
