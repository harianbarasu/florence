## Core User Flows

### 1. Confirm The Exact Thing Florence Just Asked About
- Trigger: Florence asks a specific follow-up like "Should I add Theo's music class on July 7?"
- User steps: User replies with a natural confirmation like "yes", "yep", or "sounds good"
- Visible states:
  - Prompt sent with a clear single action preview
  - User replies
  - Florence confirms exactly that one action and shows what changed
- Failure/recovery:
  - If the reply is ambiguous, Hermes asks a short clarification
  - If the action has expired or is no longer valid, Florence refuses the commit and Hermes explains why
- Existing code reused:
  - `florence/runtime/chat.py`
  - `florence/runtime/candidate_review.py`
  - `florence/runtime/operations.py`
  - `tools/florence_household_tool.py`

### 2. Generic Acknowledgement Does Not Mutate Unrelated State
- Trigger: Florence just saved one thing, and the user replies "yep", "sounds good", or "thanks"
- User steps: User acknowledges the visible result without referencing a new action
- Visible states:
  - Florence acknowledges the acknowledgement or continues normally
  - No hidden/private/pending item is silently committed
- Failure/recovery:
  - If Hermes thinks the user might be confirming a different pending item, it asks "Do you mean X?"
- Existing code reused:
  - `florence/messaging/dm_router.py`
  - `florence/runtime/chat.py`
  - `tools/florence_household_tool.py`

### 3. Background Review Item Needs Explicit Binding Before Commit
- Trigger: Background sync produces a pending candidate and Florence sends a DM review prompt
- User steps:
  - Florence sends one explicit review prompt
  - User replies naturally
  - Hermes interprets the reply in the context of that exact pending action
  - Florence commits only if Hermes references that action explicitly
- Visible states:
  - Review prompt with one candidate preview
  - Confirm/reject/skip outcome
  - Clear saved/not-saved result
- Failure/recovery:
  - If multiple pending items exist, only the surfaced one is actionable
  - If the user asks to review later, Florence leaves it pending
- Existing code reused:
  - `florence/runtime/candidate_review.py`
  - `florence/runtime/operations.py`
  - `florence/messaging/channel_log.py`

## Ownership And Reuse Map

### Pending action
- Current owner in codebase: no first-class owner yet; implicit in review prompts and last assistant message metadata
- Reused modules:
  - `florence/messaging/channel_log.py`
  - `florence/messaging/protocol_types.py`
  - `florence/runtime/chat.py`
- True gap:
  - We need an explicit `pending_action` model with `action_id`, `action_type`, `target_scope`, `preview`, and `expires_at`
- Owner alignment required: no

### Conversation interpretation
- Current owner in codebase: Hermes via `FlorenceHouseholdChatService`
- Reused modules:
  - `florence/runtime/chat.py`
  - `run_agent.py`
- True gap:
  - Hermes is not yet the default interpreter for confirmation-like turns when Florence protocol regexes intercept them first
- Owner alignment required: no

### Durable household commit
- Current owner in codebase: Florence household tools + runtime services
- Reused modules:
  - `tools/florence_household_tool.py`
  - `florence/runtime/candidate_review.py`
  - `florence/runtime/household_manager.py`
- True gap:
  - Commit calls are not yet bound to an explicit pending action id
- Owner alignment required: no

### Review queue / imported candidates
- Current owner in codebase: `florence/runtime/candidate_review.py`
- Reused modules:
  - `florence/runtime/operations.py`
  - `florence/state/store.py`
- True gap:
  - Background pending review items are too easy to leak into generic DM flow as actionable state
- Owner alignment required: no

## Decision Log

1. Decision: Hermes should own confirmation interpretation.
   Context: Generic "yep" / "sounds good" replies are currently too easy for Florence to over-handle.
   Alternatives considered:
   - Keep regex confirmation in Florence
   - Move all confirmation reasoning into Hermes
   Rationale: Hermes should interpret natural language; Florence should only guard commits.

2. Decision: Florence should remain the hard commit gate.
   Context: State commits, privacy boundaries, and household mutations must stay reliable.
   Alternatives considered:
   - Let Hermes mutate state directly without an explicit gate
   - Require Florence-side commit authorization
   Rationale: Thin Florence still needs to enforce invariants and explicit action binding.

3. Decision: Introduce explicit pending actions.
   Context: Regex-based "armed" confirmation is too fuzzy.
   Alternatives considered:
   - Last-message text matching
   - First-class pending action metadata
   Rationale: Explicit action ids prevent unrelated state from being mutated by generic assent.

4. Decision: Only surfaced pending actions are actionable.
   Context: Hidden pending review items should not be confirmed accidentally during normal DM chat.
   Alternatives considered:
   - Keep all pending private state visible/actionable
   - Limit actionability to the single surfaced prompt
   Rationale: This matches user expectation and reduces random seeming mutations.

5. Decision: Generic acknowledgements should be non-mutating by default.
   Context: "Yep, that sounds good" after one save should not commit a different item.
   Alternatives considered:
   - Treat generic assent as broad permission
   - Require exact action binding or clarification
   Rationale: Safer and more consistent with a Hermes-first model.

## Implementation Plan

1. Add a first-class `pending_action` envelope to Florence assistant messages.
   Files:
   - `florence/messaging/protocol_types.py`
   - `florence/messaging/channel_log.py`
   - `florence/runtime/operations.py`
   - `florence/runtime/candidate_review.py`
   Done when:
   - Review prompts and other confirmable asks persist a stable `action_id`
   - The last assistant message can be inspected for a concrete pending action

2. Remove fuzzy regex confirmation as the primary review commit path.
   Files:
   - `florence/messaging/review_protocol.py`
   - `florence/messaging/dm_router.py`
   Done when:
   - Generic `yes/yep/sure` does not commit anything unless tied to one explicit pending action
   - Review queue requests still work

3. Pass the active pending action into Hermes as structured context.
   Files:
   - `florence/runtime/chat.py`
   - `tools/florence_household_tool.py`
   Done when:
   - Hermes can see the currently armed action preview and id
   - Hidden pending review items are not treated as active mutation targets during generic chat

4. Add a commit-by-action-id path.
   Files:
   - `tools/florence_household_tool.py`
   - `florence/runtime/candidate_review.py`
   - `florence/runtime/household_manager.py`
   Done when:
   - Hermes must reference the specific `action_id` to confirm/reject/skip
   - Florence refuses commits that do not bind to a valid current action

5. Tighten Florence-side Hermes instructions.
   Files:
   - `florence/runtime/chat.py`
   Done when:
   - Prompts explicitly say generic assent must not mutate unrelated state
   - Hermes asks a short clarification if more than one interpretation is possible

6. Expand auditability for action commits.
   Files:
   - `florence/runtime/operations.py`
   - `florence/runtime/household_manager.py`
   - `florence/state/store.py`
   Done when:
   - Florence records `pending_action_created`, `pending_action_committed`, `pending_action_rejected`, and `pending_action_expired` events with action ids
