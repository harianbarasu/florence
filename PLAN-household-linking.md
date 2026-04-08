# Household Linking Plan

## Core User Flows

### Flow 1: Link A Second Parent Without Group Chat

- Flow name: Parent invites another parent into the same household
- Trigger: Jackson asks Florence to link Kendall by phone number from his private DM
- User steps:
  1. Jackson gives Kendall's phone number in his DM with Florence.
  2. Florence creates a pending household link request tied to Jackson's household and Kendall's normalized phone number.
  3. Florence tells Jackson that Kendall can join from her side and that her 1:1 thread will stay private.
  4. When Kendall next messages Florence from that phone number, Florence detects the pending request and asks if she wants to join Jackson's household here.
  5. Kendall replies yes or no.
  6. If yes, Florence links or merges based on the maturity of Kendall's current household footprint.
- Visible states:
  - Success: Kendall is linked into the same household and her DM remains private.
  - Waiting: Jackson sees that Florence is waiting on Kendall's confirmation.
  - Declined: Jackson sees that Kendall did not join yet, without any extra private detail.
  - Expired: Jackson can resend or recreate the request.
- Failure and recovery:
  - Wrong number: request never completes or Kendall declines; Jackson can replace the number.
  - Invited parent messages from a different number: Florence says it cannot match the request and asks for the invited number to message directly.
  - Ambiguous identity: Florence does not disclose existing thread state and keeps the flow pending until the exact number confirms.
- Existing code reused:
  - [resolver.py](./florence/runtime/resolver.py)
  - [entrypoints.py](./florence/runtime/entrypoints.py)
  - [contracts.py](./florence/contracts.py)
  - [store.py](./florence/state/store.py)

### Flow 2: Invited Parent Already Has A 1:1 Thread

- Flow name: Existing private DM joins an existing household
- Trigger: Kendall already has a DM with Florence, then Jackson starts a household link request for Kendall's number
- User steps:
  1. Jackson creates the request from his DM.
  2. Kendall later messages Florence in her own DM.
  3. Florence recognizes the pending request using Kendall's normalized number.
  4. Florence asks Kendall to confirm joining Jackson's household.
  5. On yes:
     - If Kendall's current household is lightweight, Florence merges it silently into Jackson's household.
     - If Kendall's current household is mature, Florence marks merge confirmation required and asks both parents to confirm.
  6. After confirmation, Florence merges the households and keeps both DMs as private channels under the merged household.
- Visible states:
  - Auto-merge complete
  - Waiting for Kendall confirmation
  - Waiting for both parents to confirm a mature merge
  - Merge complete, with follow-up cleanup/review if needed
- Failure and recovery:
  - Kendall declines: the request is closed and both households remain separate.
  - Mature merge has conflicting data: Florence merges structural identity/channel ownership, then surfaces duplicate cleanup tasks for review.
  - Source household is too complex for silent merge: Florence blocks auto-merge and stays in guided confirmation.
- Existing code reused:
  - [household_merge.py](./florence/runtime/household_merge.py)
  - [store.py](./florence/state/store.py)
  - [test_resolver.py](./tests/florence/test_resolver.py)
  - [test_entrypoints.py](./tests/florence/test_entrypoints.py)

### Flow 3: Group Chat Arrives Later

- Flow name: Attach the future SendBlue family group chat to the already-linked household
- Trigger: a SendBlue group thread arrives once group support is available
- User steps:
  1. Florence resolves the group participants.
  2. If all non-Florence participants are already known members of exactly one household, Florence attaches the thread as that household's `HOUSEHOLD_GROUP` channel.
  3. Florence uses the new group as the shared household coordination lane.
  4. If one participant is unknown or maps ambiguously, Florence does not create or merge households off the group alone; it asks the unknown participant to DM Florence.
- Visible states:
  - Group attached
  - Group unresolved because a participant is unknown
  - Group unresolved because identities map to multiple households
- Failure and recovery:
  - Unknown participant: ask them to DM Florence from their own number
  - Ambiguous participants: do not auto-merge; require explicit household linking from DMs
- Existing code reused:
  - [resolver.py](./florence/runtime/resolver.py)
  - [group_router.py](./florence/messaging/group_router.py)
  - [entrypoints.py](./florence/runtime/entrypoints.py)

## Ownership And Reuse Map

### Household

- Product noun: family / shared household
- Current owner: [contracts.py](./florence/contracts.py) `Household`, [store.py](./florence/state/store.py)
- Reused tables/services/modules:
  - `households`
  - `FlorenceStateDB`
- True gap that remains: none
- Owner alignment required: no

### Parent Identity

- Product noun: parent, second parent, linked parent
- Current owner: [contracts.py](./florence/contracts.py) `Member`, `MemberIdentity`; [resolver.py](./florence/runtime/resolver.py)
- Reused tables/services/modules:
  - `members`
  - `member_identities`
  - `FlorenceIdentityResolver`
- True gap that remains: no explicit invite/link request state
- Owner alignment required: no

### Private Parent DM

- Product noun: private 1:1 lane
- Current owner: [contracts.py](./florence/contracts.py) `ChannelType.PARENT_DM`
- Reused tables/services/modules:
  - `channels`
  - `channel_messages`
  - messaging ingress and channel log services
- True gap that remains: a protocol for pending household-link confirmation inside that DM
- Owner alignment required: no

### Shared Group Chat

- Product noun: family group lane
- Current owner: [contracts.py](./florence/contracts.py) `ChannelType.HOUSEHOLD_GROUP`
- Reused tables/services/modules:
  - `channels`
  - `FlorenceGroupRouter`
  - SendBlue/Linq entrypoints and resolver
- True gap that remains: transport support and revised attach logic once linking no longer depends on the group
- Owner alignment required: no

### Household Merge

- Product noun: merge duplicate family records
- Current owner: [household_merge.py](./florence/runtime/household_merge.py)
- Reused tables/services/modules:
  - `FlorenceHouseholdMergeService`
  - `FlorenceStateDB.merge_households(...)`
- True gap that remains:
  - classify lightweight vs mature source households
  - guided confirmation flow for mature merges
  - reconciliation pass for duplicate family content after merge
- Owner alignment required: no

### Pending Household Link Request

- Product noun: Jackson has asked Kendall to join the same household
- Current owner in codebase: none
- Reused tables/services/modules:
  - identity normalization in [resolver.py](./florence/runtime/resolver.py)
  - DM messaging surfaces in entrypoints and ingress
- True gap that remains: new persistence and orchestration is required
- Owner alignment required: no
- Migration gate:
  - A new table is justified here because there is no existing owner for pending cross-parent linking state.
  - This should not be overloaded onto onboarding sessions or channels because it is neither setup-state nor transport-state.

## Decision Log

1. Decision: The canonical family object is the `Household`, not the group thread.
   Context: The product needs to support second-parent linking before SendBlue group chat exists.
   Alternatives considered: making the group thread the defining family object.
   Rationale: The codebase already models `Household`, `Member`, and `Channel` separately, and that is the correct architecture.

2. Decision: Group chat is not required to link parents into the same household.
   Context: Kendall should be able to join Jackson's household before group-chat transport is available.
   Alternatives considered: waiting for group support before family linking.
   Rationale: Group chat is only a shared coordination surface; it should not define family identity.

3. Decision: Phone number is the v1 invite target.
   Context: Jackson and Kendall are already texting Florence on their phones.
   Alternatives considered: email or invite codes as the primary path.
   Rationale: Phone-number identity already exists in the resolver and transport stack, and it is the simplest high-confidence key for v1.

4. Decision: No verification code in v1.
   Context: The linking flow already has two-sided confirmation.
   Alternatives considered: one-time codes or links.
   Rationale: Jackson nominates the number and Kendall confirms from that same number; a code adds friction without enough additional value for the first version.

5. Decision: Florence must not disclose to Jackson whether Kendall already has a DM or standalone household.
   Context: Existing-thread existence is private.
   Alternatives considered: telling Jackson Florence found Kendall's existing thread.
   Rationale: That leaks private engagement history and creates a privacy problem.

6. Decision: Kendall-side copy can explicitly say Jackson wants to link households, but must sound human.
   Context: Kendall needs to understand what she is confirming.
   Alternatives considered: generic robotic request-state language.
   Rationale: Explicit, calm copy is clearer and avoids system-language leakage.

7. Decision: Lightweight invited households auto-merge.
   Context: The invited parent may already have a minimal DM-created standalone household.
   Alternatives considered: always requiring manual merge confirmation.
   Rationale: Lightweight households are structurally simple and should not create friction.

8. Decision: Mature invited households require both parents to confirm.
   Context: Both sides may already have saved meaningful state.
   Alternatives considered: one parent approval or silent merge.
   Rationale: Mature merges can combine Gmail mirrors, calendars, events, and family structure, so both parents should consent.

9. Decision: For mature merges, Florence should merge first and reconcile duplicates afterward.
   Context: Identity/channel unification is more important than previewing every possible duplicate.
   Alternatives considered: preview-before-merge.
   Rationale: Structural merge logic already exists; the main remaining risk is content dedupe, which can be handled post-merge.

10. Decision: Florence should not proactively cold-text Kendall in v1.
    Context: Jackson may enter the wrong number, and an unsolicited ping would disclose the household-link intent.
    Alternatives considered: immediately messaging Kendall once Jackson enters her number.
    Rationale: Safer v1 is pending request plus target-side confirmation when Kendall next messages from that number or replies in her own existing DM.

11. Decision: Future group chats should auto-attach only when all participants already map to exactly one household.
    Context: Once SendBlue group support exists, the group should be adopted smoothly.
    Alternatives considered: using the group as the primary merge signal again.
    Rationale: Explicit household-linking should remain the source of truth; group-based merge becomes only a legacy recovery path.

## Implementation Plan

### Phase 1: Add Pending Household Link State

- Add a new contract for a pending household link request in [contracts.py](./florence/contracts.py).
- Add a new table in [store.py](./florence/state/store.py), for example `household_link_requests`, with:
  - `id`
  - `target_household_id`
  - `inviting_member_id`
  - `invited_identity_kind`
  - `invited_identity_normalized_value`
  - `status`
  - `source_household_id` nullable
  - `invited_member_id` nullable
  - `requires_merge_confirmation`
  - `metadata_json`
  - timestamps and expiry
- Add store methods:
  - create pending request
  - find active request by normalized phone number
  - update request status
  - list requests for a household/member if needed
- Definition of done:
  - Florence can persist and load a pending link request keyed to a normalized phone number.

### Phase 2: Add Household Linking Service

- Add a dedicated runtime service, e.g. `florence/runtime/household_link.py`.
- Responsibilities:
  - create link request from Jackson's DM
  - classify source household as lightweight or mature
  - resolve whether a source household exists for the invited number
  - execute direct link, auto-merge, or mature-merge pending confirmation
  - produce human DM copy for Jackson and Kendall
- Reuse:
  - [resolver.py](./florence/runtime/resolver.py) for identity normalization
  - [household_merge.py](./florence/runtime/household_merge.py) for structural merge
  - [store.py](./florence/state/store.py#L675) `count_household_state_rows(...)` as the base maturity signal
- Definition of done:
  - one service owns link-request creation, status transitions, and merge execution policy.

### Phase 3: Add DM Protocol For Link Confirmation

- Add a new DM-side protocol, similar to existing review/reminder/onboarding protocols.
- Behavior:
  - if a pending link request exists for the current sender and is still awaiting Kendall's confirmation, Florence surfaces a human confirmation prompt before normal onboarding/chat
  - if Kendall replies yes/no while that prompt is armed, Florence accepts or declines the request
  - if a mature merge is required, Florence records Kendall's approval and waits for Jackson's confirmation
- Reuse:
  - existing prompt-armed metadata pattern from review/nudge protocols
  - `ChannelLog` / message metadata machinery
- Definition of done:
  - Kendall can confirm or decline from her DM without entering a code or running through full onboarding.

### Phase 4: Add Jackson-Side Link Initiation Tool/Flow

- Add a Florence tool or explicit chat action that Hermes can call from Jackson's DM, e.g. `household_request_parent_link`.
- Inputs:
  - phone number
  - optional display name label
- Outputs:
  - human reply to Jackson
  - stored link request
- Jackson-facing copy must never reveal whether Florence already knows the number.
- Definition of done:
  - Jackson can start the link flow naturally from chat.

### Phase 5: Implement Lightweight Vs Mature Classification

- Add classification logic in the new linking service using:
  - one active parent check
  - no `HOUSEHOLD_GROUP` channel
  - row counts from [store.py](./florence/state/store.py#L675)
  - targeted checks for child profiles, confirmed events, routines, profile items, and non-trivial onboarding state
- Suggested v1 policy:
  - lightweight if there is only one active parent, no group channel, and household content is limited to DM, onboarding, Google connection, and mirrors
  - mature otherwise
- Definition of done:
  - auto-merge triggers only for lightweight source households
  - mature merges require confirmation

### Phase 6: Reconciliation Queue After Mature Merge

- After a mature merge completes, run a reconciliation pass that identifies overlapping items for review.
- Start narrow in v1:
  - child profiles with similar names
  - events with similar title/time
  - profile items/preferences with overlapping labels
  - routines with overlapping title/cadence
- Use the existing review/candidate-style machinery where possible, or create a lightweight household cleanup queue.
- Definition of done:
  - Florence does not silently leave obvious duplicate family structure unaddressed after mature merges.

### Phase 7: Future Group Chat Attachment

- Update [resolver.py](./florence/runtime/resolver.py) and [entrypoints.py](./florence/runtime/entrypoints.py) so that once SendBlue group support exists:
  - if all participants resolve to one household, attach the group channel automatically
  - if a participant is unknown, do not create or merge a household off the group alone
  - if participant resolution is ambiguous across households, do not auto-merge; fall back to DM linking
- Remove or soften group-first copy such as the current ambiguous-household fallback in [entrypoints.py](./florence/runtime/entrypoints.py).
- Definition of done:
  - group chat becomes an attachment step, not the primary identity-linking step.

### Phase 8: Tests

- Add store tests for the new link-request table and CRUD.
- Add service tests for:
  - pending request creation
  - lightweight auto-merge
  - mature merge requiring both confirmations
  - decline and expiry
- Add entrypoint/ingress tests for:
  - Jackson initiating a link request
  - Kendall confirming from an existing DM
  - invited unknown parent confirming from a first DM
  - future group attach after both parents are already linked
  - privacy rule that Jackson never learns whether Kendall already had a DM
- Definition of done:
  - link flows are covered end-to-end, including privacy-sensitive copy and merge policy.

