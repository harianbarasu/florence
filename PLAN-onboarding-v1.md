# Florence V1 Onboarding Plan

## Core User Flows

### Flow 1: SMS Bootstrap To Secure Web Link
- Trigger: A new parent texts Florence for the first time.
- User steps:
  - Florence asks for first name.
  - Florence optionally asks for household name.
  - Florence optionally asks whether there is another adult to invite now.
  - Florence sends a secure resume-safe web link.
- Visible states:
  - Success: user gets a secure link and understands Florence continues on web.
  - Partial: user skips household name or second-adult invite and still gets the link.
  - Error: Florence cannot generate a link or persist bootstrap state.
- Recovery:
  - User can text again and Florence re-sends the current valid link.
  - Bootstrap answers can be corrected by natural language before web setup starts.
- Existing code reused:
  - SMS/webhook ingress in `florence/messaging/ingress.py`
  - onboarding session and onboarding link logic in `florence/runtime/production.py`
  - setup link routing through `florence/server.py` and `web/src/app/setup/page.tsx`

### Flow 2: Core Web Setup To First Activation
- Trigger: User opens the secure `/setup` link from SMS.
- User steps:
  - Accept welcome/consent.
  - Connect Google.
  - Classify connected calendars as planning+conflicts, conflicts-only, or ignore.
  - Fill minimum household graph: adults, children, schools, activities.
  - Select top priorities and biggest failure points.
  - Set household trust defaults.
  - Continue into first scan / activation.
- Visible states:
  - Loading: existing setup and sync status load from `/v1/web/setup`.
  - In-progress: Google connected and first scan running.
  - Partial: user has connected Google but not finished household grounding.
  - Attention needed: connection or sync error.
  - Success: Florence shows first inferred orgs/events/rules and the user can act on them.
- Recovery:
  - Every step autosaves.
  - User can leave and come back to `/setup`.
  - Google reconnect does not wipe prior answers.
- Existing code reused:
  - `GET /v1/web/setup` and `POST /v1/web/setup/profile` in `florence/server.py`
  - web data client in `web/src/lib/florence-api.ts`
  - setup data types in `web/src/lib/types.ts`
  - current setup shell in `web/src/components/setup/setup-screen.tsx`
  - Google connection and sync state in `florence/runtime/services.py`

### Flow 3: Post-Onboarding Product Home
- Trigger: User finishes activation or skips out after first useful setup.
- User steps:
  - Open web again and land on a real household surface.
  - Review confirmed household events on a Florence calendar.
  - Review pending inferred items and approve/reject them.
  - Manage connected Google accounts and settings.
- Visible states:
  - Ready: household calendar and pending review queue are visible.
  - Partial: sync still running, but review/history is visible.
  - Empty: no inferred items yet.
  - Attention needed: connection or sync issue.
- Recovery:
  - User can always revisit `Review` to clean up low-confidence items.
  - User can reconnect Google from `Connections`.
- Existing code reused:
  - Google connection panel in `web/src/components/accounts/accounts-screen.tsx`
  - settings surface in `web/src/app/settings/page.tsx`
  - imported candidate lifecycle in `florence/runtime/services.py`
  - confirmed household event model in `florence/contracts.py`
- True gap:
  - There is no dedicated `Calendar` or `Review` route yet. Those need to be added.

## Ownership And Reuse Map

| Product noun | Current owner in codebase | Reuse | True gap |
| --- | --- | --- | --- |
| SMS bootstrap state | onboarding + messaging runtime | `florence/messaging/ingress.py`, `florence/runtime/production.py` | Need explicit step machine for name / household / second-adult capture |
| Secure setup link | onboarding link service | `florence/runtime/production.py`, setup link routing | Mostly exists |
| Web setup payload | setup API and setup screen | `florence/server.py`, `web/src/lib/florence-api.ts`, `web/src/components/setup/setup-screen.tsx` | Need to reshape into tighter phase model |
| Google account connection | Google connection + sync status | `GoogleConnection` in `florence/contracts.py`, `build_google_connection_sync_status()` in `florence/runtime/services.py` | Good reuse; Google-only v1 removes multi-provider complexity |
| Calendar classification | none as a first-class surface | partial reuse of connection metadata | Need new persisted classification fields and UI |
| Household graph | household/member/profile state | `Household`, `Member`, `ChildProfile`, `HouseholdProfileItem` in `florence/contracts.py` | Good reuse; caregiver model can be deferred |
| Top priorities / pain points | household settings / manager profile | existing household settings payloads | Need explicit schema + save path |
| Trust defaults | household settings / source rules | `HouseholdSourceRule`, candidate source policy in `florence/runtime/services.py` | Need onboarding UI and stored defaults |
| First inferred items | imported candidates | `ImportedCandidate`, `CandidateReviewService` | Good reuse |
| Confirmed tracked events | household events | `HouseholdEvent` | Good reuse |
| Post-onboarding connections panel | accounts/settings web pages | `web/src/components/accounts/accounts-screen.tsx`, `web/src/app/settings/page.tsx` | Good reuse |
| Post-onboarding review home | candidate review service | backend exists | Missing first-class web route |
| Post-onboarding calendar home | household events | backend event model exists | Missing first-class web route |

Owner alignment required before coding:
- The web shell/navigation must shift from `Setup / Accounts / Settings` to `Calendar / Review / Connections` once onboarding is complete.
- Calendar classification and trust defaults must be added to persistent Florence-owned household/connection state, not invented in the web layer only.

## Decision Log

1. Decision: Florence v1 onboarding is Google-only.
   Context: The team does not want Outlook/iCloud or provider chooser complexity in the critical path.
   Alternatives considered: multi-provider setup in v1, separate email/calendar provider steps.
   Rationale: Google-only sharply reduces branching and lets onboarding focus on household value instead of integration sprawl.

2. Decision: Non-parent caregivers are out of v1 onboarding.
   Context: The immediate product is for parents/adults in the household.
   Alternatives considered: model caregivers from day one.
   Rationale: Caregiver roles add privacy and routing complexity without being necessary for the first activation loop.

3. Decision: Top priorities remain part of onboarding.
   Context: This is valuable product-learning signal and should shape activation.
   Alternatives considered: defer to later progressive prompts.
   Rationale: Priorities influence what Florence surfaces first and produce direct company learning about user demand.

4. Decision: Detailed routines move to progressive onboarding, not core setup.
   Context: Routines matter, but manual routine setup is high-friction.
   Alternatives considered: full routines screen during onboarding.
   Rationale: Florence should infer routines from Google data and ask contextual follow-ups later.

5. Decision: Core onboarding ends with activation, not with a static completion page.
   Context: The product moment is Florence showing what it found and asking for confirmation.
   Alternatives considered: end on “you are set up.”
   Rationale: Trust is earned through review and correction, not just successful OAuth.

6. Decision: Post-onboarding web home should become Calendar + Review + Connections.
   Context: After initial setup the current `Setup` surface is mostly administrative.
   Alternatives considered: keep setup wizard as the primary home.
   Rationale: Ongoing value lives in tracked events, pending review, and connection management.

7. Decision: Second-adult invite stays lightweight in the critical path.
   Context: Another adult matters, but should not block first-parent activation.
   Alternatives considered: require second adult setup before activation.
   Rationale: Invite early if convenient, but treat second-adult onboarding as a separate flow.

8. Decision: Calendar classification is required in core setup.
   Context: Personal/work/family calendars need different trust and disclosure treatment.
   Alternatives considered: infer classification automatically or defer it.
   Rationale: This is a core trust boundary and should be explicit before Florence starts surfacing logistics.

9. Decision: Notification tuning moves out of core onboarding.
   Context: Detailed notification defaults are useful but not required for the first product moment.
   Alternatives considered: dedicated notification screen in onboarding.
   Rationale: Better as post-activation or progressive prompts after the household sees value.

## Implementation Plan

### Phase A: Tighten The Onboarding State Machine
1. Replace the current loose setup progression with this v1 state model:
   - `sms_first_name`
   - `sms_household_name_optional`
   - `sms_second_adult_optional`
   - `web_consent`
   - `web_connect_google`
   - `web_classify_calendars`
   - `web_household_basics`
   - `web_top_priorities`
   - `web_trust_defaults`
   - `activation_first_scan`
   - `activation_review`
   - `ready`
2. Keep all states resumable from the same secure setup link.
3. Acceptance criteria:
   - User can leave and return at any phase.
   - Google reconnect or sync failure does not wipe prior onboarding progress.

### Phase B: SMS Bootstrap
1. Make SMS onboarding capture only:
   - first name
   - optional household name
   - optional second adult invite
   - secure setup link handoff
2. Add explicit correction handling for first/household name before web setup begins.
3. Acceptance criteria:
   - New user can go from first text to secure link in under 4 turns.

### Phase C: Reshape Core Web Setup
1. Replace the current setup screen with a tighter step sequence:
   - Welcome + consent
   - Connect Google
   - Classify calendars
   - Household basics
   - Top priorities
   - Trust defaults
2. Household basics includes:
   - current adult
   - optional second adult invite status
   - children
   - schools
   - activities
3. Top priorities includes:
   - pick up to 3 priorities
   - pick up to 2 failure points
   - optional free-text “other”
4. Trust defaults covers:
   - Florence may process connected Google data
   - household adults can receive structured logistics
   - private calendars can remain conflict-only
   - sensitive items ask first
5. Acceptance criteria:
   - Setup requires only one provider: Google.
   - No caregiver fields exist in v1 onboarding.
   - No notification-tuning screen blocks activation.

### Phase D: Activation
1. After core setup, show:
   - likely orgs
   - likely upcoming items
   - trusted-source suggestions
2. Require the user to confirm/reject at least one suggestion or explicitly skip.
3. Use top priorities to rank the activation feed.
4. Acceptance criteria:
   - User sees clear “Florence found this for your family” output.
   - No household events are silently auto-created without user confirmation in v1.

### Phase E: Post-Onboarding Product Home
1. Replace setup as the default post-onboarding destination.
2. Add or plan these routes:
   - `Calendar`
   - `Review`
   - `Connections`
3. Keep `Settings` as secondary admin/config.
4. Acceptance criteria:
   - Once `ready`, opening the web app lands on product, not the wizard.
   - `Review` shows pending candidates.
   - `Calendar` shows confirmed tracked events.

### Phase F: Progressive Onboarding
1. Move these items out of the critical path:
   - detailed routines
   - notification tuning
   - advanced trust rules
   - household calendar creation
   - second adult full source setup
2. Trigger contextual prompts over the first week:
   - “I’ve seen Lincoln School emails 4 times. Trust routine logistics from them?”
   - “Looks like swim is every Tuesday. Add it as a recurring activity?”
   - “Want a daily digest at 6pm?”
3. Acceptance criteria:
   - These prompts do not block first-use activation.
   - Accepted responses persist to existing Florence state.

### Phase G: Instrumentation
1. Track the following funnel events:
   - first text
   - first name captured
   - household shell created
   - setup link opened
   - consent accepted
   - Google connected
   - calendar classification completed
   - household basics completed
   - top priorities submitted
   - trust defaults submitted
   - first scan completed
   - first suggestion accepted
   - ready
2. Track key metrics:
   - time from first text to Google connected
   - time from first text to ready
   - time from first text to first accepted suggestion
   - sync error rate
   - calendar-classification abandonment rate
   - top-priority distribution
3. Acceptance criteria:
   - Funnel drop-off is measurable per step.
   - Activation quality can be correlated with top priorities.
