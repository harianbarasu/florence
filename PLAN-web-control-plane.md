# Florence Web Control Plane Plan

## Core User Flows

### 1. First Parent Onboarding
- Trigger: A parent asks Florence for setup in iMessage and opens the signed onboarding link.
- User steps:
  1. Open signed link from Florence DM.
  2. Sign in with Google in the Next app.
  3. Consent to Gmail + Calendar for the same Google account.
  4. Watch phased initial sync progress.
  5. Review lightweight sync preview and suggestion chips.
  6. Enter required household grounding: kids, schools/daycares, activities.
  7. Finish setup and return to iMessage.
- Visible states:
  - Loading: token validation, Google redirect, sync phases.
  - Partial: signed in but Google data consent incomplete.
  - Partial: Google connected, sync still running, safe to leave.
  - Success: first sync complete and required grounding submitted.
  - Error: invalid/expired token, denied consent, sync failure.
- Recovery:
  - Retry consent from Setup.
  - Leave during slow sync; Florence DMs when ready.
  - Ask Florence in chat for a fresh link if the token is invalid/expired.
- Existing code reused:
  - [florence/runtime/onboarding_links.py](/Users/harianbarasu/Projects/florence/florence/runtime/onboarding_links.py)
  - [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py)
  - [florence/runtime/production.py](/Users/harianbarasu/Projects/florence/florence/runtime/production.py)
  - [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py)

### 2. Add Another Google Account
- Trigger: A parent opens `Accounts` and clicks `Add another Google account`.
- User steps:
  1. Open `Accounts`.
  2. Start secondary Google consent flow.
  3. Return to `Accounts`.
  4. Watch per-account sync state progress.
- Visible states:
  - Loading: redirecting to Google, sync starting.
  - Partial: account connected, initial sync running.
  - Success: account ready.
  - Error: consent failure, disconnected account, attention needed.
- Recovery:
  - Retry consent from `Accounts`.
  - Disconnect a broken account and reconnect later.
  - Florence remains usable in iMessage because only the first account blocks setup readiness.
- Existing code reused:
  - [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py)
  - [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py)

### 3. Later Settings / Billing Re-entry
- Trigger: A parent opens a saved Florence bookmark later or asks Florence for a settings link in chat.
- User steps:
  1. Open saved bookmark or fresh Florence DM link.
  2. If session is valid, land in `Accounts` or `Settings`.
  3. If session expired, sign in with Google again.
  4. Next resolves the signed-in Google identity back to the correct Florence member.
- Visible states:
  - Loading: session check and member resolution.
  - Success: lands directly on requested page.
  - Error: signed-in Google account is not linked to any Florence member.
- Recovery:
  - Ask Florence in chat for a fresh control-plane link.
  - Sign in with the Google account already linked to Florence.
  - Never create a new household silently from a stray web sign-in.
- Existing code reused:
  - [florence/runtime/onboarding_links.py](/Users/harianbarasu/Projects/florence/florence/runtime/onboarding_links.py)
  - [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py)
  - [florence/contracts.py](/Users/harianbarasu/Projects/florence/florence/contracts.py)

## Ownership And Reuse Map

| Product noun | Current owner in codebase | Reused tables/services/modules | True gap remaining | Owner alignment required |
|---|---|---|---|---|
| Signed onboarding link | Python backend | [florence/runtime/onboarding_links.py](/Users/harianbarasu/Projects/florence/florence/runtime/onboarding_links.py) | Expose JSON-friendly validation/binding state to Next | No |
| Onboarding session | Python backend | [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py), [florence/onboarding/state.py](/Users/harianbarasu/Projects/florence/florence/onboarding/state.py), [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py) | Reorder web-facing setup sequence to Google-first | No |
| Household, member, child, profile items | Python backend | [florence/contracts.py](/Users/harianbarasu/Projects/florence/florence/contracts.py), [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py) | Add JSON APIs that save kids/schools/activities directly | No |
| Google account connection | Python backend | [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py), `google_connections` in [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py) | Expose per-account sync state and start/add/disconnect APIs | No |
| Web session auth | Next app | Rivermill [auth.ts](/Users/harianbarasu/Projects/Rivermill/web/src/lib/auth.ts), [[...nextauth]/route.ts](/Users/harianbarasu/Projects/Rivermill/web/src/app/api/auth/[...nextauth]/route.ts), [providers.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/app/providers.tsx) | Florence-specific Google-only auth configuration and session-to-member resolution | No |
| Web-to-member binding | Existing Google connection email | [GoogleConnection](/Users/harianbarasu/Projects/florence/florence/contracts.py), `google_connections.email` in [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py) | No new table; implement lookup path in web API | No |
| Setup UI shell | Next app | Rivermill [layout.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/app/layout.tsx), [app-frame.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/components/app-frame.tsx), [app-sidebar.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/components/app-sidebar.tsx), [top-nav.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/components/top-nav.tsx) | Florence-specific nav/content and lighter control-plane IA | No |
| Onboarding components | Next app | old-florence [stepper.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/stepper.tsx), [connect-google-step.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/connect-google-step.tsx), [create-household-step.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/create-household-step.tsx), [add-members-step.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/add-members-step.tsx) | Rework copy and fields for Google-first sync-driven setup | No |
| Readiness/sync status | Python backend | [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py), Google sync worker | Make status first-class and JSON-readable for Next | No |

## Decision Log

1. Decision: Florence remains chat-first, with web as onboarding and control plane only.
   Context: The core product stays in iMessage; web should not become a second primary surface.
   Alternatives considered: app-first product, full web dashboard, permanent web inbox.
   Rationale: Families should use Florence in chat; web only handles setup, accounts, and later billing/settings.

2. Decision: First web entry comes from a signed link sent by Florence in DM.
   Context: Setup begins in chat and should bind to the right household/member without standalone signup complexity.
   Alternatives considered: standalone web-first signup, manual account linking.
   Rationale: The DM link is the cleanest bootstrap and reuses existing onboarding-link ownership.

3. Decision: Persistent re-entry uses Google web sessions, not repeated DM links.
   Context: Users need bookmarks, later settings access, and eventually billing without re-requesting a link.
   Alternatives considered: chat-link-only access, custom email auth.
   Rationale: Bookmark/session re-entry is normal UX; Florence can still provide a fresh link on request.

4. Decision: Web auth is Google-only for V1.
   Context: Google is already required for product value.
   Alternatives considered: Apple + Google, email/password, custom auth.
   Rationale: Keeps V1 simpler and aligns identity with the first synced account.

5. Decision: The first signed-in Google account is also the first Gmail/Calendar-connected account.
   Context: First-run UX should feel like one Google flow.
   Alternatives considered: sign in with one account, sync another immediately.
   Rationale: Prevents identity/data mismatch in the initial setup path.

6. Decision: Setup order becomes Google-first, then sync, then family grounding.
   Context: Matching works better once Florence has seen inbox/calendar hints.
   Alternatives considered: collect family info first, then connect Google.
   Rationale: Sync-derived hints improve the quality of school/activity capture.

7. Decision: Onboarding blocks on the first account’s initial sync, but not on later accounts.
   Context: Florence should not claim readiness before it has real source data, but later account adds should not stall product use.
   Alternatives considered: no blocking, block on every account.
   Rationale: This is the cleanest readiness rule for V1.

8. Decision: Setup form uses freeform inputs plus sync-derived suggestion chips/cards.
   Context: Sync can suggest likely schools/activities/contacts, but should not silently invent household grounding.
   Alternatives considered: freeform only, silent autofill from sync.
   Rationale: Freeform stays authoritative while still benefiting from sync.

9. Decision: Required setup data is kids, schools/daycares, and recurring activities.
   Context: These fields directly improve matching and shared family context.
   Alternatives considered: collect preferences and ops details in the core path.
   Rationale: Preferences are useful but should not slow the first real setup loop.

10. Decision: Next is presentation/auth; Python remains the only owner of readiness, sync status, suggestions, and profile truth.
    Context: Avoid duplicating onboarding state logic in two stacks.
    Alternatives considered: compute readiness in Next, split logic between stacks.
    Rationale: The backend already owns product state and should remain canonical.

11. Decision: Reuse Rivermill for infra and shell, old-florence for onboarding/product patterns.
    Context: Rivermill has the strongest modern Next/Auth.js/shadcn base; old-florence has more relevant Florence onboarding components.
    Alternatives considered: greenfield Next app, port only old-florence, port only Rivermill.
    Rationale: This is the highest leverage reuse split.

12. Decision: Use a separate Next app deployment that talks to Florence over JSON.
    Context: The Python server currently owns backend logic but is not the right place to host a modern TS UI.
    Alternatives considered: embed TS UI into Python server, continue with server-rendered HTML.
    Rationale: Keeps responsibilities clean and matches the Rivermill base.

13. Decision: Web re-entry binds to a Florence member through existing `google_connections.email`, with no new Python auth-binding table in V1.
    Context: Persistent member ownership for connected Google accounts already exists.
    Alternatives considered: add a new auth-user table in Python, invent separate web-member binding storage.
    Rationale: Existing Google connection records already provide the canonical mapping needed for V1.

14. Decision: Slow sync should degrade into a safe waiting state and Florence should DM when ready.
    Context: Initial sync may take long enough that users leave the page.
    Alternatives considered: indefinite blocking spinner, immediate completion without sync.
    Rationale: Better UX without weakening readiness guarantees.

15. Decision: Expired sessions recover through Google re-sign-in; unmatched identities never create new households silently.
    Context: Users will return later via bookmarks.
    Alternatives considered: silent household creation, chat-only recovery.
    Rationale: Prevents account confusion and keeps household ownership explicit.

## Implementation Plan

### Phase 1: Backend JSON API and readiness model
1. Add web-facing JSON endpoints to the Python backend.
   Key files:
   - [florence/server.py](/Users/harianbarasu/Projects/florence/florence/server.py)
   - [florence/runtime/production.py](/Users/harianbarasu/Projects/florence/florence/runtime/production.py)
   - [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py)
   Done when:
   - all agreed `/v1/web/*` routes exist
   - routes return domain state instead of HTML
   - route auth assumptions are documented

2. Introduce explicit setup/readiness projection.
   Key files:
   - [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py)
   - [florence/state/store.py](/Users/harianbarasu/Projects/florence/florence/state/store.py)
   Done when:
   - backend can report first-account sync phase/status
   - backend can report missing required grounding fields
   - backend can return sync-derived suggestions and lightweight preview data

3. Rework onboarding sequencing for web.
   Key files:
   - [florence/onboarding/state.py](/Users/harianbarasu/Projects/florence/florence/onboarding/state.py)
   - [florence/runtime/services.py](/Users/harianbarasu/Projects/florence/florence/runtime/services.py)
   Done when:
   - web flow is Google-first
   - grounding is submitted after sync begins/completes
   - chat copy and readiness rules match the new sequence

### Phase 2: Next app scaffold from Rivermill
4. Create `web/` in this repo from Rivermill’s base.
   Key files/components to port first:
   - [../Rivermill/web/src/lib/auth.ts](/Users/harianbarasu/Projects/Rivermill/web/src/lib/auth.ts)
   - [../Rivermill/web/src/app/api/auth/[...nextauth]/route.ts](/Users/harianbarasu/Projects/Rivermill/web/src/app/api/auth/[...nextauth]/route.ts)
   - [../Rivermill/web/src/app/providers.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/app/providers.tsx)
   - [../Rivermill/web/src/app/layout.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/app/layout.tsx)
   - [../Rivermill/web/src/components/app-frame.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/components/app-frame.tsx)
   - [../Rivermill/web/src/components/app-sidebar.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/components/app-sidebar.tsx)
   - [../Rivermill/web/src/components/top-nav.tsx](/Users/harianbarasu/Projects/Rivermill/web/src/components/top-nav.tsx)
   Done when:
   - Next app boots independently
   - Google auth works
   - shell/nav renders on desktop and mobile web

5. Florence-brand the shell and shrink IA to Setup / Accounts / Settings.
   Key files:
   - new Florence nav/layout components in `web/`
   Done when:
   - Rivermill role-heavy nav is removed
   - Florence control-plane routes exist
   - mobile web remains usable

### Phase 3: Florence onboarding UI
6. Port and adapt old-florence onboarding components.
   Key files/components:
   - [../old-florence/web/src/components/product/stepper.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/stepper.tsx)
   - [../old-florence/web/src/components/product/connect-google-step.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/connect-google-step.tsx)
   - [../old-florence/web/src/components/product/create-household-step.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/create-household-step.tsx)
   - [../old-florence/web/src/components/product/add-members-step.tsx](/Users/harianbarasu/Projects/old-florence/web/src/components/product/add-members-step.tsx)
   Done when:
   - `Setup` shows phased sync progress
   - `Setup` shows lightweight preview
   - kids/schools/activities form supports suggestion chips

7. Implement Accounts and Settings pages.
   Key files:
   - new `web/` route components
   - JSON client based on [../old-florence/web/src/lib/api-client.ts](/Users/harianbarasu/Projects/old-florence/web/src/lib/api-client.ts)
   Done when:
   - additional account connect/disconnect works
   - per-account sync states render
   - minimal settings render and save

### Phase 4: Binding, recovery, and product handoff
8. Implement member binding from signed DM link to Google-authenticated web session.
   Key files:
   - Next auth callbacks in `web/`
   - new Python lookup API using `google_connections.email`
   Done when:
   - first visit binds correctly
   - later bookmark re-entry resolves to the same member
   - unmatched sessions show explicit recovery UI

9. Wire iMessage handoff and recovery links.
   Key files:
   - [florence/messaging/ingress.py](/Users/harianbarasu/Projects/florence/florence/messaging/ingress.py)
   - [florence/runtime/onboarding_links.py](/Users/harianbarasu/Projects/florence/florence/runtime/onboarding_links.py)
   Done when:
   - Florence can send setup/settings links on request
   - Florence DMs when sync completes after a leave-safe waiting state

### Phase 5: Verification
10. Add backend and web tests for all three core flows.
   Backend:
   - setup/readiness projection tests
   - Google add-account/disconnect tests
   - expired session/member resolution tests
   Frontend:
   - onboarding happy path
   - consent failure path
   - slow sync leave-and-return path
   - bookmark/session recovery path
   Done when:
   - core flows can be storyboarded and exercised end to end
   - no step requires manual backend mutation to recover
