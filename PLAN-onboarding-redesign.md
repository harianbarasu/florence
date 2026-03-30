# Florence Onboarding Redesign

## Decision Log

### 1. Wizard-based onboarding, not a dashboard
- **Decision**: Replace the current multi-tab dashboard (Setup/Accounts/Settings) with a single-page wizard that shows one thing at a time based on backend state.
- **Context**: Florence is an iMessage-first product. Parents get a link from the bot, land on this page, and need to get through setup fast — possibly on their phone. A dashboard with tabs, cards, and stat grids is the wrong metaphor.
- **Alternatives considered**: Simplified dashboard with fewer cards; sidebar layout like Rivermill; top-nav with streamlined pages.
- **Rationale**: One-thing-at-a-time reduces cognitive load and matches the linear nature of the onboarding flow.

### 2. State-driven flow from backend, no client-side step routing
- **Decision**: Single `/setup` URL. The page reads `setup.phase` and boolean flags (`googleConnected`, `initialSyncComplete`, `requiredProfileComplete`, `readyForChat`) from the backend and renders the appropriate screen. No step numbers in the URL, no back button, no manual navigation.
- **Context**: The backend already tracks exactly where the user is. Duplicating that state client-side adds complexity and desync risk.
- **Alternatives considered**: Multi-route wizard (`/setup/connect`, `/setup/kids`, etc.); client-side step state.
- **Rationale**: If the user closes the tab and comes back, the page picks up where they left off automatically. Zero state to manage.

### 3. Accounts & Settings hidden during onboarding
- **Decision**: During onboarding (`readyForChat === false`), the user sees only the wizard. No nav tabs, no links to Accounts or Settings. After onboarding completes, those pages become accessible.
- **Context**: Accounts and Settings are needed long-term (add/remove Google accounts, billing later), but they're noise during first-time setup.
- **Alternatives considered**: Kill the pages entirely; keep them visible but de-emphasized.
- **Rationale**: Clean separation between onboarding mode and management mode. Post-onboarding layout is deferred — not part of this redesign.

### 4. Centered, minimal visual design
- **Decision**: Each wizard screen is centered content with ~480px max-width. Florence logo at top, content in the middle, one clear CTA. No nav bar, no sidebar, no progress indicator, no chrome. Mobile-first since entry is from iMessage.
- **Context**: Parents are likely on their phone, mid-conversation with Florence. The page should feel like a quick form, not an app they need to learn.
- **Alternatives considered**: Stepper/progress dots; top nav bar with branding; wider layout with side panels.
- **Rationale**: With only ~5 screens, a progress indicator is heavier than the flow itself. The content speaks for itself.

### 5. Sync waiting: non-blocking with auto-advance
- **Decision**: Show a centered spinner with "Florence is scanning your inbox and calendar..." and a note: "This can take a few minutes. We'll text you in iMessage when it's ready — feel free to close this page." If the user stays, auto-advance when `initialSyncComplete` flips to true. On `attention_needed`, show an error message with a Retry button.
- **Context**: Sync duration is unpredictable. Could be seconds or minutes.
- **Alternatives considered**: Blocking spinner with no escape; skip sync waiting entirely and let iMessage handle it.
- **Rationale**: Covers both fast and slow syncs. User never feels stuck.

### 6. Profile split into one field per screen
- **Decision**: Break the household profile into individual screens:
  1. "What should Florence call you?" — prefilled from Google auth, editable
  2. "Who are your kids?" — at least one required, add more button
  3. "What schools or daycares?" — optional with skip, suggestions from sync shown as tappable chips
  4. "Any recurring activities?" — optional with skip, suggestions from sync shown as tappable chips
- **Context**: "One thing at a time" principle. On mobile, a single long form with textareas is overwhelming.
- **Alternatives considered**: All fields on one scrollable page; two screens (name+kids, schools+activities).
- **Rationale**: Each screen has one clear question and one clear action. Suggestions become the primary UI when available, reducing typing on mobile.

### 7. Validation rules
- **Decision**: Kids names required (at least one). Schools and activities are optional — each screen gets a "Skip" link alongside the "Next" button.
- **Context**: Not every family has recurring activities. Some families homeschool.
- **Alternatives considered**: All fields required (current behavior); everything optional.
- **Rationale**: Kids are the minimum Florence needs to anchor on. Schools and activities improve matching but aren't blocking.

### 8. Generic placeholder text
- **Decision**: Remove real children's names (Theo, Violet) from placeholder/sample text. Use generic examples.
- **Context**: Current placeholders reference actual kids.
- **Rationale**: Privacy.

---

## Implementation Plan

### Step 1: Create the wizard screen components
**Files**: New file `web/src/components/setup/onboarding-wizard.tsx`

Create a single client component that:
- Takes the `FlorenceSetupResponse` data and renders the correct screen based on state
- Screens (each a simple function component or section within the wizard):
  1. **ConnectGoogleScreen** — auto-redirect logic (existing), fallback button if blocked
  2. **SyncWaitingScreen** — spinner, message, "feel free to close" note
  3. **SyncErrorScreen** — error message + retry button (when `attention_needed`)
  4. **ParentNameScreen** — single input prefilled from `member.displayName` or Google auth name, "Next" button
  5. **KidsScreen** — add kids (name + optional details), "Add another" button, "Next" button (requires ≥1)
  6. **SchoolsScreen** — suggestion chips (if available) + manual input, "Next" and "Skip" buttons
  7. **ActivitiesScreen** — suggestion chips (if available) + manual input, "Finish" and "Skip" buttons
  8. **DoneScreen** — "You're all set. Go back to iMessage." with Florence branding

Screen routing logic (no client state needed):
```
if loading → loading spinner
if error → error screen
if !googleConnected → ConnectGoogleScreen
if !initialSyncComplete && phase === "attention_needed" → SyncErrorScreen
if !initialSyncComplete → SyncWaitingScreen
if !requiredProfileComplete → profile sub-steps (ParentName → Kids → Schools → Activities)
if readyForChat → DoneScreen
```

For the profile sub-steps, use local state to track which profile screen to show (since the backend only knows "profile incomplete", not which field they're on).

**Acceptance criteria**: Each screen renders in isolation with one clear CTA. No cards-within-cards, no stat grids, no side panels.

### Step 2: Create the onboarding layout wrapper
**Files**: Modify `web/src/components/app-shell.tsx` or create a new `web/src/components/onboarding-layout.tsx`

Simple centered layout:
- Florence logo (Sparkles icon + "Florence" text) centered at top
- Content area: `max-w-md mx-auto` (~480px), centered vertically with generous padding
- No nav bar, no tabs, no header
- Clean white/light background

**Acceptance criteria**: Full viewport, centered content, no navigation chrome. Looks good on mobile (375px) and desktop.

### Step 3: Update the setup page to use the wizard
**Files**: `web/src/app/setup/page.tsx`

- If not authenticated → show Google sign-in (existing `GoogleSignInCard`)
- If authenticated → render the onboarding layout with the wizard component
- If `readyForChat` → either show the done screen or redirect to a management view (for now, show done screen)

**Acceptance criteria**: `/setup` shows the right wizard screen based on backend state. No reference to the old dashboard layout during onboarding.

### Step 4: Conditionally show management mode
**Files**: `web/src/components/app-shell.tsx`, `web/src/app/accounts/page.tsx`, `web/src/app/settings/page.tsx`

- Keep the existing AppShell with nav tabs for Accounts and Settings
- These pages are only accessible after onboarding is complete
- The setup page post-completion could link to them or show a simple "manage your account" section
- Defer the full management mode redesign — current AppShell is fine for now

**Acceptance criteria**: Accounts and Settings pages still work. They're just not visible during onboarding.

### Step 5: Clean up placeholder text
**Files**: All setup components

- Replace "Theo", "Violet", "Wish Community School", "Young Minds Preschool", "Theo baseball", "Violet dance", "Both - Musical Beginnings" with generic examples
- Use names like "Alex", "Sam" and schools like "Westlake Elementary"

**Acceptance criteria**: No real children's names in the codebase.

### Dependencies
- Step 1 and Step 2 can be done in parallel
- Step 3 depends on Steps 1 and 2
- Step 4 depends on Step 3
- Step 5 is independent, can be done anytime
