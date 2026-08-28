Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 04, 06, 10

# Give Florence authenticated browser and computer use

## Question

How can Florence use a browser or computer like a capable assistant for sites without an API—researching, comparing, signing in with the owning adult, navigating, filling forms, and carrying one real errand through to its actual commitment point—while the durable task keeps reporting progress and can be steered or cancelled?

Port the navigation, accessibility snapshot, click/type/scroll, screenshot, session, and cleanup behavior from Hermes's `tools/browser_tool.py`, browser plugin sources, and focused browser tests. Exclude terminal and model-written-Python paths. Prove the port with one named family errand; do not stop at a public-page reader or build a generic browser-policy framework.

## Answer

Florence's existing durable private-work loop can now operate an interactive or authenticated website instead of stopping at search results. It can navigate, read a compact accessibility snapshot, use fresh element references to click/fill/select/check/press/scroll/go back, wait for the page, inspect a bounded screenshot, and return every action's current page state to the model. A parent can take over the exact same live browser session for sign-in or MFA and tell Florence to continue; the task resumes that session across checkpoints and process claims.

Browser state lives inside the existing `family_task` checkpoint. Pending action attempts are persisted before execution so a reclaimed worker observes instead of blindly repeating an ambiguous click. New or replacement sessions are released if they never reach a checkpoint, stored sessions are released after terminal settlement or cancellation, and stale workers cannot strand the session they created. This adds no second runtime, queue, registry, policy layer, credential store, or connector framework.

The focused family journey is a camp portal: Florence opens the portal, asks the parent to use its live takeover URL to sign in, resumes the same task after the parent's message, fills the child and session controls from current references, captures a screenshot, and reaches the review page. The browser operation surface is not review-only; the proof stops there to keep the assertion deterministic while subsequent tickets prove provider-confirmed booking, payment, cancellation, and receipt outcomes one workflow at a time.

### Upstream reuse

- **Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `tools/browser_tool.py`: direct TypeScript adaptation** of the compact accessibility snapshot, reference-based browser actions, bounded command execution, 500px scroll behavior, screenshots, and task-scoped `agent-browser` sessions.
- **Hermes Agent at the same commit, `agent/browser_provider.py` and `plugins/browser/browserbase/provider.py`: direct TypeScript adaptation** of Browserbase session creation/resume/release, CDP attachment, live debug/takeover URL, provider timeout handling, and the narrow retry without `keepAlive` when the account rejects it.
- **`agent-browser@0.26.0`: exact runtime dependency** rather than a Florence reimplementation of browser automation.
- Florence deliberately omits Hermes's provider registry, local-browser fallback, terminal, shell, eval, console, plugin, and generic policy machinery because none is needed for this family behavior.

### Verification

- `pnpm check` passes: Biome, every package/app typecheck, 36 default API tests with four database-gated cases skipped locally, and every build.
- The focused camp-portal narrative proves live owner handoff, persisted resume, current-ref actions, screenshot delivery followed by artifact compaction, and one terminal result.
- The existing database-gated durable-work test proves one-time cancelled-session take-and-clear, retry-attempt advancement after a claim takeover, browser-session persistence across a checkpoint, and terminal clearing. It remains skipped without `TEST_DATABASE_URL` rather than adding another test matrix.
- An exact local `agent-browser` smoke journey opened `https://example.com`, returned its accessibility snapshot and refs, closed successfully, and left no matching daemon sidecars.
- Two focused reviewer passes found no remaining concrete defect in replacement-session cleanup, ambiguous-action recovery, cancellation, stale settlement, or daemon cleanup.

The Browserbase feature remains optional at application startup. Production needs `BROWSERBASE_API_KEY`; `BROWSERBASE_PROJECT_ID` is optional. Without the API key Florence starts normally but does not advertise `browser_work` to the durable reasoner.
