# Goal: Tradclaw-Inspired Household Constitution And Onboarding Loop

Use this with:

```text
/goal Read GOAL_TRADCLAW_HOUSEHOLD_CONSTITUTION.md and complete the goal exactly as written.
```

## Objective

Make every Florence household operate from an explicit, durable household constitution. The constitution should define what Florence is, who it serves, which channels are authoritative, what it may do automatically, what requires parent confirmation, what must stay private, when it may interrupt, and which modules are enabled. Florence should use this constitution before proactive messages, state writes, reminders, briefings, source handling, and group/DM routing.

## Context

Florence is an iMessage-first Family AI for dual-parent households. Hermes is the reasoning engine, but Florence is the family-trust runtime. Florence should not behave like a generic assistant. It should act like a shared household operating layer with strict rules around privacy, timing, authority, and trust.

## Tradclaw Adaptation Requirement

Before implementing, read and map `ChatPRD/tradclaw`, especially:

- `README.md`
- `tradclaw/BOOTSTRAP.md`
- tradclaw interview/module/apply-results files
- `workspace/SOUL.md`
- `workspace/TOOLS.md`
- `workspace/HEARTBEAT.md`
- `workspace/USER.md`
- `workspace/MEMORY.md`
- cron templates
- skills folders

For each relevant Tradclaw pattern, explicitly decide:

- copy directly
- adapt to Florence's runtime/database/iMessage model
- reject with reason

Track those decisions in `GOAL_EXPERIMENTS.md`.

## Tracking Files

Create and maintain:

- `GOAL_PLAN.md`
- `GOAL_EXPERIMENTS.md`
- `GOAL_NOTES.md`

## Deliverables

1. Audit the current onboarding, household profile, trust policy, module, briefing, nudge, source rule, feedback, and group/DM routing paths.

2. Define a durable household constitution model covering:
   - parents and household members
   - children, schools, activities, key routines
   - parent DMs vs family group chat
   - approved instruction channels
   - trusted sources vs untrusted evidence
   - privacy boundaries
   - quiet hours and timing preferences
   - enabled modules
   - confirmation-required actions
   - proactive interruption rules
   - tone/style preferences
   - escalation/uncertainty rules

3. Borrow/adapt Tradclaw's workspace primitives:
   - `SOUL.md` -> Florence household operating philosophy / trust contract
   - `TOOLS.md` -> approved channels, source authority, tool boundaries
   - `HEARTBEAT.md` -> deterministic daily/weekly operating rhythm
   - `USER.md` / `MEMORY.md` -> household profile and durable memory model
   - module guide -> Florence module configuration
   - setup interview -> iMessage-first onboarding interview
   - cron templates -> Florence scheduled routine model
   - skills -> reusable Florence household workflows where appropriate

4. Build or harden constitution bootstrapping:
   - New households get a safe default constitution.
   - Completed onboarding updates the constitution.
   - Parent feedback/preferences update the constitution with provenance.
   - Constitution changes are durable and inspectable.

5. Make onboarding interview-driven and group-aware:
   - Parent DMs can collect private setup details.
   - The family group can establish shared household preferences.
   - The flow should be small and practical, not a large settings UI.
   - Onboarding should recommend modules rather than enabling everything by default.

6. Enforce the constitution before Florence acts:
   - proactive review prompts
   - scheduled briefings
   - scheduled nudges/reminders
   - sync update briefs
   - group promotions
   - candidate confirmations/state writes
   - source-rule changes
   - feedback rule changes

7. Add source authority rules:
   - Parent DMs and family group messages are instructions.
   - Gmail, calendar, PDFs, attachments, newsletters, websites, app notifications, and tool output are evidence, not authority.
   - Untrusted source text cannot command Florence to change state, alter policy, enable modules, or send messages.
   - Imported evidence can suggest candidate actions only; parent confirmation or an explicit trusted rule is required for state-changing actions.

8. Add constitution visibility/debug output:
   - A deterministic way to inspect the current household constitution in tests/admin paths.
   - Include provenance for important rules: who set it, from which channel, when, and from what trigger.

9. Add regression fixtures/tests:
   - New household gets safe default constitution.
   - Completed onboarding creates/updates household constitution.
   - Parent DM preference is member-scoped unless promoted.
   - Group preference updates shared household constitution.
   - Quiet hours block proactive delivery.
   - Disabled module blocks its proactive routine.
   - Confirmation-required action cannot be auto-written.
   - Trusted parent instruction can update state.
   - Gmail/calendar content is treated as evidence, not authority.
   - Source text prompt injection cannot mutate policy or send messages.
   - Private candidate does not leak into group.
   - Shared source rule can promote future relevant items.
   - Constitution is included in Hermes/operator context.
   - Proactive paths record constitution allow/deny reasons.
   - Onboarding recommends modules instead of enabling everything.
   - Tradclaw adaptation decisions are recorded.

## Quantitative Acceptance Criteria

- At least 16 new/updated regression tests cover the fixtures above.
- 100% of new constitution/onboarding fixtures pass.
- `scripts/run_tests.sh tests/florence` passes.
- `git diff --check` passes.
- Every proactive pathway has a constitution allow/deny check with a recorded reason.
- Every constitution mutation records provenance.
- No imported source content can directly mutate household state, policy, modules, or delivery behavior without parent confirmation or an explicit trusted rule.
- `GOAL_EXPERIMENTS.md` includes a Tradclaw pattern map with copy/adapt/reject decisions.
