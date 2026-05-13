# Goal Plan: Pilot-Ready iMessage-First Family AI

## Objective
Make Florence pilot-ready for real dual-parent households through parent DMs and the family group chat. Hermes remains the reasoning engine; Florence owns household identity, shared context, timing, durable state, delivery, privacy, and follow-through.

## Acceptance Checklist
- [x] Define the pilot product spine.
- [x] Audit inbound webhook, resolution, routing, Hermes, tool context, media, state writes, scheduled jobs, proactive sends, delivery skips/failures, and traceability.
- [x] Build a pilot readiness checklist with pass/fail evidence.
- [x] Harden or verify school calendar/screenshot -> event/reminder flow.
- [x] Harden or verify recipe/photos -> grocery/action-list input flow.
- [x] Harden or verify email triage -> action list/review queue flow.
- [x] Harden or verify handled/ignore/share feedback creates durable source behavior.
- [x] Harden or verify briefing timing and concise action-first output.
- [x] Harden or verify private DM to group-safe sharing without privacy leaks.
- [x] Harden or verify blocked/opted-out delivery records explicit skip/failure.
- [x] Add pilot-grade trace/debug helper for receive -> route -> Hermes -> outcome.
- [x] Add at least 12 pilot-critical scenario fixtures with evidence references.
- [x] Ensure every pilot fixture declares trace expectations plus state/delivery expectations where applicable.
- [x] `scripts/run_tests.sh tests/florence` passes.
- [x] Relevant tool/harness tests pass.
- [x] `git diff --check` passes.
- [x] Completion audit maps every explicit goal requirement to evidence.

## Pilot Product Spine
- Parent DM: private parent lane for setup, review, and parent-specific context.
- Family group chat: shared household operating lane for group-safe logistics.
- Household onboarding: practical setup for parents, children, schools, routines, and data links.
- Google OAuth/data connection: calendar/Gmail connection and initial sync state.
- School/email/calendar ingestion: evidence intake from school, calendar, email, media, and attachments.
- Calendar/reminder creation: durable events, tasks, nudges, and reminder scheduling.
- Daily/weekly briefings: morning, pickup, evening, meal, school, and weekly rhythms.
- Source review rules: review queue and share/private/ignore source behavior.
- Delivery reliability: sent, skipped, failed, opted-out, and blocked-channel paths.
- Admin/debug visibility: traceable receive-route-Hermes-tool-state-schedule-delivery path.

## Work Performed
1. Audited existing reliability, media, privacy, reminder, source-rule, briefing, and delivery coverage.
2. Added `florence/runtime/pilot_readiness.py` with:
   - pilot product spine,
   - pilot scenario matrix,
   - pilot readiness checklist,
   - operator trace report builder.
3. Added `tests/florence/test_pilot_readiness.py` covering:
   - checklist/spine/scenario coverage,
   - successful inbound Linq DM through delivery trace,
   - unresolved group missed-message trace.

## Verification
1. Full Florence suite: `scripts/run_tests.sh tests/florence` -> 388 passed.
2. Relevant tool/harness test: `scripts/run_tests.sh tests/tools/test_florence_household_tool.py::test_household_apply_candidate_review_denies_non_dm_channel` -> passed.
3. Diff check: `git diff --check` -> passed.
4. Completion audit: recorded in `GOAL_NOTES.md`.
