Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 05, 06, 10

# Answer “what's on the docket?” with prioritized next actions

## Question

How can Florence use the complete 90-day source accounting and near-term calendars to identify the few unfinished or time-sensitive family items, explain why each matters, and propose or perform the next action without repeating stale reminders or dumping raw scan results?

Directly adapt the workflow content and coverage checks in Hermes's `skills/productivity/google-workspace/references/daily-brief.md`, `skills/productivity/document-to-action-items/SKILL.md`, `skills/productivity/weekly-review-planning/SKILL.md`, and `skills/productivity/product-price-monitor/SKILL.md`. Deepen Florence's existing scan, fact, monitor, and duplicate-suppression paths rather than building a new context engine.

## Answer

Florence now retains a current household docket from the complete 90-day review instead of treating the initial briefing as the only usable copy of those findings. The arrival message contains the three highest-consequence, nearest-due items and says how many lower-priority items remain. It no longer dumps every candidate into the group.

Every household-safe candidate remains on the completed private review even when it is not selected for the arrival message. Both parents' candidates are projected as one ranked household docket and supplied to every ordinary conversation alongside reminders, active or waiting family work, finite follow-ups, and Calendar offers. A parent can therefore ask “what's on the docket?” later and Florence can reconcile the retained review with current work and a near-term family-Calendar read, lead with at most three items, explain why they matter, and name the next decision or action.

Incremental personal Google reviews now reconcile that same docket. A materially changed revision replaces the matching action, an unchanged revision preserves it without sending another message, and a dismissal or provider removal removes it. A real urgency change can surface again. Grouping requires overlapping provider evidence; Florence deliberately does not condense disjoint actions from different parents merely because their anchor or copy looks similar.

Either parent can say that a supplied docket item is handled, finished, cancelled, or no longer relevant. Florence records the exact provider-stable action as completed, removes the current candidate group atomically, closes its finite monitor, and cancels its matching unclaimed Calendar offer before acknowledging it. Thanks, silence, agreement, and reactions never imply completion. Past family dates and conflicts disappear after their local calendar day, while overdue deadlines and loose ends remain until they are actually resolved.

The artificial deferred-review lifecycle is gone. Lower-ranked findings no longer become 24-hour finite monitors, real monitors keep their actual evidence-check schedule, and the scheduler no longer emits “One more thing from the review” without new evidence. Incremental batches above three findings retain every household candidate and execute any real monitor or Calendar proposal, while only the ranked three may send prose.

The document/photo contract now preserves the upstream action shape in ordinary document work: outcome, explicit or unresolved owner, explicit or unresolved due date, dependency, acceptance condition, risk, and page/section citation. It preserves may/should/must and does not invent absent fields.

No table, runtime, scheduler, queue, registry, policy layer, or generic context system was added.

### Upstream reuse

- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `skills/productivity/google-workspace/references/daily-brief.md` — directly adapted complete-source accounting with selective surfacing, consequence-and-time ranking, the schedule/conflict/preparation/deadline/follow-up ordering, and the requirement that every surfaced item have a reason.
- Same commit, `skills/productivity/google-workspace/weekly-review-planning/SKILL.md` — directly adapted unfinished-versus-waiting reconciliation, silence-is-not-completion, explicit deferral, and the small ranked current-work set.
- Same commit, `skills/productivity/google-workspace/document-to-action-items/SKILL.md` — directly adapted the outcome/owner/due/dependency/acceptance/risk/citation contract, unresolved-field handling, and modality preservation into Florence's existing document reasoning path.
- Same commit, `skills/productivity/product-price-monitor/SKILL.md` — adapted stable item identity, meaningful-state reconciliation, and unchanged scheduler suppression into the existing Google action keys, candidate state, and source-removal path. Florence intentionally did not import Hermes's `[SILENT]` behavior for inbound people, provider wrappers, local JSON state, cron runtime, or generic skill infrastructure.

### Verification

- `pnpm check` passes: lint, all workspace typechecks, 35 tests, and all builds.
- The existing database-gated household journey now proves six distinct retained findings produce three ranked briefing items, disjoint cross-parent actions are not condensed, all six remain queryable, an ordinary group “what's on the docket?” turn receives all six as structured context, an explicitly handled item is removed without deleting another action, and no delayed review-replay message exists.
- Four database-backed tests are skipped on this machine because `TEST_DATABASE_URL` is unset and the local Docker daemon is not running. Their TypeScript, SQL assertions, and surrounding journey compile; no local database execution is claimed.
- Pre-upgrade messages without urgency metadata stay conservatively deduplicated instead of being replayed; new messages distinguish urgency per action. A pre-upgrade Calendar offer receives its action key on the next authoritative staging pass.
