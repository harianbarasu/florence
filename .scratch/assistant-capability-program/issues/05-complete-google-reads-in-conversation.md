Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 04

# Complete Google reads in conversation

## Question

How can a parent ask Florence to inspect relevant Gmail attachments and any or all personal calendars in ordinary conversation, using the provider behavior Florence already has, without new OAuth scope, attachment retention, a second Google runtime, or private-source leakage?

Adapt the output contracts and coverage behavior from Hermes's `skills/productivity/google-workspace/scripts/google_api.py`, `skills/productivity/google-workspace/references/daily-brief.md`, and `tests/skills/test_google_workspace_api.py`, but deepen Florence's existing Google adapter and background attachment/all-calendar paths rather than importing Hermes credentials or its Python CLI.

## Answer

The family can now ask Florence to search the current adult's Gmail, open a supported PDF/image attachment from the result, list every Calendar readable in the current conversation, and read the primary, all, or selected Calendars through app-scoped references.

Private conversation uses that adult's personal Google connection and excludes the Florence-created Family Calendar. The family group can read only the exact Family Calendar, trying either adult's valid shared-calendar credential before reporting it unavailable. Calendar results preserve per-calendar status, access role, time zone, all-day/timed shape, busy/tentative meaning, event references, total counts, and honest partial/truncated coverage.

The background first pass remains a different complete accounting path: it pages every retained received Gmail message from the prior 90 days and every readable personal Calendar in the full review window. Free/busy-only Calendars are explicitly accounted for without pretending their events were read. Foreground Gmail search is focused and model-authored; the complete 90-day baseline is uncapped and does not dump all source material into chat.

Supported attachment bytes are fetched only after the model selects an attachment reference returned by Gmail search, verified against the source metadata, and passed ephemerally into the next model turn. Raw provider IDs are not exposed as model-authored account or Calendar selectors.

### Upstream reuse

- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` — adapted port of Gmail query/result completeness, attachment-reference behavior, Calendar-list/window output shape, pagination expectations, and daily-brief coverage from `skills/productivity/google-workspace/scripts/google_api.py`, `skills/productivity/google-workspace/references/daily-brief.md`, and `tests/skills/test_google_workspace_api.py`.
- Florence keeps its existing TypeScript Google adapter, OAuth credentials, PostgreSQL scan cursors, household Calendar identity, and Linq conversation path instead of importing Hermes's Python CLI or credential runtime.

### Scope correction

The implementation originally accumulated generalized stale-delivery replay, universal source/fact dependencies, per-bubble evidence envelopes, isolated Gmail planning, and forced monitor-review machinery. Those additions were removed because they did not unlock this family behavior. The small Pi/Hermes tool runner, real work cues, concrete private-versus-family Calendar boundary, attachment reads, and complete Calendar/Gmail coverage remain.

### Verification

The Google and database packages build, the API typechecks, and the focused reasoner tool-loop tests pass. PostgreSQL integration cases remain conditional on `TEST_DATABASE_URL`.
