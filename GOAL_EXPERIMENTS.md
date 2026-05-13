# Goal Experiments

## E1: Existing Pilot-Critical Coverage Audit
- Status: complete
- Result: much of the pilot spine already existed from the prior reliability/constitution work.

| Area | Evidence | Decision |
| --- | --- | --- |
| Linq/iMessage inbound -> route -> Hermes -> reply -> delivery | `tests/florence/test_linq_production.py::test_production_linq_dm_turn_records_full_reliability_trace` | Reuse as primary successful production trace fixture. |
| Unresolved group/missed-message debug | `tests/florence/test_linq_production.py::test_production_linq_unresolved_group_records_resolution_failure` | Reuse and add pilot trace report coverage. |
| Multiple image recipe/photo payloads | `tests/florence/test_chat_service.py::test_household_chat_service_passes_multiple_image_attachments_with_ordered_metadata` | Reuse as media/Hermes-payload fixture. |
| School calendar media extraction | `tests/florence/test_linq_media.py::test_enrich_linq_payload_with_media_text_preserves_multiline_schedule_blocks` | Reuse as school/media ingestion fixture. |
| Bring a Stuffie reminders | `tests/florence/test_runtime_services.py::test_household_manager_bring_stuffy_day_gets_morning_reminder_before_school` | Reuse as timing/state fixture. |
| Past school item suppression | `tests/florence/test_runtime_services.py::test_operations_review_nudge_skips_stale_schedule_candidate` | Reuse as stale-item fixture. |
| Already handled email | `tests/florence/test_messaging_ingress.py::test_review_prompt_then_already_handled_closes_candidate_without_hermes` | Reuse as deterministic feedback fixture. |
| Source share/ignore rules | `tests/florence/test_messaging_ingress.py::test_review_prompt_then_share_persists_source_rule_without_hermes`; `tests/florence/test_review_and_events.py::test_review_feedback_ignore_sender_creates_ignored_source_rule_and_suppresses_future_candidates` | Reuse as durable source behavior fixture. |
| Action-list directness | `tests/florence/test_chat_service.py::test_household_chat_service_operator_update_strips_action_list_invitation` | Reuse as action-first output fixture. |
| Private/group boundary | `tests/florence/test_chat_service.py::test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm` | Reuse as privacy fixture. |
| Delivery opt-out/blocked channel | `tests/florence/test_runtime_services.py::test_delivery_service_disables_sendblue_channel_after_opt_out`; `tests/florence/test_runtime_services.py::test_operations_skips_blocked_briefing_channel_without_composing` | Reuse as delivery failure/skip fixture. |
| Quiet/heartbeat behavior | `tests/florence/test_runtime_services.py::test_operations_records_heartbeat_ok_briefing_as_quiet_skip` | Reuse as stay-quiet fixture. |

## E2: Pilot Readiness Helper
- Status: complete
- Hypothesis: a small runtime helper is enough to make pilot readiness auditable without building a web UI.
- Change: added `florence/runtime/pilot_readiness.py`.
- Result: pilot spine, checklist, scenario matrix, and trace report are programmatic and testable.

## E3: Trace Report Fixtures
- Status: complete
- Added `tests/florence/test_pilot_readiness.py`.
- Results:
  - Checklist covers all product-spine entries and at least 12 pilot scenarios.
  - Successful Linq parent DM report links inbound, route, Hermes, reply, outbound attempt, and outbound sent.
  - Unresolved group report shows inbound receipt, identity/channel failure, no delivery, and explicit failure reason.

## E4: Verification
- Status: complete
- Current result:
  - `scripts/run_tests.sh tests/florence/test_pilot_readiness.py` -> 3 passed.
  - `scripts/run_tests.sh tests/florence` -> 388 passed.
  - `scripts/run_tests.sh tests/tools/test_florence_household_tool.py::test_household_apply_candidate_review_denies_non_dm_channel` -> passed.
  - `git diff --check` -> passed.
