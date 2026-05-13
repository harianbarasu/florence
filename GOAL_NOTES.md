# Goal Notes

## Product Principles
- Florence is iMessage-first: parent DMs and the family group are the product, not a web dashboard.
- Hermes reasons; Florence decides what is allowed, private, shared, timely, durable, and worth interrupting for.
- Pilot readiness means a real operator can see what happened without guessing.
- Do not overfit Jackson's household preferences into global defaults.
- Prefer boring deterministic gates around Hermes.

## Pilot Readiness Checklist

| Check | Status | Evidence |
| --- | --- | --- |
| Can a new dual-parent household onboard? | pass | `tests/florence/test_entrypoints.py::test_entrypoints_group_activation_adds_private_dm_hint_for_unlinked_parent` |
| Can both parents share context? | pass | `tests/florence/test_entrypoints.py::test_entrypoints_second_parent_dm_links_to_existing_household_after_group_activation` |
| Can Florence distinguish group vs private DM context? | pass | `tests/florence/test_chat_service.py::test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm` |
| Can Florence ingest school/calendar/email examples? | pass | `tests/florence/test_linq_media.py::test_enrich_linq_payload_with_media_text_preserves_multiline_schedule_blocks`; `tests/florence/test_google_sync.py::test_google_sync_preserves_source_provenance_and_temporal_evidence` |
| Can Florence create durable events/reminders/tasks? | pass | `tests/florence/test_chat_service.py::test_household_chat_service_allows_added_claim_when_backing_nudge_exists` |
| Can Florence send reminders at the right local time? | pass | `tests/florence/test_runtime_services.py::test_household_manager_bring_stuffy_day_gets_morning_reminder_before_school` |
| Can Florence stay quiet when nothing matters? | pass | `tests/florence/test_runtime_services.py::test_operations_records_heartbeat_ok_briefing_as_quiet_skip` |
| Can Florence explain what happened when something fails? | pass | `tests/florence/test_linq_production.py::test_production_linq_unresolved_group_records_resolution_failure`; `tests/florence/test_runtime_services.py::test_delivery_service_disables_sendblue_channel_after_opt_out` |
| Can an operator debug a missed message in under 5 minutes? | pass | `tests/florence/test_pilot_readiness.py::test_pilot_trace_report_links_inbound_to_delivery_outcome`; `tests/florence/test_pilot_readiness.py::test_pilot_trace_report_explains_unresolved_group_miss` |

## Pilot Scenario Matrix

| Scenario | Required Assertions | Evidence |
| --- | --- | --- |
| Linq parent DM full trace | response, turn record, trace, delivery | `tests/florence/test_linq_production.py::test_production_linq_dm_turn_records_full_reliability_trace` |
| Unresolved group debug | trace, failure reason, no delivery | `tests/florence/test_linq_production.py::test_production_linq_unresolved_group_records_resolution_failure` |
| Multiple recipe images | trace, media payload, all images present | `tests/florence/test_chat_service.py::test_household_chat_service_passes_multiple_image_attachments_with_ordered_metadata` |
| Young Minds school calendar media | trace, media payload, school calendar text | `tests/florence/test_linq_media.py::test_enrich_linq_payload_with_media_text_preserves_multiline_schedule_blocks` |
| Stuffie action integrity | trace, state write, scheduled work, success backed by state | `tests/florence/test_chat_service.py::test_household_chat_service_allows_added_claim_when_backing_nudge_exists` |
| Morning-of school reminder | trace, state write, scheduled work, local time | `tests/florence/test_runtime_services.py::test_household_manager_bring_stuffy_day_gets_morning_reminder_before_school` |
| Past item suppressed | trace, no state write, no delivery | `tests/florence/test_runtime_services.py::test_operations_review_nudge_skips_stale_schedule_candidate` |
| Already handled email | trace, state write, no event created, response | `tests/florence/test_messaging_ingress.py::test_review_prompt_then_already_handled_closes_candidate_without_hermes` |
| Source share/ignore rules | trace, state write, source rule, provenance | `tests/florence/test_messaging_ingress.py::test_review_prompt_then_share_persists_source_rule_without_hermes`; `tests/florence/test_review_and_events.py::test_review_feedback_ignore_sender_creates_ignored_source_rule_and_suppresses_future_candidates` |
| Direct action list | trace, response policy, concise output | `tests/florence/test_chat_service.py::test_household_chat_service_operator_update_strips_action_list_invitation` |
| Private context guard | trace, privacy, group-safe context | `tests/florence/test_chat_service.py::test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm` |
| Blocked delivery skip | delivery skip, failure reason, trace | `tests/florence/test_runtime_services.py::test_delivery_service_disables_sendblue_channel_after_opt_out`; `tests/florence/test_runtime_services.py::test_operations_skips_blocked_briefing_channel_without_composing` |

## Completion Audit Requirements
- Map every explicit objective requirement to code/test/docs evidence.
- Do not rely on the checklist alone; verify the referenced tests pass.
- Confirm every relevant trace/debug helper returns real persisted events/turn records.
- Run `scripts/run_tests.sh tests/florence`.
- Run relevant harness/tool tests.
- Run `git diff --check`.

## Completion Audit

| Requirement | Evidence |
| --- | --- |
| Pilot product spine defined | `GOAL_PLAN.md` product spine; `florence/runtime/pilot_readiness.py::PILOT_PRODUCT_SPINE`; `tests/florence/test_pilot_readiness.py::test_pilot_readiness_checklist_covers_spine_and_scenarios` |
| Pilot-critical path audit completed | `GOAL_EXPERIMENTS.md` E1 maps inbound, routing, Hermes, media, state writes, scheduled jobs, proactive sends, delivery, and traceability to concrete tests. |
| Pilot readiness checklist exists with pass/fail evidence | `GOAL_NOTES.md` checklist; `build_pilot_readiness_checklist`; checklist test asserts pass statuses and evidence. |
| School calendar/screenshot -> events/reminders | `test_enrich_linq_payload_with_media_text_preserves_multiline_schedule_blocks`; `test_household_manager_bring_stuffy_day_gets_morning_reminder_before_school` |
| Recipe/photos -> grocery/action-list input | `test_household_chat_service_passes_multiple_image_attachments_with_ordered_metadata`; action-first prompt fixtures. |
| Email triage -> action list/review queue | `test_production_linq_dm_turn_records_full_reliability_trace`; review/source tests in scenario matrix. |
| Handled/ignore/share feedback -> durable behavior | `test_review_prompt_then_already_handled_closes_candidate_without_hermes`; `test_review_prompt_then_share_persists_source_rule_without_hermes`; `test_review_feedback_ignore_sender_creates_ignored_source_rule_and_suppresses_future_candidates` |
| Briefings timing and concise output | `test_operations_skips_stale_morning_briefing_in_afternoon`; `test_household_chat_service_operator_update_strips_action_list_invitation`; `test_operations_records_heartbeat_ok_briefing_as_quiet_skip` |
| Parent asks action list -> direct action list | `test_household_chat_service_operator_update_strips_action_list_invitation` |
| Group-safe sharing/private no leak | `test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm`; group-share prompt fixtures in chat service tests. |
| Delivery opt-out/blocked channel -> explicit skip/failure | `test_delivery_service_disables_sendblue_channel_after_opt_out`; `test_operations_skips_blocked_briefing_channel_without_composing` |
| Trace/correlation id from inbound to outcome | `build_pilot_trace_report`; `test_pilot_trace_report_links_inbound_to_delivery_outcome` asserts inbound, route, Hermes, reply, outbound attempted, outbound sent, one turn record, and shared correlation id. |
| Missed-message debug from stored events | `test_pilot_trace_report_explains_unresolved_group_miss` asserts inbound, resolution failure, no outbound sent, and `unresolved_group_household` failure reason. |
| At least 12 pilot-critical scenarios | `PILOT_SCENARIOS` contains 12 scenarios; checklist test asserts scenario count and evidence. |
| Every successful added/scheduled/reminded claim backed by durable state | Action integrity tests in `tests/florence/test_chat_service.py` and scenario matrix. |
| Every proactive send has allow/deny/skip reason | Constitution policy and outbound skip tests in `tests/florence/test_runtime_services.py`. |
| Required commands pass | `scripts/run_tests.sh tests/florence` -> 388 passed; household tool gate -> passed; `git diff --check` -> passed. |
