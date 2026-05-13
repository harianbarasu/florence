"""Pilot-readiness checklist and trace helpers for Florence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from florence.runtime.reliability import FlorenceReliabilityEvent, UNKNOWN_HOUSEHOLD_ID
from florence.state import FlorenceStateDB


@dataclass(frozen=True, slots=True)
class PilotReadinessCheck:
    id: str
    question: str
    status: str
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PilotScenario:
    id: str
    workflow: str
    required_assertions: tuple[str, ...]
    evidence_tests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PilotTraceReport:
    household_id: str
    correlation_id: str | None
    turn_id: str | None
    message_id: str | None
    events: tuple[dict[str, Any], ...]
    turn_records: tuple[dict[str, Any], ...]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "household_id": self.household_id,
            "correlation_id": self.correlation_id,
            "turn_id": self.turn_id,
            "message_id": self.message_id,
            "events": list(self.events),
            "turn_records": list(self.turn_records),
            "summary": dict(self.summary),
        }


PILOT_PRODUCT_SPINE: tuple[tuple[str, str], ...] = (
    ("parent_dm", "Private parent lane for setup, review, and parent-specific context."),
    ("family_group_chat", "Shared household operating lane for group-safe logistics."),
    ("household_onboarding", "Small practical setup for parents, children, schools, routines, and data links."),
    ("google_oauth_data_connection", "Calendar/Gmail connection and initial sync state."),
    ("school_email_calendar_ingestion", "Evidence intake from school, calendar, email, media, and attachments."),
    ("calendar_reminder_creation", "Durable events, tasks, nudges, and reminder scheduling."),
    ("daily_weekly_briefings", "Morning, pickup, evening, meal, school, and weekly rhythms."),
    ("source_review_rules", "Review queue, share/private/ignore source rules, and handled-item feedback."),
    ("delivery_reliability", "Outbound delivery success, opt-out, blocked-channel, skip, and failure records."),
    ("admin_debug_visibility", "Traceable receive-route-Hermes-tool-state-schedule-delivery path."),
)


PILOT_SCENARIOS: tuple[PilotScenario, ...] = (
    PilotScenario(
        id="linq_dm_full_trace",
        workflow="Inbound Linq parent DM is received, resolved, routed, handled by Hermes, replied, and delivered.",
        required_assertions=("trace", "response", "turn_record", "delivery"),
        evidence_tests=("tests/florence/test_linq_production.py::test_production_linq_dm_turn_records_full_reliability_trace",),
    ),
    PilotScenario(
        id="unresolved_group_debug",
        workflow="Unresolved family group message leaves operator-visible receive and resolution-failure events.",
        required_assertions=("trace", "failure_reason", "no_delivery"),
        evidence_tests=("tests/florence/test_linq_production.py::test_production_linq_unresolved_group_records_resolution_failure",),
    ),
    PilotScenario(
        id="multi_image_recipe",
        workflow="Multiple recipe images are represented in the Hermes turn payload.",
        required_assertions=("trace", "media_payload", "all_images_present"),
        evidence_tests=("tests/florence/test_chat_service.py::test_household_chat_service_passes_multiple_image_attachments_with_ordered_metadata",),
    ),
    PilotScenario(
        id="school_calendar_media",
        workflow="Young Minds school-calendar media is extracted into message context.",
        required_assertions=("trace", "media_payload", "school_calendar_text"),
        evidence_tests=("tests/florence/test_linq_media.py::test_enrich_linq_payload_with_media_text_preserves_multiline_schedule_blocks",),
    ),
    PilotScenario(
        id="stuffy_reminder_action_integrity",
        workflow="Bring a Stuffie Day creates durable reminder state before Florence claims success.",
        required_assertions=("trace", "state_write", "scheduled_work", "success_claim_backed_by_state"),
        evidence_tests=("tests/florence/test_chat_service.py::test_household_chat_service_allows_added_claim_when_backing_nudge_exists",),
    ),
    PilotScenario(
        id="morning_of_school_reminder",
        workflow="School bring/wear/pack item creates morning-of reminder before school local time.",
        required_assertions=("trace", "state_write", "scheduled_work", "local_time"),
        evidence_tests=("tests/florence/test_runtime_services.py::test_household_manager_bring_stuffy_day_gets_morning_reminder_before_school",),
    ),
    PilotScenario(
        id="past_item_suppressed",
        workflow="Past imported item does not create a reminder or proactive review prompt.",
        required_assertions=("trace", "no_state_write", "no_delivery"),
        evidence_tests=("tests/florence/test_runtime_services.py::test_operations_review_nudge_skips_stale_schedule_candidate",),
    ),
    PilotScenario(
        id="already_handled_email",
        workflow="Already-handled email review feedback closes the candidate without Hermes guessing.",
        required_assertions=("trace", "state_write", "no_event_created", "response"),
        evidence_tests=("tests/florence/test_messaging_ingress.py::test_review_prompt_then_already_handled_closes_candidate_without_hermes",),
    ),
    PilotScenario(
        id="source_share_ignore_rules",
        workflow="Always-share or ignore-source feedback creates durable source behavior.",
        required_assertions=("trace", "state_write", "source_rule", "constitution_provenance"),
        evidence_tests=(
            "tests/florence/test_messaging_ingress.py::test_review_prompt_then_share_persists_source_rule_without_hermes",
            "tests/florence/test_review_and_events.py::test_review_feedback_ignore_sender_creates_ignored_source_rule_and_suppresses_future_candidates",
        ),
    ),
    PilotScenario(
        id="action_list_direct",
        workflow="Parent asks what needs action and Florence is instructed to return the action list directly.",
        required_assertions=("trace", "response_policy", "concise_output"),
        evidence_tests=("tests/florence/test_chat_service.py::test_household_chat_service_operator_update_strips_action_list_invitation",),
    ),
    PilotScenario(
        id="private_context_group_guard",
        workflow="Private parent context does not leak into family group or the other parent's DM.",
        required_assertions=("trace", "privacy", "group_safe_context"),
        evidence_tests=("tests/florence/test_chat_service.py::test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm",),
    ),
    PilotScenario(
        id="blocked_delivery_skip",
        workflow="Blocked or opted-out delivery records explicit skip/failure instead of disappearing.",
        required_assertions=("trace", "delivery_skip", "failure_reason"),
        evidence_tests=(
            "tests/florence/test_runtime_services.py::test_delivery_service_disables_sendblue_channel_after_opt_out",
            "tests/florence/test_runtime_services.py::test_operations_skips_blocked_briefing_channel_without_composing",
        ),
    ),
)


def build_pilot_readiness_checklist() -> list[PilotReadinessCheck]:
    """Return the current pilot-readiness checklist with concrete evidence references."""

    return [
        PilotReadinessCheck(
            id="new_dual_parent_household_onboards",
            question="Can a new dual-parent household onboard?",
            status="pass",
            evidence=("tests/florence/test_entrypoints.py::test_entrypoints_group_activation_adds_private_dm_hint_for_unlinked_parent",),
        ),
        PilotReadinessCheck(
            id="both_parents_share_context",
            question="Can both parents share context?",
            status="pass",
            evidence=("tests/florence/test_entrypoints.py::test_entrypoints_second_parent_dm_links_to_existing_household_after_group_activation",),
        ),
        PilotReadinessCheck(
            id="group_vs_private_context",
            question="Can Florence distinguish group context from private parent DM context?",
            status="pass",
            evidence=("tests/florence/test_chat_service.py::test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm",),
        ),
        PilotReadinessCheck(
            id="school_calendar_email_examples",
            question="Can Florence ingest school, calendar, email, and media examples?",
            status="pass",
            evidence=(
                "tests/florence/test_linq_media.py::test_enrich_linq_payload_with_media_text_preserves_multiline_schedule_blocks",
                "tests/florence/test_google_sync.py::test_google_sync_preserves_source_provenance_and_temporal_evidence",
            ),
        ),
        PilotReadinessCheck(
            id="durable_events_reminders_tasks",
            question="Can Florence create durable events, reminders, and tasks?",
            status="pass",
            evidence=("tests/florence/test_chat_service.py::test_household_chat_service_allows_added_claim_when_backing_nudge_exists",),
        ),
        PilotReadinessCheck(
            id="local_time_reminders",
            question="Can Florence send reminders at the right local time?",
            status="pass",
            evidence=("tests/florence/test_runtime_services.py::test_household_manager_bring_stuffy_day_gets_morning_reminder_before_school",),
        ),
        PilotReadinessCheck(
            id="quiet_when_nothing_matters",
            question="Can Florence stay quiet when nothing matters?",
            status="pass",
            evidence=("tests/florence/test_runtime_services.py::test_operations_records_heartbeat_ok_briefing_as_quiet_skip",),
        ),
        PilotReadinessCheck(
            id="failure_explainability",
            question="Can Florence explain what happened when something fails?",
            status="pass",
            evidence=(
                "tests/florence/test_linq_production.py::test_production_linq_unresolved_group_records_resolution_failure",
                "tests/florence/test_runtime_services.py::test_delivery_service_disables_sendblue_channel_after_opt_out",
            ),
        ),
        PilotReadinessCheck(
            id="debug_under_five_minutes",
            question="Can an operator debug a missed message from stored events without guessing?",
            status="pass",
            evidence=("tests/florence/test_pilot_readiness.py::test_pilot_trace_report_links_inbound_to_delivery_outcome",),
        ),
    ]


def build_pilot_scenario_matrix() -> list[PilotScenario]:
    """Return the canonical pilot scenarios that must stay covered by regression tests."""

    return list(PILOT_SCENARIOS)


def build_pilot_trace_report(
    store: FlorenceStateDB,
    *,
    household_id: str | None = None,
    correlation_id: str | None = None,
    turn_id: str | None = None,
    message_id: str | None = None,
    limit: int = 100,
) -> PilotTraceReport:
    """Build an operator-facing trace report from persisted reliability events and turn records."""

    target_household_id = household_id or UNKNOWN_HOUSEHOLD_ID
    event_household_ids = [target_household_id]
    if target_household_id != UNKNOWN_HOUSEHOLD_ID:
        event_household_ids.append(UNKNOWN_HOUSEHOLD_ID)
    events = []
    for event_household_id in event_household_ids:
        events.extend(
            _event_to_dict(event)
            for event in reversed(store.list_pilot_events(household_id=event_household_id, limit=limit))
            if _matches_trace(
                event.metadata,
                correlation_id=correlation_id,
                turn_id=turn_id,
                message_id=message_id,
            )
        )
    events.sort(key=lambda event: float(event.get("created_at") or 0))
    turn_records = [
        record
        for record in reversed(store.list_turn_records(household_id=target_household_id, limit=limit))
        if _turn_matches_trace(
            record,
            correlation_id=correlation_id,
            turn_id=turn_id,
            message_id=message_id,
        )
    ]
    summary = _summarize_trace(events=events, turn_records=turn_records)
    return PilotTraceReport(
        household_id=target_household_id,
        correlation_id=correlation_id,
        turn_id=turn_id,
        message_id=message_id,
        events=tuple(events),
        turn_records=tuple(turn_records),
        summary=summary,
    )


def _event_to_dict(event: Any) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "household_id": event.household_id,
        "member_id": event.member_id,
        "channel_id": event.channel_id,
        "metadata": dict(event.metadata or {}),
        "created_at": event.created_at,
    }


def _matches_trace(
    metadata: dict[str, Any],
    *,
    correlation_id: str | None,
    turn_id: str | None,
    message_id: str | None,
) -> bool:
    if not any((correlation_id, turn_id, message_id)):
        return True
    if correlation_id and str(metadata.get("correlation_id") or "") == correlation_id:
        return True
    if turn_id and str(metadata.get("turn_id") or "") == turn_id:
        return True
    if message_id and str(metadata.get("message_id") or metadata.get("provider_message_id") or "") == message_id:
        return True
    return False


def _turn_matches_trace(
    record: dict[str, Any],
    *,
    correlation_id: str | None,
    turn_id: str | None,
    message_id: str | None,
) -> bool:
    if not any((correlation_id, turn_id, message_id)):
        return True
    if turn_id and str(record.get("id") or "") == turn_id:
        return True
    envelope = dict(record.get("envelope") or {})
    outcome = dict(record.get("outcome") or {})
    reply_metadata = dict(outcome.get("reply_metadata") or {})
    if correlation_id and str(reply_metadata.get("correlation_id") or "") == correlation_id:
        return True
    if message_id and str(envelope.get("provider_message_id") or "") == message_id:
        return True
    return False


def _summarize_trace(*, events: list[dict[str, Any]], turn_records: list[dict[str, Any]]) -> dict[str, Any]:
    event_types = [str(event.get("event_type") or "") for event in events]
    failures = [
        dict(event.get("metadata") or {}).get("failure_reason") or dict(event.get("metadata") or {}).get("skipped_reason")
        for event in events
        if dict(event.get("metadata") or {}).get("failure_reason")
        or dict(event.get("metadata") or {}).get("skipped_reason")
    ]
    return {
        "event_types": event_types,
        "turn_count": len(turn_records),
        "received": FlorenceReliabilityEvent.INBOUND_RECEIVED.value in event_types,
        "routed": FlorenceReliabilityEvent.ROUTE_SELECTED.value in event_types,
        "hermes_started": FlorenceReliabilityEvent.HERMES_TURN_STARTED.value in event_types,
        "hermes_completed": FlorenceReliabilityEvent.HERMES_TURN_COMPLETED.value in event_types,
        "reply_generated": FlorenceReliabilityEvent.REPLY_GENERATED.value in event_types,
        "outbound_attempted": FlorenceReliabilityEvent.OUTBOUND_ATTEMPTED.value in event_types,
        "outbound_sent": FlorenceReliabilityEvent.OUTBOUND_SENT.value in event_types,
        "outbound_skipped": FlorenceReliabilityEvent.OUTBOUND_SKIPPED.value in event_types,
        "outbound_failed": FlorenceReliabilityEvent.OUTBOUND_FAILED.value in event_types,
        "failures": [failure for failure in failures if failure],
        "debuggable": bool(events or turn_records),
    }
