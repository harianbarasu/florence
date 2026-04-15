"""Florence household-state tools for Hermes-backed household management."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from florence.calendar_feed import parse_calendar_feed
from florence.contracts import (
    CandidateState,
    ChannelType,
    GoogleSourceKind,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdLinkRequest,
    HouseholdMeal,
    HouseholdMealStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    HouseholdShoppingItem,
    HouseholdShoppingItemStatus,
    HouseholdSourceVisibility,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    ImportedCandidate,
)
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    PENDING_ACTION_TARGET_ID_KEY,
    PENDING_ACTION_TARGET_IDS_KEY,
    build_google_connect_prompt_metadata,
)
from florence.onboarding import OnboardingStage
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.household_calendar_projection import (
    HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY,
    FlorenceHouseholdCalendarProjectionService,
)
from florence.runtime.household_link import FlorenceHouseholdLinkService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.onboarding_service import FlorenceOnboardingSessionService
from florence.runtime.visibility import (
    member_scoped_item_visible,
    owner_scoped_item_visible,
    recipient_scoped_item_visible,
    resolve_conversation_scope,
    resolve_google_calendar_scope,
    resolve_google_inbox_scope,
)
from florence.state import FlorenceStateDB
from tools.registry import registry
from tools.url_safety import is_safe_url
from tools.website_policy import check_website_access


@dataclass(slots=True)
class FlorenceHouseholdToolContext:
    store: FlorenceStateDB
    household_id: str
    actor_member_id: str | None
    channel_id: str
    household_chat_service: Any | None = None


_context_lock = threading.Lock()
_tool_contexts: dict[str, FlorenceHouseholdToolContext] = {}


def set_household_tool_context(
    task_id: str,
    *,
    store: FlorenceStateDB,
    household_id: str,
    actor_member_id: str | None,
    channel_id: str,
    household_chat_service: Any | None = None,
) -> None:
    with _context_lock:
        _tool_contexts[task_id] = FlorenceHouseholdToolContext(
            store=store,
            household_id=household_id,
            actor_member_id=actor_member_id,
            channel_id=channel_id,
            household_chat_service=household_chat_service,
        )


def clear_household_tool_context(task_id: str) -> None:
    with _context_lock:
        _tool_contexts.pop(task_id, None)


def _check_household_tool_requirements() -> bool:
    with _context_lock:
        return bool(_tool_contexts)


def _get_context(task_id: str | None) -> FlorenceHouseholdToolContext | None:
    if not task_id:
        return None
    with _context_lock:
        return _tool_contexts.get(task_id)


def _require_context(task_id: str | None) -> FlorenceHouseholdToolContext:
    context = _get_context(task_id)
    if context is None:
        raise RuntimeError("florence_household_context_missing")
    return context


def _build_onboarding_service(context: FlorenceHouseholdToolContext) -> FlorenceOnboardingSessionService:
    service = FlorenceOnboardingSessionService(context.store)
    try:
        from florence.config import FlorenceSettings
        from florence.runtime.google_services import FlorenceGoogleAccountLinkService

        settings = FlorenceSettings.from_env()
        if settings.google.configured:
            google_account_link_service = FlorenceGoogleAccountLinkService(
                context.store,
                service,
                client_id=settings.google.client_id or "",
                client_secret=settings.google.client_secret or "",
                redirect_uri=settings.google.redirect_uri or "",
                state_secret=settings.google.state_secret or "",
            )
            service.set_link_url_builder(
                lambda household_id, member_id, thread_id: google_account_link_service.build_connect_link(
                    household_id=household_id,
                    member_id=member_id,
                    thread_id=thread_id,
                ).url
            )
    except Exception:
        pass
    return service


def _find_parent_dm_channel_by_phone(
    store: FlorenceStateDB,
    *,
    phone_number: str,
    member_id: str | None = None,
    household_id: str | None = None,
    provider: str | None = None,
) -> Any | None:
    normalized_phone = _normalize_text(phone_number)
    if not normalized_phone:
        return None
    candidate_household_ids: list[str] = []
    if household_id:
        candidate_household_ids.append(household_id)
    if member_id:
        member = store.get_member(member_id)
        if member is not None and member.household_id not in candidate_household_ids:
            candidate_household_ids.append(member.household_id)
    if not candidate_household_ids:
        households = store.list_households()
        candidate_household_ids.extend(household.id for household in households)
    for candidate_household_id in candidate_household_ids:
        for channel in store.list_channels(
            household_id=candidate_household_id,
            channel_type=ChannelType.PARENT_DM,
        ):
            if provider and channel.provider != provider:
                continue
            metadata = dict(getattr(channel, "metadata", {}) or {})
            sender_handle = _normalize_text(metadata.get("sender_handle"))
            if sender_handle and sender_handle == normalized_phone:
                return channel
            provider_channel_id = _normalize_text(getattr(channel, "provider_channel_id", ""))
            if provider_channel_id.endswith(f"|{normalized_phone}"):
                return channel
    return None


def _sendblue_number_for_channel(channel: Any) -> str | None:
    metadata = dict(getattr(channel, "metadata", {}) or {})
    explicit = _normalize_text(metadata.get("sendblue_number"))
    if explicit:
        return explicit
    provider_channel_id = _normalize_text(getattr(channel, "provider_channel_id", ""))
    if "|" not in provider_channel_id:
        return None
    line_handle, _ = provider_channel_id.split("|", 1)
    return _normalize_text(line_handle) or None


def _send_parent_link_invite_via_current_transport(
    context: FlorenceHouseholdToolContext,
    *,
    request: HouseholdLinkRequest,
    invite_text: str,
) -> dict[str, object]:
    channel = context.store.get_channel(context.channel_id)
    if channel is None:
        raise ValueError("unknown_channel_id")
    if channel.provider != "sendblue":
        raise ValueError("parent_link_invite_transport_unsupported")

    from florence.config import FlorenceSettings
    from florence.messaging.protocol_types import build_household_link_prompt_metadata
    from florence.sendblue import FlorenceSendblueClient, build_sendblue_thread_id

    sendblue_number = _sendblue_number_for_channel(channel)
    if not sendblue_number:
        raise ValueError("sendblue_thread_id_required")

    settings = FlorenceSettings.from_env()
    client = FlorenceSendblueClient(settings.sendblue)
    thread_id = build_sendblue_thread_id(
        sendblue_number=sendblue_number,
        contact_number=request.invited_identity_normalized_value,
    )
    client.send_text(thread_id=thread_id, message=invite_text)

    target_channel = _find_parent_dm_channel_by_phone(
        context.store,
        phone_number=request.invited_identity_normalized_value,
        member_id=request.invited_member_id,
        household_id=request.source_household_id,
        provider="sendblue",
    )
    if target_channel is not None:
        FlorenceChannelLog(context.store).append_assistant_message(
            household_id=target_channel.household_id,
            channel_id=target_channel.id,
            body=invite_text,
            metadata={
                "provider": "sendblue",
                "transport_thread_id": target_channel.provider_channel_id or thread_id,
                "direct_parent_link_invite": True,
                **build_household_link_prompt_metadata(request.id, role="invited"),
            },
        )

    return {
        "provider": "sendblue",
        "thread_id": thread_id,
        "target_channel_id": target_channel.id if target_channel is not None else None,
    }


def _stable_id(prefix: str, *parts: str) -> str:
    raw = ":".join(parts).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:20]
    return f"{prefix}_{digest}"


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_optional_text(value: Any) -> str | None:
    normalized = _normalize_text(value)
    return normalized or None


def _normalize_metadata(value: Any) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _enum_value(enum_cls, value: Any, default):
    if value is None:
        return default
    normalized = _normalize_text(value).lower()
    for item in enum_cls:
        if item.value == normalized:
            return item
    raise ValueError(f"invalid_{enum_cls.__name__.lower()}:{value}")


def _resolve_member_id(
    context: FlorenceHouseholdToolContext,
    *,
    member_id: str | None = None,
    member_name: str | None = None,
) -> str | None:
    if member_id:
        member = context.store.get_member(member_id)
        if member is None or member.household_id != context.household_id:
            raise ValueError("unknown_household_member_id")
        return member.id
    normalized_name = _normalize_text(member_name).lower()
    if not normalized_name:
        return None
    if normalized_name in {"me", "myself"}:
        return context.actor_member_id
    matches = []
    for member in context.store.list_members(context.household_id):
        display = _normalize_text(member.display_name).lower()
        first = display.split()[0] if display else ""
        if normalized_name in {display, first}:
            matches.append(member)
    if len(matches) == 1:
        return matches[0].id
    if len(matches) > 1:
        raise ValueError("ambiguous_household_member_name")
    raise ValueError("unknown_household_member_name")


def _resolve_meal_id(
    context: FlorenceHouseholdToolContext,
    *,
    meal_id: str | None = None,
    meal_title: str | None = None,
) -> str | None:
    if meal_id:
        meal = context.store.get_household_meal(meal_id)
        if meal is None or meal.household_id != context.household_id:
            raise ValueError("unknown_household_meal_id")
        return meal.id
    normalized_title = _normalize_text(meal_title).lower()
    if not normalized_title:
        return None
    matches = [
        meal
        for meal in context.store.list_household_meals(household_id=context.household_id)
        if _normalize_text(meal.title).lower() == normalized_title
    ]
    if len(matches) == 1:
        return matches[0].id
    if len(matches) > 1:
        raise ValueError("ambiguous_household_meal_title")
    raise ValueError("unknown_household_meal_title")


def _resolve_child_id(
    context: FlorenceHouseholdToolContext,
    *,
    child_id: str | None = None,
    child_name: str | None = None,
) -> str | None:
    if child_id:
        matches = [
            child
            for child in context.store.list_child_profiles(household_id=context.household_id)
            if child.id == child_id
        ]
        if not matches:
            raise ValueError("unknown_household_child_id")
        return matches[0].id
    normalized_name = _normalize_text(child_name).lower()
    if not normalized_name:
        return None
    matches = []
    for child in context.store.list_child_profiles(household_id=context.household_id):
        display = _normalize_text(child.full_name).lower()
        first = display.split()[0] if display else ""
        aliases = {
            _normalize_text(alias).lower()
            for alias in child.metadata.get("aliases", [])
            if _normalize_text(alias)
        } if isinstance(child.metadata, dict) else set()
        if normalized_name in {display, first, *aliases}:
            matches.append(child)
    if len(matches) == 1:
        return matches[0].id
    if len(matches) > 1:
        raise ValueError("ambiguous_household_child_name")
    raise ValueError("unknown_household_child_name")


def _serialize_work_item(item: HouseholdWorkItem) -> dict[str, Any]:
    return {
        "id": item.id,
        "title": item.title,
        "description": item.description,
        "status": item.status.value,
        "owner_member_id": item.owner_member_id,
        "due_at": item.due_at,
        "starts_at": item.starts_at,
        "completed_at": item.completed_at,
        "metadata": item.metadata,
    }


def _serialize_routine(routine: HouseholdRoutine) -> dict[str, Any]:
    return {
        "id": routine.id,
        "title": routine.title,
        "cadence": routine.cadence,
        "description": routine.description,
        "status": routine.status.value,
        "owner_member_id": routine.owner_member_id,
        "next_due_at": routine.next_due_at,
        "last_completed_at": routine.last_completed_at,
        "metadata": routine.metadata,
    }


def _serialize_nudge(nudge) -> dict[str, Any]:
    return {
        "id": nudge.id,
        "target_kind": nudge.target_kind.value,
        "target_id": nudge.target_id,
        "message": nudge.message,
        "status": nudge.status.value,
        "recipient_member_id": nudge.recipient_member_id,
        "channel_id": nudge.channel_id,
        "scheduled_for": nudge.scheduled_for,
        "sent_at": nudge.sent_at,
        "acknowledged_at": nudge.acknowledged_at,
        "metadata": nudge.metadata,
    }


def _serialize_meal(meal: HouseholdMeal) -> dict[str, Any]:
    return {
        "id": meal.id,
        "title": meal.title,
        "meal_type": meal.meal_type,
        "scheduled_for": meal.scheduled_for,
        "description": meal.description,
        "status": meal.status.value,
        "metadata": meal.metadata,
    }


def _serialize_shopping_item(
    item: HouseholdShoppingItem,
    *,
    meal_title: str | None = None,
) -> dict[str, Any]:
    return {
        "id": item.id,
        "title": item.title,
        "list_name": item.list_name,
        "status": item.status.value,
        "quantity": item.quantity,
        "unit": item.unit,
        "notes": item.notes,
        "meal_id": item.meal_id,
        "meal_title": meal_title,
        "needed_by": item.needed_by,
        "metadata": item.metadata,
    }


def _serialize_event(event) -> dict[str, Any]:
    return {
        "id": event.id,
        "title": event.title,
        "starts_at": event.starts_at,
        "ends_at": event.ends_at,
        "timezone": event.timezone,
        "all_day": event.all_day,
        "location": event.location,
        "description": event.description,
        "status": event.status.value,
        "metadata": event.metadata,
    }


def _serialize_profile_item(item) -> dict[str, Any]:
    return {
        "id": item.id,
        "kind": item.kind.value,
        "label": item.label,
        "member_id": item.member_id,
        "child_id": item.child_id,
        "metadata": item.metadata,
    }


def _serialize_child(child) -> dict[str, Any]:
    return {
        "id": child.id,
        "full_name": child.full_name,
        "birthdate": child.birthdate,
        "metadata": child.metadata,
    }


def _matches_query(fields: list[Any], query: str) -> bool:
    if not query:
        return True
    lowered = query.lower()
    for field in fields:
        if field is None:
            continue
        if lowered in str(field).lower():
            return True
    return False


_SPECIFIC_DATE_REFERENCE_RE = re.compile(
    r"\b(?:"
    r"today|tomorrow|tonight|yesterday|"
    r"monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|"
    r"\d{4}-\d{2}-\d{2}|"
    r"\d{1,2}/\d{1,2}(?:/\d{2,4})?"
    r")\b",
    re.IGNORECASE,
)
_DATE_LOGISTICS_KEYWORDS = (
    "school",
    "pickup",
    "pick up",
    "pick-up",
    "dropoff",
    "drop off",
    "drop-off",
    "dismissal",
    "schedule",
    "class",
    "practice",
    "game",
    "travel",
    "flight",
    "trip",
    "camp",
    "holiday",
    "break",
    "release",
)
_NO_SCHOOL_EVENT_HINTS = (
    "no school",
    "school closed",
    "student holiday",
    "holiday",
    "spring break",
    "winter break",
    "fall break",
    "summer break",
    "teacher workday",
    "teacher work day",
    "in-service",
    "in service",
    "professional development",
    "pd day",
    "campus closed",
)
_SCHEDULE_EVENT_HINTS = (
    "school day",
    "pickup",
    "pick-up",
    "dropoff",
    "drop-off",
    "dismissal",
    "early release",
    "class",
    "practice",
    "game",
    "appointment",
    "lesson",
    "flight",
    "trip",
    "camp",
    "dance",
    "music",
)


def _query_needs_target_date(query: str) -> bool:
    if not query:
        return False
    if not _SPECIFIC_DATE_REFERENCE_RE.search(query):
        return False
    return any(keyword in query for keyword in _DATE_LOGISTICS_KEYWORDS)


def _resolve_zoneinfo(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(str(name or "").strip() or "UTC")
    except Exception:
        return ZoneInfo("UTC")


def _parse_iso_date_argument(value: str | None) -> date | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    try:
        return date.fromisoformat(normalized)
    except Exception:
        return None


def _coerce_datetime_for_zone(value: datetime | None, *, zone_name: str | None) -> datetime | None:
    if value is None:
        return None
    zone = _resolve_zoneinfo(zone_name)
    if value.tzinfo is None:
        return value.replace(tzinfo=zone)
    return value.astimezone(zone)


def _event_overlaps_local_date(
    event: HouseholdEvent,
    *,
    target_date: date,
    household_timezone: str | None,
) -> bool:
    start = _parse_iso_datetime_argument(event.starts_at)
    end = _parse_iso_datetime_argument(event.ends_at) or start
    if start is None and end is None:
        return False
    zone_name = _normalize_optional_text(event.timezone) or household_timezone or "UTC"
    start_local = _coerce_datetime_for_zone(start or end, zone_name=zone_name)
    end_local = _coerce_datetime_for_zone(end or start, zone_name=zone_name)
    if start_local is None or end_local is None:
        return False
    start_date = start_local.date()
    end_date = end_local.date()
    if event.all_day and end_date > start_date:
        end_date -= timedelta(days=1)
    if end_date < start_date:
        end_date = start_date
    return start_date <= target_date <= end_date


def _event_date_evidence_labels(event: HouseholdEvent) -> list[str]:
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    text = " ".join(
        str(part or "")
        for part in [
            event.title,
            event.description,
            event.location,
            metadata.get("child_name"),
            metadata.get("activity_name"),
            metadata.get("calendar_summary"),
            metadata.get("source_label"),
            metadata.get("school_name"),
        ]
    ).lower()
    labels: list[str] = []
    has_no_school_signal = any(phrase in text for phrase in _NO_SCHOOL_EVENT_HINTS)
    has_schedule_signal = any(phrase in text for phrase in _SCHEDULE_EVENT_HINTS)
    if not has_no_school_signal and "school" in text:
        has_schedule_signal = True
    if has_no_school_signal:
        labels.append("no_school_or_holiday")
    if has_schedule_signal:
        labels.append("school_or_schedule_logistics")
    if not labels:
        labels.append("dated_schedule_event")
    return labels


def _build_date_answer_evidence_contract() -> dict[str, Any]:
    return {
        "scope": "Use for school, pickup, travel, holiday, and schedule questions tied to one exact date.",
        "requirements": [
            "Resolve the exact date first and pass target_date as YYYY-MM-DD for date-specific schedule questions.",
            "Treat confirmed dated events, mirrored inbox/calendar evidence, and current web research as valid exact-date evidence.",
            "Do not treat routines, profile labels, weekday defaults, or nearby recurring patterns as proof for one exact future date.",
        ],
        "claim_readiness_levels": {
            "needs_target_date": "The query looks date-specific, but Florence still needs the exact YYYY-MM-DD date before using tracked state as evidence.",
            "verified": "Tracked state includes confirmed dated evidence for that exact date.",
            "unverified": "Tracked state does not yet justify a firm exact-date answer. Florence should verify with inbox/calendar/web evidence or say it cannot verify yet.",
            "conflicting": "Tracked state contains conflicting exact-date evidence. Florence should explain the conflict instead of picking a side.",
            "invalid_target_date": "The provided target_date could not be parsed. Re-run with YYYY-MM-DD.",
        },
        "routine_pattern_warning": "Routines, weekday defaults, and recurring school patterns do not verify one exact date.",
    }


def _build_date_coverage(
    *,
    query: str,
    target_date_raw: str | None,
    target_date_value: date | None,
    household_timezone: str | None,
    events: list[HouseholdEvent],
) -> dict[str, Any] | None:
    if target_date_raw and target_date_value is None:
        return {
            "target_date": target_date_raw,
            "claim_readiness": "invalid_target_date",
            "status_signal": "invalid_target_date",
            "matched_confirmed_events": [],
            "matched_tentative_events": [],
            "matched_cancelled_events": [],
            "confirmed_evidence_labels": [],
            "guidance": "target_date must use YYYY-MM-DD before Florence can treat tracked state as exact-date evidence.",
            "routine_pattern_warning": "Do not infer from routines or weekday patterns while the date is unresolved.",
        }

    if target_date_value is None:
        if _query_needs_target_date(query):
            return {
                "target_date": None,
                "claim_readiness": "needs_target_date",
                "status_signal": "target_date_required",
                "matched_confirmed_events": [],
                "matched_tentative_events": [],
                "matched_cancelled_events": [],
                "confirmed_evidence_labels": [],
                "guidance": (
                    "This looks like a date-specific schedule question. Re-run household_search_state with "
                    "target_date=YYYY-MM-DD before treating tracked state as evidence."
                ),
                "routine_pattern_warning": "Do not infer from routines or weekday patterns for an exact-date question.",
            }
        return None

    matched_events = [
        event
        for event in events
        if _event_overlaps_local_date(
            event,
            target_date=target_date_value,
            household_timezone=household_timezone,
        )
    ]
    confirmed_events = [event for event in matched_events if event.status == HouseholdEventStatus.CONFIRMED]
    tentative_events = [event for event in matched_events if event.status == HouseholdEventStatus.TENTATIVE]
    cancelled_events = [event for event in matched_events if event.status == HouseholdEventStatus.CANCELLED]
    confirmed_labels = sorted({label for event in confirmed_events for label in _event_date_evidence_labels(event)})

    if confirmed_events:
        has_no_school_signal = "no_school_or_holiday" in confirmed_labels
        has_schedule_signal = "school_or_schedule_logistics" in confirmed_labels
        if has_no_school_signal and has_schedule_signal:
            claim_readiness = "conflicting"
            status_signal = "conflicting_explicit_date_evidence"
            guidance = (
                f"Tracked state has conflicting confirmed date evidence for {target_date_value.isoformat()}. "
                "Explain the conflict and verify with inbox, calendar, or web evidence before answering firmly."
            )
        elif has_no_school_signal:
            claim_readiness = "verified"
            status_signal = "confirmed_no_school_or_holiday"
            guidance = (
                f"Tracked state has confirmed dated no-school or holiday evidence for {target_date_value.isoformat()}. "
                "Answer from that evidence and name the exact date."
            )
        elif has_schedule_signal:
            claim_readiness = "verified"
            status_signal = "confirmed_school_or_schedule"
            guidance = (
                f"Tracked state has confirmed dated schedule evidence for {target_date_value.isoformat()}. "
                "Answer only to the extent those confirmed events actually support."
            )
        else:
            claim_readiness = "verified"
            status_signal = "confirmed_other_dated_evidence"
            guidance = (
                f"Tracked state has confirmed dated events for {target_date_value.isoformat()}, but not a clear school/holiday signal. "
                "Answer only from what those confirmed events actually say."
            )
    elif tentative_events:
        claim_readiness = "unverified"
        status_signal = "tentative_only"
        guidance = (
            f"Only tentative dated events exist for {target_date_value.isoformat()}. "
            "Do not answer firmly from tracked state. Verify with inbox, calendar, or web evidence, or say Florence cannot verify yet."
        )
    else:
        claim_readiness = "unverified"
        status_signal = "missing_explicit_date_evidence"
        guidance = (
            f"There is no confirmed dated event evidence in tracked state for {target_date_value.isoformat()}. "
            "Do not infer from routines or weekday patterns. Verify with inbox, calendar, or web evidence, or say Florence cannot verify yet."
        )

    return {
        "target_date": target_date_value.isoformat(),
        "claim_readiness": claim_readiness,
        "status_signal": status_signal,
        "matched_confirmed_events": [_serialize_event(event) for event in confirmed_events[:10]],
        "matched_tentative_events": [_serialize_event(event) for event in tentative_events[:10]],
        "matched_cancelled_events": [_serialize_event(event) for event in cancelled_events[:10]],
        "confirmed_evidence_labels": confirmed_labels,
        "guidance": guidance,
        "routine_pattern_warning": "Routines, weekday defaults, and recurring school patterns do not verify one exact date.",
    }


_DUPLICATE_EVENT_STOPWORDS = {
    "the",
    "and",
    "with",
    "for",
    "from",
    "class",
    "lesson",
    "event",
    "schedule",
    "session",
}


def _event_tokens(event: HouseholdEvent, *, child_tokens: set[str]) -> set[str]:
    fields = [event.title, event.description, event.location]
    if isinstance(event.metadata, dict):
        fields.extend(
            [
                event.metadata.get("child_name"),
                event.metadata.get("activity_name"),
                event.metadata.get("calendar_summary"),
                event.metadata.get("source_label"),
            ]
        )
    tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", " ".join(str(field or "").lower() for field in fields))
        if len(token) >= 3 and token not in _DUPLICATE_EVENT_STOPWORDS
    }
    root_tokens = {token[:5] for token in tokens if len(token) >= 5}
    matched_children = {token for token in child_tokens if token in tokens}
    return tokens | root_tokens | matched_children


def _events_look_duplicate(
    first: HouseholdEvent,
    second: HouseholdEvent,
    *,
    child_tokens: set[str],
) -> bool:
    if first.id == second.id:
        return False
    if first.status == HouseholdEventStatus.CANCELLED or second.status == HouseholdEventStatus.CANCELLED:
        return False
    if not first.starts_at or not second.starts_at or first.starts_at != second.starts_at:
        return False
    if first.ends_at and second.ends_at and first.ends_at != second.ends_at:
        return False
    first_title = _normalize_text(first.title).lower()
    second_title = _normalize_text(second.title).lower()
    if first_title and first_title == second_title:
        return True
    shared_tokens = _event_tokens(first, child_tokens=child_tokens) & _event_tokens(second, child_tokens=child_tokens)
    if len(shared_tokens) >= 2:
        return True
    child_overlap = shared_tokens & child_tokens
    return bool(child_overlap) and bool(shared_tokens - child_tokens)


def _event_duplicate_sort_key(event: HouseholdEvent) -> tuple[int, int, int, str]:
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    return (
        0 if event.status == HouseholdEventStatus.CONFIRMED else 1,
        0 if str(metadata.get(HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY) or "").strip() else 1,
        0 if event.source_candidate_id else 1,
        event.id,
    )


def _build_event_duplicate_groups(
    context: FlorenceHouseholdToolContext,
    *,
    events: list[HouseholdEvent],
    limit: int,
) -> list[dict[str, Any]]:
    child_tokens: set[str] = set()
    for child in context.store.list_child_profiles(household_id=context.household_id):
        full_name = _normalize_text(child.full_name).lower()
        if full_name:
            child_tokens.add(full_name)
            first_name = full_name.split()[0]
            if first_name:
                child_tokens.add(first_name)
        if isinstance(child.metadata, dict):
            for alias in child.metadata.get("aliases", []):
                normalized = _normalize_text(alias).lower()
                if normalized:
                    child_tokens.add(normalized)

    adjacency: dict[str, set[str]] = {event.id: set() for event in events}
    indexed = {event.id: event for event in events}
    for index, first in enumerate(events):
        for second in events[index + 1 :]:
            if _events_look_duplicate(first, second, child_tokens=child_tokens):
                adjacency[first.id].add(second.id)
                adjacency[second.id].add(first.id)

    groups: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        if event.id in seen or not adjacency.get(event.id):
            continue
        stack = [event.id]
        component_ids: list[str] = []
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            component_ids.append(current)
            stack.extend(sorted(adjacency.get(current, set()) - seen))
        component = sorted((indexed[event_id] for event_id in component_ids), key=_event_duplicate_sort_key)
        if len(component) < 2:
            continue
        shared_tokens = set.intersection(*[_event_tokens(item, child_tokens=child_tokens) for item in component])
        canonical = component[0]
        groups.append(
            {
                "canonical_event_id": canonical.id,
                "duplicate_event_ids": [item.id for item in component[1:]],
                "shared_tokens": sorted(shared_tokens)[:6],
                "events": [_serialize_event(item) for item in component],
            }
        )

    groups.sort(key=lambda item: (str(item["events"][0].get("starts_at") or ""), item["canonical_event_id"]))
    return groups[:limit]


def _normalize_calendar_feed_url(url: str) -> str:
    text = str(url or "").strip()
    lowered = text.lower()
    if lowered.startswith("webcal://"):
        return "https://" + text.split("://", 1)[1]
    return text


def _fetch_calendar_feed_text(url: str) -> tuple[str, str]:
    normalized_url = _normalize_calendar_feed_url(url)
    if not normalized_url:
        raise ValueError("Missing required parameter: url")
    if not is_safe_url(normalized_url):
        raise ValueError("Calendar feed URL is not allowed.")
    blocked = check_website_access(normalized_url)
    if blocked is not None:
        raise ValueError(str(blocked.get("message") or "Calendar feed URL is blocked by website policy."))
    response = httpx.get(
        normalized_url,
        headers={
            "User-Agent": "Florence/1.0 (+https://hermes-agent.nousresearch.com)",
            "Accept": "text/calendar,application/octet-stream,text/plain,*/*",
        },
        follow_redirects=True,
        timeout=30.0,
    )
    response.raise_for_status()
    return normalized_url, response.text


SEARCH_STATE_SCHEMA = {
    "name": "household_search_state",
    "description": (
        "Search Florence household state when you need to pull the latest tracked work, routines, nudges, meals, "
        "shopping items, events, children, or profile items including household preferences. "
        "The result also includes structured scope context showing the current channel scope and tentative tracked "
        "state. Private review details are hidden by default unless you explicitly request them. Event searches also "
        "surface likely duplicate-event groups when Florence sees overlapping schedule entries. For school, pickup, "
        "holiday, travel, or schedule questions about one exact date, pass target_date in YYYY-MM-DD so the result "
        "can tell you whether tracked state is verified, unverified, conflicting, or still missing the target date. "
        "Use this before updating existing state if the current household picture is unclear."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional search text like 'groceries', 'lunch', 'soccer', or 'birthday'.",
            },
            "entity_types": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "work_items",
                        "routines",
                        "nudges",
                        "meals",
                        "shopping_items",
                        "events",
                        "children",
                        "profile_items",
                        "schools",
                        "activities",
                        "contacts",
                        "places",
                        "providers",
                        "assets",
                        "preferences",
                    ],
                },
                "description": "Optional list of entity buckets to search. Omit to search across household state.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum results per entity bucket. Default 10.",
            },
            "include_private_review_state": {
                "type": "boolean",
                "description": (
                    "When true in a parent DM, include the current private review queue details. "
                    "Leave false for ordinary household chat."
                ),
            },
            "target_date": {
                "type": "string",
                "description": (
                    "Optional exact household-local date in YYYY-MM-DD for school, pickup, holiday, travel, or "
                    "schedule questions tied to one specific day. When provided, the result includes date_coverage "
                    "showing whether tracked state can verify that date or whether Florence still needs inbox, "
                    "calendar, or web evidence."
                ),
            },
        },
        "required": [],
    },
}

APPLY_CANDIDATE_REVIEW_SCHEMA = {
    "name": "household_apply_candidate_review",
    "description": (
        "Resolve the one currently surfaced imported-item review action in a parent DM. Use this only when the "
        "user is clearly responding to that exact review item. It can confirm, reject, skip, or confirm with "
        "corrected event fields."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "candidate_id": {"type": "string", "description": "The exact imported candidate id being resolved."},
            "resolution": {
                "type": "string",
                "enum": ["confirm", "reject", "skip"],
                "description": "How to resolve the surfaced review item.",
            },
            "source_visibility": {
                "type": "string",
                "enum": ["shared", "private"],
                "description": "Optional source-sharing rule to apply for future items from the same source.",
            },
            "title": {"type": "string", "description": "Optional corrected event title when confirming."},
            "starts_at": {"type": "string", "description": "Optional corrected ISO start timestamp when confirming."},
            "ends_at": {"type": "string", "description": "Optional corrected ISO end timestamp when confirming."},
            "timezone": {"type": "string", "description": "Optional corrected timezone when confirming."},
            "all_day": {"type": "boolean", "description": "Optional corrected all-day flag when confirming."},
            "location": {"type": "string", "description": "Optional corrected location when confirming."},
            "description": {"type": "string", "description": "Optional corrected description when confirming."},
        },
        "required": ["candidate_id", "resolution"],
    },
}

APPLY_NUDGE_ACTION_SCHEMA = {
    "name": "household_apply_nudge_action",
    "description": (
        "Resolve the one currently surfaced reminder/nudge action in a parent DM. Use this only when the user is "
        "clearly responding to that exact reminder. It can mark it done, snooze it until a new time, or leave it "
        "alone for now."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "nudge_id": {"type": "string", "description": "The exact household nudge id being updated."},
            "action": {
                "type": "string",
                "enum": ["done", "snooze", "skip"],
                "description": "How to resolve the surfaced nudge.",
            },
            "scheduled_for": {
                "type": "string",
                "description": "Required ISO timestamp when action is snooze.",
            },
        },
        "required": ["nudge_id", "action"],
    },
}

APPLY_ONBOARDING_UPDATE_SCHEMA = {
    "name": "household_apply_onboarding_update",
    "description": (
        "Store explicit onboarding facts for the current incomplete parent DM. Use this only for concrete setup facts "
        "the user actually provided, such as parent name, child names, one or more children's age/school/activities, or that Google is connected."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "parent_name": {"type": "string", "description": "Parent display name when the user gives it."},
            "child_names": {
                "type": "array",
                "items": {"type": "string"},
                "description": "One or more child names when the user lists them explicitly.",
            },
            "child_name": {"type": "string", "description": "Child name for a specific child detail update."},
            "age": {"type": "string", "description": "Age detail for the named or current child."},
            "school": {"type": "string", "description": "School detail for the named or current child."},
            "activities": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Activity labels for the named or current child.",
            },
            "child_updates": {
                "type": "array",
                "description": "Optional batch of explicit child detail updates when the user provides multiple kids' facts in one message.",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Child name for this explicit update."},
                        "age": {"type": "string", "description": "Age detail for this child."},
                        "school": {"type": "string", "description": "School detail for this child."},
                        "activities": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Activity labels for this child.",
                        },
                    },
                    "required": ["name"],
                },
            },
            "google_connected": {
                "type": "boolean",
                "description": "Set true when the user clearly says Google is now connected.",
            },
        },
        "required": [],
    },
}

REQUEST_PARENT_LINK_SCHEMA = {
    "name": "household_request_parent_link",
    "description": (
        "Start a private second-parent household link request by phone number. Use this when a parent wants Florence "
        "to connect another parent into the same household without making them redo the family setup. "
        "The same tool can also send the invite text once the current parent says yes. Jackson-facing copy must stay "
        "privacy-safe and should not reveal whether Florence already knew that number."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "phone_number": {
                "type": "string",
                "description": "Phone number for the other parent Florence should link. Required when starting a new request.",
            },
            "display_name": {
                "type": "string",
                "description": "Optional name label like Kendall for a more natural reply.",
            },
            "request_id": {
                "type": "string",
                "description": "Existing pending parent-link request id. Use this when the parent is replying yes to Florence texting the other parent now.",
            },
            "send_invite_now": {
                "type": "boolean",
                "description": "Set true when the current parent explicitly wants Florence to text the other parent now.",
            },
        },
        "required": [],
    },
}


def _serialize_review_candidate(candidate: ImportedCandidate) -> dict[str, Any]:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    return {
        "id": candidate.id,
        "title": candidate.title,
        "summary": candidate.summary,
        "state": candidate.state.value,
        "source_kind": candidate.source_kind.value,
        "candidate_scope": str(metadata.get("candidate_scope") or "").strip() or None,
        "source_visibility": str(metadata.get("source_visibility") or "").strip() or None,
        "source_rule_label": str(metadata.get("source_rule_label") or "").strip() or None,
        "requires_confirmation": candidate.requires_confirmation,
    }


def _build_visibility_summary(
    context: FlorenceHouseholdToolContext,
    *,
    query: str,
    limit: int,
    include_private_review_state: bool = False,
) -> dict[str, Any]:
    scope = resolve_conversation_scope(
        context.store,
        channel_id=context.channel_id,
        actor_member_id=context.actor_member_id,
    )
    tentative_events = [
        _serialize_event(event)
        for event in context.store.list_household_events(household_id=context.household_id)
        if event.status != HouseholdEventStatus.CONFIRMED
        and _matches_query([event.title, event.description, event.location, event.status.value, event.metadata], query)
    ]
    confirmed_event_count = sum(
        1
        for event in context.store.list_household_events(household_id=context.household_id)
        if event.status == HouseholdEventStatus.CONFIRMED
    )
    projection = FlorenceHouseholdCalendarProjectionService(context.store).get_projection_config(
        household_id=context.household_id,
    )
    visibility: dict[str, Any] = {
        "current_scope": {
            "channel_type": scope.channel_type.value if scope.channel_type is not None else None,
            "scope": scope.scope,
            "actor_member_id": context.actor_member_id,
            "private_review_available": scope.private_review_available,
        },
        "shared_household_state": {
            "confirmed_event_count": confirmed_event_count,
        },
        "shared_calendar_projection": {
            "available": projection is not None,
            "status": str(projection.get("status") or "").strip() if projection else None,
            "calendar_id": str(projection.get("calendar_id") or "").strip() if projection else None,
            "calendar_summary": str(projection.get("calendar_summary") or "").strip() if projection else None,
            "calendar_web_url": str(projection.get("calendar_web_url") or "").strip() if projection else None,
            "host_email": str(projection.get("host_email") or "").strip() if projection else None,
            "shared_with_emails": list(projection.get("shared_with_emails") or []) if projection else [],
            "last_synced_at": str(projection.get("last_synced_at") or "").strip() if projection else None,
        },
        "tentative_state": {
            "event_count": len(tentative_events),
            "events": tentative_events[:limit],
        },
    }

    if scope.private_review_available and include_private_review_state:
        pending_candidates = [
            _serialize_review_candidate(candidate)
            for candidate in context.store.list_imported_candidates(
                household_id=context.household_id,
                member_id=context.actor_member_id,
                state=CandidateState.PENDING_REVIEW,
            )
            if _matches_query(
                [
                    candidate.title,
                    candidate.summary,
                    candidate.state.value,
                    candidate.source_kind.value,
                    candidate.metadata,
                ],
                query,
            )
        ]
        visibility["private_review_state"] = {
            "available_in_current_scope": True,
            "included_in_response": True,
            "pending_candidate_count": len(pending_candidates),
            "pending_candidates": pending_candidates[:limit],
        }
    else:
        visibility["private_review_state"] = {
            "available_in_current_scope": scope.private_review_available,
            "included_in_response": False,
            "pending_candidate_count": 0,
            "pending_candidates": [],
        }

    return visibility
SEARCH_GOOGLE_INBOX_SCHEMA = {
    "name": "household_search_google_inbox",
    "description": (
        "Search Florence's mirrored Gmail inbox for the message, invite, or email-derived detail that best answers the current request. "
        "Use this when inbox context is likely to be the fastest grounded source of truth, instead of asking the user to restate or forward the email when "
        "Google is already connected. In a parent DM, this defaults to that parent's inbox unless the request clearly "
        "matches shared household source rules. In the family group, only shared-household inbox scope is allowed."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional free-text search such as 'spring break musical beginnings'.",
            },
            "sender": {
                "type": "string",
                "description": "Optional sender name or email fragment, such as 'Linda' or 'school@district.org'.",
            },
            "subject": {
                "type": "string",
                "description": "Optional subject phrase to prioritize.",
            },
            "newer_than_days": {
                "type": "integer",
                "description": "How far back to search. Default 120 days.",
            },
            "max_results": {
                "type": "integer",
                "description": "Maximum number of matching messages to return. Default 5.",
            },
        },
        "required": [],
    },
}

SEARCH_GOOGLE_CALENDAR_SCHEMA = {
    "name": "household_search_google_calendar",
    "description": (
        "Search Florence's mirrored Google Calendar events for the schedule, class, practice, appointment, or calendar-derived detail that best answers the current request. "
        "Use this when calendar context is likely to already contain the answer, instead of asking the user to restate schedule details that may already be on their calendar. "
        "In a parent DM, this defaults to that parent's mirrored calendar unless the request clearly matches shared household source rules. "
        "In the family group, only shared-household calendar scope is allowed."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional free-text search such as 'Theo DRALL baseball' or 'Violet music Thursdays'.",
            },
            "calendar_summary": {
                "type": "string",
                "description": "Optional calendar name or summary to prioritize, such as 'GameChanger' or 'Family'.",
            },
            "newer_than_days": {
                "type": "integer",
                "description": "How far back to include events that may still be relevant. Default 180 days.",
            },
            "max_results": {
                "type": "integer",
                "description": "Maximum number of matching events to return. Default 5.",
            },
        },
        "required": [],
    },
}

IMPORT_CALENDAR_FEED_SCHEMA = {
    "name": "household_import_calendar_feed",
    "description": (
        "Fetch and ingest a shareable calendar feed such as a webcal:// or .ics schedule link into durable Florence household events. "
        "Use this when a parent pastes a team, school, class, or activity calendar feed they want Florence to remember or keep on the calendar."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "Required calendar-feed URL. Florence accepts webcal:// or https:// links to .ics feeds.",
            },
            "child_name": {
                "type": "string",
                "description": "Optional child the schedule belongs to, such as 'Theo'.",
            },
            "title_prefix": {
                "type": "string",
                "description": "Optional label to prefix imported event titles when the feed titles are too generic.",
            },
            "max_events": {
                "type": "integer",
                "description": "Maximum number of feed events to ingest from this fetch. Default 75.",
            },
            "metadata": {
                "type": "object",
                "description": "Optional structured metadata to store on every imported event.",
            },
        },
        "required": ["url"],
    },
}


UPSERT_EVENT_SCHEMA = {
    "name": "household_upsert_event",
    "description": (
        "Create or update a household event Florence should remember across threads, such as camps, school days, "
        "sports practices, trips, appointments, and deadlines."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Existing event id to update. Omit to upsert by title + start time."},
            "title": {"type": "string", "description": "Event title. Required when creating a new event, optional when updating an existing id."},
            "starts_at": {"type": "string", "description": "Optional ISO timestamp when the event starts."},
            "ends_at": {"type": "string", "description": "Optional ISO timestamp when the event ends."},
            "timezone": {"type": "string", "description": "Optional timezone id."},
            "all_day": {"type": "boolean", "description": "Whether this is an all-day event."},
            "location": {"type": "string", "description": "Optional location."},
            "description": {"type": "string", "description": "Optional details or notes."},
            "status": {
                "type": "string",
                "enum": [status.value for status in HouseholdEventStatus],
                "description": "Event status. Use tentative when plans are not locked.",
            },
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": [],
    },
}


UPSERT_WORK_ITEM_SCHEMA = {
    "name": "household_upsert_work_item",
    "description": (
        "Create or update a persistent household work item such as a return, school form, tax task, gift purchase, "
        "trip-planning task, or repair follow-up."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Existing work-item id to update. Omit to upsert by normalized title."},
            "title": {"type": "string", "description": "Short work-item title. Required when creating a new item, optional when updating an existing id."},
            "description": {"type": "string", "description": "Optional notes or details."},
            "status": {
                "type": "string",
                "enum": [status.value for status in HouseholdWorkItemStatus],
                "description": "Lifecycle state.",
            },
            "owner_member_id": {"type": "string", "description": "Optional owner member id."},
            "owner_member_name": {"type": "string", "description": "Optional owner member display name, e.g. 'Maya' or 'me'."},
            "due_at": {"type": "string", "description": "Optional ISO timestamp when this item is due."},
            "starts_at": {"type": "string", "description": "Optional ISO timestamp for when this item starts or becomes relevant."},
            "completed_at": {"type": "string", "description": "Optional ISO timestamp when this item was finished."},
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": [],
    },
}


RESOLVE_MERGE_FOLLOWUP_SCHEMA = {
    "name": "household_resolve_merge_followup",
    "description": (
        "Resolve a shared-household merge follow-up after linking parents, especially when Florence needs one clear child fact "
        "such as the correct birthdate or school. Use this to apply the chosen shared fact and close or shrink the follow-up item."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "work_item_id": {
                "type": "string",
                "description": "Required merge follow-up work item id.",
            },
            "birthdate": {
                "type": "string",
                "description": "Optional canonical child birthdate to keep, if this merge follow-up is about conflicting birthdates.",
            },
            "school": {
                "type": "string",
                "description": "Optional canonical school name to keep for this child, if the merge follow-up is about conflicting schools.",
            },
            "resolution_note": {
                "type": "string",
                "description": "Optional short note about why this was resolved this way.",
            },
            "group_index": {
                "type": "integer",
                "description": "Optional conflict-group index within the work item. Default 0.",
            },
        },
        "required": ["work_item_id"],
    },
}


UPSERT_ROUTINE_SCHEMA = {
    "name": "household_upsert_routine",
    "description": (
        "Create or update a recurring household routine such as weekly meal planning, Friday lunch ordering, "
        "monthly bill review, or plant watering."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Existing routine id to update. Omit to upsert by normalized title."},
            "title": {"type": "string", "description": "Routine title."},
            "cadence": {"type": "string", "description": "Human-readable cadence, e.g. 'weekly on Sunday evening'."},
            "description": {"type": "string", "description": "Optional notes or scope."},
            "status": {
                "type": "string",
                "enum": [status.value for status in HouseholdRoutineStatus],
                "description": "Routine status.",
            },
            "owner_member_id": {"type": "string", "description": "Optional owner member id."},
            "owner_member_name": {"type": "string", "description": "Optional owner member display name, e.g. 'Chris' or 'me'."},
            "next_due_at": {"type": "string", "description": "Optional ISO timestamp for the next due moment."},
            "last_completed_at": {"type": "string", "description": "Optional ISO timestamp when it was last completed."},
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": ["title", "cadence"],
    },
}


SCHEDULE_NUDGE_SCHEMA = {
    "name": "household_schedule_nudge",
    "description": (
        "Schedule a Florence reminder or follow-up nudge to be sent later in the household thread or a parent DM."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "message": {"type": "string", "description": "Reminder text Florence should send later."},
            "scheduled_for": {"type": "string", "description": "ISO timestamp for when Florence should send the nudge."},
            "target_kind": {
                "type": "string",
                "enum": [kind.value for kind in HouseholdNudgeTargetKind],
                "description": "Optional target type tied to this nudge.",
            },
            "target_id": {"type": "string", "description": "Optional linked event, work-item, or routine id."},
            "recipient_member_id": {"type": "string", "description": "Optional recipient member id."},
            "recipient_member_name": {"type": "string", "description": "Optional recipient member name, e.g. 'Maya' or 'me'."},
            "channel_id": {"type": "string", "description": "Optional explicit Florence channel id."},
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": ["message", "scheduled_for"],
    },
}


UPSERT_MEAL_SCHEMA = {
    "name": "household_upsert_meal",
    "description": (
        "Create or update a meal-plan entry so Florence can help with planning, reminders, and grocery tracking."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Existing meal id to update. Omit to upsert by date, meal type, and title."},
            "title": {"type": "string", "description": "Meal title, e.g. 'Taco night'."},
            "meal_type": {"type": "string", "description": "Meal slot such as breakfast, lunch, dinner, snack, or prep."},
            "scheduled_for": {"type": "string", "description": "ISO timestamp for when the meal happens or is planned."},
            "description": {"type": "string", "description": "Optional notes or recipe detail."},
            "status": {
                "type": "string",
                "enum": [status.value for status in HouseholdMealStatus],
                "description": "Meal status.",
            },
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": ["title", "meal_type", "scheduled_for"],
    },
}


UPSERT_SHOPPING_ITEM_SCHEMA = {
    "name": "household_upsert_shopping_item",
    "description": (
        "Create or update a household shopping or grocery-list item, optionally linking it to a meal plan entry."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Existing shopping-item id to update. Omit to upsert by list name and title."},
            "title": {"type": "string", "description": "Item title, e.g. 'tortillas'."},
            "list_name": {"type": "string", "description": "List name such as groceries, Costco, or Target."},
            "status": {
                "type": "string",
                "enum": [status.value for status in HouseholdShoppingItemStatus],
                "description": "Item status.",
            },
            "quantity": {"type": "string", "description": "Optional quantity string."},
            "unit": {"type": "string", "description": "Optional unit string."},
            "notes": {"type": "string", "description": "Optional notes, preference, or brand hint."},
            "meal_id": {"type": "string", "description": "Optional linked meal id."},
            "meal_title": {"type": "string", "description": "Optional linked meal title when the id is not known."},
            "needed_by": {"type": "string", "description": "Optional ISO timestamp for when the item is needed."},
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": ["title"],
    },
}


RECORD_PREFERENCE_SCHEMA = {
    "name": "household_record_preference",
    "description": (
        "Persist a durable household preference or rule Florence should remember across threads, such as reminder style, "
        "quiet hours, meal constraints, sharing defaults, kid food preferences, or other operating preferences."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "label": {"type": "string", "description": "Short preference label, e.g. 'Reminder style' or 'Kid spice preference'."},
            "value": {"type": "string", "description": "The actual preference Florence should remember."},
            "category": {
                "type": "string",
                "enum": [
                    "general",
                    "reminder_style",
                    "operating_rule",
                    "quiet_hours",
                    "support_type",
                    "automation_boundary",
                    "sensitive_topic",
                    "household_profile",
                    "meal_preference",
                    "sharing_preference",
                    "child_preference",
                ],
                "description": "Preference category. Use operating_rule, support_type, automation_boundary, and quiet_hours when it should directly shape Florence behavior.",
            },
            "member_id": {"type": "string", "description": "Optional member the preference applies to."},
            "member_name": {"type": "string", "description": "Optional member name such as 'Maya' or 'me'."},
            "child_id": {"type": "string", "description": "Optional child id the preference applies to."},
            "child_name": {"type": "string", "description": "Optional child name such as 'Ava'."},
            "metadata": {"type": "object", "description": "Optional structured metadata."},
        },
        "required": ["label", "value"],
    },
}


def _handle_search_state(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    query = _normalize_text(args.get("query")).lower()
    limit = max(1, min(int(args.get("limit", 10) or 10), 25))
    include_private_review_state = bool(args.get("include_private_review_state"))
    target_date_raw = _normalize_optional_text(args.get("target_date"))
    target_date_value = _parse_iso_date_argument(target_date_raw)
    household = context.store.get_household(context.household_id)
    household_timezone = household.timezone if household is not None else None
    scope = resolve_conversation_scope(
        context.store,
        channel_id=context.channel_id,
        actor_member_id=context.actor_member_id,
    )
    requested_types = args.get("entity_types")
    if isinstance(requested_types, list) and requested_types:
        entity_types = {_normalize_text(item) for item in requested_types if _normalize_text(item)}
    else:
        entity_types = {
            "work_items",
            "routines",
            "nudges",
            "meals",
            "shopping_items",
            "events",
            "children",
            "schools",
            "activities",
            "contacts",
            "places",
            "providers",
            "assets",
            "preferences",
        }

    results: dict[str, list[dict[str, Any]]] = {}
    all_events: list[HouseholdEvent] = []
    duplicate_event_groups: list[dict[str, Any]] = []

    if "work_items" in entity_types:
        matches = [
            _serialize_work_item(item)
            for item in context.store.list_household_work_items(household_id=context.household_id)
            if owner_scoped_item_visible(scope, owner_member_id=item.owner_member_id)
            if _matches_query([item.title, item.description, item.status.value, item.metadata], query)
        ]
        results["work_items"] = matches[:limit]

    if "routines" in entity_types:
        matches = [
            _serialize_routine(item)
            for item in context.store.list_household_routines(household_id=context.household_id)
            if owner_scoped_item_visible(scope, owner_member_id=item.owner_member_id)
            if _matches_query([item.title, item.description, item.cadence, item.status.value, item.metadata], query)
        ]
        results["routines"] = matches[:limit]

    if "nudges" in entity_types:
        matches = [
            _serialize_nudge(item)
            for item in context.store.list_household_nudges(household_id=context.household_id)
            if recipient_scoped_item_visible(scope, recipient_member_id=item.recipient_member_id)
            if _matches_query([item.message, item.status.value, item.metadata], query)
        ]
        results["nudges"] = matches[:limit]

    if "meals" in entity_types:
        matches = [
            _serialize_meal(item)
            for item in context.store.list_household_meals(household_id=context.household_id)
            if _matches_query([item.title, item.description, item.meal_type, item.status.value, item.metadata], query)
        ]
        results["meals"] = matches[:limit]

    if "shopping_items" in entity_types:
        matches = []
        for item in context.store.list_household_shopping_items(household_id=context.household_id):
            meal_title = None
            if item.meal_id:
                meal = context.store.get_household_meal(item.meal_id)
                if meal is not None:
                    meal_title = meal.title
            if not _matches_query(
                [item.title, item.notes, item.list_name, item.status.value, meal_title, item.metadata],
                query,
            ):
                continue
            matches.append(_serialize_shopping_item(item, meal_title=meal_title))
        results["shopping_items"] = matches[:limit]

    if "events" in entity_types:
        all_events = context.store.list_household_events(household_id=context.household_id)
        event_matches = [
            item
            for item in all_events
            if _matches_query([item.title, item.description, item.location, item.status.value, item.metadata], query)
        ]
        results["events"] = [_serialize_event(item) for item in event_matches[:limit]]
        duplicate_event_groups = _build_event_duplicate_groups(context, events=all_events, limit=max(limit, 25))
        if query:
            duplicate_event_groups = [
                group
                for group in duplicate_event_groups
                if any(
                    _matches_query(
                        [
                            event.get("title"),
                            event.get("description"),
                            event.get("location"),
                            event.get("status"),
                            event.get("metadata"),
                        ],
                        query,
                    )
                    for event in group["events"]
                )
            ]
    elif target_date_raw or _query_needs_target_date(query):
        all_events = context.store.list_household_events(household_id=context.household_id)

    date_coverage = _build_date_coverage(
        query=query,
        target_date_raw=target_date_raw,
        target_date_value=target_date_value,
        household_timezone=household_timezone,
        events=all_events,
    )

    if "children" in entity_types:
        matches = [
            _serialize_child(item)
            for item in context.store.list_child_profiles(household_id=context.household_id)
            if _matches_query([item.full_name, item.birthdate, item.metadata], query)
        ]
        results["children"] = matches[:limit]

    profile_mapping = {
        "profile_items": None,
        "schools": HouseholdProfileKind.SCHOOL,
        "activities": HouseholdProfileKind.ACTIVITY,
        "contacts": HouseholdProfileKind.CONTACT,
        "places": HouseholdProfileKind.PLACE,
        "providers": HouseholdProfileKind.PROVIDER,
        "assets": HouseholdProfileKind.ASSET,
        "preferences": HouseholdProfileKind.PREFERENCE,
    }
    for entity_type, kind in profile_mapping.items():
        if entity_type not in entity_types:
            continue
        items = (
            context.store.list_household_profile_items(household_id=context.household_id)
            if kind is None
            else context.store.list_household_profile_items(household_id=context.household_id, kind=kind)
        )
        matches = [
            _serialize_profile_item(item)
            for item in items
            if member_scoped_item_visible(scope, member_id=item.member_id)
            if _matches_query([item.label, item.kind.value, item.metadata], query)
        ]
        results[entity_type] = matches[:limit]

    return json.dumps(
        {
            "household_id": context.household_id,
            "visibility": _build_visibility_summary(
                context,
                query=query,
                limit=limit,
                include_private_review_state=include_private_review_state,
            ),
            "results": results,
            "event_insights": {
                "likely_duplicate_groups": duplicate_event_groups[:limit],
            },
            "date_answer_evidence_contract": _build_date_answer_evidence_contract(),
            "date_coverage": date_coverage,
        }
    )


def _handle_apply_candidate_review(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    candidate_id = _normalize_text(args.get("candidate_id"))
    if not candidate_id:
        return json.dumps({"error": "Missing required parameter: candidate_id"})
    resolution = _normalize_text(args.get("resolution")).lower()
    if resolution not in {"confirm", "reject", "skip"}:
        return json.dumps({"error": "Missing or invalid parameter: resolution"})
    source_visibility_raw = _normalize_optional_text(args.get("source_visibility"))
    source_visibility = None
    if source_visibility_raw:
        source_visibility = _enum_value(HouseholdSourceVisibility, source_visibility_raw, None)
    active_review_candidate_id = _active_review_candidate_id(context, requested_candidate_id=candidate_id)
    if active_review_candidate_id != candidate_id:
        return json.dumps(
            {
                "error": "Candidate review is only allowed for the one currently surfaced review item in this DM.",
                "active_candidate_id": active_review_candidate_id,
            }
        )
    overrides = {
        key: value
        for key, value in {
            "title": _normalize_optional_text(args.get("title")),
            "starts_at": _normalize_optional_text(args.get("starts_at")),
            "ends_at": _normalize_optional_text(args.get("ends_at")),
            "timezone": _normalize_optional_text(args.get("timezone")),
            "all_day": args.get("all_day") if "all_day" in args else None,
            "location": _normalize_optional_text(args.get("location")),
            "description": _normalize_optional_text(args.get("description")),
        }.items()
        if value is not None
    }
    review_service = FlorenceCandidateReviewService(context.store)
    related_candidate_ids = review_service.resolve_review_group_candidate_ids(
        household_id=context.household_id,
        member_id=context.actor_member_id or "",
        candidate_id=candidate_id,
        candidate_ids=_active_review_candidate_ids(context),
    )
    reply = review_service.apply_review_response(
        candidate_id=candidate_id,
        member_id=context.actor_member_id,
        candidate_ids=related_candidate_ids,
        source_visibility=source_visibility,
        resolution=resolution,
        overrides=overrides if resolution == "confirm" else None,
    )
    candidate = context.store.get_imported_candidate(candidate_id)
    event_id = None
    event = None
    work_item_id = None
    work_item = None
    if candidate is not None:
        event_id = str(candidate.metadata.get("confirmed_event_id") or "").strip() or None
        work_item_id = str(candidate.metadata.get("confirmed_work_item_id") or "").strip() or None
        if event_id:
            event = next(
                (
                    item
                    for item in context.store.list_household_events(household_id=context.household_id)
                    if item.id == event_id
                ),
                None,
            )
        if work_item_id:
            work_item = context.store.get_household_work_item(work_item_id)
    return json.dumps(
        {
            "result": {
                "candidate_id": candidate_id,
                "candidate_ids": related_candidate_ids,
                "resolution": resolution,
                "reply_text": reply.reply_text,
                "group_announcement": reply.group_announcement,
                "event": _serialize_event(event) if event is not None else None,
                "work_item": _serialize_work_item(work_item) if work_item is not None else None,
            }
        }
    )


def _parse_iso_datetime_argument(value: str | None) -> datetime | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except Exception:
        return None


def _sync_household_calendar_projection(context: FlorenceHouseholdToolContext) -> None:
    try:
        FlorenceHouseholdCalendarProjectionService(context.store).sync_household(
            household_id=context.household_id,
        )
    except Exception:
        return


def _active_nudge_id(context: FlorenceHouseholdToolContext) -> str | None:
    latest_assistant = FlorenceChannelLog(context.store).latest_assistant_message(channel_id=context.channel_id, limit=8)
    if latest_assistant is None:
        return None
    return str(latest_assistant.metadata.get(PENDING_ACTION_TARGET_ID_KEY) or "").strip() or None


def _handle_apply_nudge_action(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    nudge_id = _normalize_text(args.get("nudge_id"))
    if not nudge_id:
        return json.dumps({"error": "Missing required parameter: nudge_id"})
    action = _normalize_text(args.get("action")).lower()
    if action not in {"done", "snooze", "skip"}:
        return json.dumps({"error": "Missing or invalid parameter: action"})
    active_nudge_id = _active_nudge_id(context)
    if active_nudge_id != nudge_id:
        return json.dumps(
            {
                "error": "Reminder updates are only allowed for the one currently surfaced reminder in this DM.",
                "active_nudge_id": active_nudge_id,
            }
        )

    manager = FlorenceHouseholdManagerService(context.store)
    if context.actor_member_id is None:
        return json.dumps({"error": "Reminder updates require an active household member."})

    if action == "skip":
        return json.dumps({"result": {"nudge_id": nudge_id, "action": action, "reply_text": "Okay. I’ll leave that reminder as-is for now."}})

    if action == "done":
        result = manager.complete_actionable_nudge(
            household_id=context.household_id,
            member_id=context.actor_member_id,
            channel_id=context.channel_id,
            nudge_id=nudge_id,
        )
        if result is None:
            return json.dumps({"error": "Unable to resolve the active reminder."})
        return json.dumps({"result": {"nudge_id": nudge_id, "action": action, "reply_text": result.reply_text}})

    scheduled_for = _parse_iso_datetime_argument(args.get("scheduled_for"))
    if scheduled_for is None:
        return json.dumps({"error": "Missing or invalid parameter: scheduled_for"})
    result = manager.snooze_actionable_nudge(
        household_id=context.household_id,
        member_id=context.actor_member_id,
        channel_id=context.channel_id,
        nudge_id=nudge_id,
        scheduled_for=scheduled_for,
    )
    if result is None:
        return json.dumps({"error": "Unable to resolve the active reminder."})
    return json.dumps(
        {
            "result": {
                "nudge_id": nudge_id,
                "action": action,
                "scheduled_for": scheduled_for.isoformat(),
                "reply_text": result.reply_text,
            }
        }
    )


def _handle_apply_onboarding_update(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    if context.actor_member_id is None:
        return json.dumps({"error": "Onboarding updates require an active household member."})
    channel = context.store.get_channel(context.channel_id)
    if channel is None or not channel.provider_channel_id:
        return json.dumps({"error": "Onboarding updates require a DM thread context."})

    service = _build_onboarding_service(context)
    previous_stage = service.get_or_create_session(
        household_id=context.household_id,
        member_id=context.actor_member_id,
        thread_id=channel.provider_channel_id,
    ).stage
    transition = service.apply_explicit_update(
        household_id=context.household_id,
        member_id=context.actor_member_id,
        thread_id=channel.provider_channel_id,
        parent_name=_normalize_optional_text(args.get("parent_name")),
        child_names=[
            item
            for item in (
                _normalize_optional_text(item)
                for item in (args.get("child_names") if isinstance(args.get("child_names"), list) else [])
            )
            if item
        ],
        child_name=_normalize_optional_text(args.get("child_name")),
        age=_normalize_optional_text(args.get("age")),
        school=_normalize_optional_text(args.get("school")),
        activities=[
            item
            for item in (
                _normalize_optional_text(item)
                for item in (args.get("activities") if isinstance(args.get("activities"), list) else [])
            )
            if item
        ]
        or None,
        google_connected=bool(args.get("google_connected")) if "google_connected" in args else None,
    )
    raw_child_updates = args.get("child_updates")
    if isinstance(raw_child_updates, list):
        for raw_update in raw_child_updates:
            if not isinstance(raw_update, dict):
                continue
            transition = service.apply_explicit_update(
                household_id=context.household_id,
                member_id=context.actor_member_id,
                thread_id=channel.provider_channel_id,
                child_name=_normalize_optional_text(raw_update.get("name")),
                age=_normalize_optional_text(raw_update.get("age")),
                school=_normalize_optional_text(raw_update.get("school")),
                activities=[
                    item
                    for item in (
                        _normalize_optional_text(item)
                        for item in (raw_update.get("activities") if isinstance(raw_update.get("activities"), list) else [])
                    )
                    if item
                ]
                or None,
            )
    if transition.state.is_complete:
        manager = FlorenceHouseholdManagerService(context.store)
        existing = context.store.list_pilot_events(
            household_id=context.household_id,
            event_type="onboarding_complete",
            limit=5,
        )
        if not any(event.member_id == context.actor_member_id for event in existing):
            manager.finalize_onboarding_completion(
                household_id=context.household_id,
                member_id=context.actor_member_id,
                channel_id=context.channel_id,
            )
    prompt_messages = service.get_transition_messages(
        transition,
        previous_stage=previous_stage,
        household_id=context.household_id,
        member_id=context.actor_member_id,
        thread_id=channel.provider_channel_id,
    )
    reply_metadata: dict[str, object] = {}
    if (
        transition.state.stage == OnboardingStage.CONNECT_GOOGLE
        or (previous_stage == OnboardingStage.COLLECT_PARENT_NAME and not transition.state.google_connected)
    ):
        reply_metadata = build_google_connect_prompt_metadata()
    return json.dumps(
        {
            "result": {
                "stage": transition.state.stage.value,
                "is_complete": transition.state.is_complete,
                "child_names": list(transition.state.child_names),
                "reply_messages": list(prompt_messages),
                "reply_metadata": reply_metadata,
            }
        }
    )


def _handle_request_parent_link(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    if context.actor_member_id is None:
        return json.dumps({"error": "Parent linking requires an active household member."})

    phone_number = _normalize_text(args.get("phone_number"))
    display_name = _normalize_optional_text(args.get("display_name"))
    request_id = _normalize_optional_text(args.get("request_id"))
    send_invite_now = bool(args.get("send_invite_now"))
    service = FlorenceHouseholdLinkService(context.store)
    request = None
    if request_id:
        request = context.store.get_household_link_request(request_id)
        if request is None:
            return json.dumps({"error": "Unknown parent-link request id."})
    elif phone_number:
        request = service.create_phone_link_request(
            household_id=context.household_id,
            inviting_member_id=context.actor_member_id,
            invited_phone=phone_number,
            invited_display_name=display_name,
        )
    elif send_invite_now:
        request = service.find_sendable_request_for_inviting_member(
            household_id=context.household_id,
            inviting_member_id=context.actor_member_id,
        )
        if request is None:
            return json.dumps({"error": "No pending parent-link request is waiting for an invite text."})
    else:
        return json.dumps({"error": "Missing required parameter: phone_number"})

    if request.inviting_member_id != context.actor_member_id:
        return json.dumps({"error": "Parent-link request belongs to a different household member."})

    if send_invite_now:
        try:
            action_result = service.send_invited_parent_invite(
                request_id=request.id,
                inviting_member_id=context.actor_member_id,
                send_invite=lambda active_request, invite_text: _send_parent_link_invite_via_current_transport(
                    context,
                    request=active_request,
                    invite_text=invite_text,
                ),
            )
        except ValueError as exc:
            error_code = str(exc)
            if error_code == "parent_link_invite_transport_unsupported":
                return json.dumps(
                    {
                        "error": "Parent-link invite texts are only available from a Sendblue-backed parent DM right now."
                    }
                )
            return json.dumps({"error": error_code})
        request = action_result.request
        reply_text = action_result.reply_text
        invite_sent = True
    else:
        reply_text = service.build_inviting_request_reply(request)
        invite_sent = bool(str((request.metadata or {}).get("invited_message_sent_at") or "").strip())
    return json.dumps(
        {
            "result": {
                "request_id": request.id,
                "status": request.status.value,
                "requires_merge_confirmation": bool(request.requires_merge_confirmation),
                "invited_display_name": request.invited_display_name,
                "expires_at": request.expires_at,
                "invite_sent": invite_sent,
                "reply_text": reply_text,
            }
        }
    )


def _active_review_candidate_id(
    context: FlorenceHouseholdToolContext,
    *,
    requested_candidate_id: str,
) -> str | None:
    latest_assistant = FlorenceChannelLog(context.store).latest_assistant_message(channel_id=context.channel_id, limit=8)
    if latest_assistant is None:
        return None
    if latest_assistant.metadata.get("protocol_kind") != CANDIDATE_REVIEW_PROMPT_KIND:
        return None
    explicit_candidate_ids = [
        str(candidate_id).strip()
        for candidate_id in list(latest_assistant.metadata.get(PENDING_ACTION_TARGET_IDS_KEY) or [])
        if str(candidate_id).strip()
    ]
    if explicit_candidate_ids:
        if requested_candidate_id in explicit_candidate_ids:
            return requested_candidate_id
        return explicit_candidate_ids[0]
    explicit_candidate_id = str(latest_assistant.metadata.get(PENDING_ACTION_TARGET_ID_KEY) or "").strip()
    if explicit_candidate_id:
        return explicit_candidate_id
    if context.actor_member_id is None:
        return None
    prompt = FlorenceCandidateReviewService(context.store).build_next_dm_review_prompt(
        household_id=context.household_id,
        member_id=context.actor_member_id,
    )
    if prompt is None:
        return None
    if prompt.candidate.id == requested_candidate_id:
        return requested_candidate_id
    return prompt.candidate.id


def _active_review_candidate_ids(context: FlorenceHouseholdToolContext) -> list[str]:
    latest_assistant = FlorenceChannelLog(context.store).latest_assistant_message(channel_id=context.channel_id, limit=8)
    if latest_assistant is None:
        return []
    if latest_assistant.metadata.get("protocol_kind") != CANDIDATE_REVIEW_PROMPT_KIND:
        return []
    return [
        str(candidate_id).strip()
        for candidate_id in list(latest_assistant.metadata.get(PENDING_ACTION_TARGET_IDS_KEY) or [])
        if str(candidate_id).strip()
    ]


def _gmail_query_token(value: str) -> str:
    escaped = value.replace('"', "").strip()
    if not escaped:
        return ""
    return f"\"{escaped}\"" if re.search(r"\s", escaped) else escaped


def _build_google_inbox_query(
    *,
    query: str | None,
    sender: str | None,
    subject: str | None,
    newer_than_days: int,
) -> str:
    parts = [f"newer_than:{newer_than_days}d"]
    if sender:
        token = _gmail_query_token(sender)
        if token:
            parts.append(f"from:{token}")
    if subject:
        token = _gmail_query_token(subject)
        if token:
            parts.append(f"subject:{token}")
    if query:
        parts.append(query)
    return " ".join(part for part in parts if part)


def _serialize_gmail_item(item, *, connection_email: str) -> dict[str, Any]:
    return {
        "connection_email": connection_email,
        "gmail_message_id": item.gmail_message_id,
        "thread_id": item.thread_id,
        "from_address": item.from_address,
        "subject": item.subject,
        "snippet": item.snippet,
        "body_text": item.body_text,
        "attachment_text": item.attachment_text,
        "attachment_count": item.attachment_count,
        "received_at": item.received_at.isoformat() if item.received_at is not None else None,
    }


def _serialize_calendar_item(item, *, connection_email: str) -> dict[str, Any]:
    return {
        "connection_email": connection_email,
        "google_event_id": item.google_event_id,
        "title": item.title,
        "description": item.description,
        "location": item.location,
        "html_link": item.html_link,
        "starts_at": item.starts_at.isoformat() if item.starts_at is not None else None,
        "ends_at": item.ends_at.isoformat() if item.ends_at is not None else None,
        "timezone": item.timezone,
        "all_day": bool(item.all_day),
        "updated_at": item.updated_at.isoformat() if item.updated_at is not None else None,
        "calendar_summary": item.calendar_summary,
        "family_member_names": list(item.family_member_names),
        "calendar_id": item.calendar_id,
        "calendar_primary": bool(item.calendar_primary),
    }


def _handle_search_google_inbox(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    query = _normalize_optional_text(args.get("query"))
    sender = _normalize_optional_text(args.get("sender"))
    subject = _normalize_optional_text(args.get("subject"))
    newer_than_days = max(1, min(int(args.get("newer_than_days", 120) or 120), 365))
    max_results = max(1, min(int(args.get("max_results", 5) or 5), 10))

    google_scope = resolve_google_inbox_scope(
        context.store,
        household_id=context.household_id,
        channel_id=context.channel_id,
        actor_member_id=context.actor_member_id,
        query=query,
        sender=sender,
        subject=subject,
    )
    if google_scope.error:
        return json.dumps(
            {
                "error": google_scope.error,
                "search_scope": google_scope.search_scope,
                "scope_reason": google_scope.scope_reason,
                "searched_connection_emails": [],
                "results": [],
            }
        )

    connections = google_scope.connections
    search_scope = google_scope.search_scope
    scope_reason = google_scope.scope_reason
    connection_sync_statuses = [
        {
            "email": connection.email,
            "initial_sync_state": (dict(connection.metadata).get("initial_sync_state") if isinstance(connection.metadata, dict) else None),
            "last_sync_status": (dict(connection.metadata).get("last_sync_status") if isinstance(connection.metadata, dict) else None),
            "last_sync_error": (dict(connection.metadata).get("last_sync_error") if isinstance(connection.metadata, dict) else None),
            "sync_phase": (dict(connection.metadata).get("sync_phase") if isinstance(connection.metadata, dict) else None),
            "gmail_last_synced_at": (dict(connection.metadata).get("gmail_last_synced_at") if isinstance(connection.metadata, dict) else None),
            "last_gmail_item_count": (dict(connection.metadata).get("last_gmail_item_count") if isinstance(connection.metadata, dict) else None),
        }
        for connection in connections
    ]
    mirror_sync_running = any(
        status.get("initial_sync_state") == "running" or status.get("last_sync_status") == "running"
        for status in connection_sync_statuses
    )

    if not connections:
        return json.dumps(
            {
                "error": "No active Google inbox is connected for this household member.",
                "search_scope": search_scope,
                "scope_reason": scope_reason,
                "mirror_sync_running": False,
                "connection_sync_statuses": [],
                "results": [],
            }
        )

    gmail_query = _build_google_inbox_query(
        query=query,
        sender=sender,
        subject=subject,
        newer_than_days=newer_than_days,
    )
    searched_connection_emails = [connection.email for connection in connections]
    mirror_results = context.store.search_google_gmail_messages(
        household_id=context.household_id,
        connection_ids=[connection.id for connection in connections],
        query=query,
        sender=sender,
        subject=subject,
        newer_than_days=newer_than_days,
        limit=max_results,
    )
    results = [
        _serialize_gmail_item(item, connection_email=item.connection_email)
        for item in mirror_results
    ]

    return json.dumps(
        {
            "gmail_query": gmail_query,
            "search_backend": "local_mirror",
            "search_scope": search_scope,
            "scope_reason": scope_reason,
            "mirror_sync_running": mirror_sync_running,
            "connection_sync_statuses": connection_sync_statuses,
            "searched_connection_emails": searched_connection_emails,
            "results": results[:max_results],
        }
    )


def _handle_search_google_calendar(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    query = _normalize_optional_text(args.get("query"))
    calendar_summary = _normalize_optional_text(args.get("calendar_summary"))
    newer_than_days = max(1, min(int(args.get("newer_than_days", 180) or 180), 365))
    max_results = max(1, min(int(args.get("max_results", 5) or 5), 10))

    google_scope = resolve_google_calendar_scope(
        context.store,
        household_id=context.household_id,
        channel_id=context.channel_id,
        actor_member_id=context.actor_member_id,
        query=query,
        calendar_summary=calendar_summary,
    )
    if google_scope.error:
        return json.dumps(
            {
                "error": google_scope.error,
                "search_scope": google_scope.search_scope,
                "scope_reason": google_scope.scope_reason,
                "searched_connection_emails": [],
                "results": [],
            }
        )

    connections = google_scope.connections
    search_scope = google_scope.search_scope
    scope_reason = google_scope.scope_reason
    connection_sync_statuses = [
        {
            "email": connection.email,
            "initial_sync_state": (dict(connection.metadata).get("initial_sync_state") if isinstance(connection.metadata, dict) else None),
            "last_sync_status": (dict(connection.metadata).get("last_sync_status") if isinstance(connection.metadata, dict) else None),
            "last_sync_error": (dict(connection.metadata).get("last_sync_error") if isinstance(connection.metadata, dict) else None),
            "sync_phase": (dict(connection.metadata).get("sync_phase") if isinstance(connection.metadata, dict) else None),
            "calendar_last_synced_at": (dict(connection.metadata).get("calendar_last_synced_at") if isinstance(connection.metadata, dict) else None),
            "last_calendar_item_count": (dict(connection.metadata).get("last_calendar_item_count") if isinstance(connection.metadata, dict) else None),
        }
        for connection in connections
    ]
    mirror_sync_running = any(
        status.get("initial_sync_state") == "running" or status.get("last_sync_status") == "running"
        for status in connection_sync_statuses
    )

    if not connections:
        return json.dumps(
            {
                "error": "No active Google Calendar is connected for this household member.",
                "search_scope": search_scope,
                "scope_reason": scope_reason,
                "mirror_sync_running": False,
                "connection_sync_statuses": [],
                "results": [],
            }
        )

    searched_connection_emails = [connection.email for connection in connections]
    mirror_results = context.store.search_google_calendar_events(
        household_id=context.household_id,
        connection_ids=[connection.id for connection in connections],
        query=query,
        calendar_summary=calendar_summary,
        newer_than_days=newer_than_days,
        limit=max_results,
    )
    results = [
        _serialize_calendar_item(item, connection_email=item.connection_email)
        for item in mirror_results
    ]

    return json.dumps(
        {
            "search_backend": "local_mirror",
            "search_scope": search_scope,
            "scope_reason": scope_reason,
            "mirror_sync_running": mirror_sync_running,
            "connection_sync_statuses": connection_sync_statuses,
            "searched_connection_emails": searched_connection_emails,
            "results": results[:max_results],
        }
    )


def _handle_import_calendar_feed(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    raw_url = _normalize_text(args.get("url"))
    if not raw_url:
        return json.dumps({"error": "Missing required parameter: url"})
    household = context.store.get_household(context.household_id)
    default_timezone = household.timezone if household is not None else "UTC"
    title_prefix = _normalize_optional_text(args.get("title_prefix"))
    child_name = _normalize_optional_text(args.get("child_name"))
    child_id = _resolve_child_id(context, child_name=child_name) if child_name else None
    child_display_name = None
    if child_id:
        child = next(
            (item for item in context.store.list_child_profiles(household_id=context.household_id) if item.id == child_id),
            None,
        )
        child_display_name = child.full_name if child is not None else child_name
    max_events = max(1, min(int(args.get("max_events", 75) or 75), 250))
    base_metadata = _normalize_metadata(args.get("metadata"))

    try:
        normalized_url, feed_text = _fetch_calendar_feed_text(raw_url)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    except httpx.HTTPError as exc:
        return json.dumps({"error": f"Failed to fetch calendar feed: {exc}"})

    calendar_summary, parsed_events = parse_calendar_feed(feed_text, default_timezone=default_timezone)
    imported_events: list[dict[str, Any]] = []
    imported_count = 0
    skipped_without_start = 0
    for item in parsed_events[:max_events]:
        if not item.starts_at:
            skipped_without_start += 1
            continue
        title = _normalize_text(item.summary)
        if title_prefix:
            if title:
                title = f"{title_prefix} — {title}"
            else:
                title = title_prefix
        if child_display_name and child_display_name.lower() not in title.lower():
            title = f"{child_display_name} — {title}" if title else child_display_name
        title = title or "Calendar event"
        metadata = {
            "imported_from_calendar_feed": True,
            "calendar_feed_url": normalized_url,
            "calendar_feed_uid": item.uid,
            "calendar_feed_recurrence_id": item.recurrence_id,
            "calendar_summary": calendar_summary,
        }
        if child_id:
            metadata["child_id"] = child_id
        if child_display_name:
            metadata["child_name"] = child_display_name
        metadata.update(base_metadata)
        event_id = _stable_id(
            "evt_feed",
            context.household_id,
            normalized_url,
            str(item.uid or title),
            str(item.recurrence_id or item.starts_at or ""),
        )
        existing_event = context.store.get_household_event(event_id)
        merged_metadata = dict(existing_event.metadata) if existing_event is not None and isinstance(existing_event.metadata, dict) else {}
        merged_metadata.update(metadata)
        event = context.store.upsert_household_event(
            HouseholdEvent(
                id=event_id,
                household_id=context.household_id,
                title=title,
                starts_at=item.starts_at,
                ends_at=item.ends_at or item.starts_at,
                timezone=item.timezone or default_timezone,
                all_day=item.all_day,
                location=_normalize_optional_text(item.location),
                description=_normalize_optional_text(item.description),
                source_candidate_id=existing_event.source_candidate_id if existing_event is not None else None,
                status=HouseholdEventStatus.CONFIRMED,
                metadata=merged_metadata,
            )
        )
        imported_events.append(_serialize_event(event))
        imported_count += 1

    _sync_household_calendar_projection(context)
    return json.dumps(
        {
            "feed_url": normalized_url,
            "calendar_summary": calendar_summary,
            "imported_count": imported_count,
            "skipped_without_start_count": skipped_without_start,
            "results": imported_events,
        }
    )


def _handle_upsert_work_item(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(context.store)
    work_item_id = _normalize_optional_text(args.get("id"))
    existing_item = context.store.get_household_work_item(work_item_id) if work_item_id else None
    title = _normalize_text(args.get("title")) or (existing_item.title if existing_item is not None else "")
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    owner_member_id = _resolve_member_id(
        context,
        member_id=_normalize_optional_text(args.get("owner_member_id")),
        member_name=_normalize_optional_text(args.get("owner_member_name")),
    )
    item = manager.upsert_work_item(
        HouseholdWorkItem(
            id=work_item_id or _stable_id("work", context.household_id, title.lower()),
            household_id=context.household_id,
            title=title,
            description=_normalize_optional_text(args.get("description")) or (existing_item.description if existing_item is not None else None),
            status=_enum_value(
                HouseholdWorkItemStatus,
                args.get("status"),
                existing_item.status if existing_item is not None else HouseholdWorkItemStatus.OPEN,
            ),
            owner_member_id=owner_member_id or (existing_item.owner_member_id if existing_item is not None else None),
            child_id=existing_item.child_id if existing_item is not None else None,
            due_at=_normalize_optional_text(args.get("due_at")) or (existing_item.due_at if existing_item is not None else None),
            starts_at=_normalize_optional_text(args.get("starts_at")) or (existing_item.starts_at if existing_item is not None else None),
            completed_at=_normalize_optional_text(args.get("completed_at")) or (existing_item.completed_at if existing_item is not None else None),
            metadata=(
                {
                    **(dict(existing_item.metadata) if existing_item is not None and isinstance(existing_item.metadata, dict) else {}),
                    **_normalize_metadata(args.get("metadata")),
                }
                if "metadata" in args or existing_item is not None
                else _normalize_metadata(args.get("metadata"))
            ),
        )
    )
    return json.dumps({"result": _serialize_work_item(item)})


def _handle_resolve_merge_followup(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    service = FlorenceHouseholdLinkService(context.store)
    work_item_id = _normalize_optional_text(args.get("work_item_id"))
    if not work_item_id:
        return json.dumps({"error": "Missing required parameter: work_item_id"})
    try:
        group_index = max(0, int(args.get("group_index") or 0))
    except (TypeError, ValueError):
        return json.dumps({"error": "Invalid parameter: group_index"})
    try:
        result = service.resolve_merge_followup(
            household_id=context.household_id,
            work_item_id=work_item_id,
            actor_member_id=context.actor_member_id,
            birthdate=_normalize_optional_text(args.get("birthdate")),
            school=_normalize_optional_text(args.get("school")),
            resolution_note=_normalize_optional_text(args.get("resolution_note")),
            group_index=group_index,
        )
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(
        {
            "result": _serialize_work_item(result.work_item),
            "child": _serialize_child(result.child) if result.child is not None else None,
            "remaining_conflicts": list(result.remaining_conflicts or []),
            "resolved": result.work_item.status == HouseholdWorkItemStatus.DONE,
        }
    )


def _handle_upsert_event(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    event_id = _normalize_optional_text(args.get("id"))
    existing_event = context.store.get_household_event(event_id) if event_id else None
    title = _normalize_text(args.get("title")) or (existing_event.title if existing_event is not None else "")
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    starts_at = _normalize_optional_text(args.get("starts_at")) or (existing_event.starts_at if existing_event is not None else None)
    ends_at = _normalize_optional_text(args.get("ends_at")) or (existing_event.ends_at if existing_event is not None else None)
    timezone_name = _normalize_optional_text(args.get("timezone")) or (existing_event.timezone if existing_event is not None else None)
    location = _normalize_optional_text(args.get("location")) or (existing_event.location if existing_event is not None else None)
    description = _normalize_optional_text(args.get("description")) or (existing_event.description if existing_event is not None else None)
    if "all_day" in args:
        all_day = bool(args.get("all_day"))
    else:
        all_day = existing_event.all_day if existing_event is not None else False
    metadata = dict(existing_event.metadata) if existing_event is not None and isinstance(existing_event.metadata, dict) else {}
    if "metadata" in args:
        metadata.update(_normalize_metadata(args.get("metadata")))
    event = context.store.upsert_household_event(
        HouseholdEvent(
            id=event_id or _stable_id("evt", context.household_id, starts_at or "unscheduled", title.lower()),
            household_id=context.household_id,
            title=title,
            starts_at=starts_at,
            ends_at=ends_at,
            timezone=timezone_name,
            all_day=all_day,
            location=location,
            description=description,
            source_candidate_id=existing_event.source_candidate_id if existing_event is not None else None,
            status=_enum_value(
                HouseholdEventStatus,
                args.get("status"),
                existing_event.status
                if existing_event is not None
                else (HouseholdEventStatus.CONFIRMED if starts_at else HouseholdEventStatus.TENTATIVE),
            ),
            metadata=metadata,
        )
    )
    _sync_household_calendar_projection(context)
    return json.dumps({"result": _serialize_event(event)})


def _handle_upsert_routine(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(context.store)
    title = _normalize_text(args.get("title"))
    cadence = _normalize_text(args.get("cadence"))
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    if not cadence:
        return json.dumps({"error": "Missing required parameter: cadence"})
    owner_member_id = _resolve_member_id(
        context,
        member_id=_normalize_optional_text(args.get("owner_member_id")),
        member_name=_normalize_optional_text(args.get("owner_member_name")),
    )
    routine = manager.upsert_routine(
        HouseholdRoutine(
            id=_normalize_optional_text(args.get("id"))
            or _stable_id("routine", context.household_id, title.lower()),
            household_id=context.household_id,
            title=title,
            cadence=cadence,
            description=_normalize_optional_text(args.get("description")),
            status=_enum_value(HouseholdRoutineStatus, args.get("status"), HouseholdRoutineStatus.ACTIVE),
            owner_member_id=owner_member_id,
            next_due_at=_normalize_optional_text(args.get("next_due_at")),
            last_completed_at=_normalize_optional_text(args.get("last_completed_at")),
            metadata=_normalize_metadata(args.get("metadata")),
        )
    )
    return json.dumps({"result": _serialize_routine(routine)})


def _handle_schedule_nudge(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(context.store)
    message = _normalize_text(args.get("message"))
    scheduled_for = _normalize_text(args.get("scheduled_for"))
    if not message:
        return json.dumps({"error": "Missing required parameter: message"})
    if not scheduled_for:
        return json.dumps({"error": "Missing required parameter: scheduled_for"})
    recipient_member_id = _resolve_member_id(
        context,
        member_id=_normalize_optional_text(args.get("recipient_member_id")),
        member_name=_normalize_optional_text(args.get("recipient_member_name")),
    )
    nudge = manager.schedule_nudge(
        household_id=context.household_id,
        message=message,
        scheduled_for=scheduled_for,
        target_kind=_enum_value(HouseholdNudgeTargetKind, args.get("target_kind"), HouseholdNudgeTargetKind.GENERAL),
        target_id=_normalize_optional_text(args.get("target_id")),
        recipient_member_id=recipient_member_id,
        channel_id=_normalize_optional_text(args.get("channel_id")),
        metadata=_normalize_metadata(args.get("metadata")),
    )
    return json.dumps({"result": _serialize_nudge(nudge)})


def _handle_upsert_meal(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(context.store)
    title = _normalize_text(args.get("title"))
    meal_type = _normalize_text(args.get("meal_type"))
    scheduled_for = _normalize_text(args.get("scheduled_for"))
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    if not meal_type:
        return json.dumps({"error": "Missing required parameter: meal_type"})
    if not scheduled_for:
        return json.dumps({"error": "Missing required parameter: scheduled_for"})
    meal = manager.upsert_meal(
        HouseholdMeal(
            id=_normalize_optional_text(args.get("id"))
            or _stable_id("meal", context.household_id, scheduled_for, meal_type.lower(), title.lower()),
            household_id=context.household_id,
            title=title,
            meal_type=meal_type,
            scheduled_for=scheduled_for,
            description=_normalize_optional_text(args.get("description")),
            status=_enum_value(HouseholdMealStatus, args.get("status"), HouseholdMealStatus.PLANNED),
            metadata=_normalize_metadata(args.get("metadata")),
        )
    )
    return json.dumps({"result": _serialize_meal(meal)})


def _handle_upsert_shopping_item(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(context.store)
    title = _normalize_text(args.get("title"))
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    list_name = _normalize_optional_text(args.get("list_name")) or "groceries"
    meal_id = _resolve_meal_id(
        context,
        meal_id=_normalize_optional_text(args.get("meal_id")),
        meal_title=_normalize_optional_text(args.get("meal_title")),
    )
    item = manager.upsert_shopping_item(
        HouseholdShoppingItem(
            id=_normalize_optional_text(args.get("id"))
            or _stable_id("shopping", context.household_id, list_name.lower(), title.lower()),
            household_id=context.household_id,
            title=title,
            list_name=list_name,
            status=_enum_value(
                HouseholdShoppingItemStatus,
                args.get("status"),
                HouseholdShoppingItemStatus.NEEDED,
            ),
            quantity=_normalize_optional_text(args.get("quantity")),
            unit=_normalize_optional_text(args.get("unit")),
            notes=_normalize_optional_text(args.get("notes")),
            meal_id=meal_id,
            needed_by=_normalize_optional_text(args.get("needed_by")),
            metadata=_normalize_metadata(args.get("metadata")),
        )
    )
    return json.dumps({"result": _serialize_shopping_item(item)})


def _handle_record_preference(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(
        context.store,
        household_chat_service_getter=(
            (lambda: context.household_chat_service)
            if context.household_chat_service is not None
            else None
        ),
    )
    label = _normalize_text(args.get("label"))
    value = _normalize_text(args.get("value"))
    if not label:
        return json.dumps({"error": "Missing required parameter: label"})
    if not value:
        return json.dumps({"error": "Missing required parameter: value"})
    subject_member_id = _resolve_member_id(
        context,
        member_id=_normalize_optional_text(args.get("member_id")),
        member_name=_normalize_optional_text(args.get("member_name")),
    )
    subject_child_id = _resolve_child_id(
        context,
        child_id=_normalize_optional_text(args.get("child_id")),
        child_name=_normalize_optional_text(args.get("child_name")),
    )
    preference_item = manager.record_preference(
        household_id=context.household_id,
        label=label,
        value=value,
        category=_normalize_optional_text(args.get("category")) or "general",
        member_id=subject_member_id,
        child_id=subject_child_id,
        recorded_by_member_id=context.actor_member_id,
        channel_id=context.channel_id,
        metadata=_normalize_metadata(args.get("metadata")),
    )
    category = str(preference_item.metadata.get("category") or "").strip().lower()
    refreshed_routines: list[dict[str, Any]] = []
    if category in {"operating_rule", "operating_preference", "support_type", "automation_boundary"}:
        refreshed_routines = [
            _serialize_routine(routine)
            for routine in manager.ensure_briefing_routines(household_id=context.household_id)
        ]
    return json.dumps(
        {
            "result": _serialize_profile_item(preference_item),
            "briefing_routines_refreshed": bool(refreshed_routines),
            "briefing_routines": refreshed_routines,
        }
    )


registry.register(
    name="household_search_state",
    toolset="florence_household",
    schema=SEARCH_STATE_SCHEMA,
    handler=_handle_search_state,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_search_google_inbox",
    toolset="florence_household",
    schema=SEARCH_GOOGLE_INBOX_SCHEMA,
    handler=_handle_search_google_inbox,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_search_google_calendar",
    toolset="florence_household",
    schema=SEARCH_GOOGLE_CALENDAR_SCHEMA,
    handler=_handle_search_google_calendar,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_import_calendar_feed",
    toolset="florence_household",
    schema=IMPORT_CALENDAR_FEED_SCHEMA,
    handler=_handle_import_calendar_feed,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_apply_candidate_review",
    toolset="florence_household",
    schema=APPLY_CANDIDATE_REVIEW_SCHEMA,
    handler=_handle_apply_candidate_review,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_apply_nudge_action",
    toolset="florence_household",
    schema=APPLY_NUDGE_ACTION_SCHEMA,
    handler=_handle_apply_nudge_action,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_apply_onboarding_update",
    toolset="florence_household",
    schema=APPLY_ONBOARDING_UPDATE_SCHEMA,
    handler=_handle_apply_onboarding_update,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_request_parent_link",
    toolset="florence_household",
    schema=REQUEST_PARENT_LINK_SCHEMA,
    handler=_handle_request_parent_link,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_upsert_event",
    toolset="florence_household",
    schema=UPSERT_EVENT_SCHEMA,
    handler=_handle_upsert_event,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_upsert_work_item",
    toolset="florence_household",
    schema=UPSERT_WORK_ITEM_SCHEMA,
    handler=_handle_upsert_work_item,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_resolve_merge_followup",
    toolset="florence_household",
    schema=RESOLVE_MERGE_FOLLOWUP_SCHEMA,
    handler=_handle_resolve_merge_followup,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_upsert_routine",
    toolset="florence_household",
    schema=UPSERT_ROUTINE_SCHEMA,
    handler=_handle_upsert_routine,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_schedule_nudge",
    toolset="florence_household",
    schema=SCHEDULE_NUDGE_SCHEMA,
    handler=_handle_schedule_nudge,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_upsert_meal",
    toolset="florence_household",
    schema=UPSERT_MEAL_SCHEMA,
    handler=_handle_upsert_meal,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_upsert_shopping_item",
    toolset="florence_household",
    schema=UPSERT_SHOPPING_ITEM_SCHEMA,
    handler=_handle_upsert_shopping_item,
    check_fn=_check_household_tool_requirements,
)
registry.register(
    name="household_record_preference",
    toolset="florence_household",
    schema=RECORD_PREFERENCE_SCHEMA,
    handler=_handle_record_preference,
    check_fn=_check_household_tool_requirements,
)
