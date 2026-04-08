"""Florence household-state tools for Hermes-backed household management."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime
from dataclasses import dataclass
from typing import Any

from florence.contracts import (
    CandidateState,
    GoogleSourceKind,
    HouseholdEvent,
    HouseholdEventStatus,
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
from florence.messaging.protocol_types import CANDIDATE_REVIEW_PROMPT_KIND, PENDING_ACTION_TARGET_ID_KEY
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.household_calendar_projection import FlorenceHouseholdCalendarProjectionService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.onboarding_service import FlorenceOnboardingSessionService
from florence.runtime.visibility import resolve_conversation_scope, resolve_google_inbox_scope
from florence.state import FlorenceStateDB
from tools.registry import registry


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


SEARCH_STATE_SCHEMA = {
    "name": "household_search_state",
    "description": (
        "Search Florence household state when you need to pull the latest tracked work, routines, nudges, meals, "
        "shopping items, events, children, or profile items including household preferences. "
        "The result also includes structured scope context showing the current channel scope and tentative tracked "
        "state. Private review details are hidden by default unless you explicitly request them. Use this before "
        "updating existing state if the current household picture is unclear."
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


def _serialize_review_candidate(candidate: ImportedCandidate) -> dict[str, Any]:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    return {
        "id": candidate.id,
        "title": candidate.title,
        "summary": candidate.summary,
        "state": candidate.state.value,
        "source_kind": candidate.source_kind.value,
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
        "Search Florence's mirrored Gmail inbox when the user explicitly asks Florence to check email from a sender, "
        "school, camp, teacher, coach, or keyword. Use this instead of asking the user to forward the email when "
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
            "title": {"type": "string", "description": "Event title."},
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
        "required": ["title"],
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
            "title": {"type": "string", "description": "Short work-item title."},
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
        "required": ["title"],
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
                    "meal_preference",
                    "sharing_preference",
                    "child_preference",
                ],
                "description": "Preference category. Use reminder_style and operating_rule when it should directly shape Florence behavior.",
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

    if "work_items" in entity_types:
        matches = [
            _serialize_work_item(item)
            for item in context.store.list_household_work_items(household_id=context.household_id)
            if _matches_query([item.title, item.description, item.status.value, item.metadata], query)
        ]
        results["work_items"] = matches[:limit]

    if "routines" in entity_types:
        matches = [
            _serialize_routine(item)
            for item in context.store.list_household_routines(household_id=context.household_id)
            if _matches_query([item.title, item.description, item.cadence, item.status.value, item.metadata], query)
        ]
        results["routines"] = matches[:limit]

    if "nudges" in entity_types:
        matches = [
            _serialize_nudge(item)
            for item in context.store.list_household_nudges(household_id=context.household_id)
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
        matches = [
            _serialize_event(item)
            for item in context.store.list_household_events(household_id=context.household_id)
            if _matches_query([item.title, item.description, item.location, item.status.value, item.metadata], query)
        ]
        results["events"] = matches[:limit]

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
    reply = review_service.apply_review_response(
        candidate_id=candidate_id,
        member_id=context.actor_member_id,
        source_visibility=source_visibility,
        resolution=resolution,
        overrides=overrides if resolution == "confirm" else None,
    )
    candidate = context.store.get_imported_candidate(candidate_id)
    event_id = None
    event = None
    if candidate is not None:
        event_id = str(candidate.metadata.get("confirmed_event_id") or "").strip() or None
        if event_id:
            event = next(
                (
                    item
                    for item in context.store.list_household_events(household_id=context.household_id)
                    if item.id == event_id
                ),
                None,
            )
    return json.dumps(
        {
            "result": {
                "candidate_id": candidate_id,
                "resolution": resolution,
                "reply_text": reply.reply_text,
                "group_announcement": reply.group_announcement,
                "event": _serialize_event(event) if event is not None else None,
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

    service = FlorenceOnboardingSessionService(context.store)
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
    return json.dumps(
        {
            "result": {
                "stage": transition.state.stage.value,
                "is_complete": transition.state.is_complete,
                "child_names": list(transition.state.child_names),
                "reply_messages": list(prompt_messages),
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

    if not connections:
        return json.dumps(
            {
                "error": "No active Google inbox is connected for this household member.",
                "search_scope": search_scope,
                "scope_reason": scope_reason,
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
            "searched_connection_emails": searched_connection_emails,
            "results": results[:max_results],
        }
    )


def _handle_upsert_work_item(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    manager = FlorenceHouseholdManagerService(context.store)
    title = _normalize_text(args.get("title"))
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    owner_member_id = _resolve_member_id(
        context,
        member_id=_normalize_optional_text(args.get("owner_member_id")),
        member_name=_normalize_optional_text(args.get("owner_member_name")),
    )
    item = manager.upsert_work_item(
        HouseholdWorkItem(
            id=_normalize_optional_text(args.get("id"))
            or _stable_id("work", context.household_id, title.lower()),
            household_id=context.household_id,
            title=title,
            description=_normalize_optional_text(args.get("description")),
            status=_enum_value(HouseholdWorkItemStatus, args.get("status"), HouseholdWorkItemStatus.OPEN),
            owner_member_id=owner_member_id,
            due_at=_normalize_optional_text(args.get("due_at")),
            starts_at=_normalize_optional_text(args.get("starts_at")),
            completed_at=_normalize_optional_text(args.get("completed_at")),
            metadata=_normalize_metadata(args.get("metadata")),
        )
    )
    return json.dumps({"result": _serialize_work_item(item)})


def _handle_upsert_event(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    title = _normalize_text(args.get("title"))
    if not title:
        return json.dumps({"error": "Missing required parameter: title"})
    starts_at = _normalize_optional_text(args.get("starts_at"))
    event = context.store.upsert_household_event(
        HouseholdEvent(
            id=_normalize_optional_text(args.get("id"))
            or _stable_id("evt", context.household_id, starts_at or "unscheduled", title.lower()),
            household_id=context.household_id,
            title=title,
            starts_at=starts_at,
            ends_at=_normalize_optional_text(args.get("ends_at")),
            timezone=_normalize_optional_text(args.get("timezone")),
            all_day=bool(args.get("all_day")),
            location=_normalize_optional_text(args.get("location")),
            description=_normalize_optional_text(args.get("description")),
            status=_enum_value(
                HouseholdEventStatus,
                args.get("status"),
                HouseholdEventStatus.CONFIRMED if starts_at else HouseholdEventStatus.TENTATIVE,
            ),
            metadata=_normalize_metadata(args.get("metadata")),
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
    if category in {"operating_rule", "operating_preference"}:
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
