"""Florence household-state tools for Hermes-backed household management."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from dataclasses import dataclass
from typing import Any

from florence.contracts import (
    HouseholdMeal,
    HouseholdMealStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    HouseholdShoppingItem,
    HouseholdShoppingItemStatus,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
)
from florence.runtime.services import FlorenceHouseholdManagerService
from florence.state import FlorenceStateDB
from tools.registry import registry

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FlorenceHouseholdToolContext:
    store: FlorenceStateDB
    household_id: str
    actor_member_id: str | None
    channel_id: str


_context_lock = threading.Lock()
_tool_contexts: dict[str, FlorenceHouseholdToolContext] = {}


def set_household_tool_context(
    task_id: str,
    *,
    store: FlorenceStateDB,
    household_id: str,
    actor_member_id: str | None,
    channel_id: str,
) -> None:
    with _context_lock:
        _tool_contexts[task_id] = FlorenceHouseholdToolContext(
            store=store,
            household_id=household_id,
            actor_member_id=actor_member_id,
            channel_id=channel_id,
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
        "shopping items, confirmed events, children, or profile items. Use this before updating existing state "
        "if the current household picture is unclear."
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


def _handle_search_state(args: dict, *, task_id: str | None = None, **_: Any) -> str:
    context = _require_context(task_id)
    query = _normalize_text(args.get("query")).lower()
    limit = max(1, min(int(args.get("limit", 10) or 10), 25))
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
            "results": results,
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


registry.register(
    name="household_search_state",
    toolset="florence_household",
    schema=SEARCH_STATE_SCHEMA,
    handler=_handle_search_state,
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
