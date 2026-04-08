"""Household operating-state helpers for Florence."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from florence.contracts import (
    ChannelType,
    HouseholdBriefingKind,
    HouseholdMeal,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    HouseholdShoppingItem,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    MemberRole,
    PilotEvent,
)
from florence.runtime.services import (
    _clean_label,
    _extract_local_time_from_preferences,
    _local_schedule_days,
    _next_due_local_schedule_iso,
    _parse_iso_datetime,
    _parse_local_time_spec,
    _stable_id,
    _utc_now,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _ReminderActionResult:
    reply_text: str


@dataclass(slots=True)
class _BriefingRoutineSpec:
    kind: HouseholdBriefingKind
    title: str
    hour: int
    minute: int
    days: list[int]
    disabled: bool
    planning_source: str
    planning_preferences_fingerprint: str


class FlorenceHouseholdManagerService:
    """Generic Florence operating-state service for the household agent."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        household_chat_service_getter: Callable[[], Any] | None = None,
    ):
        self.store = store
        self._household_chat_service_getter = household_chat_service_getter

    def upsert_work_item(self, work_item: HouseholdWorkItem) -> HouseholdWorkItem:
        return self.store.upsert_household_work_item(work_item)

    def upsert_routine(self, routine: HouseholdRoutine) -> HouseholdRoutine:
        return self.store.upsert_household_routine(routine)

    def upsert_meal(self, meal: HouseholdMeal) -> HouseholdMeal:
        return self.store.upsert_household_meal(meal)

    def upsert_shopping_item(self, item: HouseholdShoppingItem) -> HouseholdShoppingItem:
        return self.store.upsert_household_shopping_item(item)

    @staticmethod
    def _preference_category(item: HouseholdProfileItem) -> str:
        return (_clean_label(str(item.metadata.get("category") or "")) or "general").lower().replace(" ", "_")

    @staticmethod
    def _preference_statement(item: HouseholdProfileItem) -> str:
        value = str(item.metadata.get("value") or item.metadata.get("summary") or "").strip()
        if not value:
            return item.label
        if item.label.lower() in value.lower():
            return value
        return f"{item.label}: {value}"

    def preference_statements(
        self,
        *,
        household_id: str,
        categories: set[str] | None = None,
    ) -> list[str]:
        normalized_categories = {category.strip().lower() for category in (categories or set()) if category.strip()}
        statements: list[str] = []
        seen: set[str] = set()
        for item in self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        ):
            category = self._preference_category(item)
            if normalized_categories and category not in normalized_categories:
                continue
            statement = self._preference_statement(item)
            key = statement.lower()
            if key in seen:
                continue
            seen.add(key)
            statements.append(statement)
        return statements

    def record_preference(
        self,
        *,
        household_id: str,
        label: str,
        value: str,
        category: str = "general",
        member_id: str | None = None,
        child_id: str | None = None,
        recorded_by_member_id: str | None = None,
        channel_id: str | None = None,
        metadata: dict[str, object] | None = None,
        now: datetime | None = None,
    ) -> HouseholdProfileItem:
        cleaned_label = _clean_label(label)
        cleaned_value = " ".join(str(value).split()).strip()
        if cleaned_label is None:
            raise ValueError("missing_preference_label")
        if not cleaned_value:
            raise ValueError("missing_preference_value")

        normalized_category = _clean_label(category) or "general"
        lowered_category = normalized_category.lower().replace(" ", "_")
        captured_at = (now or _utc_now()).isoformat()
        existing_items = self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        )
        preference_id = _stable_id(
            "pref",
            household_id,
            member_id or "",
            child_id or "",
            cleaned_label.lower(),
        )
        existing_item = next((item for item in existing_items if item.id == preference_id), None)
        item_metadata = dict(existing_item.metadata) if existing_item is not None else {}
        item_metadata.update(dict(metadata or {}))
        item_metadata.update(
            {
                "category": lowered_category,
                "value": cleaned_value,
                "captured_at": captured_at,
                "recorded_by_member_id": recorded_by_member_id,
                "channel_id": channel_id,
            }
        )
        preference_item = HouseholdProfileItem(
            id=preference_id,
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
            label=cleaned_label,
            member_id=member_id,
            child_id=child_id,
            metadata=item_metadata,
        )
        updated_items = [item for item in existing_items if item.id != preference_id]
        updated_items.append(preference_item)
        self.store.replace_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
            items=updated_items,
        )
        self.record_pilot_event(
            household_id=household_id,
            event_type="preference_recorded",
            member_id=recorded_by_member_id,
            channel_id=channel_id,
            metadata={
                "label": cleaned_label,
                "value": cleaned_value,
                "category": lowered_category,
                "subject_member_id": member_id,
                "subject_child_id": child_id,
            },
            created_at=now,
        )
        return preference_item

    def record_pilot_event(
        self,
        *,
        household_id: str,
        event_type: str,
        member_id: str | None = None,
        channel_id: str | None = None,
        metadata: dict[str, object] | None = None,
        created_at: datetime | None = None,
    ) -> PilotEvent:
        created = created_at or _utc_now()
        event = PilotEvent(
            id=_stable_id("pilot", household_id, event_type, str(time.time_ns())),
            household_id=household_id,
            event_type=event_type,
            member_id=member_id,
            channel_id=channel_id,
            metadata=dict(metadata or {}),
            created_at=created.timestamp(),
        )
        return self.store.upsert_pilot_event(event)

    def finalize_onboarding_completion(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
    ) -> None:
        self.ensure_briefing_routines(household_id=household_id)
        self.record_pilot_event(
            household_id=household_id,
            event_type="onboarding_complete",
            member_id=member_id,
            channel_id=channel_id,
        )

    def record_reminder_feedback(
        self,
        *,
        household_id: str,
        feedback_text: str,
        member_id: str | None = None,
        channel_id: str | None = None,
        now: datetime | None = None,
    ) -> HouseholdProfileItem | None:
        cleaned = " ".join(feedback_text.split()).strip()
        if not cleaned:
            return None
        preference_item = self.record_preference(
            household_id=household_id,
            label="Reminder style",
            value=cleaned,
            category="reminder_style",
            recorded_by_member_id=member_id,
            channel_id=channel_id,
            metadata={
                "source": "reminder_feedback",
            },
            now=now,
        )
        self.record_pilot_event(
            household_id=household_id,
            event_type="reminder_feedback_received",
            member_id=member_id,
            channel_id=channel_id,
            metadata={"text": cleaned},
            created_at=now,
        )
        return preference_item

    def ensure_briefing_routines(
        self,
        *,
        household_id: str,
        now: datetime | None = None,
    ) -> list[HouseholdRoutine]:
        household = self.store.get_household(household_id)
        if household is None:
            return []
        current = now or _utc_now()
        timezone_name = household.timezone or "America/Los_Angeles"
        operating_preference_statements = self.preference_statements(
            household_id=household_id,
            categories={"operating_rule", "operating_preference"},
        )
        operating_preferences = " | ".join(operating_preference_statements)
        preferences_fingerprint = _stable_id("briefing_pref", household_id, operating_preferences or "__empty__")
        default_owner = self.default_recipient_member_id(household_id)
        default_channel = self.default_dm_channel_id(household_id=household_id, member_id=default_owner)
        if default_owner is None:
            return []
        routine_specs = (
            self._stored_briefing_routine_specs(
                household_id=household_id,
                preferences_fingerprint=preferences_fingerprint,
            )
            or self._hermes_briefing_routine_specs(
                household_id=household_id,
                channel_id=default_channel,
                actor_member_id=default_owner,
                operating_preferences=operating_preference_statements,
                preferences_fingerprint=preferences_fingerprint,
            )
            or self._fallback_briefing_routine_specs(
                operating_preferences=operating_preferences,
                preferences_fingerprint=preferences_fingerprint,
            )
        )

        upserted: list[HouseholdRoutine] = []
        for spec in routine_specs:
            routine_id = _stable_id("routine", household_id, "briefing", spec.kind.value)
            existing = self.store.get_household_routine(routine_id)
            metadata = {
                "automation_kind": "briefing",
                "brief_kind": spec.kind.value,
                "local_time": f"{spec.hour:02d}:{spec.minute:02d}",
                "days": list(spec.days),
                "channel_id": default_channel,
                "planning_source": spec.planning_source,
                "planning_preferences_fingerprint": spec.planning_preferences_fingerprint,
            }
            cadence = (
                f"briefing on weekdays at {spec.hour:02d}:{spec.minute:02d} local"
                if spec.days == [0, 1, 2, 3, 4]
                else f"briefing at {spec.hour:02d}:{spec.minute:02d} local on days {','.join(str(day) for day in spec.days)}"
            )
            if spec.disabled:
                paused = HouseholdRoutine(
                    id=routine_id,
                    household_id=household_id,
                    title=spec.title,
                    cadence=cadence,
                    description="Automatic Florence household briefing routine",
                    status=HouseholdRoutineStatus.PAUSED,
                    owner_member_id=(existing.owner_member_id if existing is not None and existing.owner_member_id else default_owner),
                    next_due_at=None,
                    last_completed_at=existing.last_completed_at if existing is not None else None,
                    metadata=metadata,
                )
                upserted.append(self.store.upsert_household_routine(paused))
                continue

            next_due = _next_due_local_schedule_iso(
                household_timezone=timezone_name,
                hour=int(spec.hour),
                minute=int(spec.minute),
                days=list(spec.days),
                now=current,
            )
            routine = HouseholdRoutine(
                id=routine_id,
                household_id=household_id,
                title=spec.title,
                cadence=cadence,
                description="Automatic Florence household briefing routine",
                status=HouseholdRoutineStatus.ACTIVE,
                owner_member_id=(existing.owner_member_id if existing is not None and existing.owner_member_id else default_owner),
                next_due_at=next_due if existing is None or existing.status != HouseholdRoutineStatus.ACTIVE else (existing.next_due_at or next_due),
                last_completed_at=existing.last_completed_at if existing is not None else None,
                metadata=metadata,
            )
            upserted.append(self.store.upsert_household_routine(routine))
        return upserted

    def _stored_briefing_routine_specs(
        self,
        *,
        household_id: str,
        preferences_fingerprint: str,
    ) -> list[_BriefingRoutineSpec] | None:
        specs: list[_BriefingRoutineSpec] = []
        for kind in (HouseholdBriefingKind.MORNING, HouseholdBriefingKind.EVENING, HouseholdBriefingKind.WEEKLY):
            routine = self.store.get_household_routine(_stable_id("routine", household_id, "briefing", kind.value))
            if routine is None:
                return None
            metadata = dict(routine.metadata) if isinstance(routine.metadata, dict) else {}
            if str(metadata.get("planning_preferences_fingerprint") or "").strip() != preferences_fingerprint:
                return None
            parsed_time = _parse_local_time_spec(str(metadata.get("local_time") or ""))
            days = self._coerce_routine_days(metadata.get("days"))
            if parsed_time is None or not days:
                return None
            specs.append(
                _BriefingRoutineSpec(
                    kind=kind,
                    title=self._briefing_title(kind),
                    hour=parsed_time[0],
                    minute=parsed_time[1],
                    days=days,
                    disabled=routine.status != HouseholdRoutineStatus.ACTIVE,
                    planning_source=str(metadata.get("planning_source") or "stored").strip() or "stored",
                    planning_preferences_fingerprint=preferences_fingerprint,
                )
            )
        return specs

    def _hermes_briefing_routine_specs(
        self,
        *,
        household_id: str,
        channel_id: str | None,
        actor_member_id: str | None,
        operating_preferences: list[str],
        preferences_fingerprint: str,
    ) -> list[_BriefingRoutineSpec] | None:
        if not operating_preferences or channel_id is None or self._household_chat_service_getter is None:
            return None
        chat_service = self._household_chat_service_getter()
        planner = getattr(chat_service, "compose_briefing_routine_plan", None)
        if not callable(planner):
            return None
        try:
            plan = planner(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                operating_preferences=operating_preferences,
            )
        except Exception:
            logger.exception("Failed to compose Hermes briefing routine plan household_id=%s", household_id)
            return None
        if not isinstance(plan, list) or not plan:
            return None

        specs: list[_BriefingRoutineSpec] = []
        plan_by_kind = {
            str(item.get("kind") or "").strip().lower(): item
            for item in plan
            if isinstance(item, dict)
        }
        for kind in (HouseholdBriefingKind.MORNING, HouseholdBriefingKind.EVENING, HouseholdBriefingKind.WEEKLY):
            default_hour, default_minute = self._default_briefing_time(kind)
            item = plan_by_kind.get(kind.value, {})
            try:
                hour = int(item.get("hour", default_hour))
                minute = int(item.get("minute", default_minute))
            except (TypeError, ValueError):
                hour, minute = default_hour, default_minute
            if not (0 <= hour <= 23):
                hour = default_hour
            if not (0 <= minute <= 59):
                minute = default_minute
            specs.append(
                _BriefingRoutineSpec(
                    kind=kind,
                    title=self._briefing_title(kind),
                    hour=hour,
                    minute=minute,
                    days=self._coerce_routine_days(item.get("days")) or self._default_briefing_days(kind),
                    disabled=not bool(item.get("enabled", True)),
                    planning_source="hermes",
                    planning_preferences_fingerprint=preferences_fingerprint,
                )
            )
        return specs

    def _fallback_briefing_routine_specs(
        self,
        *,
        operating_preferences: str,
        preferences_fingerprint: str,
    ) -> list[_BriefingRoutineSpec]:
        disable_morning = bool(re.search(r"\b(?:no|skip|disable)\s+morning\s+brief\b", operating_preferences, re.IGNORECASE))
        disable_evening = bool(re.search(r"\b(?:no|skip|disable)\s+evening\s+(?:check[- ]?in|brief)\b", operating_preferences, re.IGNORECASE))
        disable_weekly = bool(re.search(r"\b(?:no|skip|disable)\s+(?:weekly\s+brief|weekend\s+preview)\b", operating_preferences, re.IGNORECASE))

        morning_hour, morning_minute = _extract_local_time_from_preferences(
            operating_preferences,
            keywords=("morning brief", "morning"),
            default_hour=6,
            default_minute=45,
        )
        evening_hour, evening_minute = _extract_local_time_from_preferences(
            operating_preferences,
            keywords=("evening check-in", "evening check in", "evening brief", "evening"),
            default_hour=20,
            default_minute=15,
        )
        weekly_hour, weekly_minute = _extract_local_time_from_preferences(
            operating_preferences,
            keywords=("weekly brief", "weekend preview", "weekly", "weekend"),
            default_hour=17,
            default_minute=30,
        )
        return [
            _BriefingRoutineSpec(
                kind=HouseholdBriefingKind.MORNING,
                title=self._briefing_title(HouseholdBriefingKind.MORNING),
                hour=morning_hour,
                minute=morning_minute,
                days=_local_schedule_days(text=operating_preferences, kind=HouseholdBriefingKind.MORNING),
                disabled=disable_morning,
                planning_source="deterministic_fallback",
                planning_preferences_fingerprint=preferences_fingerprint,
            ),
            _BriefingRoutineSpec(
                kind=HouseholdBriefingKind.EVENING,
                title=self._briefing_title(HouseholdBriefingKind.EVENING),
                hour=evening_hour,
                minute=evening_minute,
                days=_local_schedule_days(text=operating_preferences, kind=HouseholdBriefingKind.EVENING),
                disabled=disable_evening,
                planning_source="deterministic_fallback",
                planning_preferences_fingerprint=preferences_fingerprint,
            ),
            _BriefingRoutineSpec(
                kind=HouseholdBriefingKind.WEEKLY,
                title=self._briefing_title(HouseholdBriefingKind.WEEKLY),
                hour=weekly_hour,
                minute=weekly_minute,
                days=_local_schedule_days(text=operating_preferences, kind=HouseholdBriefingKind.WEEKLY),
                disabled=disable_weekly,
                planning_source="deterministic_fallback",
                planning_preferences_fingerprint=preferences_fingerprint,
            ),
        ]

    @staticmethod
    def _briefing_title(kind: HouseholdBriefingKind) -> str:
        if kind == HouseholdBriefingKind.MORNING:
            return "Morning brief"
        if kind == HouseholdBriefingKind.EVENING:
            return "Evening check-in"
        return "Weekly preview"

    @staticmethod
    def _default_briefing_time(kind: HouseholdBriefingKind) -> tuple[int, int]:
        if kind == HouseholdBriefingKind.MORNING:
            return (6, 45)
        if kind == HouseholdBriefingKind.EVENING:
            return (20, 15)
        return (17, 30)

    @staticmethod
    def _default_briefing_days(kind: HouseholdBriefingKind) -> list[int]:
        if kind == HouseholdBriefingKind.WEEKLY:
            return [6]
        return [0, 1, 2, 3, 4]

    @staticmethod
    def _coerce_routine_days(raw_value: object) -> list[int]:
        if not isinstance(raw_value, list):
            return []
        days: list[int] = []
        for item in raw_value:
            try:
                day = int(item)
            except (TypeError, ValueError):
                continue
            if 0 <= day <= 6 and day not in days:
                days.append(day)
        return days

    def list_due_briefing_routines(
        self,
        *,
        household_id: str,
        now: datetime | None = None,
    ) -> list[HouseholdRoutine]:
        current = now or _utc_now()
        due: list[HouseholdRoutine] = []
        for routine in self.store.list_household_routines(
            household_id=household_id,
            status=HouseholdRoutineStatus.ACTIVE,
        ):
            if str(routine.metadata.get("automation_kind") or "") != "briefing":
                continue
            scheduled_at = _parse_iso_datetime(routine.next_due_at)
            if scheduled_at is None or scheduled_at <= current:
                due.append(routine)
        return due

    def mark_briefing_routine_sent(
        self,
        *,
        routine_id: str,
        sent_at: datetime | None = None,
    ) -> HouseholdRoutine | None:
        routine = self.store.get_household_routine(routine_id)
        if routine is None:
            return None
        household = self.store.get_household(routine.household_id)
        if household is None:
            return None
        metadata = dict(routine.metadata)
        local_time = str(metadata.get("local_time") or "06:45")
        parsed_time = _parse_local_time_spec(local_time) or (6, 45)
        raw_days = metadata.get("days")
        days = [int(item) for item in raw_days if isinstance(item, int) and 0 <= int(item) <= 6] if isinstance(raw_days, list) else [0, 1, 2, 3, 4]
        now_value = sent_at or _utc_now()
        next_due_at = _next_due_local_schedule_iso(
            household_timezone=household.timezone,
            hour=parsed_time[0],
            minute=parsed_time[1],
            days=days or [0, 1, 2, 3, 4],
            now=now_value,
        )
        updated = replace(
            routine,
            last_completed_at=now_value.isoformat(),
            next_due_at=next_due_at,
        )
        return self.store.upsert_household_routine(updated)

    def schedule_nudge(
        self,
        *,
        household_id: str,
        message: str,
        scheduled_for: str,
        target_kind: HouseholdNudgeTargetKind = HouseholdNudgeTargetKind.GENERAL,
        target_id: str | None = None,
        recipient_member_id: str | None = None,
        channel_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> HouseholdNudge:
        resolved_member_id = recipient_member_id or self.default_recipient_member_id(household_id)
        resolved_channel_id = channel_id or self.default_dm_channel_id(
            household_id=household_id,
            member_id=resolved_member_id,
        )
        normalized_message = " ".join(message.split()).strip()
        normalized_scheduled_for = str(scheduled_for).strip()
        nudge_id = _stable_id(
            "nudge",
            household_id,
            target_kind.value,
            target_id or "general",
            normalized_message,
            normalized_scheduled_for,
        )
        return self.store.upsert_household_nudge(
            HouseholdNudge(
                id=nudge_id,
                household_id=household_id,
                target_kind=target_kind,
                target_id=target_id,
                message=normalized_message,
                recipient_member_id=resolved_member_id,
                channel_id=resolved_channel_id,
                scheduled_for=normalized_scheduled_for,
                metadata=dict(metadata or {}),
            )
        )

    def list_due_nudges(
        self,
        *,
        household_id: str,
        now: datetime | None = None,
    ) -> list[HouseholdNudge]:
        current = now or _utc_now()
        due: list[HouseholdNudge] = []
        for nudge in self.store.list_household_nudges(
            household_id=household_id,
            status=HouseholdNudgeStatus.SCHEDULED,
        ):
            scheduled_at = _parse_iso_datetime(nudge.scheduled_for)
            if scheduled_at is None or scheduled_at <= current:
                due.append(nudge)
        return due

    def list_pending_nudges(
        self,
        *,
        household_id: str,
        recipient_member_id: str | None = None,
        channel_id: str | None = None,
    ) -> list[HouseholdNudge]:
        candidates = [
            nudge
            for nudge in self.store.list_household_nudges(household_id=household_id)
            if nudge.status in {HouseholdNudgeStatus.SCHEDULED, HouseholdNudgeStatus.SENT}
        ]
        if recipient_member_id:
            scoped = [nudge for nudge in candidates if nudge.recipient_member_id == recipient_member_id]
            if scoped:
                candidates = scoped
        if channel_id:
            scoped = [nudge for nudge in candidates if nudge.channel_id == channel_id]
            if scoped:
                candidates = scoped

        def sort_key(nudge: HouseholdNudge) -> tuple[int, datetime]:
            priority = 0 if nudge.status == HouseholdNudgeStatus.SENT else 1
            scheduled = _parse_iso_datetime(nudge.scheduled_for) or datetime.max.replace(tzinfo=timezone.utc)
            return (priority, scheduled)

        return sorted(candidates, key=sort_key)

    def complete_actionable_nudge(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        nudge_id: str | None = None,
        now: datetime | None = None,
    ) -> _ReminderActionResult | None:
        actionable_nudge = self._resolve_actionable_nudge(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel_id,
            nudge_id=nudge_id,
        )
        if actionable_nudge is None:
            return None

        current = now or _utc_now()
        self.acknowledge_nudge(
            nudge_id=actionable_nudge.id,
            acknowledged_at=current,
        )
        completed_work_item_title: str | None = None
        if actionable_nudge.target_kind == HouseholdNudgeTargetKind.WORK_ITEM and actionable_nudge.target_id:
            work_item = self.store.get_household_work_item(actionable_nudge.target_id)
            if work_item is not None and work_item.status not in {
                HouseholdWorkItemStatus.DONE,
                HouseholdWorkItemStatus.CANCELLED,
            }:
                updated_work_item = replace(
                    work_item,
                    status=HouseholdWorkItemStatus.DONE,
                    completed_at=current.isoformat(),
                )
                self.upsert_work_item(updated_work_item)
                completed_work_item_title = updated_work_item.title

        self.record_pilot_event(
            household_id=household_id,
            event_type="reminder_done",
            member_id=member_id,
            channel_id=channel_id,
            metadata={
                "nudge_id": actionable_nudge.id,
                "target_kind": actionable_nudge.target_kind.value,
                "target_id": actionable_nudge.target_id,
                "marked_work_item_done": bool(completed_work_item_title),
            },
            created_at=current,
        )
        if completed_work_item_title:
            return _ReminderActionResult(
                reply_text=f'Done. I marked "{completed_work_item_title}" complete and stopped that reminder.'
            )
        return _ReminderActionResult(reply_text="Done. I marked that reminder complete.")

    def snooze_actionable_nudge(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        scheduled_for: datetime,
        nudge_id: str | None = None,
        now: datetime | None = None,
    ) -> _ReminderActionResult | None:
        actionable_nudge = self._resolve_actionable_nudge(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel_id,
            nudge_id=nudge_id,
        )
        if actionable_nudge is None:
            return None

        current = now or _utc_now()
        updated_nudge = self.snooze_nudge(
            nudge_id=actionable_nudge.id,
            scheduled_for=scheduled_for,
            snoozed_at=current,
        )
        self.record_pilot_event(
            household_id=household_id,
            event_type="reminder_snoozed",
            member_id=member_id,
            channel_id=channel_id,
            metadata={
                "nudge_id": actionable_nudge.id,
                "target_kind": actionable_nudge.target_kind.value,
                "target_id": actionable_nudge.target_id,
                "snoozed_until": (updated_nudge.scheduled_for if updated_nudge else scheduled_for.isoformat()),
            },
            created_at=current,
        )
        until_text = (updated_nudge.scheduled_for if updated_nudge else scheduled_for.isoformat()).replace("T", " ").replace("+00:00", "Z")
        return _ReminderActionResult(reply_text=f"Okay, snoozed. I’ll remind you again around {until_text}.")

    def _resolve_actionable_nudge(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        nudge_id: str | None = None,
    ) -> HouseholdNudge | None:
        if nudge_id:
            nudge = self.store.get_household_nudge(nudge_id)
            if nudge is None or nudge.household_id != household_id:
                return None
            if nudge.recipient_member_id and nudge.recipient_member_id != member_id:
                return None
            if nudge.channel_id and nudge.channel_id != channel_id:
                return None
            if nudge.status not in {HouseholdNudgeStatus.SCHEDULED, HouseholdNudgeStatus.SENT}:
                return None
            return nudge
        return self._actionable_nudge(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel_id,
        )

    def mark_nudge_sent(
        self,
        *,
        nudge_id: str,
        sent_at: datetime | None = None,
    ) -> HouseholdNudge | None:
        nudge = self.store.get_household_nudge(nudge_id)
        if nudge is None:
            return None
        updated = replace(
            nudge,
            status=HouseholdNudgeStatus.SENT,
            sent_at=(sent_at or _utc_now()).isoformat(),
        )
        return self.store.upsert_household_nudge(updated)

    def acknowledge_nudge(
        self,
        *,
        nudge_id: str,
        acknowledged_at: datetime | None = None,
    ) -> HouseholdNudge | None:
        nudge = self.store.get_household_nudge(nudge_id)
        if nudge is None:
            return None
        updated = replace(
            nudge,
            status=HouseholdNudgeStatus.ACKNOWLEDGED,
            acknowledged_at=(acknowledged_at or _utc_now()).isoformat(),
        )
        return self.store.upsert_household_nudge(updated)

    def snooze_nudge(
        self,
        *,
        nudge_id: str,
        scheduled_for: datetime,
        snoozed_at: datetime | None = None,
    ) -> HouseholdNudge | None:
        nudge = self.store.get_household_nudge(nudge_id)
        if nudge is None:
            return None
        metadata = dict(nudge.metadata)
        metadata["snoozed_count"] = int(metadata.get("snoozed_count", 0) or 0) + 1
        metadata["last_snoozed_at"] = (snoozed_at or _utc_now()).isoformat()
        updated = replace(
            nudge,
            status=HouseholdNudgeStatus.SCHEDULED,
            scheduled_for=scheduled_for.isoformat(),
            sent_at=None,
            acknowledged_at=None,
            metadata=metadata,
        )
        return self.store.upsert_household_nudge(updated)

    def default_recipient_member_id(self, household_id: str) -> str | None:
        members = self.store.list_members(household_id)
        if not members:
            return None
        priority = {
            MemberRole.ADMIN: 0,
            MemberRole.PARENT: 1,
            MemberRole.CAREGIVER: 2,
            MemberRole.GRANDPARENT: 3,
            MemberRole.CHILD_LIMITED: 4,
        }
        ranked = sorted(members, key=lambda member: (priority.get(member.role, 99), member.display_name.lower()))
        return ranked[0].id if ranked else None

    def default_dm_channel_id(self, *, household_id: str, member_id: str | None = None) -> str | None:
        channels = self.store.list_channels(household_id=household_id, channel_type=ChannelType.PARENT_DM)
        if member_id:
            sessions = self.store.list_member_onboarding_sessions(household_id=household_id, member_id=member_id)
            for session in sessions:
                for channel in channels:
                    if channel.provider_channel_id == session.thread_id:
                        return channel.id
        return channels[0].id if channels else None

    def _actionable_nudge(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
    ) -> HouseholdNudge | None:
        pending_nudges = self.list_pending_nudges(
            household_id=household_id,
            recipient_member_id=member_id,
            channel_id=channel_id,
        )
        sent_nudges = [nudge for nudge in pending_nudges if nudge.status == HouseholdNudgeStatus.SENT]
        if not sent_nudges:
            return pending_nudges[0] if pending_nudges else None
        min_dt = datetime.min.replace(tzinfo=timezone.utc)
        return max(
            sent_nudges,
            key=lambda nudge: _parse_iso_datetime(nudge.sent_at) or _parse_iso_datetime(nudge.scheduled_for) or min_dt,
        )
