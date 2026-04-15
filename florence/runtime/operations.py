"""Household operations and sync-side effects for Florence runtime."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from florence.contracts import CandidateState, ChannelMessageRole, ChannelType, GoogleSourceKind, HouseholdBriefingKind, HouseholdProfileKind
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    PENDING_ACTION_TARGET_ID_KEY,
    PENDING_ACTION_TARGET_IDS_KEY,
    build_candidate_review_prompt_metadata,
    build_household_nudge_metadata,
)
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.delivery import FlorenceChannelDeliveryService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.services import _parse_local_time_spec
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)
_ACTIVE_REVIEW_NUDGE_WINDOW_SECONDS = 15 * 60
_REVIEW_SWEEP_INTERVAL_SECONDS = 12 * 60 * 60
_SYNC_UPDATE_BRIEF_INTERVAL_SECONDS = 6 * 60 * 60
_REVIEW_BATCH_LIMIT = 3
_REVIEW_QUIET_HOURS_START = 21
_REVIEW_QUIET_HOURS_END = 8
_HEARTBEAT_OK_SENTINEL = "HEARTBEAT_OK"


class FlorenceHouseholdOperationsService:
    """Non-transport household operations used by production flows."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        delivery_service: FlorenceChannelDeliveryService,
        household_chat_service_getter: Callable[[], Any],
        candidate_review_service: FlorenceCandidateReviewService | None = None,
        household_manager_service: FlorenceHouseholdManagerService | None = None,
    ) -> None:
        self.store = store
        self.delivery_service = delivery_service
        self._household_chat_service_getter = household_chat_service_getter
        self._candidate_review_service = candidate_review_service or FlorenceCandidateReviewService(store)
        self._household_manager_service = household_manager_service or FlorenceHouseholdManagerService(store)

    def has_onboarding_completion_event(
        self,
        *,
        household_id: str,
        member_id: str,
        store: FlorenceStateDB | None = None,
    ) -> bool:
        target_store = store or self.store
        events = target_store.list_pilot_events(
            household_id=household_id,
            event_type="onboarding_complete",
            limit=5,
        )
        return any(event.member_id == member_id for event in events)

    def record_onboarding_completion(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        store: FlorenceStateDB | None = None,
    ) -> None:
        manager_service = self._manager_service(store)
        try:
            manager_service.finalize_onboarding_completion(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel_id,
            )
        except Exception:
            logger.exception("Failed to finalize onboarding completion hooks for household_id=%s", household_id)

    def nudge_for_new_pending_candidates(
        self,
        *,
        household_id: str,
        member_id: str,
        candidates: list[Any],
        store: FlorenceStateDB | None = None,
    ) -> bool:
        target_store = store or self.store
        candidate_review_service = self._review_service(store)
        newly_pending = []
        for candidate in candidates:
            if candidate.state != CandidateState.PENDING_REVIEW:
                continue
            if candidate.metadata.get("review_nudged_at"):
                continue
            newly_pending.append(candidate)

        if not newly_pending:
            return False
        proactive_candidates = [
            candidate
            for candidate in newly_pending
            if self._candidate_warrants_proactive_review_prompt(candidate, store=target_store)
        ]
        if not proactive_candidates:
            return False

        channel = self._review_dm_channel(
            household_id=household_id,
            member_id=member_id,
            store=target_store,
        )
        if channel is None:
            return False
        if self._has_armed_pending_review_prompt(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel.id,
            store=target_store,
        ):
            return False
        if self._is_review_quiet_hours(household_id=household_id, store=target_store):
            return False
        prompt = candidate_review_service.build_dm_review_batch_prompt(
            household_id=household_id,
            member_id=member_id,
            candidate_filter=lambda candidate: self._candidate_warrants_proactive_review_prompt(
                candidate,
                store=target_store,
            ),
            limit=_REVIEW_BATCH_LIMIT,
        )
        if prompt is None:
            return False

        sent_prompt = False
        if channel is not None:
            if self._should_defer_review_nudge_for_active_conversation(
                channel_id=channel.id,
                store=target_store,
            ):
                logger.info(
                    "Deferring review nudge during active conversation household_id=%s member_id=%s channel_id=%s",
                    household_id,
                    member_id,
                    channel.id,
                )
                return False
            prompt_text = prompt.text
            if not self._review_prompt_should_use_raw_text(prompt):
                try:
                    source_prompt = candidate_review_service.source_rule_service.build_candidate_source_prompt(prompt.candidate)
                    rendered = self._household_chat_service_getter().compose_operator_message(
                        household_id=household_id,
                        channel_id=channel.id,
                        actor_member_id=member_id,
                        kind="review_prompt",
                        payload={
                            "candidate": {
                                "title": str(getattr(prompt.candidate, "title", "") or "").strip(),
                                "summary": str(getattr(prompt.candidate, "summary", "") or "").strip(),
                                "state": str(getattr(prompt.candidate, "state", "") or "").strip(),
                                "confirmation_question": str(getattr(prompt.candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                                "candidate_scope": str(getattr(prompt.candidate, "metadata", {}).get("candidate_scope") or "").strip(),
                            },
                            "items": self._review_prompt_items(prompt),
                            "source_prompt": source_prompt,
                            "pending_review_count": len(
                                target_store.list_imported_candidates(
                                    household_id=household_id,
                                    member_id=member_id,
                                    state=CandidateState.PENDING_REVIEW,
                                )
                            ),
                            "trigger": "new_pending_candidate",
                        },
                    )
                    if rendered is not None and rendered.strip():
                        prompt_text = rendered.strip()
                except Exception:
                    logger.exception(
                        "Florence review nudge compose failed household_id=%s member_id=%s candidate_id=%s",
                        household_id,
                        member_id,
                        prompt.candidate.id,
                    )
            sent_prompt = self.delivery_service.send_channel_message(
                channel=channel,
                message=prompt_text,
                store=target_store,
                message_metadata=build_candidate_review_prompt_metadata(
                    prompt.candidate.id,
                    candidate_ids=self._review_prompt_target_ids(prompt),
                ),
            )
            if sent_prompt:
                candidate_metadata = dict(prompt.candidate.metadata) if isinstance(prompt.candidate.metadata, dict) else {}
                self._manager_service(store).record_pilot_event(
                    household_id=household_id,
                    event_type="review_prompt_sent",
                    member_id=member_id,
                    channel_id=channel.id,
                    metadata={
                        "candidate_id": prompt.candidate.id,
                        "source_kind": prompt.candidate.source_kind.value,
                        "source_identifier": prompt.candidate.source_identifier,
                        "candidate_title": prompt.candidate.title,
                        "candidate_summary": prompt.candidate.summary,
                        "confirmation_question": str(candidate_metadata.get("confirmation_question") or "").strip() or None,
                        "source_visibility": str(candidate_metadata.get("source_visibility") or "").strip() or None,
                        "source_rule_label": str(candidate_metadata.get("source_rule_label") or "").strip() or None,
                        "newly_pending_count": len(newly_pending),
                        "candidate_ids": self._review_prompt_target_ids(prompt),
                        "batch_count": len(prompt.candidates),
                        "trigger": "new_pending_candidate",
                    },
                )

        if not sent_prompt:
            return False

        nudged_at = self._utc_now().isoformat()
        for candidate in prompt.candidates:
            metadata = dict(candidate.metadata)
            metadata["review_nudged_at"] = nudged_at
            target_store.upsert_imported_candidate(replace(candidate, metadata=metadata))
        return True

    def dispatch_due_review_sweeps(
        self,
        *,
        household_id: str,
        store: FlorenceStateDB | None = None,
    ) -> int:
        target_store = store or self.store
        pending_candidates = target_store.list_imported_candidates(
            household_id=household_id,
            state=CandidateState.PENDING_REVIEW,
        )
        if not pending_candidates:
            return 0

        candidate_review_service = self._review_service(store)
        sent = 0
        member_ids = sorted(
            {
                candidate.member_id
                for candidate in pending_candidates
                if str(candidate.member_id or "").strip()
            }
        )
        for member_id in member_ids:
            member_pending = [
                candidate
                for candidate in pending_candidates
                if candidate.member_id == member_id
            ]
            if not member_pending:
                continue
            channel = self._review_dm_channel(
                household_id=household_id,
                member_id=member_id,
                store=target_store,
            )
            if channel is None:
                continue
            if self._has_armed_pending_review_prompt(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel.id,
                store=target_store,
            ):
                continue
            if self._is_review_quiet_hours(household_id=household_id, store=target_store):
                continue
            if self._should_defer_review_nudge_for_active_conversation(
                channel_id=channel.id,
                store=target_store,
            ):
                continue
            if self._review_prompt_sent_recently(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel.id,
                store=target_store,
                window_seconds=_REVIEW_SWEEP_INTERVAL_SECONDS,
            ):
                continue
            proactive_pending = [
                candidate
                for candidate in member_pending
                if self._candidate_warrants_proactive_review_prompt(candidate, store=target_store)
            ]
            if not proactive_pending:
                continue
            prompt = candidate_review_service.build_dm_review_batch_prompt(
                household_id=household_id,
                member_id=member_id,
                candidate_filter=lambda candidate: self._candidate_warrants_proactive_review_prompt(
                    candidate,
                    store=target_store,
                ),
                limit=_REVIEW_BATCH_LIMIT,
            )
            if prompt is None:
                continue
            prompt_text = prompt.text
            if not self._review_prompt_should_use_raw_text(prompt):
                try:
                    source_prompt = candidate_review_service.source_rule_service.build_candidate_source_prompt(prompt.candidate)
                    rendered = self._household_chat_service_getter().compose_operator_message(
                        household_id=household_id,
                        channel_id=channel.id,
                        actor_member_id=member_id,
                        kind="review_prompt",
                        payload={
                            "candidate": {
                                "title": str(getattr(prompt.candidate, "title", "") or "").strip(),
                                "summary": str(getattr(prompt.candidate, "summary", "") or "").strip(),
                                "state": str(getattr(prompt.candidate, "state", "") or "").strip(),
                                "confirmation_question": str(getattr(prompt.candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                                "candidate_scope": str(getattr(prompt.candidate, "metadata", {}).get("candidate_scope") or "").strip(),
                            },
                            "items": self._review_prompt_items(prompt),
                            "source_prompt": source_prompt,
                            "pending_review_count": len(member_pending),
                            "trigger": "scheduled_review_sweep",
                        },
                    )
                    if rendered is not None and rendered.strip():
                        prompt_text = rendered.strip()
                except Exception:
                    logger.exception(
                        "Florence review sweep compose failed household_id=%s member_id=%s candidate_id=%s",
                        household_id,
                        member_id,
                        prompt.candidate.id,
                    )
            if not self.delivery_service.send_channel_message(
                channel=channel,
                message=prompt_text,
                store=target_store,
                message_metadata=build_candidate_review_prompt_metadata(
                    prompt.candidate.id,
                    candidate_ids=self._review_prompt_target_ids(prompt),
                ),
            ):
                continue
            metadata = dict(prompt.candidate.metadata) if isinstance(prompt.candidate.metadata, dict) else {}
            nudged_at = self._utc_now().isoformat()
            for candidate in prompt.candidates:
                candidate_metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
                candidate_metadata["review_nudged_at"] = nudged_at
                target_store.upsert_imported_candidate(replace(candidate, metadata=candidate_metadata))
            self._manager_service(store).record_pilot_event(
                household_id=household_id,
                event_type="review_prompt_sent",
                member_id=member_id,
                channel_id=channel.id,
                metadata={
                    "candidate_id": prompt.candidate.id,
                    "source_kind": prompt.candidate.source_kind.value,
                    "source_identifier": prompt.candidate.source_identifier,
                    "candidate_title": prompt.candidate.title,
                    "candidate_summary": prompt.candidate.summary,
                    "confirmation_question": str(metadata.get("confirmation_question") or "").strip() or None,
                    "source_visibility": str(metadata.get("source_visibility") or "").strip() or None,
                    "source_rule_label": str(metadata.get("source_rule_label") or "").strip() or None,
                    "pending_review_count": len(member_pending),
                    "candidate_ids": self._review_prompt_target_ids(prompt),
                    "batch_count": len(prompt.candidates),
                    "trigger": "scheduled_review_sweep",
                },
            )
            sent += 1
        return sent

    def dispatch_due_household_nudges(
        self,
        *,
        household_id: str,
        store: FlorenceStateDB | None = None,
    ) -> int:
        target_store = store or self.store
        manager_service = self._manager_service(store)
        sent = 0
        for nudge in manager_service.list_due_nudges(household_id=household_id):
            channel = target_store.get_channel(nudge.channel_id) if nudge.channel_id else None
            if channel is None and nudge.recipient_member_id:
                fallback_channel_id = manager_service.default_dm_channel_id(
                    household_id=household_id,
                    member_id=nudge.recipient_member_id,
                )
                if fallback_channel_id:
                    channel = target_store.get_channel(fallback_channel_id)
            if channel is None or not nudge.message.strip():
                continue
            nudge_metadata = dict(nudge.metadata) if isinstance(nudge.metadata, dict) else {}
            custom_delivery_metadata = (
                dict(nudge_metadata.get("delivery_message_metadata"))
                if isinstance(nudge_metadata.get("delivery_message_metadata"), dict)
                else {}
            )
            if self.delivery_service.send_channel_message(
                channel=channel,
                message=nudge.message,
                store=target_store,
                message_metadata={
                    **build_household_nudge_metadata(nudge.id),
                    **custom_delivery_metadata,
                },
            ):
                manager_service.mark_nudge_sent(nudge_id=nudge.id)
                manager_service.record_pilot_event(
                    household_id=household_id,
                    event_type="nudge_sent",
                    member_id=nudge.recipient_member_id,
                    channel_id=channel.id,
                    metadata={
                        "nudge_id": nudge.id,
                        "target_kind": nudge.target_kind.value,
                    },
                )
                sent += 1
        return sent

    def _should_defer_review_nudge_for_active_conversation(
        self,
        *,
        channel_id: str,
        store: FlorenceStateDB,
    ) -> bool:
        cutoff = datetime.now(timezone.utc).timestamp() - _ACTIVE_REVIEW_NUDGE_WINDOW_SECONDS
        recent_messages = [
            message
            for message in store.list_channel_messages(channel_id=channel_id, limit=8)
            if message.created_at >= cutoff
        ]
        if not recent_messages:
            return False
        return any(
            message.sender_role != ChannelMessageRole.ASSISTANT
            or message.metadata.get("protocol_kind") != CANDIDATE_REVIEW_PROMPT_KIND
            for message in recent_messages
        )

    def _has_recent_channel_activity(
        self,
        *,
        channel_id: str,
        store: FlorenceStateDB,
        window_seconds: int = _ACTIVE_REVIEW_NUDGE_WINDOW_SECONDS,
    ) -> bool:
        cutoff = datetime.now(timezone.utc).timestamp() - window_seconds
        return any(
            message.created_at >= cutoff
            for message in store.list_channel_messages(channel_id=channel_id, limit=8)
        )

    def _review_prompt_sent_recently(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        store: FlorenceStateDB,
        window_seconds: int,
    ) -> bool:
        cutoff = datetime.now(timezone.utc).timestamp() - window_seconds
        for event in store.list_pilot_events(
            household_id=household_id,
            event_type="review_prompt_sent",
            limit=20,
        ):
            if event.created_at < cutoff:
                break
            if event.member_id == member_id and event.channel_id == channel_id:
                return True
        return False

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _is_review_quiet_hours(
        self,
        *,
        household_id: str,
        store: FlorenceStateDB,
    ) -> bool:
        return self._is_household_quiet_hours(
            household_id=household_id,
            store=store,
            fallback_window=(_REVIEW_QUIET_HOURS_START * 60, _REVIEW_QUIET_HOURS_END * 60),
        )

    def _is_household_quiet_hours(
        self,
        *,
        household_id: str,
        store: FlorenceStateDB,
        fallback_window: tuple[int, int] | None = None,
    ) -> bool:
        household = store.get_household(household_id)
        if household is None or not str(household.timezone or "").strip():
            return False
        try:
            local_now = self._utc_now().astimezone(ZoneInfo(household.timezone))
        except Exception:
            return False
        window = self._quiet_hours_window(household_id=household_id, store=store) or fallback_window
        if window is None:
            return False
        start_minutes, end_minutes = window
        current_minutes = local_now.hour * 60 + local_now.minute
        if start_minutes == end_minutes:
            return False
        if start_minutes < end_minutes:
            return start_minutes <= current_minutes < end_minutes
        return current_minutes >= start_minutes or current_minutes < end_minutes

    def _quiet_hours_window(
        self,
        *,
        household_id: str,
        store: FlorenceStateDB,
    ) -> tuple[int, int] | None:
        items = store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        )
        category_priority = ("quiet_hours", "automation_boundary", "reminder_style", "operating_rule")
        ordered_items = sorted(
            items,
            key=lambda item: (
                category_priority.index(str(item.metadata.get("category") or "").strip().lower())
                if str(item.metadata.get("category") or "").strip().lower() in category_priority
                else len(category_priority)
            ),
        )
        for item in ordered_items:
            value = str(item.metadata.get("value") or "").strip()
            label = str(item.label or "").strip()
            parsed = self._parse_quiet_hours_window(f"{label}: {value}".strip())
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _parse_quiet_hours_window(text: str) -> tuple[int, int] | None:
        lowered = str(text or "").strip().lower()
        if not lowered:
            return None
        matches = list(re.finditer(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm)?\b", lowered))
        parsed_times: list[int] = []
        for match in matches:
            parsed = _parse_local_time_spec(match.group(0))
            if parsed is None:
                continue
            parsed_times.append(parsed[0] * 60 + parsed[1])
        if len(parsed_times) >= 2:
            return (parsed_times[0], parsed_times[1])
        if parsed_times and "after" in lowered:
            return (parsed_times[0], 8 * 60)
        if parsed_times and "before" in lowered:
            return (21 * 60, parsed_times[0])
        return None

    @staticmethod
    def _review_prompt_items(prompt: Any) -> list[dict[str, object]]:
        return [
            {
                "index": index,
                "title": str(getattr(candidate, "title", "") or "").strip(),
                "summary": str(getattr(candidate, "summary", "") or "").strip(),
                "confirmation_question": str(getattr(candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                "candidate_scope": str(getattr(candidate, "metadata", {}).get("candidate_scope") or "").strip(),
            }
            for index, candidate in enumerate(tuple(getattr(prompt, "candidates", ()) or ()), start=1)
        ]

    @staticmethod
    def _review_prompt_target_ids(prompt: Any) -> list[str]:
        normalized: list[str] = []
        candidates = tuple(getattr(prompt, "candidates", ()) or ())
        for candidate in candidates:
            metadata = dict(getattr(candidate, "metadata", {}) or {})
            group_ids = [
                str(candidate_id).strip()
                for candidate_id in list(metadata.get("review_group_candidate_ids") or [])
                if str(candidate_id).strip()
            ]
            if not group_ids:
                group_ids = [str(getattr(candidate, "id", "") or "").strip()]
            for candidate_id in group_ids:
                if candidate_id and candidate_id not in normalized:
                    normalized.append(candidate_id)
        return normalized

    @staticmethod
    def _review_prompt_should_use_raw_text(prompt: Any) -> bool:
        candidates = tuple(getattr(prompt, "candidates", ()) or ())
        if len(candidates) != 1:
            return False
        candidate = candidates[0]
        metadata = dict(getattr(candidate, "metadata", {}) or {})
        group_ids = [
            str(candidate_id).strip()
            for candidate_id in list(metadata.get("review_group_candidate_ids") or [])
            if str(candidate_id).strip()
        ]
        summary = str(getattr(candidate, "summary", "") or "").strip()
        title = str(getattr(candidate, "title", "") or "").strip()
        is_calendar_candidate = getattr(candidate, "source_kind", None) == GoogleSourceKind.GOOGLE_CALENDAR
        return bool(group_ids and len(group_ids) > 1) or (
            is_calendar_candidate and bool(summary) and summary != title
        )

    def _has_armed_pending_review_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        store: FlorenceStateDB,
    ) -> bool:
        latest_assistant = FlorenceChannelLog(store).latest_assistant_message(channel_id=channel_id, limit=8)
        if latest_assistant is None:
            return False
        if latest_assistant.metadata.get("protocol_kind") != CANDIDATE_REVIEW_PROMPT_KIND:
            return False
        candidate_ids = [
            str(candidate_id).strip()
            for candidate_id in list(latest_assistant.metadata.get(PENDING_ACTION_TARGET_IDS_KEY) or [])
            if str(candidate_id).strip()
        ]
        if not candidate_ids:
            candidate_id = str(latest_assistant.metadata.get(PENDING_ACTION_TARGET_ID_KEY) or "").strip()
            if candidate_id:
                candidate_ids = [candidate_id]
        if not candidate_ids:
            return False
        for candidate_id in candidate_ids:
            candidate = store.get_imported_candidate(candidate_id)
            if candidate is None:
                continue
            if (
                candidate.household_id == household_id
                and candidate.member_id == member_id
                and candidate.state == CandidateState.PENDING_REVIEW
            ):
                return True
        return False

    def _sync_update_brief_sent_recently(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        store: FlorenceStateDB,
        window_seconds: int,
    ) -> bool:
        cutoff = datetime.now(timezone.utc).timestamp() - window_seconds
        for event in store.list_pilot_events(
            household_id=household_id,
            event_type="sync_update_brief_sent",
            limit=20,
        ):
            if event.created_at < cutoff:
                break
            if event.member_id == member_id and event.channel_id == channel_id:
                return True
        return False

    def _review_dm_channel(
        self,
        *,
        household_id: str,
        member_id: str,
        store: FlorenceStateDB,
    ) -> Any | None:
        sessions = store.list_member_onboarding_sessions(household_id=household_id, member_id=member_id)
        if not sessions:
            return None
        dm_thread_id = sessions[0].thread_id
        return self.delivery_service.find_channel_by_provider_id(dm_thread_id, store=store)

    def dispatch_due_household_briefings(
        self,
        *,
        household_id: str,
        store: FlorenceStateDB | None = None,
    ) -> int:
        target_store = store or self.store
        manager_service = self._manager_service(store)
        sent = 0
        for routine in manager_service.list_due_briefing_routines(household_id=household_id):
            if self._is_household_quiet_hours(
                household_id=household_id,
                store=target_store,
            ):
                continue
            metadata = dict(routine.metadata)
            kind_raw = str(metadata.get("brief_kind") or HouseholdBriefingKind.MORNING.value).strip().lower()
            try:
                brief_kind = HouseholdBriefingKind(kind_raw)
            except ValueError:
                brief_kind = HouseholdBriefingKind.MORNING
            recipient_member_id = routine.owner_member_id or manager_service.default_recipient_member_id(household_id)
            channel_id = str(metadata.get("channel_id") or "").strip()
            if not channel_id:
                channel_id = manager_service.default_dm_channel_id(
                    household_id=household_id,
                    member_id=recipient_member_id,
                ) or ""
            channel = target_store.get_channel(channel_id) if channel_id else None
            if channel is None:
                continue
            channel = self.delivery_service.preferred_household_channel(
                household_id=household_id,
                fallback_channel=channel,
                store=target_store,
            )
            try:
                brief_message = self._household_chat_service_getter().compose_brief(
                    household_id=household_id,
                    channel_id=channel.id,
                    actor_member_id=recipient_member_id,
                    brief_kind=brief_kind,
                )
            except Exception:
                logger.exception(
                    "Florence briefing compose failed household_id=%s routine_id=%s",
                    household_id,
                    routine.id,
                )
                continue
            if not brief_message or not brief_message.strip():
                continue
            if brief_message.strip() == _HEARTBEAT_OK_SENTINEL:
                manager_service.mark_briefing_routine_sent(routine_id=routine.id)
                continue
            if self.delivery_service.send_channel_message(channel=channel, message=brief_message, store=target_store):
                manager_service.mark_briefing_routine_sent(routine_id=routine.id)
                manager_service.record_pilot_event(
                    household_id=household_id,
                    event_type="briefing_sent",
                    member_id=recipient_member_id,
                    channel_id=channel.id,
                    metadata={
                        "routine_id": routine.id,
                        "brief_kind": brief_kind.value,
                    },
                )
                sent += 1
        return sent

    def dispatch_due_sync_update_briefs(
        self,
        *,
        household_id: str,
        sync_results: list[Any],
        previous_connections: dict[str, Any] | None = None,
        store: FlorenceStateDB | None = None,
    ) -> int:
        target_store = store or self.store
        sent = 0
        prior_connections = dict(previous_connections or {})
        for result in sync_results:
            connection = target_store.get_google_connection(result.connection.id) or result.connection
            if not self.sync_activation_brief_already_sent(connection=connection):
                continue
            candidates = list(getattr(result.sync_result, "candidates", []) or [])
            if not candidates:
                continue
            channel = self._preferred_sync_brief_channel(
                connection=connection,
                fallback_channel=None,
                store=target_store,
            )
            if channel is None:
                continue
            if self._has_recent_channel_activity(
                channel_id=channel.id,
                store=target_store,
            ):
                continue
            if self._sync_update_brief_sent_recently(
                household_id=household_id,
                member_id=connection.member_id,
                channel_id=channel.id,
                store=target_store,
                window_seconds=_SYNC_UPDATE_BRIEF_INTERVAL_SECONDS,
            ):
                continue
            if self.deliver_sync_update_brief(
                connection=connection,
                candidates=candidates,
                fallback_channel=channel,
                previous_connection=prior_connections.get(connection.id),
                store=target_store,
                trigger="scheduled_sync_pass",
            ):
                sent += 1
        return sent

    def deliver_sync_activation_brief(
        self,
        *,
        connection: Any,
        candidates: list[Any],
        fallback_channel: Any,
        store: FlorenceStateDB | None = None,
    ) -> bool:
        target_store = store or self.store
        if self.sync_activation_brief_already_sent(connection=connection):
            return False
        sync_snapshot = self._sync_brief_snapshot(connection=connection, candidates=candidates)
        group_channel = self.delivery_service.find_group_channel(
            connection.household_id,
            provider=fallback_channel.provider,
            store=target_store,
        )
        primary_channel = self.delivery_service.preferred_household_channel(
            household_id=connection.household_id,
            fallback_channel=fallback_channel,
            store=target_store,
        )
        deliver_to_group = primary_channel.channel_type == ChannelType.HOUSEHOLD_GROUP
        activation_message: str | None = None
        group_message: str | None = None
        try:
            activation_message = self._household_chat_service_getter().compose_operator_message(
                household_id=connection.household_id,
                channel_id=primary_channel.id,
                actor_member_id=connection.member_id,
                kind="activation_brief",
                payload={
                    "gmail_count": int(connection.metadata.get("last_gmail_item_count") or 0),
                    "calendar_count": int(connection.metadata.get("last_calendar_item_count") or 0),
                    "candidates": [
                        {
                            "title": str(getattr(candidate, "title", "") or "").strip(),
                            "summary": str(getattr(candidate, "summary", "") or "").strip(),
                            "state": str(getattr(candidate, "state", "") or "").strip(),
                            "confirmation_question": str(getattr(candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                        }
                        for candidate in candidates
                    ],
                },
            )
        except Exception:
            logger.exception(
                "Florence activation brief compose failed household_id=%s connection_id=%s",
                connection.household_id,
                connection.id,
            )
            activation_message = None
        if activation_message and group_channel is not None and not deliver_to_group:
            try:
                group_message = self._household_chat_service_getter().compose_operator_message(
                    household_id=connection.household_id,
                    channel_id=primary_channel.id,
                    actor_member_id=connection.member_id,
                    kind="group_promotion",
                    payload={"source_text": activation_message},
                )
            except Exception:
                logger.exception(
                    "Florence activation brief group-promotion compose failed household_id=%s connection_id=%s",
                    connection.household_id,
                    connection.id,
                )
        if not activation_message:
            activation_message, group_message = self.fallback_sync_activation_brief_messages(
                deliver_to_group=deliver_to_group,
                group_available=group_channel is not None,
            )
        sent_activation = self.delivery_service.send_channel_message(
            channel=primary_channel,
            message=activation_message,
            store=target_store,
            message_metadata=(
                {
                    "promotion_kind": "sync_activation_brief",
                    "promotable_group_message": group_message,
                }
                if group_message and not deliver_to_group
                else None
            ),
        )
        if sent_activation:
            self.mark_sync_activation_brief_sent(
                connection=connection,
                store=target_store,
                channel_id=primary_channel.id,
                snapshot=sync_snapshot,
            )
        return sent_activation

    def deliver_sync_update_brief(
        self,
        *,
        connection: Any,
        candidates: list[Any],
        fallback_channel: Any,
        previous_connection: Any | None = None,
        store: FlorenceStateDB | None = None,
        trigger: str = "notify_when_finished",
    ) -> bool:
        target_store = store or self.store
        if not self.sync_activation_brief_already_sent(connection=connection):
            return False
        current_snapshot = self._sync_brief_snapshot(connection=connection, candidates=candidates)
        if not self._sync_brief_has_meaningful_content(current_snapshot):
            return False
        previous_snapshot = self.last_sync_brief_snapshot(connection=connection)
        if previous_snapshot is None and previous_connection is not None:
            previous_snapshot = self._sync_brief_snapshot(connection=previous_connection, candidates=[])
        if previous_snapshot == current_snapshot:
            return False

        primary_channel = self._preferred_sync_brief_channel(
            connection=connection,
            fallback_channel=fallback_channel,
            store=target_store,
        )
        if primary_channel is None:
            return False
        group_channel = self.delivery_service.find_group_channel(
            connection.household_id,
            provider=primary_channel.provider,
            store=target_store,
        )
        deliver_to_group = primary_channel.channel_type == ChannelType.HOUSEHOLD_GROUP

        update_payload = {
            "previous_sync": dict(previous_snapshot or {}),
            "current_sync": self._sync_brief_payload(connection=connection, candidates=candidates),
        }
        update_message: str | None = None
        group_message: str | None = None
        try:
            update_message = self._household_chat_service_getter().compose_operator_message(
                household_id=connection.household_id,
                channel_id=primary_channel.id,
                actor_member_id=connection.member_id,
                kind="sync_update_brief",
                payload=update_payload,
            )
        except Exception:
            logger.exception(
                "Florence sync update brief compose failed household_id=%s connection_id=%s",
                connection.household_id,
                connection.id,
            )
            update_message = None
        if update_message and group_channel is not None and not deliver_to_group:
            try:
                group_message = self._household_chat_service_getter().compose_operator_message(
                    household_id=connection.household_id,
                    channel_id=primary_channel.id,
                    actor_member_id=connection.member_id,
                    kind="group_promotion",
                    payload={"source_text": update_message},
                )
            except Exception:
                logger.exception(
                    "Florence sync update group-promotion compose failed household_id=%s connection_id=%s",
                    connection.household_id,
                    connection.id,
                )
        if not update_message:
            update_message, group_message = self.fallback_sync_update_brief_messages(
                deliver_to_group=deliver_to_group,
                group_available=group_channel is not None,
                candidates=candidates,
            )
        sent_update = self.delivery_service.send_channel_message(
            channel=primary_channel,
            message=update_message,
            store=target_store,
            message_metadata=(
                {
                    "promotion_kind": "sync_update_brief",
                    "promotable_group_message": group_message,
                }
                if group_message and not deliver_to_group
                else None
            ),
        )
        if not sent_update:
            return False
        self.mark_sync_update_brief_sent(
            connection=connection,
            store=target_store,
            channel_id=primary_channel.id,
            snapshot=current_snapshot,
        )
        self._manager_service(store).record_pilot_event(
            household_id=connection.household_id,
            event_type="sync_update_brief_sent",
            member_id=connection.member_id,
            channel_id=primary_channel.id,
            metadata={
                "connection_id": connection.id,
                "gmail_count": int(current_snapshot.get("gmail_count") or 0),
                "calendar_count": int(current_snapshot.get("calendar_count") or 0),
                "candidate_count": int(current_snapshot.get("candidate_count") or 0),
                "trigger": trigger,
            },
        )
        return True

    @staticmethod
    def fallback_sync_activation_brief_messages(
        *,
        deliver_to_group: bool,
        group_available: bool,
    ) -> tuple[str, str | None]:
        message = (
            "Your recent email and calendar are connected, and the first pass is ready. "
            "Ask me what matters this week, what might slip, or what Florence should handle next."
        )
        if not deliver_to_group and group_available:
            message += " If you want, I can also share a short version with the parent group."
        return message, None

    @staticmethod
    def fallback_sync_update_brief_messages(
        *,
        deliver_to_group: bool,
        group_available: bool,
        candidates: list[Any] | None = None,
    ) -> tuple[str, str | None]:
        candidate_list = list(candidates or [])
        titled_candidates = [
            candidate for candidate in candidate_list
            if str(getattr(candidate, "title", "") or "").strip()
        ]
        if titled_candidates:
            first_title = str(getattr(titled_candidates[0], "title", "") or "").strip()
            first_summary = str(getattr(titled_candidates[0], "summary", "") or "").strip()
            if len(titled_candidates) == 1:
                message = f"I finished another sync pass. The main thing I want you to check is {first_title}."
                if first_summary and first_summary != first_title:
                    message += f" {first_summary}"
            else:
                second_title = str(getattr(titled_candidates[1], "title", "") or "").strip()
                message = (
                    "I finished another sync pass. "
                    f"The main things I want you to check are {first_title} and {second_title}."
                )
                if len(titled_candidates) > 2:
                    message += " There are a couple more updates behind those too."
        else:
            message = (
                "I finished another sync pass and there are a few household updates worth checking. "
                "I can walk through the important ones."
            )
        if not deliver_to_group and group_available:
            message += " If you want, I can also share a short version with the parent group."
        return message, None

    @staticmethod
    def sync_activation_brief_already_sent(*, connection: Any) -> bool:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        return bool(str(metadata.get("initial_sync_activation_brief_sent_at") or "").strip())

    @staticmethod
    def mark_sync_activation_brief_sent(
        *,
        connection: Any,
        store: FlorenceStateDB,
        channel_id: str,
        snapshot: dict[str, object] | None = None,
    ) -> None:
        sent_at = datetime.now(timezone.utc).isoformat()
        metadata = dict(getattr(connection, "metadata", {}) or {})
        metadata["initial_sync_activation_brief_sent_at"] = sent_at
        metadata["initial_sync_activation_brief_channel_id"] = channel_id
        metadata["last_sync_brief_sent_at"] = sent_at
        metadata["last_sync_brief_channel_id"] = channel_id
        metadata["last_sync_brief_kind"] = "activation"
        if snapshot is not None:
            metadata["last_sync_brief_snapshot"] = dict(snapshot)
        store.upsert_google_connection(replace(connection, metadata=metadata))

    @staticmethod
    def last_sync_brief_snapshot(*, connection: Any) -> dict[str, object] | None:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        raw_snapshot = metadata.get("last_sync_brief_snapshot")
        return dict(raw_snapshot) if isinstance(raw_snapshot, dict) else None

    @staticmethod
    def mark_sync_update_brief_sent(
        *,
        connection: Any,
        store: FlorenceStateDB,
        channel_id: str,
        snapshot: dict[str, object],
    ) -> None:
        sent_at = datetime.now(timezone.utc).isoformat()
        metadata = dict(getattr(connection, "metadata", {}) or {})
        metadata["last_sync_update_brief_sent_at"] = sent_at
        metadata["last_sync_update_brief_channel_id"] = channel_id
        metadata["last_sync_brief_sent_at"] = sent_at
        metadata["last_sync_brief_channel_id"] = channel_id
        metadata["last_sync_brief_kind"] = "update"
        metadata["last_sync_brief_snapshot"] = dict(snapshot)
        store.upsert_google_connection(replace(connection, metadata=metadata))

    @staticmethod
    def mark_connection_sync_queued(store: FlorenceStateDB, *, connection_id: str) -> None:
        connection = store.get_google_connection(connection_id)
        if connection is None:
            return
        metadata = dict(connection.metadata)
        if metadata.get("initial_sync_completed_at"):
            return
        metadata["initial_sync_state"] = "queued"
        metadata["sync_phase"] = "account_connected"
        metadata["initial_sync_queued_at"] = datetime.now(timezone.utc).isoformat()
        metadata["last_sync_status"] = "queued"
        metadata.pop("last_sync_error", None)
        store.upsert_google_connection(replace(connection, metadata=metadata))

    @staticmethod
    def mark_connection_sync_error(
        store: FlorenceStateDB,
        *,
        connection_id: str,
        error_message: str,
    ) -> None:
        connection = store.get_google_connection(connection_id)
        if connection is None:
            return
        metadata = dict(connection.metadata)
        metadata["last_sync_status"] = "error"
        metadata["last_sync_error"] = error_message
        metadata["last_sync_failed_at"] = datetime.now(timezone.utc).isoformat()
        if not metadata.get("initial_sync_completed_at"):
            metadata["initial_sync_state"] = "attention_needed"
            metadata["sync_phase"] = "attention_needed"
        store.upsert_google_connection(replace(connection, metadata=metadata))

    @staticmethod
    def _sync_brief_payload(
        *,
        connection: Any,
        candidates: list[Any],
    ) -> dict[str, object]:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        candidate_payloads = []
        for candidate in candidates:
            candidate_payloads.append(
                {
                    "id": str(getattr(candidate, "id", "") or "").strip(),
                    "title": str(getattr(candidate, "title", "") or "").strip(),
                    "summary": str(getattr(candidate, "summary", "") or "").strip(),
                    "state": str(getattr(candidate, "state", "") or "").strip(),
                    "confirmation_question": str(getattr(candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                }
            )
        return {
            "gmail_count": int(metadata.get("last_gmail_item_count") or 0),
            "calendar_count": int(metadata.get("last_calendar_item_count") or 0),
            "candidate_count": int(metadata.get("last_candidate_count") or len(candidates)),
            "candidates": candidate_payloads,
        }

    @staticmethod
    def _sync_brief_snapshot(
        *,
        connection: Any,
        candidates: list[Any],
    ) -> dict[str, object]:
        payload = FlorenceHouseholdOperationsService._sync_brief_payload(
            connection=connection,
            candidates=candidates,
        )
        titles = [
            str(candidate.get("title") or "").strip()
            for candidate in payload["candidates"]
            if str(candidate.get("title") or "").strip()
        ]
        ids = [
            str(candidate.get("id") or "").strip()
            for candidate in payload["candidates"]
            if str(candidate.get("id") or "").strip()
        ]
        return {
            "gmail_count": int(payload.get("gmail_count") or 0),
            "calendar_count": int(payload.get("calendar_count") or 0),
            "candidate_count": int(payload.get("candidate_count") or 0),
            "candidate_titles": titles[:5],
            "candidate_ids": ids[:5],
            "signature": FlorenceHouseholdOperationsService._sync_brief_signature(
                gmail_count=int(payload.get("gmail_count") or 0),
                calendar_count=int(payload.get("calendar_count") or 0),
                candidate_count=int(payload.get("candidate_count") or 0),
                candidate_ids=ids[:5],
                candidate_titles=titles[:5],
            ),
        }

    @staticmethod
    def _sync_brief_signature(
        *,
        gmail_count: int,
        calendar_count: int,
        candidate_count: int,
        candidate_ids: list[str],
        candidate_titles: list[str],
    ) -> str:
        return json.dumps(
            {
                "gmail_count": gmail_count,
                "calendar_count": calendar_count,
                "candidate_count": candidate_count,
                "candidate_ids": candidate_ids,
                "candidate_titles": candidate_titles,
            },
            sort_keys=True,
            ensure_ascii=True,
        )

    @staticmethod
    def _sync_brief_has_meaningful_content(snapshot: dict[str, object]) -> bool:
        return int(snapshot.get("candidate_count") or 0) > 0

    def _preferred_sync_brief_channel(
        self,
        *,
        connection: Any,
        fallback_channel: Any | None,
        store: FlorenceStateDB,
    ) -> Any | None:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        channel_id = str(
            metadata.get("last_sync_brief_channel_id")
            or metadata.get("initial_sync_activation_brief_channel_id")
            or ""
        ).strip()
        if channel_id:
            existing_channel = store.get_channel(channel_id)
            if existing_channel is not None:
                return existing_channel
        resolved_fallback = fallback_channel
        if resolved_fallback is None:
            default_channel_id = self._manager_service(store).default_dm_channel_id(
                household_id=connection.household_id,
                member_id=connection.member_id,
            )
            if default_channel_id:
                resolved_fallback = store.get_channel(default_channel_id)
        if resolved_fallback is None:
            return None
        return self.delivery_service.preferred_household_channel(
            household_id=connection.household_id,
            fallback_channel=resolved_fallback,
            store=store,
        )

    def _candidate_warrants_proactive_review_prompt(
        self,
        candidate: Any,
        *,
        store: FlorenceStateDB,
    ) -> bool:
        if getattr(candidate, "state", None) != CandidateState.PENDING_REVIEW:
            return False
        if not self._review_service(store).is_candidate_reviewable_now(candidate=candidate):
            return False

        metadata = dict(getattr(candidate, "metadata", {}) or {})
        candidate_scope = str(metadata.get("candidate_scope") or "shared_household").strip().lower()
        raw_metadata = dict(metadata.get("raw_metadata") or {})
        temporal_evidence = dict(raw_metadata.get("temporal_evidence") or {})
        has_temporal_evidence = any(
            temporal_evidence.get(key) for key in ("date_match", "time_range", "single_time")
        )
        reason_tags = {
            str(tag).strip().lower()
            for tag in list(raw_metadata.get("reason_tags") or [])
            if str(tag).strip()
        }
        confidence_bps = int(getattr(candidate, "confidence_bps", 0) or 0)
        if candidate_scope == "private_parent":
            if getattr(candidate, "source_kind", None) == GoogleSourceKind.GOOGLE_CALENDAR:
                return True
            connection_id = str(metadata.get("google_connection_id") or "").strip()
            if connection_id:
                connection = store.get_google_connection(connection_id)
                connection_metadata = dict(getattr(connection, "metadata", {}) or {}) if connection is not None else {}
                if str(connection_metadata.get("initial_sync_state") or "").strip().lower() == "running":
                    return False
            return confidence_bps >= 7_200 and (
                has_temporal_evidence
                or "logistics_signal" in reason_tags
                or "schedule_signal" in reason_tags
            )

        if getattr(candidate, "source_kind", None) == GoogleSourceKind.GOOGLE_CALENDAR:
            return True

        if str(metadata.get("source_visibility") or "").strip().lower() == "shared":
            return True

        connection_id = str(metadata.get("google_connection_id") or "").strip()
        if connection_id:
            connection = store.get_google_connection(connection_id)
            connection_metadata = dict(getattr(connection, "metadata", {}) or {}) if connection is not None else {}
            if str(connection_metadata.get("initial_sync_state") or "").strip().lower() == "running":
                return False

        anchor_hits = int(raw_metadata.get("anchor_hits") or 0)
        sender_looks_school = bool(raw_metadata.get("sender_looks_school"))

        if sender_looks_school and (anchor_hits > 0 or has_temporal_evidence) and confidence_bps >= 7_600:
            return True
        if anchor_hits >= 2 and has_temporal_evidence and confidence_bps >= 7_200:
            return True
        if (
            anchor_hits > 0
            and "activity_signal" in reason_tags
            and has_temporal_evidence
            and confidence_bps >= 8_000
        ):
            return True
        return False

    def _manager_service(self, store: FlorenceStateDB | None) -> FlorenceHouseholdManagerService:
        target_store = store or self.store
        if target_store is self.store:
            return self._household_manager_service
        return FlorenceHouseholdManagerService(target_store)

    def _review_service(self, store: FlorenceStateDB | None) -> FlorenceCandidateReviewService:
        target_store = store or self.store
        if target_store is self.store:
            return self._candidate_review_service
        return FlorenceCandidateReviewService(target_store)
