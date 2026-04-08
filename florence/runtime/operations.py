"""Household operations and sync-side effects for Florence runtime."""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable

from florence.contracts import CandidateState, ChannelMessageRole, ChannelType, HouseholdBriefingKind
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    build_candidate_review_prompt_metadata,
    build_household_nudge_metadata,
)
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.delivery import FlorenceChannelDeliveryService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)
_ACTIVE_REVIEW_NUDGE_WINDOW_SECONDS = 15 * 60
_REVIEW_SWEEP_INTERVAL_SECONDS = 12 * 60 * 60
_SYNC_UPDATE_BRIEF_INTERVAL_SECONDS = 6 * 60 * 60


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

        channel = self._review_dm_channel(
            household_id=household_id,
            member_id=member_id,
            store=target_store,
        )
        if channel is None:
            return False
        prompt = candidate_review_service.build_next_dm_review_prompt(
            household_id=household_id,
            member_id=member_id,
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
                        },
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
                message_metadata=build_candidate_review_prompt_metadata(prompt.candidate.id),
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
                        "trigger": "new_pending_candidate",
                    },
                )

        if not sent_prompt:
            return False

        nudged_at = datetime.now(timezone.utc).isoformat()
        for candidate in newly_pending:
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
            prompt = candidate_review_service.build_next_dm_review_prompt(
                household_id=household_id,
                member_id=member_id,
            )
            if prompt is None:
                continue
            prompt_text = prompt.text
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
                        },
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
                message_metadata=build_candidate_review_prompt_metadata(prompt.candidate.id),
            ):
                continue
            metadata = dict(prompt.candidate.metadata) if isinstance(prompt.candidate.metadata, dict) else {}
            metadata["review_nudged_at"] = datetime.now(timezone.utc).isoformat()
            target_store.upsert_imported_candidate(replace(prompt.candidate, metadata=metadata))
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
    ) -> tuple[str, str | None]:
        message = (
            "I finished another sync pass and there are a few household updates worth checking. "
            "Ask what changed or what Florence wants you to look at."
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
