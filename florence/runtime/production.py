"""Production orchestration for Florence HTTP, delivery, and sync notifications."""

from __future__ import annotations

import html
import logging
import json
import threading
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from florence.config import FlorenceSettings
from florence.linq import FlorenceLinqClient
from florence.linq.media import enrich_linq_payload_with_media_text
from florence.contracts import CandidateState, ChannelType, HouseholdBriefingKind
from florence.google import decode_google_oauth_state
from florence.runtime.entrypoints import FlorenceEntrypointService, FlorenceGoogleOauthConfig
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    FlorenceHouseholdManagerService,
    FlorenceGoogleSyncPersistenceService,
    FlorenceGoogleSyncWorkerService,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FlorenceHTTPResult:
    status_code: int
    content_type: str
    body: str


class FlorenceProductionService:
    """Glue layer for real webhook handling, delivery, and sync notifications."""

    def __init__(
        self,
        settings: FlorenceSettings,
        *,
        store: FlorenceStateDB | None = None,
    ):
        self.settings = settings
        self.store = store or FlorenceStateDB(settings.server.database_url or settings.server.db_path)
        google_oauth = (
            FlorenceGoogleOauthConfig(
                client_id=settings.google.client_id or "",
                client_secret=settings.google.client_secret or "",
                redirect_uri=settings.google.redirect_uri or "",
                state_secret=settings.google.state_secret or "",
            )
            if settings.google.configured
            else None
        )
        self.entrypoints = FlorenceEntrypointService(
            self.store,
            google_oauth=google_oauth,
            household_chat_model=settings.hermes.model,
            household_chat_max_iterations=settings.hermes.max_iterations,
            household_chat_provider=settings.hermes.provider,
            household_chat_enabled_toolsets=settings.hermes.enabled_toolsets,
            household_chat_disabled_toolsets=settings.hermes.disabled_toolsets,
        )
        self.linq = FlorenceLinqClient(settings.linq)
        self.candidate_review_service = FlorenceCandidateReviewService(self.store)
        self.household_manager_service = FlorenceHouseholdManagerService(self.store)
        self.sync_worker = FlorenceGoogleSyncWorkerService(
            self.store,
            FlorenceGoogleSyncPersistenceService(self.store),
        )
        # Threaded webhook handling can race onboarding stage updates when
        # parents send multiple messages quickly. Serialize by Linq chat.
        self._linq_chat_locks_guard = threading.Lock()
        self._linq_chat_locks: dict[str, threading.Lock] = {}

    def close(self) -> None:
        self.store.close()

    def handle_linq_webhook(
        self,
        *,
        payload: dict[str, Any],
        raw_body: bytes,
        webhook_signature: str | None,
        webhook_timestamp: str | None,
    ) -> FlorenceHTTPResult:
        if not self.linq.verify_webhook_signature(
            raw_body=raw_body,
            timestamp=webhook_timestamp,
            signature=webhook_signature,
        ):
            return self._json_result(403, {"ok": False, "error": "invalid_linq_webhook_signature"})
        try:
            enrich_linq_payload_with_media_text(payload, linq_api_key=self.settings.linq.api_key)
        except Exception:
            logger.exception("Failed to enrich Linq payload with media text")
        chat_lock = self._lock_for_linq_chat(self._linq_chat_id(payload))
        with chat_lock:
            try:
                result = self.entrypoints.handle_linq_payload(payload)
                reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
                if reply_messages and result.channel_id:
                    channel = self.store.get_channel(result.channel_id)
                    if channel is not None:
                        for message in reply_messages:
                            self._safe_send_channel_message(channel=channel, message=message, record_message=False)

                if result.group_announcement and result.household_id:
                    group_channel = self._find_group_channel(result.household_id, provider="linq")
                    if group_channel is not None:
                        self._safe_send_channel_message(channel=group_channel, message=result.group_announcement)

                return self._json_result(
                    200,
                    {
                        "ok": True,
                        "consumed": result.consumed,
                        "householdId": result.household_id,
                        "memberId": result.member_id,
                        "channelId": result.channel_id,
                        "error": result.error,
                    },
                )
            except Exception:
                logger.exception("Florence Linq webhook failed")
                return self._json_result(500, {"ok": False, "error": "internal_linq_webhook_error"})

    def handle_google_callback(
        self,
        *,
        code: str | None,
        state: str | None,
        error: str | None = None,
    ) -> FlorenceHTTPResult:
        if error:
            return self._html_result(
                400,
                self._render_google_callback_page(
                    title="Google connection canceled",
                    message="No problem. Go back to Florence and try again whenever you are ready.",
                ),
            )
        if not code or not state:
            return self._html_result(
                400,
                self._render_google_callback_page(
                    title="Google connection failed",
                    message="That callback was missing the Google authorization details I need. Try the connect link again from Florence.",
                ),
            )
        if not self.settings.google.configured:
            return self._html_result(
                503,
                self._render_google_callback_page(
                    title="Google not configured",
                    message="Florence is not configured with Google OAuth credentials yet.",
                ),
            )

        try:
            oauth_state = decode_google_oauth_state(state, self.settings.google.state_secret or "")
            callback = self.entrypoints.google_account_link_service.handle_callback(code=code, raw_state=state)
            sync_result = self.sync_worker.sync_connection(
                callback.connection.id,
                client_id=self.settings.google.client_id,
                client_secret=self.settings.google.client_secret,
            )

            dm_message = callback.onboarding_transition.prompt.text if callback.onboarding_transition.prompt else "Google connected."
            review_message: str | None = None
            if callback.onboarding_transition.state.is_complete:
                review_prompt = self.candidate_review_service.build_next_review_prompt(
                    household_id=callback.connection.household_id,
                    member_id=callback.connection.member_id,
                )
                if review_prompt is not None:
                    review_message = review_prompt.text

            if oauth_state.thread_id and (dm_message or review_message):
                channel = self.store.get_channel(oauth_state.thread_id)
                if channel is not None:
                    if dm_message:
                        self._safe_send_channel_message(channel=channel, message=dm_message)
                    if review_message:
                        self._safe_send_channel_message(channel=channel, message=review_message)
                else:
                    fallback_channel = self._find_channel_by_provider_id(oauth_state.thread_id)
                    if fallback_channel is not None:
                        if dm_message:
                            self._safe_send_channel_message(channel=fallback_channel, message=dm_message)
                        if review_message:
                            self._safe_send_channel_message(channel=fallback_channel, message=review_message)

            summary = (
                f"Florence is now connected to {callback.connection.email}. "
                f"The first sync found {len(sync_result.sync_result.candidates)} candidate item"
                f"{'' if len(sync_result.sync_result.candidates) == 1 else 's'}."
            )
            return self._html_result(
                200,
                self._render_google_callback_page(
                    title="Google connected",
                    message=f"{summary} You can go back to your conversation.",
                ),
            )
        except Exception as exc:
            logger.exception("Florence Google callback failed")
            return self._html_result(
                400,
                self._render_google_callback_page(
                    title="Google connection failed",
                    message=str(exc),
                ),
            )

    def run_sync_pass(self) -> dict[str, int]:
        households = self.store.list_households()
        counters = {
            "households": 0,
            "connections": 0,
            "candidates": 0,
            "review_nudges": 0,
            "nudges_sent": 0,
            "briefings_sent": 0,
            "nudges": 0,
        }
        for household in households:
            self.household_manager_service.ensure_briefing_routines(household_id=household.id)
            results = self.sync_worker.sync_household(
                household_id=household.id,
                client_id=self.settings.google.client_id,
                client_secret=self.settings.google.client_secret,
            )
            household_touched = bool(results)
            counters["connections"] += len(results)
            for result in results:
                counters["candidates"] += len(result.sync_result.candidates)
                if self._nudge_for_new_pending_candidates(
                    household_id=result.connection.household_id,
                    member_id=result.connection.member_id,
                    candidates=result.sync_result.candidates,
                ):
                    counters["review_nudges"] += 1
                    counters["nudges"] += 1
                    household_touched = True
            sent_nudges = self._dispatch_due_household_nudges(household_id=household.id)
            counters["nudges_sent"] += sent_nudges
            counters["nudges"] += sent_nudges
            if sent_nudges:
                household_touched = True
            sent_briefings = self._dispatch_due_household_briefings(household_id=household.id)
            counters["briefings_sent"] += sent_briefings
            if sent_briefings:
                household_touched = True
            if household_touched:
                counters["households"] += 1
        return counters

    def _nudge_for_new_pending_candidates(
        self,
        *,
        household_id: str,
        member_id: str,
        candidates: list[Any],
    ) -> bool:
        newly_pending = []
        for candidate in candidates:
            if candidate.state != CandidateState.PENDING_REVIEW:
                continue
            if candidate.metadata.get("review_nudged_at"):
                continue
            newly_pending.append(candidate)

        if not newly_pending:
            return False

        sessions = self.store.list_member_onboarding_sessions(household_id=household_id, member_id=member_id)
        if not sessions:
            return False
        dm_thread_id = sessions[0].thread_id
        prompt = self.candidate_review_service.build_next_review_prompt(
            household_id=household_id,
            member_id=member_id,
        )
        if prompt is None:
            return False

        channel = self._find_channel_by_provider_id(dm_thread_id)
        if channel is not None:
            self._safe_send_channel_message(channel=channel, message=prompt.text)

        nudged_at = datetime.utcnow().isoformat() + "Z"
        for candidate in newly_pending:
            metadata = dict(candidate.metadata)
            metadata["review_nudged_at"] = nudged_at
            self.store.upsert_imported_candidate(replace(candidate, metadata=metadata))
        return True

    def _find_group_channel(self, household_id: str, *, provider: str) -> Any | None:
        channels = self.store.list_channels(household_id=household_id, channel_type=ChannelType.HOUSEHOLD_GROUP)
        for channel in channels:
            if channel.provider == provider:
                return channel
        return None

    def _find_channel_by_provider_id(self, provider_channel_id: str) -> Any | None:
        for household in self.store.list_households():
            for channel in self.store.list_channels(household_id=household.id):
                if channel.provider_channel_id == provider_channel_id:
                    return channel
        return None

    @staticmethod
    def _linq_chat_id(payload: dict[str, Any]) -> str:
        data = payload.get("data")
        if not isinstance(data, dict):
            return "__unknown_chat__"
        chat = data.get("chat")
        if not isinstance(chat, dict):
            return "__unknown_chat__"
        chat_id = str(chat.get("id") or "").strip()
        return chat_id or "__unknown_chat__"

    def _lock_for_linq_chat(self, chat_id: str) -> threading.Lock:
        key = chat_id or "__unknown_chat__"
        with self._linq_chat_locks_guard:
            lock = self._linq_chat_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._linq_chat_locks[key] = lock
            return lock

    def _dispatch_due_household_nudges(self, *, household_id: str) -> int:
        sent = 0
        for nudge in self.household_manager_service.list_due_nudges(household_id=household_id):
            channel = self.store.get_channel(nudge.channel_id) if nudge.channel_id else None
            if channel is None and nudge.recipient_member_id:
                fallback_channel_id = self.household_manager_service.default_dm_channel_id(
                    household_id=household_id,
                    member_id=nudge.recipient_member_id,
                )
                if fallback_channel_id:
                    channel = self.store.get_channel(fallback_channel_id)
            if channel is None or not nudge.message.strip():
                continue
            if self._safe_send_channel_message(channel=channel, message=nudge.message):
                self.household_manager_service.mark_nudge_sent(nudge_id=nudge.id)
                self.household_manager_service.record_pilot_event(
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

    def _dispatch_due_household_briefings(self, *, household_id: str) -> int:
        chat_service = self.entrypoints.household_chat_service
        if chat_service is None:
            return 0
        sent = 0
        for routine in self.household_manager_service.list_due_briefing_routines(household_id=household_id):
            metadata = dict(routine.metadata)
            kind_raw = str(metadata.get("brief_kind") or HouseholdBriefingKind.MORNING.value).strip().lower()
            try:
                brief_kind = HouseholdBriefingKind(kind_raw)
            except ValueError:
                brief_kind = HouseholdBriefingKind.MORNING
            recipient_member_id = routine.owner_member_id or self.household_manager_service.default_recipient_member_id(household_id)
            channel_id = str(metadata.get("channel_id") or "").strip()
            if not channel_id:
                channel_id = self.household_manager_service.default_dm_channel_id(
                    household_id=household_id,
                    member_id=recipient_member_id,
                ) or ""
            channel = self.store.get_channel(channel_id) if channel_id else None
            if channel is None:
                continue
            try:
                brief_message = chat_service.compose_brief(
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
            if self._safe_send_channel_message(channel=channel, message=brief_message):
                self.household_manager_service.mark_briefing_routine_sent(routine_id=routine.id)
                self.household_manager_service.record_pilot_event(
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

    def _safe_send_channel_message(self, *, channel: Any, message: str, record_message: bool = True) -> bool:
        try:
            if channel.provider == "linq":
                self.linq.send_text(chat_id=channel.provider_channel_id, message=message)
            else:
                return False
            if record_message:
                self.entrypoints.ingress.append_assistant_message(
                    household_id=channel.household_id,
                    channel_id=channel.id,
                    body=message,
                    metadata={
                        "provider": channel.provider,
                        "transport_thread_id": channel.provider_channel_id,
                    },
                )
            return True
        except Exception:
            logger.exception("Florence transport delivery failed for channel %s", channel.provider_channel_id)
            return False

    @staticmethod
    def _render_google_callback_page(*, title: str, message: str) -> str:
        safe_title = html.escape(title)
        safe_message = html.escape(message)
        return (
            "<!doctype html>"
            "<html><head><meta charset='utf-8'><title>"
            + safe_title
            + "</title><style>"
            "body{font-family:ui-sans-serif,system-ui,sans-serif;background:#f7f3eb;color:#1f1f1f;padding:48px;line-height:1.5;}"
            ".card{max-width:720px;margin:0 auto;background:#fffdf8;border:1px solid #d9ceb7;border-radius:16px;padding:32px;"
            "box-shadow:0 12px 32px rgba(0,0,0,0.08);}h1{margin-top:0;font-size:28px;}p{font-size:16px;}"
            "</style></head><body><div class='card'><h1>"
            + safe_title
            + "</h1><p>"
            + safe_message
            + "</p></div></body></html>"
        )

    @staticmethod
    def _json_result(status_code: int, payload: dict[str, Any]) -> FlorenceHTTPResult:
        return FlorenceHTTPResult(
            status_code=status_code,
            content_type="application/json; charset=utf-8",
            body=json.dumps(payload, separators=(",", ":")),
        )

    @staticmethod
    def _html_result(status_code: int, body: str) -> FlorenceHTTPResult:
        return FlorenceHTTPResult(
            status_code=status_code,
            content_type="text/html; charset=utf-8",
            body=body,
        )

    @staticmethod
    def _serialize_household(household) -> dict[str, Any]:
        return {
            "id": household.id,
            "name": household.name,
            "timezone": household.timezone,
            "status": household.status.value,
            "settings": household.settings,
        }

    @staticmethod
    def _serialize_member(member) -> dict[str, Any]:
        return {
            "id": member.id,
            "householdId": member.household_id,
            "displayName": member.display_name,
            "role": member.role.value,
            "status": member.status,
        }
