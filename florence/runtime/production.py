"""Production orchestration for Florence HTTP, delivery, and sync notifications."""

from __future__ import annotations

import html
import logging
import json
import threading
from contextlib import nullcontext
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlencode

from florence.config import FlorenceSettings
from florence.linq import FlorenceLinqClient
from florence.linq.media import enrich_linq_payload_with_media_text
from florence.google import decode_google_oauth_state
from florence.onboarding import (
    build_google_connected_syncing_message_sequence,
    build_onboarding_ready_message_sequence,
)
from florence.runtime.delivery import FlorenceChannelDeliveryService
from florence.runtime.google_services import (
    FlorenceGoogleSyncPersistenceService,
    FlorenceGoogleSyncWorkerService,
)
from florence.runtime.household_calendar_projection import (
    HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY,
    FlorenceHouseholdCalendarProjectionService,
)
from florence.runtime.queue import FlorenceGoogleSyncJob, FlorenceRedisGoogleSyncQueue
from florence.runtime.entrypoints import (
    FlorenceEntrypointResult,
    FlorenceEntrypointService,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.operations import FlorenceHouseholdOperationsService
from florence.sendblue import FlorenceSendblueClient
from florence.sendblue.media import enrich_sendblue_payload_with_media_text
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)

_HOUSEHOLD_CALENDAR_LINK_SHARED_MEMBER_IDS_KEY = "calendar_link_shared_member_ids"

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
        household_chat_service = FlorenceHouseholdChatService(
            self.store,
            model=settings.hermes.model,
            max_iterations=settings.hermes.max_iterations,
            provider=settings.hermes.provider,
            briefing_style=settings.briefing.style,
            briefing_emoji_mode=settings.briefing.emoji_mode,
        )
        self.entrypoints = FlorenceEntrypointService(
            self.store,
            google_oauth=(settings.google if settings.google.configured else None),
            household_chat_service=household_chat_service,
        )
        self._household_chat_service = household_chat_service
        self.linq = FlorenceLinqClient(settings.linq)
        self.sendblue = FlorenceSendblueClient(settings.sendblue)
        self.candidate_review_service = FlorenceCandidateReviewService(self.store)
        self.household_manager_service = FlorenceHouseholdManagerService(self.store)
        self.household_calendar_projection_service = FlorenceHouseholdCalendarProjectionService(
            self.store,
            client_id=settings.google.client_id,
            client_secret=settings.google.client_secret,
        )
        self.delivery_service = FlorenceChannelDeliveryService(
            self.store,
            linq_client_getter=lambda: self.linq,
            sendblue_client_getter=lambda: self.sendblue,
        )
        self.household_operations = FlorenceHouseholdOperationsService(
            self.store,
            delivery_service=self.delivery_service,
            household_chat_service_getter=lambda: self.household_chat_service,
            candidate_review_service=self.candidate_review_service,
            household_manager_service=self.household_manager_service,
        )
        self.sync_worker = FlorenceGoogleSyncWorkerService(
            self.store,
            FlorenceGoogleSyncPersistenceService(self.store),
        )
        self.google_sync_queue = FlorenceRedisGoogleSyncQueue(settings.redis)
        # Threaded webhook handling can race onboarding stage updates when
        # parents send multiple messages quickly. Serialize by Linq chat.
        self._linq_chat_locks_guard = threading.Lock()
        self._linq_chat_locks: dict[str, threading.Lock] = {}
        self._google_sync_jobs_guard = threading.Lock()
        self._google_sync_jobs: set[str] = set()
    @property
    def household_chat_service(self) -> FlorenceHouseholdChatService:
        return self._household_chat_service

    @household_chat_service.setter
    def household_chat_service(self, value: FlorenceHouseholdChatService) -> None:
        self._household_chat_service = value
        self.entrypoints.household_chat_service = value
        self.entrypoints.ingress.household_chat_service = value

    def close(self) -> None:
        self.store.close()

    def _build_household_calendar_link_message(self, *, calendar_web_url: str) -> str:
        return (
            "Your shared household calendar is here:\n"
            f"{calendar_web_url}\n"
            "I’ll keep confirmed shared events synced there."
        )

    def _maybe_send_household_calendar_link(
        self,
        *,
        household_id: str,
        member_id: str,
        channel,
        projection_config: dict[str, Any] | None,
    ) -> None:
        if channel is None or projection_config is None:
            return
        calendar_web_url = str(projection_config.get("calendar_web_url") or "").strip()
        if not calendar_web_url:
            return

        household = self.store.get_household(household_id)
        if household is None:
            return
        raw_projection = household.settings.get(HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY)
        current_projection = dict(raw_projection) if isinstance(raw_projection, dict) else dict(projection_config)
        shared_member_ids = {
            str(item).strip()
            for item in (
                current_projection.get(_HOUSEHOLD_CALENDAR_LINK_SHARED_MEMBER_IDS_KEY)
                if isinstance(current_projection.get(_HOUSEHOLD_CALENDAR_LINK_SHARED_MEMBER_IDS_KEY), list)
                else []
            )
            if str(item).strip()
        }
        if member_id in shared_member_ids:
            return

        self.delivery_service.send_channel_message(
            channel=channel,
            message=self._build_household_calendar_link_message(calendar_web_url=calendar_web_url),
        )
        shared_member_ids.add(member_id)
        current_projection[_HOUSEHOLD_CALENDAR_LINK_SHARED_MEMBER_IDS_KEY] = sorted(shared_member_ids)
        settings = dict(household.settings)
        settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY] = current_projection
        self.store.upsert_household(replace(household, settings=settings))

    def handle_linq_webhook(
        self,
        *,
        payload: dict[str, Any],
        raw_body: bytes,
        webhook_signature: str | None,
        webhook_timestamp: str | None,
    ) -> FlorenceHTTPResult:
        def _verify() -> bool:
            return self.linq.verify_webhook_signature(
                raw_body=raw_body,
                timestamp=webhook_timestamp,
                signature=webhook_signature,
            )

        def _enrich() -> None:
            enrich_linq_payload_with_media_text(payload, linq_api_key=self.settings.linq.api_key)

        return self._handle_webhook(
            provider="linq",
            payload=payload,
            verify=_verify,
            invalid_error="invalid_linq_webhook_signature",
            internal_error="internal_linq_webhook_error",
            handler=self.entrypoints.handle_linq_payload,
            lock=self._lock_for_linq_chat(self._linq_chat_id(payload)),
            enrich=_enrich,
            enrich_error_log="Failed to enrich Linq payload with media text",
            failure_log="Florence Linq webhook failed",
        )

    def handle_sendblue_webhook(
        self,
        *,
        payload: dict[str, Any],
        webhook_secret: str | None,
    ) -> FlorenceHTTPResult:
        def _enrich() -> None:
            enrich_sendblue_payload_with_media_text(payload)

        return self._handle_webhook(
            provider="sendblue",
            payload=payload,
            verify=lambda: self.sendblue.verify_webhook_signature(secret_header=webhook_secret),
            invalid_error="invalid_sendblue_webhook_signature",
            internal_error="internal_sendblue_webhook_error",
            handler=self.entrypoints.handle_sendblue_payload,
            enrich=_enrich,
            enrich_error_log="Failed to enrich Sendblue payload with media text",
            failure_log="Florence Sendblue webhook failed",
        )

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
            became_complete = callback.onboarding_transition.state.is_complete and not self.household_operations.has_onboarding_completion_event(
                household_id=oauth_state.household_id,
                member_id=oauth_state.member_id,
            )
            resolved_channel = None
            if oauth_state.thread_id:
                resolved_channel = self.store.get_channel(oauth_state.thread_id)
                if resolved_channel is None:
                    resolved_channel = self.delivery_service.find_channel_by_provider_id(oauth_state.thread_id)

            if became_complete:
                dm_messages = build_onboarding_ready_message_sequence()
            else:
                dm_messages = ()
                if resolved_channel is not None:
                    try:
                        sync_waiting = self.household_chat_service.compose_operator_message(
                            household_id=oauth_state.household_id,
                            channel_id=resolved_channel.id,
                            actor_member_id=oauth_state.member_id,
                            kind="sync_started",
                        )
                        if sync_waiting is not None and sync_waiting.strip():
                            dm_messages = (sync_waiting.strip(),)
                    except Exception:
                        logger.exception(
                            "Florence Google callback sync-status composition failed household_id=%s member_id=%s",
                            oauth_state.household_id,
                            oauth_state.member_id,
                        )
                if not dm_messages:
                    # Keep Google connect as a quiet background status update.
                    # The active onboarding question already exists in-thread, so
                    # replaying it here makes the conversation feel chaotic.
                    dm_messages = build_google_connected_syncing_message_sequence()
            if oauth_state.thread_id and dm_messages:
                channel = resolved_channel
                if channel is not None:
                    for message in dm_messages:
                        self.delivery_service.send_channel_message(channel=channel, message=message)

            if became_complete:
                self.household_operations.record_onboarding_completion(
                    household_id=callback.connection.household_id,
                    member_id=callback.connection.member_id,
                    channel_id=resolved_channel.id if resolved_channel is not None else "google_callback",
                )

            projection_config = None
            try:
                projection_config = self.household_calendar_projection_service.ensure_projection(
                    household_id=callback.connection.household_id,
                    preferred_connection_id=callback.connection.id,
                )
                projection_config = self.household_calendar_projection_service.sync_household(
                    household_id=callback.connection.household_id,
                    preferred_connection_id=callback.connection.id,
                )
            except Exception:
                logger.exception(
                    "Florence household calendar projection setup failed household_id=%s connection_id=%s",
                    callback.connection.household_id,
                    callback.connection.id,
                )
            if resolved_channel is not None:
                self._maybe_send_household_calendar_link(
                    household_id=callback.connection.household_id,
                    member_id=callback.connection.member_id,
                    channel=resolved_channel,
                    projection_config=projection_config,
                )

            self._launch_google_sync_job(
                connection_id=callback.connection.id,
                thread_id=oauth_state.thread_id or None,
                notify_when_finished=True,
            )
            summary = f"Florence is now connected to {callback.connection.email}. Your recent email and calendar are syncing in the background."
            return self._html_result(
                200,
                self._render_google_callback_page(
                    title="Google connected",
                    message=f"{summary} Go back to your Messages conversation to keep onboarding there.",
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

    def _launch_google_sync_job(
        self,
        *,
        connection_id: str,
        thread_id: str | None,
        notify_when_finished: bool,
    ) -> None:
        self.household_operations.mark_connection_sync_queued(self.store, connection_id=connection_id)
        if self.google_sync_queue.configured:
            try:
                self.google_sync_queue.enqueue(
                    FlorenceGoogleSyncJob(
                        connection_id=connection_id,
                        thread_id=thread_id,
                        notify_when_finished=notify_when_finished,
                    )
                )
                return
            except Exception:
                logger.exception("Failed to enqueue Florence Google sync job; falling back to local thread")

        with self._google_sync_jobs_guard:
            if connection_id in self._google_sync_jobs:
                return
            self._google_sync_jobs.add(connection_id)

        thread = threading.Thread(
            target=self.process_google_sync_job,
            kwargs={
                "connection_id": connection_id,
                "thread_id": thread_id,
                "notify_when_finished": notify_when_finished,
            },
            name=f"florence-google-sync-{connection_id}",
            daemon=True,
        )
        thread.start()

    def process_google_sync_job(
        self,
        *,
        connection_id: str,
        thread_id: str | None,
        notify_when_finished: bool,
        raise_on_error: bool = False,
    ) -> None:
        store = FlorenceStateDB(self.settings.server.database_url or self.settings.server.db_path)
        try:
            sync_worker = FlorenceGoogleSyncWorkerService(
                store,
                FlorenceGoogleSyncPersistenceService(store),
            )
            result = sync_worker.sync_connection(
                connection_id,
                client_id=self.settings.google.client_id,
                client_secret=self.settings.google.client_secret,
            )
            if not thread_id:
                return

            channel = store.get_channel(thread_id)
            if channel is None:
                channel = self.delivery_service.find_channel_by_provider_id(provider_channel_id=thread_id, store=store)
            if channel is None:
                return

            freshest_connection = store.get_google_connection(result.connection.id) or result.connection
            if notify_when_finished:
                self.household_operations.deliver_sync_activation_brief(
                    connection=freshest_connection,
                    candidates=list(result.sync_result.candidates),
                    fallback_channel=channel,
                    store=store,
                )
            self._nudge_for_new_pending_candidates(
                household_id=result.connection.household_id,
                member_id=result.connection.member_id,
                candidates=result.sync_result.candidates,
                store=store,
            )
        except Exception:
            logger.exception("Florence background Google sync failed connection_id=%s", connection_id)
            self.household_operations.mark_connection_sync_error(
                store,
                connection_id=connection_id,
                error_message="initial_sync_failed",
            )
            if thread_id:
                channel = store.get_channel(thread_id)
                if channel is None:
                    channel = self.delivery_service.find_channel_by_provider_id(provider_channel_id=thread_id, store=store)
                if channel is not None:
                    self.delivery_service.send_channel_message(
                        channel=channel,
                        message="Google connected, but the first sync hit an error. Ask me to retry if it keeps happening.",
                        store=store,
                    )
            if raise_on_error:
                raise
        finally:
            store.close()
            with self._google_sync_jobs_guard:
                self._google_sync_jobs.discard(connection_id)

    def run_google_sync_queue_once(self) -> bool:
        claimed = self.google_sync_queue.claim(timeout_seconds=self.settings.redis.google_sync_queue_block_seconds)
        if claimed is None:
            return False
        try:
            self.process_google_sync_job(
                connection_id=claimed.job.connection_id,
                thread_id=claimed.job.thread_id,
                notify_when_finished=claimed.job.notify_when_finished,
                raise_on_error=True,
            )
            self.google_sync_queue.acknowledge(claimed)
            return True
        except Exception:
            logger.exception(
                "Florence queued Google sync failed connection_id=%s attempt=%s",
                claimed.job.connection_id,
                claimed.job.attempt,
            )
            if claimed.job.attempt >= self.settings.redis.google_sync_max_attempts:
                self.google_sync_queue.acknowledge(claimed)
            else:
                self.google_sync_queue.retry(claimed)
            return True

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
            sent_nudges = self.household_operations.dispatch_due_household_nudges(household_id=household.id)
            counters["nudges_sent"] += sent_nudges
            counters["nudges"] += sent_nudges
            if sent_nudges:
                household_touched = True
            sent_briefings = self.household_operations.dispatch_due_household_briefings(household_id=household.id)
            counters["briefings_sent"] += sent_briefings
            if sent_briefings:
                household_touched = True
            if household_touched:
                counters["households"] += 1
        return counters

    def _handle_webhook(
        self,
        *,
        provider: str,
        payload: dict[str, Any],
        verify,
        invalid_error: str,
        internal_error: str,
        handler,
        failure_log: str,
        lock=None,
        enrich=None,
        enrich_error_log: str | None = None,
    ) -> FlorenceHTTPResult:
        if not verify():
            return self._json_result(403, {"ok": False, "error": invalid_error})
        if enrich is not None:
            try:
                enrich()
            except Exception:
                logger.exception(enrich_error_log or "Webhook enrichment failed")
        context = lock if lock is not None else nullcontext()
        with context:
            try:
                result = handler(payload)
                self.delivery_service.deliver_ingress_result(result=result, provider=provider)
                return self._webhook_success_result(result)
            except Exception:
                logger.exception(failure_log)
                return self._json_result(500, {"ok": False, "error": internal_error})

    def _webhook_success_result(self, result: FlorenceEntrypointResult) -> FlorenceHTTPResult:
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

    def _nudge_for_new_pending_candidates(
        self,
        *,
        household_id: str,
        member_id: str,
        candidates: list[Any],
        store: FlorenceStateDB | None = None,
    ) -> bool:
        return self.household_operations.nudge_for_new_pending_candidates(
            household_id=household_id,
            member_id=member_id,
            candidates=candidates,
            store=store,
        )

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
