"""Production orchestration for Florence HTTP, delivery, and sync notifications."""

from __future__ import annotations

import hmac
import html
import hashlib
import logging
import json
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlencode

from florence.config import FlorenceSettings
from florence.contracts import (
    Channel,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    Household,
    HouseholdStatus,
    Member,
)
from florence.linq import FlorenceLinqClient
from florence.linq.media import enrich_linq_payload_with_media_text
from florence.google import decode_google_oauth_state
from florence.onboarding import (
    build_google_connected_syncing_message_sequence,
    build_onboarding_ready_message_sequence,
    build_onboarding_ready_syncing_message_sequence,
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
from florence.runtime.household_merge import FlorenceHouseholdMergeService
from florence.runtime.queue import FlorenceGoogleSyncJob, FlorenceRedisGoogleSyncQueue
from florence.runtime.entrypoints import (
    FlorenceEntrypointResult,
    FlorenceEntrypointService,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.operations import FlorenceHouseholdOperationsService
from florence.runtime.reliability import (
    FlorenceReliabilityEvent,
    record_reliability_event,
    transport_event_metadata,
)
from florence.sendblue import FlorenceSendblueClient
from florence.sendblue.media import enrich_sendblue_payload_with_media_text
from florence.state import FlorenceStateDB
from florence.turns import FlorenceTurnTrigger

logger = logging.getLogger(__name__)

_HOUSEHOLD_CALENDAR_LINK_SHARED_MEMBER_IDS_KEY = "calendar_link_shared_member_ids"
_WEB_CHAT_PROVIDER = "web"


def _mask_transport_handle(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) <= 6:
        return "***"
    return f"{text[:3]}…{text[-4:]}"


def _webhook_payload_summary(provider: str, payload: dict[str, Any]) -> dict[str, Any]:
    if provider == "sendblue":
        return {
            "message_id": str(payload.get("message_handle") or "").strip() or None,
            "status": str(payload.get("status") or "").strip() or None,
            "is_outbound": bool(payload.get("is_outbound")),
            "message_type": str(payload.get("message_type") or "").strip() or None,
            "group_id": str(payload.get("group_id") or "").strip() or None,
            "from": _mask_transport_handle(payload.get("from_number")),
            "to": _mask_transport_handle(payload.get("to_number")),
            "number": _mask_transport_handle(payload.get("number")),
            "sendblue_number": _mask_transport_handle(payload.get("sendblue_number")),
            "content_length": len(str(payload.get("content") or "")),
        }
    if provider == "linq":
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        chat = data.get("chat") if isinstance(data.get("chat"), dict) else {}
        sender = data.get("sender_handle") if isinstance(data.get("sender_handle"), dict) else {}
        return {
            "event_type": str(payload.get("event_type") or "").strip() or None,
            "message_id": str(data.get("id") or "").strip() or None,
            "chat_id": str(chat.get("id") or "").strip() or None,
            "is_group": bool(chat.get("is_group")),
            "direction": str(data.get("direction") or "").strip() or None,
            "service": str(data.get("service") or "").strip() or None,
            "sender": _mask_transport_handle(sender.get("handle")),
        }
    return {}


def _stable_web_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


class _StaleGoogleSyncJobError(RuntimeError):
    """Raised when a queued Google sync job points at a deleted connection."""


def _is_unknown_google_connection_error(error: Exception) -> bool:
    return isinstance(error, ValueError) and str(error) == "unknown_google_connection"

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
        self._household_chat_service = household_chat_service
        self.household_manager_service = FlorenceHouseholdManagerService(
            self.store,
            household_chat_service_getter=lambda: self.household_chat_service,
        )
        self.household_calendar_projection_service = FlorenceHouseholdCalendarProjectionService(
            self.store,
            client_id=settings.google.client_id,
            client_secret=settings.google.client_secret,
        )
        self.entrypoints = FlorenceEntrypointService(
            self.store,
            google_oauth=(settings.google if settings.google.configured else None),
            household_chat_service=self.household_chat_service,
            household_manager_service=self.household_manager_service,
            household_merge_service=FlorenceHouseholdMergeService(
                self.store,
                household_calendar_projection_service=self.household_calendar_projection_service,
            ),
            sendblue_blocked_numbers=settings.sendblue.blocked_numbers,
        )
        self.linq = FlorenceLinqClient(settings.linq)
        self.sendblue = FlorenceSendblueClient(settings.sendblue)
        self.candidate_review_service = FlorenceCandidateReviewService(self.store)
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
        self.entrypoints.ingress.chat_bridge.household_chat_service = value

    def close(self) -> None:
        self.store.close()

    def handle_web_chat_snapshot(
        self,
        *,
        auth_email: str | None = None,
        proxy_secret: str | None = None,
    ) -> FlorenceHTTPResult:
        if not self.settings.web_chat.enabled:
            return self._json_result(404, {"ok": False, "error": "web_chat_disabled"})
        access_error = self._authorize_web_chat_request(auth_email=auth_email, proxy_secret=proxy_secret)
        if access_error is not None:
            return access_error
        try:
            household, member, channel = self._ensure_web_chat_context(auth_email=auth_email)
        except PermissionError as exc:
            return self._json_result(403, {"ok": False, "error": str(exc)})
        except ValueError as exc:
            return self._json_result(400, {"ok": False, "error": str(exc)})
        except Exception:
            logger.exception("Florence web chat snapshot failed")
            return self._json_result(500, {"ok": False, "error": "internal_web_chat_error"})
        return self._json_result(200, self._web_chat_payload(household=household, member=member, channel=channel))

    def handle_web_chat_message(
        self,
        *,
        payload: dict[str, Any],
        auth_email: str | None = None,
        proxy_secret: str | None = None,
    ) -> FlorenceHTTPResult:
        if not self.settings.web_chat.enabled:
            return self._json_result(404, {"ok": False, "error": "web_chat_disabled"})
        access_error = self._authorize_web_chat_request(auth_email=auth_email, proxy_secret=proxy_secret)
        if access_error is not None:
            return access_error

        message = str(payload.get("message") or "").strip()
        if not message:
            return self._json_result(400, {"ok": False, "error": "web_chat_message_required"})

        try:
            household, member, channel = self._ensure_web_chat_context(auth_email=auth_email)
            prior_history = self.store.list_channel_messages(channel_id=channel.id, limit=24)
            self._append_web_chat_message(
                household_id=household.id,
                channel_id=channel.id,
                role=ChannelMessageRole.USER,
                body=message,
                sender_member_id=member.id,
            )
            reply = self.household_chat_service.respond(
                household_id=household.id,
                channel_id=channel.id,
                actor_member_id=member.id,
                message_text=message,
                conversation_history=prior_history,
            )
            reply_text = str(getattr(reply, "text", "") or "").strip()
            if not reply_text:
                return self._json_result(502, {"ok": False, "error": "web_chat_empty_reply"})
            self._append_web_chat_message(
                household_id=household.id,
                channel_id=channel.id,
                role=ChannelMessageRole.ASSISTANT,
                body=reply_text,
                sender_member_id=None,
            )
            return self._json_result(
                200,
                self._web_chat_payload(
                    household=household,
                    member=member,
                    channel=channel,
                    reply=reply_text,
                ),
            )
        except PermissionError as exc:
            return self._json_result(403, {"ok": False, "error": str(exc)})
        except ValueError as exc:
            return self._json_result(400, {"ok": False, "error": str(exc)})
        except Exception:
            logger.exception("Florence web chat turn failed")
            return self._json_result(500, {"ok": False, "error": "internal_web_chat_error"})

    def _authorize_web_chat_request(
        self,
        *,
        auth_email: str | None,
        proxy_secret: str | None,
    ) -> FlorenceHTTPResult | None:
        configured_secret = (self.settings.web_chat.proxy_secret or "").strip()
        if not configured_secret:
            return self._json_result(503, {"ok": False, "error": "web_chat_proxy_secret_unconfigured"})
        supplied_secret = str(proxy_secret or "").strip()
        if not supplied_secret or not hmac.compare_digest(supplied_secret, configured_secret):
            return self._json_result(403, {"ok": False, "error": "web_chat_proxy_secret_invalid"})
        if not self._normalize_web_chat_email(auth_email):
            return self._json_result(401, {"ok": False, "error": "web_chat_auth_required"})
        return None

    def _ensure_web_chat_context(self, *, auth_email: str | None) -> tuple[Household, Member, Channel]:
        connection = self.store.find_google_connection_by_email(
            email=self._normalize_web_chat_email(auth_email),
            active_only=True,
        )
        if connection is None:
            raise PermissionError("unknown_web_google_identity")

        household = self.store.get_household(connection.household_id)
        member = self.store.get_member(connection.member_id)
        if household is None or household.status != HouseholdStatus.ACTIVE:
            raise PermissionError("unknown_web_google_identity")
        if member is None or member.household_id != household.id or member.status != "active":
            raise PermissionError("unknown_web_google_identity")

        channel = self._resolve_web_chat_channel(household=household, member=member)
        return household, member, channel

    def _resolve_web_chat_channel(self, *, household: Household, member: Member) -> Channel:
        provider_channel_id = f"web-chat:{household.id}:{member.id}"
        channel_id = _stable_web_id("chan", household.id, member.id, _WEB_CHAT_PROVIDER)

        channel = self.store.get_channel(channel_id)
        if channel is not None:
            if channel.household_id != household.id or channel.provider != _WEB_CHAT_PROVIDER:
                raise ValueError("web_chat_channel_conflict")
            return self._upsert_web_chat_channel(
                channel=channel,
                household=household,
                provider_channel_id=provider_channel_id,
            )

        channel = self.store.get_channel_by_provider_id(
            provider=_WEB_CHAT_PROVIDER,
            provider_channel_id=provider_channel_id,
        )
        if channel is not None:
            if channel.household_id != household.id:
                raise ValueError("web_chat_channel_household_mismatch")
            return self._upsert_web_chat_channel(
                channel=channel,
                household=household,
                provider_channel_id=provider_channel_id,
            )

        return self.store.upsert_channel(
            Channel(
                id=channel_id,
                household_id=household.id,
                provider=_WEB_CHAT_PROVIDER,
                provider_channel_id=provider_channel_id,
                channel_type=ChannelType.WEB_CHAT,
                title="Web chat",
                metadata={
                    "auth_required": True,
                    "external_delivery": "disabled",
                },
            )
        )

    def _upsert_web_chat_channel(
        self,
        *,
        channel: Channel,
        household: Household,
        provider_channel_id: str,
    ) -> Channel:
        metadata = dict(channel.metadata)
        metadata.pop("test_surface", None)
        metadata["auth_required"] = True
        metadata["external_delivery"] = "disabled"
        return self.store.upsert_channel(
            replace(
                channel,
                household_id=household.id,
                provider=_WEB_CHAT_PROVIDER,
                provider_channel_id=channel.provider_channel_id or provider_channel_id,
                channel_type=ChannelType.WEB_CHAT,
                title=channel.title or "Web chat",
                metadata=metadata,
            )
        )

    def _append_web_chat_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        role: ChannelMessageRole,
        body: str,
        sender_member_id: str | None,
    ) -> ChannelMessage:
        message_id = _stable_web_id(
            "chatmsg" if role == ChannelMessageRole.USER else "assistant",
            channel_id,
            role.value,
            str(time.time_ns()),
        )
        return self.store.append_channel_message(
            ChannelMessage(
                id=message_id,
                household_id=household_id,
                channel_id=channel_id,
                sender_role=role,
                sender_member_id=sender_member_id,
                body=body,
                metadata={
                    "provider": _WEB_CHAT_PROVIDER,
                    "auth_required": True,
                    "external_delivery": "disabled",
                },
                created_at=time.time(),
            )
        )

    @staticmethod
    def _normalize_web_chat_email(email: str | None) -> str:
        return str(email or "").strip().lower()

    def _web_chat_payload(
        self,
        *,
        household: Household,
        member: Member,
        channel: Channel,
        reply: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": True,
            "enabled": True,
            "household": {
                "id": household.id,
                "name": household.name,
                "timezone": household.timezone,
            },
            "member": {
                "id": member.id,
                "displayName": member.display_name,
                "role": member.role.value,
            },
            "channel": {
                "id": channel.id,
                "provider": channel.provider,
                "providerChannelId": channel.provider_channel_id,
                "type": channel.channel_type.value,
                "title": channel.title,
            },
            "messages": [
                {
                    "id": message.id,
                    "role": message.sender_role.value,
                    "body": message.body,
                    "createdAt": message.created_at,
                }
                for message in self.store.list_channel_messages(channel_id=channel.id, limit=80)
            ],
        }
        if reply is not None:
            payload["reply"] = reply
        return payload

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

        sent = self.household_operations.send_system_messages(
            trigger_kind=FlorenceTurnTrigger.SYSTEM,
            household_id=household_id,
            channel=channel,
            actor_member_id=member_id,
            messages=(self._build_household_calendar_link_message(calendar_web_url=calendar_web_url),),
            metadata={
                "operation_kind": "household_calendar_link",
                "calendar_web_url": calendar_web_url,
            },
            message_metadata={
                "delivery_kind": "household_calendar_link",
            },
        )
        if not sent:
            return
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
                dm_messages = build_onboarding_ready_syncing_message_sequence()
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
                    self.household_operations.send_system_messages(
                        trigger_kind=(
                            FlorenceTurnTrigger.ONBOARDING
                            if became_complete
                            else FlorenceTurnTrigger.SYNC_BRIEF
                        ),
                        household_id=callback.connection.household_id,
                        channel=channel,
                        actor_member_id=callback.connection.member_id,
                        messages=tuple(dm_messages),
                        metadata={
                            "operation_kind": "google_callback_followup",
                            "connection_id": callback.connection.id,
                            "became_complete": became_complete,
                        },
                        message_metadata={
                            "delivery_kind": "google_callback_followup",
                        },
                    )

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
            previous_connection = store.get_google_connection(connection_id)
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
                sent_activation = self.household_operations.deliver_sync_activation_brief(
                    connection=freshest_connection,
                    candidates=list(result.sync_result.candidates),
                    fallback_channel=channel,
                    store=store,
                )
                if not sent_activation:
                    self.household_operations.deliver_sync_update_brief(
                        connection=freshest_connection,
                        previous_connection=previous_connection,
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
        except Exception as exc:
            if _is_unknown_google_connection_error(exc):
                logger.warning(
                    "Florence background Google sync dropped stale job for deleted connection_id=%s",
                    connection_id,
                )
                if raise_on_error:
                    raise _StaleGoogleSyncJobError(connection_id) from exc
                return
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
                    failed_connection = store.get_google_connection(connection_id)
                    self.household_operations.send_system_messages(
                        trigger_kind=FlorenceTurnTrigger.SYNC_BRIEF,
                        household_id=channel.household_id,
                        channel=channel,
                        actor_member_id=failed_connection.member_id if failed_connection is not None else None,
                        messages=("Google connected, but the first sync hit an error. Ask me to retry if it keeps happening.",),
                        store=store,
                        metadata={
                            "operation_kind": "google_sync_error",
                            "connection_id": connection_id,
                            "error_message": "initial_sync_failed",
                        },
                        message_metadata={
                            "delivery_kind": "google_sync_error",
                        },
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
        except _StaleGoogleSyncJobError:
            logger.warning(
                "Florence queued Google sync acknowledged stale deleted connection_id=%s",
                claimed.job.connection_id,
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
        return self.run_sync_pass_with_options(include_automation=True)

    def run_sync_pass_with_options(self, *, include_automation: bool) -> dict[str, int]:
        households = self.store.list_households()
        counters = {
            "households": 0,
            "connections": 0,
            "candidates": 0,
            "review_nudges": 0,
            "review_sweeps": 0,
            "sync_update_briefs": 0,
            "nudges_sent": 0,
            "briefings_sent": 0,
            "nudges": 0,
        }
        if include_automation:
            automation_counters = self.run_automation_pass()
            for key, value in automation_counters.items():
                counters[key] = counters.get(key, 0) + value
        for household in households:
            self.household_manager_service.ensure_briefing_routines(household_id=household.id)
            previous_connections = {
                connection.id: connection
                for connection in self.store.list_google_connections(household_id=household.id)
            }
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
            sent_sync_update_briefs = self.household_operations.dispatch_due_sync_update_briefs(
                household_id=household.id,
                sync_results=results,
                previous_connections=previous_connections,
            )
            counters["sync_update_briefs"] += sent_sync_update_briefs
            if sent_sync_update_briefs:
                household_touched = True
            if household_touched:
                counters["households"] += 1
        return counters

    def run_automation_pass(self) -> dict[str, int]:
        households = self.store.list_households()
        counters = {
            "households": 0,
            "review_sweeps": 0,
            "nudges_sent": 0,
            "briefings_sent": 0,
            "nudges": 0,
        }
        for household in households:
            household_touched = False
            self.household_manager_service.ensure_briefing_routines(household_id=household.id)
            sent_nudges = self.household_operations.dispatch_due_household_nudges(household_id=household.id)
            counters["nudges_sent"] += sent_nudges
            counters["nudges"] += sent_nudges
            if sent_nudges:
                household_touched = True
            sent_briefings = self.household_operations.dispatch_due_household_briefings(household_id=household.id)
            counters["briefings_sent"] += sent_briefings
            if sent_briefings:
                household_touched = True
            sent_review_sweeps = self.household_operations.dispatch_due_review_sweeps(household_id=household.id)
            counters["review_sweeps"] += sent_review_sweeps
            if sent_review_sweeps:
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
            logger.warning(
                "Florence webhook rejected provider=%s error=%s summary=%s",
                provider,
                invalid_error,
                _webhook_payload_summary(provider, payload),
            )
            return self._json_result(403, {"ok": False, "error": invalid_error})
        logger.info(
            "Florence webhook received provider=%s summary=%s",
            provider,
            _webhook_payload_summary(provider, payload),
        )
        self._record_webhook_received(provider=provider, payload=payload)
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
                logger.info(
                    "Florence webhook handled provider=%s consumed=%s household_id=%s member_id=%s channel_id=%s error=%s reply_count=%d group_announcement=%s",
                    provider,
                    result.consumed,
                    result.household_id,
                    result.member_id,
                    result.channel_id,
                    result.error,
                    len(result.reply_messages or ((result.reply_text,) if result.reply_text else ())),
                    bool(result.group_announcement),
                )
                return self._webhook_success_result(result)
            except Exception:
                logger.exception(
                    "%s provider=%s summary=%s",
                    failure_log,
                    provider,
                    _webhook_payload_summary(provider, payload),
                )
                return self._json_result(500, {"ok": False, "error": internal_error})

    def _record_webhook_received(self, *, provider: str, payload: dict[str, Any]) -> None:
        summary = _webhook_payload_summary(provider, payload)
        provider_channel_id = None
        if provider == "sendblue":
            group_id = str(summary.get("group_id") or "").strip()
            line = str(payload.get("sendblue_number") or payload.get("to_number") or "").strip()
            contact = str(payload.get("number") or payload.get("from_number") or "").strip()
            if group_id and line:
                provider_channel_id = f"{line}|group:{group_id}"
            elif line and contact:
                provider_channel_id = f"{line}|{contact}"
        elif provider == "linq":
            provider_channel_id = str(summary.get("chat_id") or "").strip() or None
        try:
            record_reliability_event(
                self.store,
                FlorenceReliabilityEvent.INBOUND_RECEIVED,
                metadata=transport_event_metadata(
                    provider=provider,
                    provider_channel_id=provider_channel_id,
                    message_id=str(summary.get("message_id") or "").strip() or None,
                    correlation_id=(
                        str(payload.get("trace_id") or "").strip()
                        or str(payload.get("event_id") or "").strip()
                        or None
                    ),
                    webhook_summary=summary,
                ),
            )
        except Exception:
            logger.exception("Florence webhook reliability logging failed provider=%s", provider)

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
