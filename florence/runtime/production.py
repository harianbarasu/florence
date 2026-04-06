"""Production orchestration for Florence HTTP, delivery, and sync notifications."""

from __future__ import annotations

import html
import logging
import json
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from florence.config import FlorenceSettings
from florence.linq import FlorenceLinqClient
from florence.linq.media import enrich_linq_payload_with_media_text
from florence.contracts import (
    CandidateState,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    HouseholdBriefingKind,
    HouseholdEventStatus,
    HouseholdProfileKind,
)
from florence.google import decode_google_oauth_state
from florence.onboarding import (
    build_google_connected_syncing_message_sequence,
    build_onboarding_ready_message_sequence,
    extract_child_names,
    split_entries,
    split_labels,
)
from florence.runtime.onboarding_links import FlorenceOnboardingLinkService
from florence.runtime.queue import FlorenceGoogleSyncJob, FlorenceRedisGoogleSyncQueue
from florence.runtime.entrypoints import (
    FlorenceEntrypointResult,
    FlorenceEntrypointService,
    FlorenceGoogleOauthConfig,
)
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    build_google_connection_sync_status,
    build_grounding_suggestions,
    FlorenceHouseholdManagerService,
    FlorenceGoogleSyncPersistenceService,
    FlorenceGoogleSyncWorkerService,
)
from florence.sendblue import FlorenceSendblueClient
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)

WEB_SETUP_CALENDAR_USAGE_MODES = {
    "planning_and_conflicts",
    "conflicts_only",
    "ignore",
}
WEB_SETUP_CALENDAR_DETAIL_VISIBILITY = {
    "full_details",
    "busy_only",
}
WEB_SETUP_PRIVATE_CALENDAR_HANDLING = {
    "conflicts_only",
    "full_details",
}


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
        onboarding_base_url = settings.server.web_base_url or settings.server.public_base_url
        onboarding_link_path = "/setup" if settings.server.web_base_url else "/v1/florence/onboarding"
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
            public_base_url=onboarding_base_url,
            onboarding_link_path=onboarding_link_path,
            onboarding_state_secret=settings.server.onboarding_state_secret,
            household_chat_model=settings.hermes.model,
            household_chat_max_iterations=settings.hermes.max_iterations,
            household_chat_provider=settings.hermes.provider,
            household_chat_enabled_toolsets=settings.hermes.enabled_toolsets,
            household_chat_disabled_toolsets=settings.hermes.disabled_toolsets,
            household_chat_fallback_model=settings.hermes.fallback_model,
            household_chat_tool_use_enforcement=settings.hermes.tool_use_enforcement,
            household_chat_enable_honcho=settings.hermes.enable_honcho,
            household_chat_honcho_scope=settings.hermes.honcho_scope,
        )
        self.onboarding_link_service = self.entrypoints.onboarding_link_service
        self.linq = FlorenceLinqClient(settings.linq)
        self.sendblue = FlorenceSendblueClient(settings.sendblue)
        self.candidate_review_service = FlorenceCandidateReviewService(self.store)
        self.household_manager_service = FlorenceHouseholdManagerService(self.store)
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
        self._deferred_ingress_guard = threading.Lock()
        self._deferred_ingress_timers: dict[str, tuple[int, threading.Timer]] = {}

    def close(self) -> None:
        with self._deferred_ingress_guard:
            timers = list(self._deferred_ingress_timers.values())
            self._deferred_ingress_timers.clear()
        for _token, timer in timers:
            try:
                timer.cancel()
            except Exception:
                logger.exception("Failed to cancel deferred ingress timer")
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
                self._deliver_or_defer_ingress_result(result=result, provider="linq")

                return self._json_result(
                    200,
                    {
                        "ok": True,
                        "consumed": result.consumed,
                        "deferred": result.defer_reply,
                        "householdId": result.household_id,
                        "memberId": result.member_id,
                        "channelId": result.channel_id,
                        "error": result.error,
                    },
                )
            except Exception:
                logger.exception("Florence Linq webhook failed")
                return self._json_result(500, {"ok": False, "error": "internal_linq_webhook_error"})

    def handle_sendblue_webhook(
        self,
        *,
        payload: dict[str, Any],
        webhook_secret: str | None,
    ) -> FlorenceHTTPResult:
        if not self.sendblue.verify_webhook_signature(secret_header=webhook_secret):
            return self._json_result(403, {"ok": False, "error": "invalid_sendblue_webhook_signature"})
        try:
            result = self.entrypoints.handle_sendblue_payload(payload)
            self._deliver_or_defer_ingress_result(result=result, provider="sendblue")

            return self._json_result(
                200,
                {
                    "ok": True,
                    "consumed": result.consumed,
                    "deferred": result.defer_reply,
                    "householdId": result.household_id,
                    "memberId": result.member_id,
                    "channelId": result.channel_id,
                    "error": result.error,
                },
            )
        except Exception:
            logger.exception("Florence Sendblue webhook failed")
            return self._json_result(500, {"ok": False, "error": "internal_sendblue_webhook_error"})

    def handle_onboarding_page(
        self,
        *,
        token: str | None,
        status_message: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_onboarding_context(token)
        except Exception as exc:
            return self._html_result(
                400,
                self._render_google_callback_page(
                    title="Onboarding link invalid",
                    message=str(exc),
                ),
            )

        return self._html_result(
            200,
            self._render_onboarding_page(
                token=token or "",
                session=context["session"],
                member=context["member"],
                household=context["household"],
                google_connect_url=context["google_connect_url"],
                connected_email=context["connected_email"],
                status_message=status_message,
            ),
        )

    def handle_onboarding_submission(
        self,
        *,
        token: str | None,
        form_data: dict[str, str],
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_onboarding_context(token)
        except Exception as exc:
            return self._html_result(
                400,
                self._render_google_callback_page(
                    title="Onboarding link invalid",
                    message=str(exc),
                ),
            )

        session = context["session"]
        was_complete = session.is_complete
        household_id = session.household_id
        member_id = session.member_id
        thread_id = session.thread_id

        self.entrypoints.onboarding_service.record_parent_name(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            display_name=form_data.get("parent_display_name", ""),
        )
        self.entrypoints.onboarding_service.record_household_members(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            household_members=split_entries(form_data.get("household_members", "")),
        )
        child_details = split_entries(form_data.get("child_details", ""))
        self.entrypoints.onboarding_service.record_child_names(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            child_names=extract_child_names(child_details),
            child_details=child_details,
        )
        self.entrypoints.onboarding_service.record_school_basics(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            school_labels=split_labels(form_data.get("school_labels", "")),
        )
        self.entrypoints.onboarding_service.record_activity_basics(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            activity_labels=split_labels(form_data.get("activity_labels", "")),
        )
        self.entrypoints.onboarding_service.record_household_operations(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            household_operations=split_entries(form_data.get("household_operations", "")),
        )
        self.entrypoints.onboarding_service.record_nudge_preferences(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            nudge_preferences=form_data.get("nudge_preferences", ""),
        )
        self.entrypoints.onboarding_service.record_operating_preferences(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            operating_preferences=form_data.get("operating_preferences", ""),
        )

        updated_context = self._load_onboarding_context(token)
        updated_session = updated_context["session"]
        if updated_session.is_complete and not was_complete:
            self._record_onboarding_completion(
                household_id=updated_session.household_id,
                member_id=updated_session.member_id,
                channel_id=self._find_channel_by_provider_id(updated_session.thread_id).id
                if self._find_channel_by_provider_id(updated_session.thread_id) is not None
                else "web_onboarding",
            )
            channel = self._find_channel_by_provider_id(updated_session.thread_id)
            if channel is not None:
                for message in build_onboarding_ready_message_sequence():
                    self._safe_send_channel_message(channel=channel, message=message)

        status = (
            "You're ready. Florence will text you as the first Gmail and Calendar pass finishes."
            if updated_session.is_complete
            else (
                "Saved. Next step: connect Google to finish setup."
                if not updated_session.google_connected
                else "Saved. Finish any missing sections and Florence will be ready."
            )
        )
        return self._html_result(
            200,
            self._render_onboarding_page(
                token=token or "",
                session=updated_session,
                member=updated_context["member"],
                household=updated_context["household"],
                google_connect_url=updated_context["google_connect_url"],
                connected_email=updated_context["connected_email"],
                status_message=status,
            ),
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
            existing_session = self.entrypoints.onboarding_service.get_or_create_session(
                household_id=oauth_state.household_id,
                member_id=oauth_state.member_id,
                thread_id=oauth_state.thread_id or "",
            )
            callback = self.entrypoints.google_account_link_service.handle_callback(code=code, raw_state=state)
            updated_context = self._build_web_context(
                household_id=oauth_state.household_id,
                member_id=oauth_state.member_id,
                thread_id=oauth_state.thread_id or "",
                resolved_via="google_callback",
                auth_email=callback.connection.email,
            )
            web_setup_payload = self._serialize_web_setup(updated_context)
            web_setup_ready = bool(web_setup_payload["setup"]["readyForChat"])
            dm_onboarding_complete = bool(callback.onboarding_transition.state.is_complete)
            became_complete = (web_setup_ready or dm_onboarding_complete) and not self._has_onboarding_completion_event(
                household_id=oauth_state.household_id,
                member_id=oauth_state.member_id,
            )
            if web_setup_ready:
                dm_messages = build_onboarding_ready_message_sequence()
            else:
                next_prompt_state = self.entrypoints.onboarding_service.get_prompt(
                    household_id=oauth_state.household_id,
                    member_id=oauth_state.member_id,
                    thread_id=oauth_state.thread_id or "",
                )
                next_prompt = next_prompt_state.text if next_prompt_state is not None else None
                dm_messages = build_google_connected_syncing_message_sequence() + ((next_prompt,) if next_prompt else ())
            sync_notice = None

            if oauth_state.thread_id and (dm_messages or sync_notice):
                channel = self.store.get_channel(oauth_state.thread_id)
                if channel is not None:
                    for message in dm_messages:
                        self._safe_send_channel_message(channel=channel, message=message)
                    if sync_notice:
                        self._safe_send_channel_message(channel=channel, message=sync_notice)
                else:
                    fallback_channel = self._find_channel_by_provider_id(oauth_state.thread_id)
                    if fallback_channel is not None:
                        for message in dm_messages:
                            self._safe_send_channel_message(channel=fallback_channel, message=message)
                        if sync_notice:
                            self._safe_send_channel_message(channel=fallback_channel, message=sync_notice)

            if became_complete:
                self._record_onboarding_completion(
                    household_id=callback.connection.household_id,
                    member_id=callback.connection.member_id,
                    channel_id=self._find_channel_by_provider_id(oauth_state.thread_id or "").id
                    if oauth_state.thread_id and self._find_channel_by_provider_id(oauth_state.thread_id) is not None
                    else "google_callback",
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

    def handle_web_session(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        return self._json_result(
            200,
            {
                "ok": True,
                "resolvedVia": context["resolved_via"],
                "authEmail": context["auth_email"],
                "household": self._serialize_household(context["household"]),
                "member": self._serialize_member(context["member"]),
                "setup": self._serialize_web_setup(context),
            },
        )

    def handle_web_setup(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        return self._json_result(200, {"ok": True, **self._serialize_web_setup(context)})

    def handle_web_setup_profile(
        self,
        *,
        payload: dict[str, Any],
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)

        before_setup = self._serialize_web_setup(context)
        session = context["session"]
        household_id = session.household_id
        member_id = session.member_id
        thread_id = session.thread_id

        if "parentDisplayName" in payload:
            self.entrypoints.onboarding_service.record_parent_name(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
                display_name=str(payload.get("parentDisplayName") or ""),
            )

        child_keys_present = any(key in payload for key in ("children", "childNames", "childDetails"))
        if child_keys_present:
            child_names, child_details = self._extract_web_children(payload)
            self.entrypoints.onboarding_service.record_child_names(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
                child_names=child_names,
                child_details=child_details,
            )

        if "schools" in payload or "schoolLabels" in payload:
            self.entrypoints.onboarding_service.record_school_basics(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
                school_labels=self._coerce_label_list(payload.get("schools", payload.get("schoolLabels"))),
            )

        if "activities" in payload or "activityLabels" in payload:
            self.entrypoints.onboarding_service.record_activity_basics(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
                activity_labels=self._coerce_label_list(payload.get("activities", payload.get("activityLabels"))),
            )

        manager_updates: dict[str, object] = {}
        current_manager_profile = self.household_manager_service.get_manager_profile(household_id)

        if "consent" in payload:
            manager_updates["web_consent"] = self._merge_web_consent(
                current=current_manager_profile.get("web_consent"),
                incoming=payload.get("consent"),
            )

        if any(
            key in payload
            for key in ("topPriorities", "topPriorityOther", "painPoints", "painPointOther", "priorities")
        ):
            manager_updates.update(
                self._merge_priority_preferences(
                    current=current_manager_profile,
                    payload=payload,
                )
            )

        if "trustDefaults" in payload:
            manager_updates["trust_defaults"] = self._merge_trust_defaults(
                current=current_manager_profile.get("trust_defaults"),
                incoming=payload.get("trustDefaults"),
            )

        activation_action = str(payload.get("activationReviewAction") or "").strip().lower()
        if activation_action:
            manager_updates["activation_review"] = self._merge_activation_state(
                current=current_manager_profile.get("activation_review"),
                action=activation_action,
            )

        if manager_updates:
            self.household_manager_service.update_manager_profile(
                household_id=household_id,
                updates=manager_updates,
            )

        if "calendarSelections" in payload or "calendarClassifications" in payload:
            self._persist_web_calendar_selections(
                context=context,
                raw=payload.get("calendarSelections", payload.get("calendarClassifications")),
            )

        updated_context = self._build_web_context(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            resolved_via=context["resolved_via"],
            auth_email=context["auth_email"],
        )
        updated_setup = self._serialize_web_setup(updated_context)
        if updated_setup["setup"]["readyForChat"] and not before_setup["setup"]["readyForChat"]:
            self._maybe_send_web_ready_sequence(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            )
        return self._json_result(200, {"ok": True, **updated_setup})

    def handle_web_google_start(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        if self.entrypoints.google_account_link_service is None:
            return self._json_result(503, {"ok": False, "error": "google_oauth_not_configured"})
        session = context["session"]
        connect_url = self.entrypoints.google_account_link_service.build_connect_link(
            household_id=session.household_id,
            member_id=session.member_id,
            thread_id=session.thread_id,
        ).url
        return self._json_result(200, {"ok": True, "connectUrl": connect_url})

    def handle_web_google_connections(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        return self._json_result(
            200,
            {
                "ok": True,
                "connections": [
                    self._serialize_google_connection(connection)
                    for connection in context["connections"]
                ],
            },
        )

    def handle_web_google_add_account(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        return self.handle_web_google_start(token=token, auth_email=auth_email)

    def handle_web_google_disconnect(
        self,
        *,
        payload: dict[str, Any],
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        connection_id = str(payload.get("connectionId") or "").strip()
        if not connection_id:
            return self._json_result(400, {"ok": False, "error": "connection_id_required"})
        connection = self.store.get_google_connection(connection_id)
        if (
            connection is None
            or connection.household_id != context["household"].id
            or connection.member_id != context["member"].id
        ):
            return self._json_result(404, {"ok": False, "error": "unknown_google_connection"})
        was_primary = bool(connection.metadata.get("web_primary"))
        disconnected_metadata = dict(connection.metadata)
        disconnected_metadata["web_primary"] = False
        self.store.upsert_google_connection(replace(connection, active=False, metadata=disconnected_metadata))
        if was_primary:
            remaining = self.store.list_google_connections(
                household_id=context["household"].id,
                member_id=context["member"].id,
            )
            if remaining:
                replacement = remaining[0]
                replacement_metadata = dict(replacement.metadata)
                replacement_metadata["web_primary"] = True
                self.store.upsert_google_connection(replace(replacement, metadata=replacement_metadata))
        updated_context = self._build_web_context(
            household_id=context["household"].id,
            member_id=context["member"].id,
            thread_id=context["session"].thread_id,
            resolved_via=context["resolved_via"],
            auth_email=context["auth_email"],
        )
        return self._json_result(
            200,
            {
                "ok": True,
                "connections": [
                    self._serialize_google_connection(item)
                    for item in updated_context["connections"]
                ],
            },
        )

    def handle_web_settings(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        return self._json_result(200, {"ok": True, **self._serialize_web_settings(context)})

    def handle_web_settings_update(
        self,
        *,
        payload: dict[str, Any],
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)

        household = context["household"]
        member = context["member"]
        household_name = str(payload.get("householdName") or household.name).strip() or household.name
        household_timezone = str(payload.get("timezone") or household.timezone).strip() or household.timezone
        member_display_name = str(payload.get("memberDisplayName") or member.display_name).strip() or member.display_name

        self.store.upsert_household(replace(household, name=household_name, timezone=household_timezone))
        self.store.upsert_member(replace(member, display_name=member_display_name))

        updated_context = self._build_web_context(
            household_id=household.id,
            member_id=member.id,
            thread_id=context["session"].thread_id,
            resolved_via=context["resolved_via"],
            auth_email=context["auth_email"],
        )
        return self._json_result(200, {"ok": True, **self._serialize_web_settings(updated_context)})

    def handle_web_review(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        return self._json_result(200, {"ok": True, **self._serialize_web_review(context)})

    def handle_web_review_action(
        self,
        *,
        payload: dict[str, Any],
        token: str | None = None,
        auth_email: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)

        decision = str(payload.get("decision") or "").strip().lower()
        candidate_id = str(payload.get("candidateId") or "").strip()
        household_id = context["household"].id
        member_id = context["member"].id

        if decision == "skip_activation":
            profile = self.household_manager_service.get_manager_profile(household_id)
            activation = self._merge_activation_state(
                current=profile.get("activation_review"),
                action="skip",
            )
            self.household_manager_service.update_manager_profile(
                household_id=household_id,
                updates={"activation_review": activation},
            )
        else:
            if not candidate_id:
                return self._json_result(400, {"ok": False, "error": "candidate_id_required"})
            candidate = self.store.get_imported_candidate(candidate_id)
            if candidate is None or candidate.household_id != household_id or candidate.member_id != member_id:
                return self._json_result(404, {"ok": False, "error": "unknown_candidate"})
            if decision == "confirm":
                self.candidate_review_service.confirm_candidate(
                    candidate_id=candidate_id,
                    overrides=payload.get("overrides") if isinstance(payload.get("overrides"), dict) else None,
                )
            elif decision == "reject":
                self.candidate_review_service.reject_candidate(candidate_id=candidate_id)
            else:
                return self._json_result(400, {"ok": False, "error": "invalid_review_decision"})

        updated_context = self._build_web_context(
            household_id=household_id,
            member_id=member_id,
            thread_id=context["session"].thread_id,
            resolved_via=context["resolved_via"],
            auth_email=context["auth_email"],
        )
        return self._json_result(200, {"ok": True, **self._serialize_web_review(updated_context)})

    def handle_web_calendar(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> FlorenceHTTPResult:
        try:
            context = self._load_web_context(token=token, auth_email=auth_email)
        except Exception as exc:
            return self._web_error_result(exc)
        return self._json_result(
            200,
            {"ok": True, **self._serialize_web_calendar(context, start=start, end=end)},
        )

    def _launch_google_sync_job(
        self,
        *,
        connection_id: str,
        thread_id: str | None,
        notify_when_finished: bool,
    ) -> None:
        self._mark_connection_sync_queued(self.store, connection_id=connection_id)
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
            candidate_review_service = FlorenceCandidateReviewService(store)
            result = sync_worker.sync_connection(
                connection_id,
                client_id=self.settings.google.client_id,
                client_secret=self.settings.google.client_secret,
            )
            if not thread_id:
                return

            channel = store.get_channel(thread_id)
            if channel is None:
                channel = self._find_channel_by_provider_id(provider_channel_id=thread_id, store=store)
            if channel is None:
                return

            setup_payload = self._serialize_web_setup(
                self._build_web_context(
                    household_id=result.connection.household_id,
                    member_id=result.connection.member_id,
                    thread_id=thread_id or channel.provider_channel_id,
                    resolved_via="sync",
                    auth_email=None,
                )
            )
            ready_sequence_sent = False
            if self._is_web_setup_ready(
                household_id=result.connection.household_id,
                member_id=result.connection.member_id,
                connections=store.list_google_connections(
                    household_id=result.connection.household_id,
                    member_id=result.connection.member_id,
                ),
                store=store,
            ):
                ready_sequence_sent = self._maybe_send_web_ready_sequence(
                    household_id=result.connection.household_id,
                    member_id=result.connection.member_id,
                    thread_id=thread_id,
                    store=store,
                )
            freshest_connection = store.get_google_connection(result.connection.id) or result.connection
            if notify_when_finished and not self._sync_activation_brief_already_sent(connection=freshest_connection):
                group_channel = self._find_group_channel(
                    result.connection.household_id,
                    provider=channel.provider,
                )
                activation_message, group_message = self._build_sync_activation_brief_messages(
                    connection=freshest_connection,
                    candidates=result.sync_result.candidates,
                    group_available=group_channel is not None,
                    onboarding_link=None,
                    ready_sequence_sent=ready_sequence_sent,
                )
                sent_activation = self._safe_send_channel_message(
                    channel=channel,
                    message=activation_message,
                    store=store,
                    message_metadata=(
                        {
                            "promotion_kind": "sync_activation_brief",
                            "promotable_group_message": group_message,
                        }
                        if group_message
                        else None
                    ),
                )
                if sent_activation:
                    self._mark_sync_activation_brief_sent(
                        connection=freshest_connection,
                        store=store,
                        channel_id=channel.id,
                    )
            self._nudge_for_new_pending_candidates(
                household_id=result.connection.household_id,
                member_id=result.connection.member_id,
                candidates=result.sync_result.candidates,
            )
        except Exception:
            logger.exception("Florence background Google sync failed connection_id=%s", connection_id)
            self._mark_connection_sync_error(
                store,
                connection_id=connection_id,
                error_message="initial_sync_failed",
            )
            if thread_id:
                channel = store.get_channel(thread_id)
                if channel is None:
                    channel = self._find_channel_by_provider_id(provider_channel_id=thread_id, store=store)
                if channel is not None:
                    self._safe_send_channel_message(
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

    def _deliver_or_defer_ingress_result(
        self,
        *,
        result: FlorenceEntrypointResult,
        provider: str,
    ) -> None:
        if result.defer_reply:
            if result.household_id and result.member_id and result.channel_id:
                self._schedule_deferred_ingress_flush(
                    household_id=result.household_id,
                    member_id=result.member_id,
                    channel_id=result.channel_id,
                    delay_seconds=float(result.defer_seconds or 0.0),
                )
            return

        reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
        if reply_messages and result.channel_id:
            channel = self.store.get_channel(result.channel_id)
            if channel is not None:
                for message in reply_messages:
                    self._safe_send_channel_message(channel=channel, message=message, record_message=False)

        if result.group_announcement and result.household_id:
            group_channel = self._find_group_channel(result.household_id, provider=provider)
            if group_channel is not None:
                self._safe_send_channel_message(channel=group_channel, message=result.group_announcement)

    def _schedule_deferred_ingress_flush(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        delay_seconds: float,
    ) -> None:
        channel = self.store.get_channel(channel_id)
        if channel is None:
            return
        delay = max(0.5, delay_seconds or self.entrypoints.ingress.onboarding_reply_debounce_seconds)
        token = time.time_ns()
        timer = threading.Timer(
            delay,
            self._flush_deferred_ingress_reply,
            kwargs={
                "token": token,
                "household_id": household_id,
                "member_id": member_id,
                "channel_id": channel_id,
                "thread_id": channel.provider_channel_id,
            },
        )
        timer.daemon = True
        with self._deferred_ingress_guard:
            existing = self._deferred_ingress_timers.pop(channel_id, None)
            self._deferred_ingress_timers[channel_id] = (token, timer)
        if existing is not None:
            try:
                existing[1].cancel()
            except Exception:
                logger.exception("Failed to cancel deferred ingress timer for channel %s", channel_id)
        timer.start()

    def _flush_deferred_ingress_reply(
        self,
        *,
        token: int,
        household_id: str,
        member_id: str,
        channel_id: str,
        thread_id: str,
    ) -> None:
        try:
            result = self.entrypoints.ingress.flush_deferred_onboarding_reply(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel_id,
                thread_id=thread_id,
            )
            if not result.consumed:
                return
            reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
            if not reply_messages:
                return
            channel = self.store.get_channel(channel_id)
            if channel is None:
                return
            for message in reply_messages:
                self._safe_send_channel_message(channel=channel, message=message)
        except Exception:
            logger.exception(
                "Florence deferred ingress flush failed household_id=%s member_id=%s channel_id=%s",
                household_id,
                member_id,
                channel_id,
            )
        finally:
            with self._deferred_ingress_guard:
                current = self._deferred_ingress_timers.get(channel_id)
                if current is not None and current[0] == token:
                    self._deferred_ingress_timers.pop(channel_id, None)

    def _load_web_context(
        self,
        *,
        token: str | None = None,
        auth_email: str | None = None,
    ) -> dict[str, Any]:
        raw_token = str(token or "").strip()
        if raw_token:
            if self.onboarding_link_service is None:
                raise ValueError("web_onboarding_not_configured")
            link_state = self.onboarding_link_service.decode_token(raw_token)
            return self._build_web_context(
                household_id=link_state.household_id,
                member_id=link_state.member_id,
                thread_id=link_state.thread_id,
                resolved_via="token",
                auth_email=" ".join(str(auth_email or "").split()).strip().lower() or None,
            )

        normalized_email = " ".join(str(auth_email or "").split()).strip().lower()
        if normalized_email:
            connection = self.store.find_google_connection_by_email(email=normalized_email)
            if connection is None:
                raise ValueError("unknown_web_google_identity")
            return self._build_web_context(
                household_id=connection.household_id,
                member_id=connection.member_id,
                thread_id=self._default_provider_thread_id(
                    household_id=connection.household_id,
                    member_id=connection.member_id,
                ),
                resolved_via="google_email",
                auth_email=normalized_email,
            )

        raise ValueError("missing_web_identity")

    def _load_onboarding_context(self, token: str | None) -> dict[str, Any]:
        return self._load_web_context(token=token)

    def _build_web_context(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        resolved_via: str,
        auth_email: str | None = None,
    ) -> dict[str, Any]:
        resolved_thread_id = thread_id or self._default_provider_thread_id(household_id=household_id, member_id=member_id)
        session = self.entrypoints.onboarding_service.get_or_create_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=resolved_thread_id,
        )
        google_connect_url = None
        if self.entrypoints.google_account_link_service is not None:
            google_connect_url = self.entrypoints.google_account_link_service.build_connect_link(
                household_id=session.household_id,
                member_id=session.member_id,
                thread_id=session.thread_id,
            ).url
        household = self.store.get_household(session.household_id)
        member = self.store.get_member(session.member_id)
        if household is None:
            raise ValueError("unknown_household")
        if member is None:
            raise ValueError("unknown_member")
        connections = self.store.list_google_connections(
            household_id=session.household_id,
            member_id=session.member_id,
        )
        return {
            "session": session,
            "household": household,
            "member": member,
            "connections": connections,
            "google_connect_url": google_connect_url,
            "connected_email": connections[0].email if connections else None,
            "resolved_via": resolved_via,
            "auth_email": auth_email or (connections[0].email if connections else None),
        }

    def _default_provider_thread_id(self, *, household_id: str, member_id: str) -> str:
        sessions = self.store.list_member_onboarding_sessions(household_id=household_id, member_id=member_id)
        for session in sessions:
            if session.thread_id.strip():
                return session.thread_id
        channel_id = self.household_manager_service.default_dm_channel_id(
            household_id=household_id,
            member_id=member_id,
        )
        if not channel_id:
            return ""
        channel = self.store.get_channel(channel_id)
        return channel.provider_channel_id if channel is not None else ""

    def _has_onboarding_completion_event(
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

    def _maybe_send_web_ready_sequence(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str | None,
        store: FlorenceStateDB | None = None,
    ) -> bool:
        target_store = store or self.store
        if self._has_onboarding_completion_event(household_id=household_id, member_id=member_id, store=target_store):
            return False
        channel = target_store.get_channel(thread_id) if thread_id else None
        if channel is None and thread_id:
            channel = self._find_channel_by_provider_id(provider_channel_id=thread_id, store=target_store)
        if channel is None:
            default_thread_id = self._default_provider_thread_id(household_id=household_id, member_id=member_id)
            channel = self._find_channel_by_provider_id(provider_channel_id=default_thread_id, store=target_store)
        if channel is None:
            return False
        for message in build_onboarding_ready_message_sequence():
            self._safe_send_channel_message(channel=channel, message=message, store=target_store)
        self._record_onboarding_completion(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel.id,
            store=target_store,
        )
        return True

    def _record_onboarding_completion(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        store: FlorenceStateDB | None = None,
    ) -> None:
        manager_service = self.household_manager_service if store is None else FlorenceHouseholdManagerService(store)
        try:
            manager_service.ensure_briefing_routines(household_id=household_id)
            manager_service.record_pilot_event(
                household_id=household_id,
                event_type="onboarding_complete",
                member_id=member_id,
                channel_id=channel_id,
            )
        except Exception:
            logger.exception("Failed to finalize onboarding completion hooks for household_id=%s", household_id)

    @staticmethod
    def _joined_lines(values: list[str]) -> str:
        return "\n".join(value for value in values if value)

    @staticmethod
    def _render_onboarding_field(
        *,
        label: str,
        name: str,
        value: str,
        hint: str,
        placeholder: str,
        textarea: bool = True,
    ) -> str:
        safe_label = html.escape(label)
        safe_name = html.escape(name)
        safe_hint = html.escape(hint)
        safe_placeholder = html.escape(placeholder)
        safe_value = html.escape(value)
        if textarea:
            control = (
                f"<textarea name='{safe_name}' rows='4' placeholder='{safe_placeholder}'>{safe_value}</textarea>"
            )
        else:
            control = f"<input name='{safe_name}' value='{safe_value}' placeholder='{safe_placeholder}' />"
        return (
            "<label class='field'>"
            f"<span>{safe_label}</span>"
            f"{control}"
            f"<small>{safe_hint}</small>"
            "</label>"
        )

    @staticmethod
    def _onboarding_missing_fields(session) -> tuple[str, ...]:
        missing: list[str] = []
        if not session.parent_display_name:
            missing.append("your name")
        if session.variant.value == "concierge" and not session.household_members:
            missing.append("family unit")
        if not session.google_connected:
            missing.append("Google account")
        if not session.child_names:
            missing.append("kids")
        if not session.school_basics_collected:
            missing.append("schools or daycares")
        if not session.activity_basics_collected:
            missing.append("activities")
        return tuple(missing)

    @classmethod
    def _render_onboarding_page(
        cls,
        *,
        token: str,
        session,
        member,
        household,
        google_connect_url: str | None,
        connected_email: str | None,
        status_message: str | None,
    ) -> str:
        missing = cls._onboarding_missing_fields(session)
        household_name = household.name if household is not None else "your household"
        parent_name = session.parent_display_name or (member.display_name if member is not None else "")
        child_details = cls._joined_lines(session.child_details or session.child_names)
        household_members = cls._joined_lines(session.household_members)
        school_labels = cls._joined_lines(session.school_labels)
        activity_labels = cls._joined_lines(session.activity_labels)
        household_operations = cls._joined_lines(session.household_operations)
        nudge_preferences = session.nudge_preferences or ""
        operating_preferences = session.operating_preferences or ""
        google_status = (
            f"<div class='status-pill success'>Google connected: {html.escape(connected_email or 'connected')}</div>"
            if session.google_connected
            else (
                "<a class='google-button' href='"
                + html.escape(google_connect_url or "#")
                + "'>Connect Google</a>"
                if google_connect_url
                else "<div class='status-pill pending'>Google connect unavailable</div>"
            )
        )
        readiness_copy = (
            "Florence is ready. You can close this page and text real tasks now."
            if session.is_complete
            else (
                "Still missing: " + ", ".join(html.escape(item) for item in missing)
                if missing
                else "Keep going. Florence will be ready once these basics are in."
            )
        )
        notice_html = (
            f"<div class='notice {'success' if session.is_complete else 'info'}'>{html.escape(status_message)}</div>"
            if status_message
            else ""
        )
        return (
            "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>"
            "<title>Florence setup</title><style>"
            "body{margin:0;font-family:ui-sans-serif,system-ui,sans-serif;background:linear-gradient(180deg,#f4efe7 0%,#efe5d6 100%);color:#1f1a16;}"
            ".page{max-width:1080px;margin:0 auto;padding:32px 20px 56px;}.hero{display:grid;gap:16px;margin-bottom:20px;}"
            ".eyebrow{letter-spacing:.12em;text-transform:uppercase;font-size:12px;color:#7c624f;font-weight:700;}"
            "h1{margin:0;font-size:42px;line-height:1.05;max-width:760px;}p.lede{margin:0;font-size:18px;max-width:760px;color:#56463a;}"
            ".shell{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(280px,.8fr);gap:20px;align-items:start;}"
            ".card{background:rgba(255,252,246,.92);border:1px solid #d8c4aa;border-radius:24px;padding:24px;box-shadow:0 18px 50px rgba(56,35,18,.08);}"
            ".status-pill{display:inline-flex;align-items:center;padding:10px 14px;border-radius:999px;font-weight:600;font-size:14px;}"
            ".status-pill.success{background:#e6f2e3;color:#215533;}.status-pill.pending{background:#f7ead7;color:#7a4b10;}"
            ".google-button{display:inline-flex;align-items:center;justify-content:center;padding:12px 18px;border-radius:999px;background:#1f1a16;color:#fff7ef;text-decoration:none;font-weight:700;}"
            ".notice{padding:14px 16px;border-radius:16px;margin-bottom:16px;font-weight:600;}.notice.info{background:#efe5d6;color:#5d4534;}.notice.success{background:#e6f2e3;color:#215533;}"
            ".fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;}.field{display:grid;gap:8px;font-weight:600;color:#2e241d;}"
            ".field span{font-size:14px;}.field small{font-size:12px;line-height:1.4;color:#6d5c4e;font-weight:500;}"
            "textarea,input{width:100%;box-sizing:border-box;border:1px solid #d2bda2;border-radius:16px;padding:13px 14px;background:#fffdf9;font:inherit;color:#1f1a16;}"
            "textarea{min-height:120px;resize:vertical;}.field.compact textarea{min-height:96px;}.section-title{font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:#8c6e57;font-weight:800;margin:0 0 10px;}"
            ".actions{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-top:20px;}.save-button{border:0;border-radius:999px;background:#b35b2f;color:#fff7ef;padding:14px 22px;font:inherit;font-weight:700;cursor:pointer;}"
            ".sidebar{display:grid;gap:16px;}.sidebar h2{margin:0;font-size:18px;}.sidebar ul{margin:0;padding-left:18px;color:#5d4c3f;line-height:1.5;}"
            ".helper{font-size:14px;color:#5d4c3f;line-height:1.6;}.full{grid-column:1 / -1;}"
            "@media (max-width:900px){.shell{grid-template-columns:1fr;}.fields{grid-template-columns:1fr;}h1{font-size:34px;}}"
            "</style></head><body><div class='page'><div class='hero'><div class='eyebrow'>Florence setup</div>"
            f"<h1>Set up Florence for {html.escape(household_name)}</h1>"
            "<p class='lede'>Florence setup now happens in Messages first. This page is just a backup place to inspect or edit household context and reconnect Google if you need a bigger screen.</p>"
            "</div><div class='shell'><div class='card'>"
            + notice_html
            + f"<div class='notice {'success' if session.is_complete else 'info'}'>{readiness_copy}</div>"
            + "<form method='post' action='/v1/florence/onboarding'>"
            + f"<input type='hidden' name='token' value='{html.escape(token)}' />"
            + "<div class='fields'>"
            + cls._render_onboarding_field(
                label="What should Florence call you?",
                name="parent_display_name",
                value=parent_name,
                hint="Use the name Florence should use in texts.",
                placeholder="Maya",
                textarea=False,
            )
            + cls._render_onboarding_field(
                label="Who else is in the family unit?",
                name="household_members",
                value=household_members,
                hint="Optional for hybrid households, but helpful for richer family context.",
                placeholder="Ben - dad\nAva - daughter",
            )
            + cls._render_onboarding_field(
                label="Kids Florence should know about",
                name="child_details",
                value=child_details,
                hint="One per line works best. Include nicknames, age, or grade if helpful.",
                placeholder="Theo - 1st grade\nViolet - preschool, starting TK in fall",
            )
            + cls._render_onboarding_field(
                label="Schools, daycares, preschools, or camps",
                name="school_labels",
                value=school_labels,
                hint="The labels Florence should use when filtering email and calendar noise.",
                placeholder="Wish Community School\nYoung Minds Preschool",
            )
            + cls._render_onboarding_field(
                label="Recurring activities, teams, or lessons",
                name="activity_labels",
                value=activity_labels,
                hint="If helpful, include the child in the label.",
                placeholder="Theo baseball\nViolet dance\nBoth - Musical Beginnings",
            )
            + cls._render_onboarding_field(
                label="What should Florence help manage like a house manager?",
                name="household_operations",
                value=household_operations,
                hint="Think groceries, lunches, forms, birthday gifts, camps, appointments, returns, bills, or anything else you keep in your head.",
                placeholder="Weekly meal planning\nSchool forms\nAppointment logistics",
            )
            + cls._render_onboarding_field(
                label="Reminder style",
                name="nudge_preferences",
                value=nudge_preferences,
                hint="Plain English is fine. For example: day before + morning of for important school logistics.",
                placeholder="Day before + morning of for big kid logistics. Thirty minutes before for routine practices.",
            )
            + cls._render_onboarding_field(
                label="House rules for how Florence should operate",
                name="operating_preferences",
                value=operating_preferences,
                hint="Quiet hours, morning brief timing, spending limits, or when Florence should ask first.",
                placeholder="Morning brief at 6:45 on weekdays, evening check-in after dinner, no texts after 9pm, ask before spending money.",
            )
            + "<div class='field full'><span>Google</span>"
            + google_status
            + "<small>Florence uses Gmail and Calendar to find household-relevant items and keep the family plan current.</small></div>"
            + "</div><div class='actions'><button class='save-button' type='submit'>Save setup</button>"
            + "<div class='helper'>Florence will keep the real onboarding flow in Messages and text the main thread when Google is connected, when the first inbox scan finishes, and when setup is complete.</div></div></form></div>"
            + "<div class='sidebar'><div class='card'><h2>What Florence should be able to do next</h2><ul>"
            + "<li>Check your inbox for school, camp, and activity updates</li>"
            + "<li>Answer schedule questions from shared household state</li>"
            + "<li>Plan meals, groceries, logistics, and reminders across the family</li>"
            + "<li>Send useful morning briefs and well-timed nudges</li>"
            + "</ul></div><div class='card'><div class='section-title'>Examples</div><div class='helper'>Try texts like “What’s on the kids’ schedule next week?”, “Check my email for anything from Musical Beginnings”, “Plan dinners and groceries for next week”, or “Remind me about picture day the morning of.”</div></div></div></div></div></body></html>"
        )

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

    @staticmethod
    def _activation_candidate_titles(candidates: list[Any], *, limit: int = 3) -> tuple[list[str], int]:
        titles: list[str] = []
        for candidate in candidates[:limit]:
            title = " ".join(str(candidate.title or "").split()).strip()
            if title:
                titles.append(title)
        remaining = max(0, len(candidates) - len(titles))
        return titles, remaining

    def _build_sync_activation_brief_messages(
        self,
        *,
        connection: Any,
        candidates: list[Any],
        group_available: bool,
        onboarding_link: str | None,
        ready_sequence_sent: bool,
    ) -> tuple[str, str | None]:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        gmail_count = int(metadata.get("last_gmail_item_count") or 0)
        calendar_count = int(metadata.get("last_calendar_item_count") or 0)
        titles, remaining = self._activation_candidate_titles(candidates)

        lines = [
            "I went through your recent email and calendar activity. Here is what matters right now:",
        ]
        if titles:
            title_summary = "; ".join(titles)
            if remaining:
                title_summary += f"; plus {remaining} more"
            noun = "item" if len(candidates) == 1 else "items"
            lines.append(f"- {len(candidates)} {noun} need review: {title_summary}.")
        else:
            lines.append("- Nothing urgent jumped out from the first pass yet.")

        if gmail_count or calendar_count:
            lines.append(
                f"- I scanned {gmail_count} recent email{'s' if gmail_count != 1 else ''} and "
                f"{calendar_count} calendar item{'s' if calendar_count != 1 else ''}."
            )
        if onboarding_link:
            lines.append(f"- To sharpen future plans, finish adding kids, schools, and activities here: {onboarding_link}")
        if group_available and titles:
            lines.append("Reply 'share' and I will post a short version to the parent group.")
        elif not ready_sequence_sent:
            lines.append("Reply 'review' if you want the full queue, or text me anything else you want me to handle.")
        else:
            lines.append("Text me anything else you want me to handle.")

        group_message: str | None = None
        if group_available and titles:
            group_lines = [
                "Florence pulled together a quick household update from recent email and calendar activity:",
                *(f"- {title}" for title in titles),
            ]
            if remaining:
                group_lines.append(f"- Plus {remaining} more item{'s' if remaining != 1 else ''} to walk through.")
            group_lines.append("Reply here if you want reminders or more detail on any of these.")
            group_message = "\n".join(group_lines)

        return "\n".join(lines), group_message

    @staticmethod
    def _sync_activation_brief_already_sent(*, connection: Any) -> bool:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        return bool(str(metadata.get("initial_sync_activation_brief_sent_at") or "").strip())

    @staticmethod
    def _mark_sync_activation_brief_sent(
        *,
        connection: Any,
        store: FlorenceStateDB,
        channel_id: str,
    ) -> None:
        metadata = dict(getattr(connection, "metadata", {}) or {})
        metadata["initial_sync_activation_brief_sent_at"] = datetime.now(timezone.utc).isoformat()
        metadata["initial_sync_activation_brief_channel_id"] = channel_id
        store.upsert_google_connection(replace(connection, metadata=metadata))

    def _find_channel_by_provider_id(
        self,
        provider_channel_id: str,
        *,
        store: FlorenceStateDB | None = None,
    ) -> Any | None:
        target_store = store or self.store
        for household in target_store.list_households():
            for channel in target_store.list_channels(household_id=household.id):
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
            preferred_group_channel = self._find_group_channel(household_id, provider=channel.provider)
            if preferred_group_channel is not None:
                channel = preferred_group_channel
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

    def _safe_send_channel_message(
        self,
        *,
        channel: Any,
        message: str,
        record_message: bool = True,
        store: FlorenceStateDB | None = None,
        message_metadata: dict[str, Any] | None = None,
    ) -> bool:
        try:
            target_store = store or self.store
            if channel.provider == "linq":
                self.linq.send_text(chat_id=channel.provider_channel_id, message=message)
            elif channel.provider == "sendblue":
                metadata = dict(getattr(channel, "metadata", {}) or {})
                group_id = str(metadata.get("group_id") or "").strip() or None
                numbers = None
                if group_id is None and channel.channel_type == ChannelType.HOUSEHOLD_GROUP:
                    participant_handles = metadata.get("participant_handles")
                    sendblue_number = str(metadata.get("sendblue_number") or "").strip()
                    if isinstance(participant_handles, list):
                        numbers = [
                            str(handle).strip()
                            for handle in participant_handles
                            if isinstance(handle, str)
                            and str(handle).strip()
                            and str(handle).strip() != sendblue_number
                        ]
                        numbers = list(dict.fromkeys(numbers)) or None
                self.sendblue.send_text(
                    thread_id=channel.provider_channel_id,
                    message=message,
                    group_id=group_id,
                    numbers=numbers,
                )
            else:
                return False
            if record_message:
                target_store.append_channel_message(
                    ChannelMessage(
                        id=self._assistant_message_id(channel.id),
                        household_id=channel.household_id,
                        channel_id=channel.id,
                        sender_role=ChannelMessageRole.ASSISTANT,
                        body=message,
                        metadata={
                            "provider": channel.provider,
                            "transport_thread_id": channel.provider_channel_id,
                            **(message_metadata or {}),
                        },
                        created_at=time.time(),
                    )
                )
            return True
        except Exception:
            logger.exception("Florence transport delivery failed for channel %s", channel.provider_channel_id)
            return False

    @staticmethod
    def _assistant_message_id(channel_id: str) -> str:
        return f"msg_asst_{channel_id}_{time.time_ns()}"

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

    @staticmethod
    def _coerce_label_list(raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            return [value for value in split_labels(raw) if value]
        if isinstance(raw, list):
            values: list[str] = []
            for item in raw:
                cleaned = " ".join(str(item).split()).strip()
                if cleaned:
                    values.append(cleaned)
            return values
        cleaned = " ".join(str(raw).split()).strip()
        return [cleaned] if cleaned else []

    @classmethod
    def _extract_web_children(cls, payload: dict[str, Any]) -> tuple[list[str], list[str]]:
        children_value = payload.get("children")
        child_names_value = payload.get("childNames")
        child_details_value = payload.get("childDetails")

        child_names: list[str] = []
        child_details: list[str] = []

        if isinstance(children_value, list):
            for item in children_value:
                if isinstance(item, dict):
                    name = " ".join(str(item.get("name") or "").split()).strip()
                    details = " ".join(str(item.get("details") or item.get("notes") or "").split()).strip()
                    if name:
                        child_names.append(name)
                        child_details.append(f"{name} - {details}" if details else name)
                else:
                    cleaned = " ".join(str(item).split()).strip()
                    if cleaned:
                        child_names.append(cleaned)
                        child_details.append(cleaned)
        elif isinstance(child_names_value, list):
            for item in child_names_value:
                cleaned = " ".join(str(item).split()).strip()
                if cleaned:
                    child_names.append(cleaned)
                    child_details.append(cleaned)
        elif isinstance(child_names_value, str):
            child_names = [value for value in extract_child_names(split_entries(child_names_value)) if value]
            child_details = list(child_names)

        if isinstance(child_details_value, str) and child_details_value.strip():
            child_details = split_entries(child_details_value)
            child_names = [value for value in extract_child_names(child_details) if value]
        elif isinstance(child_details_value, list):
            child_details = [value for value in cls._coerce_label_list(child_details_value) if value]
            child_names = [value for value in extract_child_names(child_details) if value]

        deduped_names: list[str] = []
        seen_names: set[str] = set()
        for name in child_names:
            lowered = name.lower()
            if lowered in seen_names:
                continue
            seen_names.add(lowered)
            deduped_names.append(name)

        deduped_details: list[str] = []
        seen_details: set[str] = set()
        for detail in child_details:
            lowered = detail.lower()
            if lowered in seen_details:
                continue
            seen_details.add(lowered)
            deduped_details.append(detail)

        if not deduped_details:
            deduped_details = list(deduped_names)
        return deduped_names, deduped_details

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _coerce_bool(raw: Any) -> bool | None:
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)) and raw in {0, 1}:
            return bool(raw)
        if isinstance(raw, str):
            lowered = raw.strip().lower()
            if lowered in {"true", "1", "yes", "on"}:
                return True
            if lowered in {"false", "0", "no", "off"}:
                return False
        return None

    @staticmethod
    def _clean_text(raw: Any) -> str | None:
        if raw is None:
            return None
        cleaned = " ".join(str(raw).split()).strip()
        return cleaned or None

    @classmethod
    def _merge_web_consent(cls, *, current: Any, incoming: Any) -> dict[str, Any]:
        consent = dict(current) if isinstance(current, dict) else {}
        if not isinstance(incoming, dict):
            return consent
        now = cls._utc_now_iso()
        if cls._coerce_bool(incoming.get("acceptTerms")) is True and not consent.get("termsAcceptedAt"):
            consent["termsAcceptedAt"] = now
        if cls._coerce_bool(incoming.get("acceptPrivacy")) is True and not consent.get("privacyAcceptedAt"):
            consent["privacyAcceptedAt"] = now
        terms_accepted_at = cls._clean_text(incoming.get("termsAcceptedAt"))
        privacy_accepted_at = cls._clean_text(incoming.get("privacyAcceptedAt"))
        if terms_accepted_at is not None:
            consent["termsAcceptedAt"] = terms_accepted_at
        if privacy_accepted_at is not None:
            consent["privacyAcceptedAt"] = privacy_accepted_at
        return consent

    @classmethod
    def _merge_priority_preferences(
        cls,
        *,
        current: dict[str, object],
        payload: dict[str, Any],
    ) -> dict[str, object]:
        updates: dict[str, object] = {}
        nested = payload.get("priorities") if isinstance(payload.get("priorities"), dict) else {}

        if "topPriorities" in payload or "topPriorities" in nested:
            updates["top_priorities"] = cls._coerce_label_list(
                payload.get("topPriorities", nested.get("topPriorities"))
            )[:3]
        if "topPriorityOther" in payload or "topPriorityOther" in nested:
            updates["top_priority_other"] = cls._clean_text(
                payload.get("topPriorityOther", nested.get("topPriorityOther"))
            )
        if "painPoints" in payload or "painPoints" in nested:
            updates["pain_points"] = cls._coerce_label_list(
                payload.get("painPoints", nested.get("painPoints"))
            )[:2]
        if "painPointOther" in payload or "painPointOther" in nested:
            updates["pain_point_other"] = cls._clean_text(
                payload.get("painPointOther", nested.get("painPointOther"))
            )

        if updates:
            updates["priorities_updated_at"] = cls._utc_now_iso()
        return updates

    @classmethod
    def _merge_trust_defaults(cls, *, current: Any, incoming: Any) -> dict[str, Any]:
        trust_defaults = dict(current) if isinstance(current, dict) else {}
        if not isinstance(incoming, dict):
            return trust_defaults
        for key in ("allowGoogleDataProcessing", "allowHouseholdLogisticsSharing", "askBeforeSensitiveShare"):
            coerced = cls._coerce_bool(incoming.get(key))
            if coerced is not None:
                trust_defaults[key] = coerced
        private_calendar_handling = cls._clean_text(incoming.get("privateCalendarHandling"))
        if private_calendar_handling in WEB_SETUP_PRIVATE_CALENDAR_HANDLING:
            trust_defaults["privateCalendarHandling"] = private_calendar_handling
        if trust_defaults:
            trust_defaults["updatedAt"] = cls._utc_now_iso()
        return trust_defaults

    @classmethod
    def _merge_activation_state(cls, *, current: Any, action: str) -> dict[str, Any]:
        activation = dict(current) if isinstance(current, dict) else {}
        normalized_action = action.strip().lower()
        if normalized_action in {"skip", "skip_activation"}:
            activation["action"] = "skip"
            activation["completedAt"] = cls._utc_now_iso()
        elif normalized_action in {"complete", "reviewed"}:
            activation["action"] = "reviewed"
            activation["completedAt"] = cls._utc_now_iso()
        return activation

    @classmethod
    def _serialize_google_calendar_definition(
        cls,
        *,
        raw_calendar: dict[str, Any],
        preference: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        usage_mode = cls._clean_text((preference or {}).get("usage_mode"))
        if usage_mode not in WEB_SETUP_CALENDAR_USAGE_MODES:
            usage_mode = None
        detail_visibility = cls._clean_text((preference or {}).get("detail_visibility"))
        if detail_visibility not in WEB_SETUP_CALENDAR_DETAIL_VISIBILITY:
            detail_visibility = None
        return {
            "id": str(raw_calendar.get("id") or ""),
            "summary": str(raw_calendar.get("summary") or "Calendar"),
            "timezone": str(raw_calendar.get("timezone") or "America/Los_Angeles"),
            "accessRole": cls._clean_text(raw_calendar.get("access_role") or raw_calendar.get("accessRole")),
            "primary": bool(raw_calendar.get("primary")),
            "selected": not bool(raw_calendar.get("selected") is False),
            "hidden": bool(raw_calendar.get("hidden")),
            "usageMode": usage_mode,
            "detailVisibility": detail_visibility,
            "configured": usage_mode is not None
            and (usage_mode == "ignore" or detail_visibility in WEB_SETUP_CALENDAR_DETAIL_VISIBILITY),
        }

    @classmethod
    def _calendar_catalog_for_connection(cls, connection) -> list[dict[str, Any]]:
        metadata = dict(connection.metadata)
        raw_catalog = metadata.get("available_calendars")
        raw_preferences = metadata.get("calendar_preferences")
        preferences = dict(raw_preferences) if isinstance(raw_preferences, dict) else {}

        calendars: list[dict[str, Any]] = []
        if isinstance(raw_catalog, list):
            for raw_item in raw_catalog:
                if not isinstance(raw_item, dict):
                    continue
                calendar_id = cls._clean_text(raw_item.get("id"))
                if calendar_id is None:
                    continue
                calendars.append(
                    cls._serialize_google_calendar_definition(
                        raw_calendar={**raw_item, "id": calendar_id},
                        preference=preferences.get(calendar_id) if isinstance(preferences.get(calendar_id), dict) else None,
                    )
                )

        if not calendars and metadata.get("primary_calendar_id"):
            primary_id = str(metadata.get("primary_calendar_id"))
            calendars.append(
                cls._serialize_google_calendar_definition(
                    raw_calendar={
                        "id": primary_id,
                        "summary": metadata.get("primary_calendar_summary") or "Primary calendar",
                        "timezone": metadata.get("primary_calendar_timezone") or "America/Los_Angeles",
                        "access_role": metadata.get("primary_calendar_access_role"),
                        "primary": True,
                    },
                    preference=preferences.get(primary_id) if isinstance(preferences.get(primary_id), dict) else None,
                )
            )

        calendars.sort(
            key=lambda calendar: (
                0 if calendar["primary"] else 1,
                0 if calendar["selected"] else 1,
                str(calendar["summary"]).lower(),
            )
        )
        return calendars

    @classmethod
    def _calendar_classification_complete_for_connection(cls, connection) -> bool:
        calendars = cls._calendar_catalog_for_connection(connection)
        if not calendars:
            return False
        for calendar in calendars:
            usage_mode = calendar.get("usageMode")
            if usage_mode not in WEB_SETUP_CALENDAR_USAGE_MODES:
                return False
            if usage_mode != "ignore" and calendar.get("detailVisibility") not in WEB_SETUP_CALENDAR_DETAIL_VISIBILITY:
                return False
        return True

    @classmethod
    def _serialize_setup_preferences(cls, manager_profile: dict[str, object]) -> dict[str, Any]:
        consent_raw = manager_profile.get("web_consent")
        consent = dict(consent_raw) if isinstance(consent_raw, dict) else {}
        trust_raw = manager_profile.get("trust_defaults")
        trust = dict(trust_raw) if isinstance(trust_raw, dict) else {}
        activation_raw = manager_profile.get("activation_review")
        activation = dict(activation_raw) if isinstance(activation_raw, dict) else {}
        return {
            "consent": {
                "termsAcceptedAt": cls._clean_text(consent.get("termsAcceptedAt")),
                "privacyAcceptedAt": cls._clean_text(consent.get("privacyAcceptedAt")),
            },
            "priorities": {
                "topPriorities": cls._coerce_label_list(manager_profile.get("top_priorities")),
                "topPriorityOther": cls._clean_text(manager_profile.get("top_priority_other")),
                "painPoints": cls._coerce_label_list(manager_profile.get("pain_points")),
                "painPointOther": cls._clean_text(manager_profile.get("pain_point_other")),
                "updatedAt": cls._clean_text(manager_profile.get("priorities_updated_at")),
            },
            "trustDefaults": {
                "allowGoogleDataProcessing": cls._coerce_bool(trust.get("allowGoogleDataProcessing")),
                "allowHouseholdLogisticsSharing": cls._coerce_bool(trust.get("allowHouseholdLogisticsSharing")),
                "privateCalendarHandling": cls._clean_text(trust.get("privateCalendarHandling")),
                "askBeforeSensitiveShare": cls._coerce_bool(trust.get("askBeforeSensitiveShare")),
                "updatedAt": cls._clean_text(trust.get("updatedAt")),
            },
            "activation": {
                "action": cls._clean_text(activation.get("action")),
                "completedAt": cls._clean_text(activation.get("completedAt")),
            },
        }

    def _persist_web_calendar_selections(self, *, context: dict[str, Any], raw: Any) -> None:
        if not isinstance(raw, list):
            return
        connections = {connection.id: connection for connection in context["connections"]}
        default_connection = self._primary_google_connection(list(context["connections"]))
        updates_by_connection: dict[str, dict[str, dict[str, str]]] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            connection_id = self._clean_text(item.get("connectionId"))
            if connection_id is None and default_connection is not None:
                connection_id = default_connection.id
            if connection_id is None or connection_id not in connections:
                continue
            calendar_id = self._clean_text(item.get("calendarId"))
            usage_mode = self._clean_text(item.get("usageMode"))
            if calendar_id is None or usage_mode not in WEB_SETUP_CALENDAR_USAGE_MODES:
                continue
            detail_visibility = self._clean_text(item.get("detailVisibility"))
            next_preference: dict[str, str] = {"usage_mode": usage_mode}
            if usage_mode != "ignore" and detail_visibility in WEB_SETUP_CALENDAR_DETAIL_VISIBILITY:
                next_preference["detail_visibility"] = detail_visibility
            updates_by_connection.setdefault(connection_id, {})[calendar_id] = next_preference

        for connection_id, connection_updates in updates_by_connection.items():
            connection = connections[connection_id]
            metadata = dict(connection.metadata)
            preferences = dict(metadata.get("calendar_preferences")) if isinstance(metadata.get("calendar_preferences"), dict) else {}
            for calendar_id, next_preference in connection_updates.items():
                preferences[calendar_id] = next_preference
            metadata["calendar_preferences"] = preferences
            metadata["calendar_preferences_updated_at"] = self._utc_now_iso()
            self.store.upsert_google_connection(replace(connection, metadata=metadata))

    @classmethod
    def _serialize_google_connection(cls, connection) -> dict[str, Any]:
        return {
            "id": connection.id,
            "householdId": connection.household_id,
            "memberId": connection.member_id,
            "email": connection.email,
            "connectedScopes": [scope.value for scope in connection.connected_scopes],
            "active": connection.active,
            "primaryWebAccount": bool(connection.metadata.get("web_primary")),
            "calendarClassificationComplete": cls._calendar_classification_complete_for_connection(connection),
            "availableCalendars": cls._calendar_catalog_for_connection(connection),
            "metadata": dict(connection.metadata),
            "sync": build_google_connection_sync_status(connection),
        }

    @staticmethod
    def _serialize_candidate_preview(candidate) -> dict[str, Any]:
        return {
            "id": candidate.id,
            "sourceKind": candidate.source_kind.value,
            "sourceIdentifier": candidate.source_identifier,
            "title": candidate.title,
            "summary": candidate.summary,
            "state": candidate.state.value,
            "confidenceBps": candidate.confidence_bps,
            "requiresConfirmation": candidate.requires_confirmation,
            "metadata": dict(candidate.metadata),
        }

    @staticmethod
    def _serialize_child(child) -> dict[str, Any]:
        return {
            "id": child.id,
            "fullName": child.full_name,
            "birthdate": child.birthdate,
            "metadata": dict(child.metadata),
        }

    @staticmethod
    def _serialize_profile_item(item) -> dict[str, Any]:
        return {
            "id": item.id,
            "kind": item.kind.value,
            "label": item.label,
            "memberId": item.member_id,
            "childId": item.child_id,
            "metadata": dict(item.metadata),
        }

    @staticmethod
    def _serialize_household_event(event) -> dict[str, Any]:
        return {
            "id": event.id,
            "title": event.title,
            "startsAt": event.starts_at,
            "endsAt": event.ends_at,
            "timezone": event.timezone,
            "allDay": event.all_day,
            "location": event.location,
            "description": event.description,
            "sourceCandidateId": event.source_candidate_id,
            "status": event.status.value,
            "metadata": dict(event.metadata),
        }

    def _primary_google_connection(self, connections: list[Any]) -> Any | None:
        if not connections:
            return None
        for connection in connections:
            if connection.active and bool(connection.metadata.get("web_primary")):
                return connection
        return connections[0]

    def _is_web_setup_ready(
        self,
        *,
        household_id: str,
        member_id: str,
        connections: list[Any] | None = None,
        store: FlorenceStateDB | None = None,
    ) -> bool:
        target_store = store or self.store
        resolved_connections = connections if connections is not None else target_store.list_google_connections(
            household_id=household_id,
            member_id=member_id,
        )
        primary_connection = self._primary_google_connection(list(resolved_connections))
        primary_sync = (
            build_google_connection_sync_status(primary_connection)
            if primary_connection is not None
            else None
        )
        manager_service = self.household_manager_service if store is None else FlorenceHouseholdManagerService(target_store)
        manager_profile = manager_service.get_manager_profile(household_id)
        setup_preferences = self._serialize_setup_preferences(manager_profile)
        has_children = bool(target_store.list_child_profiles(household_id=household_id))
        has_schools = bool(
            target_store.list_household_profile_items(
                household_id=household_id,
                kind=HouseholdProfileKind.SCHOOL,
            )
        )
        has_activities = bool(
            target_store.list_household_profile_items(
                household_id=household_id,
                kind=HouseholdProfileKind.ACTIVITY,
            )
        )
        calendar_classification_complete = bool(
            resolved_connections
            and all(self._calendar_classification_complete_for_connection(connection) for connection in resolved_connections)
        )
        priorities_complete = bool(
            setup_preferences["priorities"]["topPriorities"]
            and (
                setup_preferences["priorities"]["painPoints"]
                or setup_preferences["priorities"]["painPointOther"]
            )
        )
        trust_defaults = setup_preferences["trustDefaults"]
        trust_defaults_complete = bool(
            trust_defaults["allowGoogleDataProcessing"] is True
            and trust_defaults["allowHouseholdLogisticsSharing"] is True
            and trust_defaults["privateCalendarHandling"] in WEB_SETUP_PRIVATE_CALENDAR_HANDLING
            and isinstance(trust_defaults["askBeforeSensitiveShare"], bool)
        )
        consent_complete = bool(
            setup_preferences["consent"]["termsAcceptedAt"]
            and setup_preferences["consent"]["privacyAcceptedAt"]
        )
        reviewed_candidates = target_store.list_imported_candidates(
            household_id=household_id,
            member_id=member_id,
        )
        activation_complete = bool(
            setup_preferences["activation"]["completedAt"]
            or any(candidate.state in {CandidateState.CONFIRMED, CandidateState.REJECTED} for candidate in reviewed_candidates)
            or not reviewed_candidates
        )
        return bool(
            consent_complete
            and primary_connection is not None
            and primary_sync is not None
            and calendar_classification_complete
            and primary_sync["initialSyncCompletedAt"]
            and has_children
            and has_schools
            and has_activities
            and priorities_complete
            and trust_defaults_complete
            and activation_complete
        )

    def _serialize_web_setup(self, context: dict[str, Any]) -> dict[str, Any]:
        household = context["household"]
        member = context["member"]
        session = context["session"]
        connections = list(context["connections"])
        adults = self.store.list_members(household.id)
        children = self.store.list_child_profiles(household_id=household.id)
        schools = self.store.list_household_profile_items(
            household_id=household.id,
            kind=HouseholdProfileKind.SCHOOL,
        )
        activities = self.store.list_household_profile_items(
            household_id=household.id,
            kind=HouseholdProfileKind.ACTIVITY,
        )
        primary_connection = self._primary_google_connection(connections)
        primary_sync = (
            build_google_connection_sync_status(primary_connection)
            if primary_connection is not None
            else {
                "initialSyncState": "pending",
                "initialSyncCompletedAt": None,
                "queuedAt": None,
                "startedAt": None,
                "phase": "connect_google",
                "lastSyncStatus": None,
                "lastSyncCompletedAt": None,
                "lastSyncError": None,
                "gmailLastSyncedAt": None,
                "calendarLastSyncedAt": None,
                "gmailItemCount": 0,
                "calendarItemCount": 0,
                "candidateCount": 0,
            }
        )
        suggestions = build_grounding_suggestions(household.settings)
        selected_school_labels = {item.label.strip().lower() for item in schools}
        selected_activity_labels = {item.label.strip().lower() for item in activities}
        for suggestion in suggestions["schools"]:
            suggestion["selected"] = suggestion["label"].strip().lower() in selected_school_labels
        for suggestion in suggestions["activities"]:
            suggestion["selected"] = suggestion["label"].strip().lower() in selected_activity_labels

        preview_candidates = [
            self._serialize_candidate_preview(candidate)
            for candidate in self.store.list_imported_candidates(
                household_id=household.id,
                member_id=member.id,
            )[:5]
        ]
        all_candidates = self.store.list_imported_candidates(
            household_id=household.id,
            member_id=member.id,
        )
        manager_profile = self.household_manager_service.get_manager_profile(household.id)
        setup_preferences = self._serialize_setup_preferences(manager_profile)
        consent_complete = bool(
            setup_preferences["consent"]["termsAcceptedAt"]
            and setup_preferences["consent"]["privacyAcceptedAt"]
        )
        calendar_classification_complete = bool(
            connections
            and all(self._calendar_classification_complete_for_connection(connection) for connection in connections)
        )
        has_children = bool(children)
        has_schools = bool(schools)
        has_activities = bool(activities)
        required_profile_complete = bool(has_children and has_schools and has_activities)
        priorities_complete = bool(
            setup_preferences["priorities"]["topPriorities"]
            and (
                setup_preferences["priorities"]["painPoints"]
                or setup_preferences["priorities"]["painPointOther"]
            )
        )
        trust_defaults_complete = bool(
            setup_preferences["trustDefaults"]["allowGoogleDataProcessing"] is True
            and setup_preferences["trustDefaults"]["allowHouseholdLogisticsSharing"] is True
            and setup_preferences["trustDefaults"]["privateCalendarHandling"] in WEB_SETUP_PRIVATE_CALENDAR_HANDLING
            and isinstance(setup_preferences["trustDefaults"]["askBeforeSensitiveShare"], bool)
        )
        initial_sync_complete = bool(primary_sync["initialSyncCompletedAt"])
        activation_complete = bool(
            setup_preferences["activation"]["completedAt"]
            or any(candidate.state in {CandidateState.CONFIRMED, CandidateState.REJECTED} for candidate in all_candidates)
            or not all_candidates
        )

        if not consent_complete:
            phase = "web_consent"
        elif not connections:
            phase = "connect_google"
        elif primary_sync["initialSyncState"] == "attention_needed":
            phase = "attention_needed"
        elif not calendar_classification_complete:
            phase = "classify_calendars"
        elif not required_profile_complete:
            phase = "collect_household_profile"
        elif not priorities_complete:
            phase = "collect_top_priorities"
        elif not trust_defaults_complete:
            phase = "collect_trust_defaults"
        elif not initial_sync_complete:
            phase = "initial_sync_running"
        elif not activation_complete:
            phase = "activation_review"
        else:
            phase = "ready"
        missing: list[str] = []
        if not consent_complete:
            missing.append("consent")
        if not connections:
            missing.append("google_account")
        if connections and not calendar_classification_complete:
            missing.append("calendar_classification")
        if not has_children:
            missing.append("kids")
        if not has_schools:
            missing.append("schools")
        if not has_activities:
            missing.append("activities")
        if not priorities_complete:
            missing.append("top_priorities")
        if not trust_defaults_complete:
            missing.append("trust_defaults")
        if connections and not initial_sync_complete:
            missing.append("initial_sync")
        if not activation_complete:
            missing.append("activation_review")

        return {
            "household": self._serialize_household(household),
            "member": self._serialize_member(member),
            "session": {
                "householdId": session.household_id,
                "memberId": session.member_id,
                "threadId": session.thread_id,
                "stage": session.stage.value,
                "variant": session.variant.value,
                "googleConnected": session.google_connected,
                "isComplete": session.is_complete,
            },
            "setup": {
                "phase": phase,
                "missing": missing,
                "googleConnected": bool(connections),
                "initialSyncComplete": initial_sync_complete,
                "requiredProfileComplete": required_profile_complete,
                "calendarClassificationComplete": calendar_classification_complete,
                "topPrioritiesComplete": priorities_complete,
                "trustDefaultsComplete": trust_defaults_complete,
                "activationComplete": activation_complete,
                "readyForChat": phase == "ready",
                "requiredFields": {
                    "consent": consent_complete,
                    "googleAccount": bool(connections),
                    "calendarClassification": calendar_classification_complete,
                    "kids": has_children,
                    "schools": has_schools,
                    "activities": has_activities,
                    "topPriorities": priorities_complete,
                    "trustDefaults": trust_defaults_complete,
                    "initialSync": initial_sync_complete,
                    "activationReview": activation_complete,
                },
            },
            "sync": {
                "primaryConnectionId": primary_connection.id if primary_connection is not None else None,
                "primary": primary_sync,
                "connections": [self._serialize_google_connection(connection) for connection in connections],
            },
            "profile": {
                "adults": [self._serialize_member(adult) for adult in adults],
                "children": [self._serialize_child(child) for child in children],
                "schools": [self._serialize_profile_item(item) for item in schools],
                "activities": [self._serialize_profile_item(item) for item in activities],
            },
            "preferences": setup_preferences,
            "suggestions": suggestions,
            "preview": {
                "candidates": preview_candidates,
                "candidateCount": len(all_candidates),
            },
            "googleConnectUrl": context["google_connect_url"],
        }

    def _serialize_web_review(self, context: dict[str, Any]) -> dict[str, Any]:
        household = context["household"]
        member = context["member"]
        setup_payload = self._serialize_web_setup(context)
        candidates = self.store.list_imported_candidates(
            household_id=household.id,
            member_id=member.id,
        )
        counts = {
            "total": len(candidates),
            "pending": 0,
            "confirmed": 0,
            "rejected": 0,
            "quarantined": 0,
        }
        for candidate in candidates:
            if candidate.state == CandidateState.PENDING_REVIEW:
                counts["pending"] += 1
            elif candidate.state == CandidateState.CONFIRMED:
                counts["confirmed"] += 1
            elif candidate.state == CandidateState.REJECTED:
                counts["rejected"] += 1
            elif candidate.state == CandidateState.QUARANTINED:
                counts["quarantined"] += 1
        next_prompt = self.candidate_review_service.build_next_review_prompt(
            household_id=household.id,
            member_id=member.id,
        )
        return {
            "household": self._serialize_household(household),
            "member": self._serialize_member(member),
            "setup": setup_payload["setup"],
            "counts": counts,
            "nextPrompt": {
                "candidateId": next_prompt.candidate.id,
                "text": next_prompt.text,
            }
            if next_prompt is not None
            else None,
            "candidates": [self._serialize_candidate_preview(candidate) for candidate in candidates],
        }

    def _serialize_web_calendar(
        self,
        context: dict[str, Any],
        *,
        start: str | None,
        end: str | None,
    ) -> dict[str, Any]:
        household = context["household"]
        member = context["member"]
        setup_payload = self._serialize_web_setup(context)
        range_start = None
        range_end = None
        try:
            range_start = datetime.fromisoformat(start) if start else None
        except ValueError:
            range_start = None
        try:
            range_end = datetime.fromisoformat(end) if end else None
        except ValueError:
            range_end = None
        if range_end is None:
            range_end = datetime.now(timezone.utc) + timedelta(days=90)
        events = self.store.list_household_events(household_id=household.id)
        filtered_events = []
        for event in events:
            event_start = None
            event_end = None
            try:
                event_start = datetime.fromisoformat(event.starts_at) if event.starts_at else None
            except ValueError:
                event_start = None
            try:
                event_end = datetime.fromisoformat(event.ends_at) if event.ends_at else None
            except ValueError:
                event_end = None
            effective_start = event_start or event_end
            effective_end = event_end or event_start
            if range_start is not None and effective_end is not None and effective_end < range_start:
                continue
            if range_end is not None and effective_start is not None and effective_start > range_end:
                continue
            filtered_events.append(event)
        counts = {
            "total": len(filtered_events),
            "confirmed": sum(1 for event in filtered_events if event.status == HouseholdEventStatus.CONFIRMED),
            "tentative": sum(1 for event in filtered_events if event.status == HouseholdEventStatus.TENTATIVE),
            "cancelled": sum(1 for event in filtered_events if event.status == HouseholdEventStatus.CANCELLED),
        }
        return {
            "household": self._serialize_household(household),
            "member": self._serialize_member(member),
            "setup": setup_payload["setup"],
            "range": {
                "start": start,
                "end": range_end.isoformat() if range_end is not None else end,
            },
            "counts": counts,
            "events": [self._serialize_household_event(event) for event in filtered_events],
        }

    def _serialize_web_settings(self, context: dict[str, Any]) -> dict[str, Any]:
        household = context["household"]
        member = context["member"]
        return {
            "household": self._serialize_household(household),
            "member": self._serialize_member(member),
            "managerProfile": self.household_manager_service.get_manager_profile(household.id),
        }

    @staticmethod
    def _web_error_result(exc: Exception) -> FlorenceHTTPResult:
        error = str(exc)
        status_code = 400
        if error in {"web_onboarding_not_configured", "google_oauth_not_configured"}:
            status_code = 503
        elif error in {"unknown_web_google_identity", "unknown_household", "unknown_member"}:
            status_code = 404
        return FlorenceHTTPResult(
            status_code=status_code,
            content_type="application/json; charset=utf-8",
            body=json.dumps({"ok": False, "error": error}, separators=(",", ":")),
        )

    def _mark_connection_sync_queued(self, store: FlorenceStateDB, *, connection_id: str) -> None:
        connection = store.get_google_connection(connection_id)
        if connection is None:
            return
        metadata = dict(connection.metadata)
        if metadata.get("initial_sync_completed_at"):
            return
        metadata["initial_sync_state"] = "queued"
        metadata["sync_phase"] = "account_connected"
        metadata["initial_sync_queued_at"] = datetime.utcnow().isoformat() + "Z"
        metadata["last_sync_status"] = "queued"
        metadata.pop("last_sync_error", None)
        store.upsert_google_connection(replace(connection, metadata=metadata))

    def _mark_connection_sync_error(
        self,
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
        metadata["last_sync_failed_at"] = datetime.utcnow().isoformat() + "Z"
        if not metadata.get("initial_sync_completed_at"):
            metadata["initial_sync_state"] = "attention_needed"
            metadata["sync_phase"] = "attention_needed"
        store.upsert_google_connection(replace(connection, metadata=metadata))
