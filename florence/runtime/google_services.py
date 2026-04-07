"""Google OAuth and sync runtime services for Florence."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from florence.contracts import CandidateState, GoogleConnection, GoogleSourceKind
from florence.google import (
    GoogleCalendarMetadata,
    FlorenceGoogleOauthState,
    FlorenceGoogleSyncBatch,
    FlorenceGoogleSyncResult,
    GoogleTokenResponse,
    build_google_grounding_hints,
    build_google_import_candidates,
    build_google_oauth_connect_url,
    decode_google_oauth_state,
    exchange_google_code_for_tokens,
    fetch_google_user_email,
    fetch_primary_google_calendar,
    list_google_calendars,
    list_recent_gmail_sync_items,
    list_recent_parent_calendar_sync_items,
    merge_google_grounding_hints,
    refresh_google_access_token,
)
from florence.onboarding import OnboardingTransition
from florence.runtime.candidate_review import (
    _SourceRuleService,
)
from florence.runtime.onboarding_service import FlorenceOnboardingSessionService
from florence.runtime.services import (
    _build_household_context,
    _clean_label,
    _grounding_hints_from_settings,
    _parse_iso_datetime,
    _stable_id,
    _utc_now,
)
from florence.state import FlorenceStateDB


logger = logging.getLogger(__name__)


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _gmail_bootstrap_max_results() -> int:
    return _int_env("FLORENCE_GMAIL_BOOTSTRAP_MAX_RESULTS", 2_000)


def _gmail_incremental_max_results() -> int:
    return _int_env("FLORENCE_GMAIL_INCREMENTAL_MAX_RESULTS", 250)


def _gmail_bootstrap_window_days() -> int:
    return _int_env("FLORENCE_GMAIL_BOOTSTRAP_WINDOW_DAYS", 365)


def _calendar_bootstrap_max_results() -> int:
    return _int_env("FLORENCE_CALENDAR_BOOTSTRAP_MAX_RESULTS", 1_000)


def _calendar_incremental_max_results() -> int:
    return _int_env("FLORENCE_CALENDAR_INCREMENTAL_MAX_RESULTS", 250)


def _calendar_bootstrap_past_window_days() -> int:
    return _int_env("FLORENCE_CALENDAR_BOOTSTRAP_PAST_WINDOW_DAYS", 365)


def _calendar_bootstrap_future_window_days() -> int:
    return _int_env("FLORENCE_CALENDAR_BOOTSTRAP_FUTURE_WINDOW_DAYS", 365)


def _calendar_incremental_past_window_days() -> int:
    return _int_env("FLORENCE_CALENDAR_INCREMENTAL_PAST_WINDOW_DAYS", 30)


def _calendar_incremental_future_window_days() -> int:
    return _int_env("FLORENCE_CALENDAR_INCREMENTAL_FUTURE_WINDOW_DAYS", 120)


def _gmail_incremental_query(
    *,
    last_synced_at: datetime | None,
    current_time: datetime,
) -> str:
    if last_synced_at is None:
        return f"newer_than:{_gmail_bootstrap_window_days()}d"
    elapsed_seconds = max(0.0, (current_time - last_synced_at).total_seconds())
    elapsed_days = int((elapsed_seconds + 86_399) // 86_400)
    overlap_days = 2
    query_days = max(3, min(_gmail_bootstrap_window_days(), elapsed_days + overlap_days))
    return f"newer_than:{query_days}d"


def _google_token_expiry_iso(
    token_response: GoogleTokenResponse,
    *,
    now: datetime | None = None,
) -> str | None:
    if token_response.expires_in is None:
        return None
    base = now or _utc_now()
    return (base + timedelta(seconds=token_response.expires_in)).isoformat()


_CALENDAR_USAGE_MODES_WITH_EVENT_IMPORT = {"planning_and_conflicts"}
_CALENDAR_USAGE_MODES_WITH_FETCH = {"planning_and_conflicts", "conflicts_only"}
_CALENDAR_DETAIL_VISIBILITY_WITH_DETAILS = {"full_details"}


@dataclass(slots=True)
class _GoogleCalendarSyncTarget:
    calendar: GoogleCalendarMetadata
    usage_mode: str
    detail_visibility: str


def _calendar_sync_targets(metadata: dict[str, object]) -> list[_GoogleCalendarSyncTarget]:
    raw_catalog = metadata.get("available_calendars")
    raw_preferences = metadata.get("calendar_preferences")
    preferences = dict(raw_preferences) if isinstance(raw_preferences, dict) else {}
    calendars: list[GoogleCalendarMetadata] = []
    if isinstance(raw_catalog, list):
        for raw_item in raw_catalog:
            if not isinstance(raw_item, dict):
                continue
            calendar_id = _clean_label(str(raw_item.get("id") or ""))
            if calendar_id is None:
                continue
            calendars.append(
                GoogleCalendarMetadata(
                    id=calendar_id,
                    summary=str(raw_item.get("summary") or "Calendar"),
                    timezone=str(raw_item.get("timezone") or "America/Los_Angeles"),
                    access_role=(
                        str(raw_item.get("access_role"))
                        if raw_item.get("access_role") is not None
                        else (
                            str(raw_item.get("accessRole"))
                            if raw_item.get("accessRole") is not None
                            else None
                        )
                    ),
                    primary=bool(raw_item.get("primary")),
                    selected=not bool(raw_item.get("selected") is False),
                    hidden=bool(raw_item.get("hidden")),
                )
            )
    if not calendars and metadata.get("primary_calendar_id") is not None:
        calendars.append(
            GoogleCalendarMetadata(
                id=str(metadata.get("primary_calendar_id") or "primary"),
                summary=str(metadata.get("primary_calendar_summary") or "Primary calendar"),
                timezone=str(metadata.get("primary_calendar_timezone") or "America/Los_Angeles"),
                access_role=(
                    str(metadata.get("primary_calendar_access_role"))
                    if metadata.get("primary_calendar_access_role") is not None
                    else None
                ),
                primary=True,
            )
        )

    targets: list[_GoogleCalendarSyncTarget] = []
    if not preferences:
        primary = next((calendar for calendar in calendars if calendar.primary), None) or (calendars[0] if calendars else None)
        if primary is None:
            return []
        return [
            _GoogleCalendarSyncTarget(
                calendar=primary,
                usage_mode="planning_and_conflicts",
                detail_visibility="full_details",
            )
        ]

    for calendar in calendars:
        preference = preferences.get(calendar.id)
        if not isinstance(preference, dict):
            continue
        usage_mode = _clean_label(str(preference.get("usage_mode") or ""))
        detail_visibility = _clean_label(str(preference.get("detail_visibility") or ""))
        if usage_mode not in _CALENDAR_USAGE_MODES_WITH_FETCH:
            continue
        if usage_mode in _CALENDAR_USAGE_MODES_WITH_EVENT_IMPORT and detail_visibility not in _CALENDAR_DETAIL_VISIBILITY_WITH_DETAILS:
            detail_visibility = "busy_only"
        elif detail_visibility is None:
            detail_visibility = "full_details"
        targets.append(
            _GoogleCalendarSyncTarget(
                calendar=calendar,
                usage_mode=usage_mode,
                detail_visibility=detail_visibility,
            )
        )
    return targets


@dataclass(slots=True)
class FlorenceGoogleConnectLink:
    url: str
    state: FlorenceGoogleOauthState


@dataclass(slots=True)
class FlorenceGoogleCallbackResult:
    connection: GoogleConnection
    onboarding_transition: OnboardingTransition


@dataclass(slots=True)
class FlorenceGoogleSyncCycleResult:
    connection: GoogleConnection
    sync_result: FlorenceGoogleSyncResult


class FlorenceGoogleSyncPersistenceService:
    """Persists Google connections and sync-derived review candidates."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        source_rule_service: _SourceRuleService | None = None,
    ):
        self.store = store
        self.source_rule_service = source_rule_service or _SourceRuleService(store)

    @staticmethod
    def _merged_candidate_state(*, existing, incoming) -> CandidateState:
        if existing.state in {CandidateState.CONFIRMED, CandidateState.REJECTED}:
            return existing.state
        if existing.state == CandidateState.PENDING_REVIEW or incoming.state == CandidateState.PENDING_REVIEW:
            return CandidateState.PENDING_REVIEW
        return incoming.state

    def _merge_with_existing_candidate(self, candidate):
        existing = self.store.get_imported_candidate(candidate.id)
        if existing is None:
            return candidate
        merged_metadata = dict(existing.metadata)
        merged_metadata.update(candidate.metadata)
        return replace(
            candidate,
            state=self._merged_candidate_state(existing=existing, incoming=candidate),
            confidence_bps=candidate.confidence_bps if candidate.confidence_bps is not None else existing.confidence_bps,
            metadata=merged_metadata,
        )

    def persist_sync_batch(self, batch: FlorenceGoogleSyncBatch) -> FlorenceGoogleSyncResult:
        self.store.upsert_google_gmail_messages(
            connection=batch.connection,
            items=list(batch.gmail_items),
        )
        self.store.upsert_google_calendar_events(
            connection=batch.connection,
            items=list(batch.calendar_items),
        )
        result = build_google_import_candidates(batch)
        persisted = [
            self.store.upsert_imported_candidate(
                self._merge_with_existing_candidate(self.source_rule_service.apply_candidate_policy(candidate))
            )
            for candidate in result.candidates
        ]
        household = self.store.get_household(batch.connection.household_id)
        if household is not None:
            settings = dict(household.settings)
            settings["grounding_hints"] = merge_google_grounding_hints(
                _grounding_hints_from_settings(settings),
                build_google_grounding_hints(batch),
            )
            self.store.upsert_household(replace(household, settings=settings))
        return FlorenceGoogleSyncResult(candidates=persisted, skipped_count=result.skipped_count)


class FlorenceGoogleAccountLinkService:
    """Builds Google connect URLs and completes OAuth callbacks."""

    def __init__(
        self,
        store: FlorenceStateDB,
        onboarding_service: FlorenceOnboardingSessionService,
        *,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        state_secret: str,
    ):
        self.store = store
        self.onboarding_service = onboarding_service
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.state_secret = state_secret

    def build_connect_link(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        now_ms: int | None = None,
        nonce: str | None = None,
    ) -> FlorenceGoogleConnectLink:
        issued_at_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        payload = FlorenceGoogleOauthState(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            nonce=nonce or _stable_id("nonce", household_id, member_id, thread_id, str(issued_at_ms)),
            issued_at_ms=issued_at_ms,
        )
        url = build_google_oauth_connect_url(
            client_id=self.client_id,
            redirect_uri=self.redirect_uri,
            state_payload=payload,
            state_secret=self.state_secret,
        )
        return FlorenceGoogleConnectLink(url=url, state=payload)

    def handle_callback(self, *, code: str, raw_state: str) -> FlorenceGoogleCallbackResult:
        payload = decode_google_oauth_state(raw_state, self.state_secret)
        tokens = exchange_google_code_for_tokens(
            code=code,
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
        )
        email = fetch_google_user_email(access_token=tokens.access_token)
        calendars = list_google_calendars(access_token=tokens.access_token)
        primary_calendar = next((calendar for calendar in calendars if calendar.primary), None)
        if primary_calendar is None:
            primary_calendar = fetch_primary_google_calendar(access_token=tokens.access_token)
            calendars = [primary_calendar]
        connection_id = _stable_id("gconn", payload.household_id, payload.member_id, email)
        existing = self.store.get_google_connection(connection_id)
        existing_metadata = dict(existing.metadata) if existing is not None else {}
        connection = GoogleConnection(
            id=connection_id,
            household_id=payload.household_id,
            member_id=payload.member_id,
            email=email,
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            access_token_expires_at=_google_token_expiry_iso(tokens),
            metadata={
                **existing_metadata,
                "primary_calendar_id": primary_calendar.id,
                "primary_calendar_summary": primary_calendar.summary,
                "primary_calendar_timezone": primary_calendar.timezone,
                "primary_calendar_access_role": primary_calendar.access_role,
                "available_calendars": [
                    {
                        "id": calendar.id,
                        "summary": calendar.summary,
                        "timezone": calendar.timezone,
                        "access_role": calendar.access_role,
                        "primary": calendar.primary,
                        "selected": calendar.selected,
                        "hidden": calendar.hidden,
                    }
                    for calendar in calendars
                ],
            },
        )
        self.store.upsert_google_connection(connection)
        transition = self.onboarding_service.record_google_connected(
            household_id=payload.household_id,
            member_id=payload.member_id,
            thread_id=payload.thread_id or "",
        )
        return FlorenceGoogleCallbackResult(connection=connection, onboarding_transition=transition)


class FlorenceGoogleSyncWorkerService:
    """Runs continuous sync cycles for persisted Google connections."""

    def __init__(self, store: FlorenceStateDB, google_sync_service: FlorenceGoogleSyncPersistenceService):
        self.store = store
        self.google_sync_service = google_sync_service

    def sync_connection(
        self,
        connection_id: str,
        *,
        max_gmail_results: int | None = None,
        max_calendar_results: int | None = None,
        window_days: int | None = None,
        now: datetime | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> FlorenceGoogleSyncCycleResult:
        connection = self.store.get_google_connection(connection_id)
        if connection is None:
            raise ValueError("unknown_google_connection")
        logger.info("Florence Google sync started connection_id=%s", connection_id)

        hydrated_connection = self._ensure_fresh_access_token(
            connection,
            now=now,
            client_id=client_id,
            client_secret=client_secret,
        )
        access_token = hydrated_connection.access_token
        if not access_token:
            raise ValueError("google_access_token_missing")
        current_time = now or _utc_now()

        onboarding_sessions = self.store.list_member_onboarding_sessions(
            household_id=hydrated_connection.household_id,
            member_id=hydrated_connection.member_id,
        )
        latest_onboarding = onboarding_sessions[0] if onboarding_sessions else None
        context = _build_household_context(
            self.store,
            household_id=hydrated_connection.household_id,
            actor_member_id=hydrated_connection.member_id,
            channel_id=latest_onboarding.thread_id if latest_onboarding is not None else "dm",
        )
        family_member_names = list(context.visible_child_names)
        metadata = dict(hydrated_connection.metadata)
        bootstrap_complete = bool(metadata.get("gmail_bootstrap_completed_at"))
        calendar_bootstrap_complete = bool(metadata.get("calendar_bootstrap_completed_at"))
        if not metadata.get("initial_sync_started_at"):
            metadata["initial_sync_started_at"] = current_time.isoformat()
        if not metadata.get("initial_sync_completed_at"):
            metadata["initial_sync_state"] = "running"
        metadata["sync_phase"] = "syncing_inbox"
        metadata["last_sync_status"] = "running"
        metadata.pop("last_sync_error", None)
        hydrated_connection = self.store.upsert_google_connection(replace(hydrated_connection, metadata=metadata))
        metadata = dict(hydrated_connection.metadata)
        last_gmail_sync_at = _parse_iso_datetime(str(metadata.get("gmail_last_synced_at") or ""))
        gmail_query = (
            _gmail_incremental_query(last_synced_at=last_gmail_sync_at, current_time=current_time)
            if bootstrap_complete
            else f"newer_than:{_gmail_bootstrap_window_days()}d"
        )
        resolved_max_gmail_results = (
            max_gmail_results
            if max_gmail_results is not None
            else (_gmail_incremental_max_results() if bootstrap_complete else _gmail_bootstrap_max_results())
        )

        gmail_items = list_recent_gmail_sync_items(
            access_token=access_token,
            max_results=resolved_max_gmail_results,
            gmail_query=gmail_query,
        )
        logger.info(
            "Florence Google sync fetched Gmail items connection_id=%s count=%s query=%s",
            connection_id,
            len(gmail_items),
            gmail_query,
        )
        metadata["sync_phase"] = "syncing_calendar"
        hydrated_connection = self.store.upsert_google_connection(replace(hydrated_connection, metadata=metadata))
        metadata = dict(hydrated_connection.metadata)
        calendar_items: list[Any] = []
        fetched_calendar_item_count = 0
        resolved_max_calendar_results = (
            max_calendar_results
            if max_calendar_results is not None
            else (_calendar_incremental_max_results() if calendar_bootstrap_complete else _calendar_bootstrap_max_results())
        )
        calendar_past_window_days = (
            _calendar_incremental_past_window_days()
            if calendar_bootstrap_complete
            else _calendar_bootstrap_past_window_days()
        )
        calendar_future_window_days = (
            window_days
            if window_days is not None
            else (
                _calendar_incremental_future_window_days()
                if calendar_bootstrap_complete
                else _calendar_bootstrap_future_window_days()
            )
        )
        for target in _calendar_sync_targets(hydrated_connection.metadata):
            fetched_items = list_recent_parent_calendar_sync_items(
                access_token=access_token,
                calendar=target.calendar,
                family_member_names=family_member_names,
                max_results=resolved_max_calendar_results,
                past_window_days=calendar_past_window_days,
                future_window_days=calendar_future_window_days,
                now=now,
            )
            fetched_calendar_item_count += len(fetched_items)
            for item in fetched_items:
                calendar_items.append(
                    replace(
                        item,
                        usage_mode=target.usage_mode,
                        detail_visibility=target.detail_visibility,
                    )
                )
        logger.info(
            "Florence Google sync fetched calendar items connection_id=%s count=%s",
            connection_id,
            fetched_calendar_item_count,
        )
        metadata["sync_phase"] = "finding_family_sources"
        hydrated_connection = self.store.upsert_google_connection(replace(hydrated_connection, metadata=metadata))
        metadata = dict(hydrated_connection.metadata)
        batch = FlorenceGoogleSyncBatch(
            connection=hydrated_connection,
            context=context,
            gmail_items=gmail_items,
            calendar_items=calendar_items,
        )
        logger.info(
            "Florence Google sync classifying household-relevant sources connection_id=%s gmail_items=%s calendar_items=%s",
            connection_id,
            len(gmail_items),
            len(calendar_items),
        )
        sync_result = self.google_sync_service.persist_sync_batch(batch)
        metadata["gmail_last_synced_at"] = current_time.isoformat()
        metadata["calendar_last_synced_at"] = current_time.isoformat()
        metadata["gmail_last_query"] = gmail_query
        metadata["gmail_last_max_results"] = resolved_max_gmail_results
        metadata["last_gmail_item_count"] = len(gmail_items)
        metadata["last_calendar_item_count"] = fetched_calendar_item_count
        metadata["calendar_last_max_results"] = resolved_max_calendar_results
        metadata["calendar_last_past_window_days"] = calendar_past_window_days
        metadata["calendar_last_future_window_days"] = calendar_future_window_days
        metadata["last_candidate_count"] = len(sync_result.candidates)
        metadata["last_sync_completed_at"] = current_time.isoformat()
        metadata["last_sync_status"] = "ok"
        metadata["sync_phase"] = "ready"
        if not bootstrap_complete:
            metadata["gmail_bootstrap_completed_at"] = current_time.isoformat()
        if not calendar_bootstrap_complete:
            metadata["calendar_bootstrap_completed_at"] = current_time.isoformat()
        if not metadata.get("initial_sync_completed_at"):
            metadata["initial_sync_completed_at"] = current_time.isoformat()
        metadata["initial_sync_state"] = "ready"
        synced_connection = self.store.upsert_google_connection(replace(hydrated_connection, metadata=metadata))
        logger.info(
            "Florence Google sync complete connection_id=%s candidates=%s skipped=%s",
            connection_id,
            len(sync_result.candidates),
            sync_result.skipped_count,
        )
        return FlorenceGoogleSyncCycleResult(connection=synced_connection, sync_result=sync_result)

    def sync_household(
        self,
        *,
        household_id: str,
        now: datetime | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> list[FlorenceGoogleSyncCycleResult]:
        results: list[FlorenceGoogleSyncCycleResult] = []
        for connection in self.store.list_google_connections(household_id=household_id):
            results.append(
                self.sync_connection(
                    connection.id,
                    now=now,
                    client_id=client_id,
                    client_secret=client_secret,
                )
            )
        return results

    def _ensure_fresh_access_token(
        self,
        connection: GoogleConnection,
        *,
        now: datetime | None,
        client_id: str | None,
        client_secret: str | None,
    ) -> GoogleConnection:
        current = connection
        expiry = _parse_iso_datetime(connection.access_token_expires_at)
        refresh_needed = connection.access_token is None or (
            expiry is not None and expiry <= (now or _utc_now()) + timedelta(minutes=5)
        )
        if not refresh_needed:
            return current
        if not connection.refresh_token or not client_id or not client_secret:
            return current

        refreshed = refresh_google_access_token(
            refresh_token=connection.refresh_token,
            client_id=client_id,
            client_secret=client_secret,
        )
        current = replace(
            connection,
            access_token=refreshed.access_token or connection.access_token,
            refresh_token=refreshed.refresh_token or connection.refresh_token,
            access_token_expires_at=_google_token_expiry_iso(refreshed, now=now),
        )
        self.store.upsert_google_connection(current)
        return current
