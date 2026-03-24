"""Persistence-backed Florence runtime services."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from florence.contracts import (
    CandidateState,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdContext,
    HouseholdEvent,
    HouseholdEventStatus,
    ImportedCandidate,
)
from florence.google import (
    GoogleCalendarMetadata,
    FlorenceGoogleOauthState,
    FlorenceGoogleSyncBatch,
    FlorenceGoogleSyncResult,
    GoogleTokenResponse,
    build_google_import_candidates,
    build_google_oauth_connect_url,
    decode_google_oauth_state,
    exchange_google_code_for_tokens,
    fetch_google_user_email,
    fetch_primary_google_calendar,
    list_recent_gmail_sync_items,
    list_recent_parent_calendar_sync_items,
    refresh_google_access_token,
)
from florence.onboarding import (
    OnboardingPrompt,
    OnboardingState,
    OnboardingTransition,
    apply_activity_basics,
    apply_child_names,
    apply_parent_name,
    apply_school_basics,
    build_onboarding_prompt,
    mark_google_connected,
    mark_group_activated,
)
from florence.state import FlorenceStateDB


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _google_token_expiry_iso(
    token_response: GoogleTokenResponse,
    *,
    now: datetime | None = None,
) -> str | None:
    if token_response.expires_in is None:
        return None
    base = now or _utc_now()
    return (base + timedelta(seconds=token_response.expires_in)).isoformat()


def _stable_id(prefix: str, *parts: str) -> str:
    raw = ":".join(parts).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:20]
    return f"{prefix}_{digest}"


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


@dataclass(slots=True)
class CandidateReviewPrompt:
    candidate: ImportedCandidate
    text: str


@dataclass(slots=True)
class CandidateReviewResult:
    candidate: ImportedCandidate
    event: HouseholdEvent | None = None
    group_announcement: str | None = None


class FlorenceCandidateReviewService:
    """Manages the review state lifecycle for imported Google candidates."""

    def __init__(self, store: FlorenceStateDB):
        self.store = store

    def release_quarantined_candidates(self, *, household_id: str, member_id: str) -> list[ImportedCandidate]:
        candidates = self.store.list_imported_candidates(
            household_id=household_id,
            member_id=member_id,
            state=CandidateState.QUARANTINED,
        )
        released: list[ImportedCandidate] = []
        for candidate in candidates:
            promoted = replace(candidate, state=CandidateState.PENDING_REVIEW)
            self.store.upsert_imported_candidate(promoted)
            released.append(promoted)
        return released

    def list_pending_candidates(self, *, household_id: str, member_id: str) -> list[ImportedCandidate]:
        return self.store.list_imported_candidates(
            household_id=household_id,
            member_id=member_id,
            state=CandidateState.PENDING_REVIEW,
        )

    def build_next_review_prompt(self, *, household_id: str, member_id: str) -> CandidateReviewPrompt | None:
        candidates = self.list_pending_candidates(household_id=household_id, member_id=member_id)
        if not candidates:
            return None
        candidate = candidates[0]
        question = str(candidate.metadata.get("confirmation_question") or "Should I add this?")
        lines = [
            f"Imported item: {candidate.title}",
            candidate.summary,
            question,
            "Reply yes to confirm it, no if it is wrong, or skip for later.",
        ]
        return CandidateReviewPrompt(candidate=candidate, text="\n".join(line for line in lines if line))

    def confirm_candidate(
        self,
        *,
        candidate_id: str,
        overrides: dict[str, Any] | None = None,
    ) -> CandidateReviewResult:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")

        event = self._candidate_to_event(candidate, overrides=overrides or {})
        self.store.upsert_household_event(event)
        confirmed_metadata = dict(candidate.metadata)
        confirmed_metadata["confirmed_event_id"] = event.id
        confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
        self.store.upsert_imported_candidate(confirmed)
        return CandidateReviewResult(
            candidate=confirmed,
            event=event,
            group_announcement=self._build_group_announcement(event),
        )

    def reject_candidate(self, *, candidate_id: str) -> CandidateReviewResult:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")
        rejected = replace(candidate, state=CandidateState.REJECTED)
        self.store.upsert_imported_candidate(rejected)
        return CandidateReviewResult(candidate=rejected)

    def _candidate_to_event(self, candidate: ImportedCandidate, *, overrides: dict[str, Any]) -> HouseholdEvent:
        proposed_fields = candidate.metadata.get("proposed_fields")
        base_fields = dict(proposed_fields) if isinstance(proposed_fields, dict) else {}
        event_fields = {**base_fields, **overrides}
        title = str(event_fields.get("title") or candidate.title).strip() or candidate.title
        starts_at = event_fields.get("starts_at")
        ends_at = event_fields.get("ends_at")
        status = (
            HouseholdEventStatus.CONFIRMED
            if isinstance(starts_at, str) and starts_at and isinstance(ends_at, str) and ends_at
            else HouseholdEventStatus.TENTATIVE
        )
        return HouseholdEvent(
            id=_stable_id("evt", candidate.household_id, candidate.id),
            household_id=candidate.household_id,
            title=title,
            starts_at=str(starts_at) if starts_at is not None else None,
            ends_at=str(ends_at) if ends_at is not None else None,
            timezone=str(event_fields.get("timezone")) if event_fields.get("timezone") is not None else None,
            all_day=bool(event_fields.get("all_day")),
            location=str(event_fields.get("location")) if event_fields.get("location") is not None else None,
            description=str(event_fields.get("description")) if event_fields.get("description") is not None else None,
            source_candidate_id=candidate.id,
            status=status,
            metadata={
                "source_kind": candidate.source_kind.value,
                "source_identifier": candidate.source_identifier,
                "candidate_summary": candidate.summary,
            },
        )

    @staticmethod
    def _build_group_announcement(event: HouseholdEvent) -> str:
        bits = [f"Added to the family plan: {event.title}"]
        if event.starts_at:
            bits.append(f"at {event.starts_at}")
        if event.location:
            bits.append(f"at {event.location}")
        return " ".join(bits)


class FlorenceOnboardingSessionService:
    """Persisted deterministic onboarding flow for a parent DM."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        candidate_review_service: FlorenceCandidateReviewService | None = None,
    ):
        self.store = store
        self.candidate_review_service = candidate_review_service

    def get_or_create_session(self, *, household_id: str, member_id: str, thread_id: str) -> OnboardingState:
        existing = self.store.get_onboarding_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        if existing is not None:
            return existing

        state = OnboardingState(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        self.store.upsert_onboarding_session(state)
        return state

    def get_prompt(self, *, household_id: str, member_id: str, thread_id: str) -> OnboardingPrompt | None:
        state = self.get_or_create_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        return build_onboarding_prompt(state)

    def record_parent_name(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        display_name: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_parent_name(state, display_name))

    def record_google_connected(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(mark_google_connected(state))

    def record_child_names(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        child_names: list[str],
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_child_names(state, child_names))

    def record_school_basics(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        school_labels: list[str],
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_school_basics(state, school_labels))

    def record_activity_basics(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        activity_labels: list[str],
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_activity_basics(state, activity_labels))

    def record_group_activated(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        group_channel_id: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(mark_group_activated(state, group_channel_id))

    def _persist_transition(self, transition: OnboardingTransition) -> OnboardingTransition:
        self.store.upsert_onboarding_session(transition.state)
        if transition.state.is_grounded_for_google_matching and self.candidate_review_service is not None:
            self.candidate_review_service.release_quarantined_candidates(
                household_id=transition.state.household_id,
                member_id=transition.state.member_id,
            )
        return transition


class FlorenceGoogleSyncPersistenceService:
    """Persists Google connections and sync-derived review candidates."""

    def __init__(self, store: FlorenceStateDB):
        self.store = store

    def save_google_connection(self, connection: GoogleConnection) -> GoogleConnection:
        return self.store.upsert_google_connection(connection)

    def persist_sync_batch(self, batch: FlorenceGoogleSyncBatch) -> FlorenceGoogleSyncResult:
        result = build_google_import_candidates(batch)
        persisted = [self.store.upsert_imported_candidate(candidate) for candidate in result.candidates]
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
        primary_calendar = fetch_primary_google_calendar(access_token=tokens.access_token)
        connection = GoogleConnection(
            id=_stable_id("gconn", payload.household_id, payload.member_id, email),
            household_id=payload.household_id,
            member_id=payload.member_id,
            email=email,
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            access_token_expires_at=_google_token_expiry_iso(tokens),
            metadata={
                "primary_calendar_id": primary_calendar.id,
                "primary_calendar_summary": primary_calendar.summary,
                "primary_calendar_timezone": primary_calendar.timezone,
                "primary_calendar_access_role": primary_calendar.access_role,
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
        max_gmail_results: int = 20,
        max_calendar_results: int = 20,
        window_days: int = 30,
        now: datetime | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> FlorenceGoogleSyncCycleResult:
        connection = self.store.get_google_connection(connection_id)
        if connection is None:
            raise ValueError("unknown_google_connection")

        hydrated_connection = self._ensure_fresh_access_token(
            connection,
            now=now,
            client_id=client_id,
            client_secret=client_secret,
        )
        access_token = hydrated_connection.access_token
        if not access_token:
            raise ValueError("google_access_token_missing")

        onboarding_sessions = self.store.list_member_onboarding_sessions(
            household_id=hydrated_connection.household_id,
            member_id=hydrated_connection.member_id,
        )
        latest_onboarding = onboarding_sessions[0] if onboarding_sessions else None
        context = HouseholdContext(
            household_id=hydrated_connection.household_id,
            actor_member_id=hydrated_connection.member_id,
            channel_id=latest_onboarding.thread_id if latest_onboarding is not None else "dm",
            visible_child_names=list(latest_onboarding.child_names) if latest_onboarding is not None else [],
            school_labels=list(latest_onboarding.school_labels) if latest_onboarding is not None else [],
            activity_labels=list(latest_onboarding.activity_labels) if latest_onboarding is not None else [],
        )
        family_member_names = list(context.visible_child_names)
        calendar_timezone = str(hydrated_connection.metadata.get("primary_calendar_timezone") or "America/Los_Angeles")
        calendar_id = str(hydrated_connection.metadata.get("primary_calendar_id") or "primary")
        calendar_summary = str(hydrated_connection.metadata.get("primary_calendar_summary") or "Primary calendar")

        gmail_items = list_recent_gmail_sync_items(
            access_token=access_token,
            max_results=max_gmail_results,
        )
        calendar_items = list_recent_parent_calendar_sync_items(
            access_token=access_token,
            calendar=(
                replace(
                    fetch_primary_google_calendar(access_token=access_token, fallback_timezone=calendar_timezone),
                    id=calendar_id,
                    summary=calendar_summary,
                    timezone=calendar_timezone,
                )
                if calendar_id == "primary" and "primary_calendar_id" not in hydrated_connection.metadata
                else GoogleCalendarMetadata(
                    id=calendar_id,
                    summary=calendar_summary,
                    timezone=calendar_timezone,
                    access_role=(
                        str(hydrated_connection.metadata.get("primary_calendar_access_role"))
                        if hydrated_connection.metadata.get("primary_calendar_access_role") is not None
                        else None
                    ),
                )
            ),
            family_member_names=family_member_names,
            max_results=max_calendar_results,
            window_days=window_days,
            now=now,
        )
        batch = FlorenceGoogleSyncBatch(
            connection=hydrated_connection,
            context=context,
            gmail_items=gmail_items,
            calendar_items=calendar_items,
        )
        sync_result = self.google_sync_service.persist_sync_batch(batch)
        return FlorenceGoogleSyncCycleResult(connection=hydrated_connection, sync_result=sync_result)

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


class FlorenceHouseholdQueryService:
    """Formats simple shared-state answers for the household group."""

    def __init__(self, store: FlorenceStateDB):
        self.store = store

    def summarize_upcoming_events(self, *, household_id: str, now: datetime | None = None, days: int = 7) -> str:
        current = now or _utc_now()
        cutoff = current + timedelta(days=days)
        events = []
        for event in self.store.list_household_events(household_id=household_id):
            if event.status == HouseholdEventStatus.CANCELLED:
                continue
            starts_at = _parse_iso_datetime(event.starts_at)
            if starts_at is not None and starts_at < current:
                continue
            if starts_at is not None and starts_at > cutoff:
                continue
            events.append(event)

        if not events:
            return "Nothing confirmed is on the family plan for the next week yet."

        lines = ["Here is the family plan for the next week:"]
        for event in events[:10]:
            if event.starts_at:
                lines.append(f"- {event.title} ({event.starts_at})")
            else:
                lines.append(f"- {event.title}")
        return "\n".join(lines)
