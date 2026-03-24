"""Persistence-backed Florence runtime services."""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from florence.contracts import (
    CandidateState,
    ChildProfile,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdContext,
    HouseholdEvent,
    HouseholdBriefingKind,
    HouseholdEventStatus,
    HouseholdMeal,
    HouseholdMealStatus,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    HouseholdShoppingItem,
    HouseholdShoppingItemStatus,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    ImportedCandidate,
    MemberRole,
    PilotEvent,
)
from florence.google import (
    GoogleCalendarMetadata,
    FlorenceGoogleOauthState,
    FlorenceGoogleSyncBatch,
    FlorenceGoogleSyncResult,
    GoogleTokenResponse,
    build_google_import_candidates,
    build_google_grounding_hints,
    build_google_oauth_connect_url,
    decode_google_oauth_state,
    exchange_google_code_for_tokens,
    fetch_google_user_email,
    fetch_primary_google_calendar,
    list_recent_gmail_sync_items,
    list_recent_parent_calendar_sync_items,
    merge_google_grounding_hints,
    refresh_google_access_token,
)
from florence.onboarding import (
    OnboardingPrompt,
    OnboardingStage,
    OnboardingState,
    OnboardingTransition,
    apply_activity_basics,
    apply_child_names,
    apply_household_members,
    apply_household_operations,
    apply_nudge_preferences,
    apply_operating_preferences,
    apply_parent_name,
    apply_school_basics,
    build_onboarding_prompt,
    mark_google_connected,
    mark_group_activated,
    OnboardingVariant,
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


def _parse_local_time_spec(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", value, flags=re.IGNORECASE)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    meridiem = (match.group(3) or "").lower()
    if meridiem == "pm" and hour < 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return (hour, minute)


def _extract_local_time_from_preferences(
    text: str | None,
    *,
    keywords: tuple[str, ...],
    default_hour: int,
    default_minute: int,
) -> tuple[int, int]:
    if text:
        lowered = text.lower()
        for keyword in keywords:
            idx = lowered.find(keyword.lower())
            if idx < 0:
                continue
            window_start = max(0, idx - 8)
            window_end = min(len(text), idx + len(keyword) + 48)
            parsed = _parse_local_time_spec(text[window_start:window_end])
            if parsed is not None:
                return parsed
    return (default_hour, default_minute)


def _local_schedule_days(*, text: str | None, kind: HouseholdBriefingKind) -> list[int]:
    lowered = (text or "").lower()
    if "daily" in lowered or "every day" in lowered:
        return [0, 1, 2, 3, 4, 5, 6]
    if kind == HouseholdBriefingKind.EVENING and "school night" in lowered:
        return [0, 1, 2, 3, 6]
    if "weekend" in lowered and "weekday" not in lowered:
        return [5, 6]
    return [0, 1, 2, 3, 4]


def _next_due_local_schedule_iso(
    *,
    household_timezone: str,
    hour: int,
    minute: int,
    days: list[int],
    now: datetime,
) -> str:
    zone = ZoneInfo(household_timezone)
    local_now = now.astimezone(zone)
    for offset in range(0, 8):
        candidate_date = local_now.date() + timedelta(days=offset)
        if candidate_date.weekday() not in days:
            continue
        candidate_local = datetime(
            candidate_date.year,
            candidate_date.month,
            candidate_date.day,
            hour,
            minute,
            tzinfo=zone,
        )
        if candidate_local <= local_now:
            continue
        return candidate_local.astimezone(timezone.utc).isoformat()
    fallback = local_now + timedelta(days=1)
    return datetime(
        fallback.year,
        fallback.month,
        fallback.day,
        hour,
        minute,
        tzinfo=zone,
    ).astimezone(timezone.utc).isoformat()


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


def _clean_label(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).split()).strip(" ,.;:-")
    return normalized or None


def _sorted_unique(values: set[str]) -> list[str]:
    return sorted(value for value in values if value)


def _metadata_list(metadata: dict[str, object], key: str) -> list[str]:
    raw = metadata.get(key)
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    seen: set[str] = set()
    for item in raw:
        cleaned = _clean_label(str(item))
        if cleaned is None:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return values


def _merge_metadata_list(metadata: dict[str, object], key: str, values: list[str]) -> None:
    merged = _metadata_list(metadata, key)
    seen = {value.lower() for value in merged}
    for raw in values:
        cleaned = _clean_label(raw)
        if cleaned is None:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        merged.append(cleaned)
    if merged:
        metadata[key] = merged


def _grounding_hints_from_settings(settings: dict[str, object] | None) -> dict[str, object]:
    if settings is None:
        return {}
    raw = settings.get("grounding_hints")
    return dict(raw) if isinstance(raw, dict) else {}


def _index_hint_entries(
    hints: dict[str, object],
    *,
    key: str,
    detail_fields: tuple[str, ...],
) -> dict[str, dict[str, list[str]]]:
    indexed: dict[str, dict[str, list[str]]] = {}
    raw_entries = hints.get(key)
    if not isinstance(raw_entries, list):
        return indexed

    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue
        label = _clean_label(str(entry.get("label") or ""))
        if label is None:
            continue
        bucket = indexed.setdefault(label.lower(), {"label": label})
        for field in detail_fields:
            values = entry.get(field)
            if isinstance(values, list):
                bucket[field] = _sorted_unique(
                    {
                        *_metadata_list(bucket, field),
                        *(
                            cleaned
                            for value in values
                            if (cleaned := _clean_label(str(value))) is not None
                        ),
                    }
                )
    return indexed


def _format_grounding_hint_line(label: str, *, primary: list[str], secondary: list[str]) -> str:
    details: list[str] = []
    if primary:
        details.append(", ".join(primary[:2]))
    if secondary:
        details.append(", ".join(secondary[:2]))
    if not details:
        return f"- {label}"
    return f"- {label} ({'; '.join(details)})"


def _augment_onboarding_prompt(
    prompt: OnboardingPrompt | None,
    *,
    settings: dict[str, object] | None,
) -> OnboardingPrompt | None:
    if prompt is None:
        return None

    hints = _grounding_hints_from_settings(settings)
    if prompt.stage == OnboardingStage.COLLECT_SCHOOL_BASICS:
        school_hints = list(_index_hint_entries(hints, key="schools", detail_fields=("domains", "platforms", "contacts")).values())
        if not school_hints:
            return prompt
        lines = [prompt.text, "Google already surfaced a few likely school signals:"]
        for entry in school_hints[:3]:
            lines.append(
                _format_grounding_hint_line(
                    str(entry["label"]),
                    primary=list(entry.get("platforms", [])),
                    secondary=list(entry.get("contacts", [])) or list(entry.get("domains", [])),
                )
            )
        lines.append("Reply with the school or daycare names I should use, even if they match the suggestions.")
        return replace(prompt, text="\n".join(lines))

    if prompt.stage == OnboardingStage.COLLECT_ACTIVITY_BASICS:
        activity_hints = list(_index_hint_entries(hints, key="activities", detail_fields=("locations", "contacts")).values())
        if not activity_hints:
            return prompt
        lines = [prompt.text, "Google also found likely activity signals:"]
        for entry in activity_hints[:4]:
            lines.append(
                _format_grounding_hint_line(
                    str(entry["label"]),
                    primary=list(entry.get("locations", [])),
                    secondary=list(entry.get("contacts", [])),
                )
            )
        lines.append("Reply with the activity names I should use. If helpful, include the child, like Ava soccer.")
        return replace(prompt, text="\n".join(lines))

    return prompt


def _build_household_context(
    store: FlorenceStateDB,
    *,
    household_id: str,
    actor_member_id: str,
    channel_id: str,
) -> HouseholdContext:
    children = store.list_child_profiles(household_id=household_id)
    schools = store.list_household_profile_items(household_id=household_id, kind=HouseholdProfileKind.SCHOOL)
    activities = store.list_household_profile_items(household_id=household_id, kind=HouseholdProfileKind.ACTIVITY)
    household = store.get_household(household_id)
    grounding_hints = _grounding_hints_from_settings(household.settings if household is not None else None)
    school_hint_entries = _index_hint_entries(
        grounding_hints,
        key="schools",
        detail_fields=("domains", "platforms", "contacts"),
    )
    activity_hint_entries = _index_hint_entries(
        grounding_hints,
        key="activities",
        detail_fields=("locations", "contacts"),
    )
    child_aliases: set[str] = set()
    school_domains: set[str] = set()
    school_platforms: set[str] = set()
    contact_names: set[str] = set()
    location_labels: set[str] = set()

    for child in children:
        child_aliases.update(_metadata_list(child.metadata, "aliases"))
        first_name = _clean_label(child.full_name.split()[0] if child.full_name.strip() else None)
        cleaned_name = _clean_label(child.full_name)
        if first_name is not None and cleaned_name is not None and first_name.lower() != cleaned_name.lower():
            child_aliases.add(first_name)

    for school in schools:
        school_domains.update(_metadata_list(school.metadata, "domains"))
        school_platforms.update(_metadata_list(school.metadata, "platforms"))
        contact_names.update(_metadata_list(school.metadata, "contacts"))

    for activity in activities:
        contact_names.update(_metadata_list(activity.metadata, "contacts"))
        location_labels.update(_metadata_list(activity.metadata, "locations"))

    for entry in school_hint_entries.values():
        school_domains.update(str(value) for value in entry.get("domains", []))
        school_platforms.update(str(value) for value in entry.get("platforms", []))
        contact_names.update(str(value) for value in entry.get("contacts", []))

    for entry in activity_hint_entries.values():
        contact_names.update(str(value) for value in entry.get("contacts", []))
        location_labels.update(str(value) for value in entry.get("locations", []))

    raw_contacts = grounding_hints.get("contacts")
    if isinstance(raw_contacts, list):
        contact_names.update(
            cleaned
            for value in raw_contacts
            if (cleaned := _clean_label(str(value))) is not None
        )

    raw_locations = grounding_hints.get("locations")
    if isinstance(raw_locations, list):
        location_labels.update(
            cleaned
            for value in raw_locations
            if (cleaned := _clean_label(str(value))) is not None
        )

    return HouseholdContext(
        household_id=household_id,
        actor_member_id=actor_member_id,
        channel_id=channel_id,
        visible_child_names=[child.full_name for child in children],
        child_aliases=_sorted_unique(child_aliases),
        school_labels=[item.label for item in schools],
        school_domains=_sorted_unique(school_domains),
        school_platforms=_sorted_unique(school_platforms),
        activity_labels=[item.label for item in activities],
        contact_names=_sorted_unique(contact_names),
        location_labels=_sorted_unique(location_labels),
    )


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
        variant_selector: Callable[[str, str], OnboardingVariant] | None = None,
    ):
        self.store = store
        self.candidate_review_service = candidate_review_service
        self.variant_selector = variant_selector or self._select_variant

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
            metadata={"variant": self.variant_selector(household_id, member_id).value},
        )
        self.store.upsert_onboarding_session(state)
        return state

    @staticmethod
    def _select_variant(household_id: str, member_id: str) -> OnboardingVariant:
        digest = hashlib.sha256(f"{household_id}:{member_id}".encode("utf-8")).hexdigest()
        return OnboardingVariant.CONCIERGE if int(digest[-1], 16) % 2 == 0 else OnboardingVariant.HYBRID

    def get_prompt(self, *, household_id: str, member_id: str, thread_id: str) -> OnboardingPrompt | None:
        state = self.get_or_create_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        household = self.store.get_household(household_id)
        return _augment_onboarding_prompt(
            build_onboarding_prompt(state),
            settings=household.settings if household is not None else None,
        )

    def record_parent_name(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        display_name: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        member = self.store.get_member(member_id)
        if member is not None:
            self.store.upsert_member(replace(member, display_name=display_name.strip() or member.display_name))
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
        child_details: list[str] | None = None,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_child_names(state, child_names, child_details=child_details))

    def record_household_members(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        household_members: list[str],
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_household_members(state, household_members))

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

    def record_household_operations(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        household_operations: list[str],
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_household_operations(state, household_operations))

    def record_nudge_preferences(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        nudge_preferences: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_nudge_preferences(state, nudge_preferences))

    def record_operating_preferences(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        operating_preferences: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_operating_preferences(state, operating_preferences))

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
        self._sync_household_grounding(transition.state)
        household = self.store.get_household(transition.state.household_id)
        prompt = _augment_onboarding_prompt(
            transition.prompt,
            settings=household.settings if household is not None else None,
        )
        if transition.state.is_grounded_for_google_matching and self.candidate_review_service is not None:
            self.candidate_review_service.release_quarantined_candidates(
                household_id=transition.state.household_id,
                member_id=transition.state.member_id,
            )
        return replace(transition, prompt=prompt)

    def _sync_household_grounding(self, state: OnboardingState) -> None:
        household = self.store.get_household(state.household_id)
        grounding_hints = _grounding_hints_from_settings(household.settings if household is not None else None)
        school_hints = _index_hint_entries(
            grounding_hints,
            key="schools",
            detail_fields=("domains", "platforms", "contacts"),
        )
        activity_hints = _index_hint_entries(
            grounding_hints,
            key="activities",
            detail_fields=("locations", "contacts"),
        )
        if state.child_names or state.stage not in {
            OnboardingStage.COLLECT_PARENT_NAME,
            OnboardingStage.COLLECT_HOUSEHOLD_MEMBERS,
            OnboardingStage.CONNECT_GOOGLE,
            OnboardingStage.COLLECT_CHILD_NAMES,
        }:
            existing_children = {
                child.full_name.strip().lower(): child
                for child in self.store.list_child_profiles(household_id=state.household_id)
            }
            children: list[ChildProfile] = []
            for child_name in state.child_names:
                cleaned_name = child_name.strip()
                if not cleaned_name:
                    continue
                existing_child = existing_children.get(cleaned_name.lower())
                metadata = dict(existing_child.metadata) if existing_child is not None else {}
                first_name = _clean_label(cleaned_name.split()[0] if cleaned_name else None)
                if first_name is not None and first_name.lower() != cleaned_name.lower():
                    _merge_metadata_list(metadata, "aliases", [first_name])
                children.append(
                    ChildProfile(
                        id=_stable_id("child", state.household_id, cleaned_name.lower()),
                        household_id=state.household_id,
                        full_name=cleaned_name,
                        metadata=metadata,
                    )
                )
            self.store.replace_child_profiles(household_id=state.household_id, children=children)

        if state.school_basics_collected:
            existing_schools = {
                item.label.strip().lower(): item
                for item in self.store.list_household_profile_items(
                    household_id=state.household_id,
                    kind=HouseholdProfileKind.SCHOOL,
                )
            }
            schools = [
                self._build_school_profile_item(
                    state=state,
                    label=label.strip(),
                    existing=existing_schools.get(label.strip().lower()),
                    hint=school_hints.get(label.strip().lower()),
                )
                for label in state.school_labels
                if label.strip()
            ]
            self.store.replace_household_profile_items(
                household_id=state.household_id,
                kind=HouseholdProfileKind.SCHOOL,
                items=schools,
            )

        if state.activity_basics_collected:
            existing_activities = {
                item.label.strip().lower(): item
                for item in self.store.list_household_profile_items(
                    household_id=state.household_id,
                    kind=HouseholdProfileKind.ACTIVITY,
                )
            }
            activities = [
                self._build_activity_profile_item(
                    state=state,
                    label=label.strip(),
                    existing=existing_activities.get(label.strip().lower()),
                    hint=activity_hints.get(label.strip().lower()),
                )
                for label in state.activity_labels
                if label.strip()
            ]
            self.store.replace_household_profile_items(
                household_id=state.household_id,
                kind=HouseholdProfileKind.ACTIVITY,
                items=activities,
            )

        if household is not None:
            settings = dict(household.settings)
            settings["manager_profile"] = {
                "onboarding_variant": state.variant.value,
                "household_members": state.household_members,
                "child_details": state.child_details,
                "household_operations": state.household_operations,
                "nudge_preferences": state.nudge_preferences,
                "operating_preferences": state.operating_preferences,
            }
            self.store.upsert_household(replace(household, settings=settings))

    def _build_school_profile_item(
        self,
        *,
        state: OnboardingState,
        label: str,
        existing: HouseholdProfileItem | None,
        hint: dict[str, list[str]] | None,
    ) -> HouseholdProfileItem:
        metadata = dict(existing.metadata) if existing is not None else {}
        if hint is not None:
            _merge_metadata_list(metadata, "domains", list(hint.get("domains", [])))
            _merge_metadata_list(metadata, "platforms", list(hint.get("platforms", [])))
            _merge_metadata_list(metadata, "contacts", list(hint.get("contacts", [])))
        return HouseholdProfileItem(
            id=_stable_id("school", state.household_id, label.lower()),
            household_id=state.household_id,
            kind=HouseholdProfileKind.SCHOOL,
            label=label,
            member_id=state.member_id,
            child_id=existing.child_id if existing is not None else None,
            metadata=metadata,
        )

    def _build_activity_profile_item(
        self,
        *,
        state: OnboardingState,
        label: str,
        existing: HouseholdProfileItem | None,
        hint: dict[str, list[str]] | None,
    ) -> HouseholdProfileItem:
        metadata = dict(existing.metadata) if existing is not None else {}
        if hint is not None:
            _merge_metadata_list(metadata, "locations", list(hint.get("locations", [])))
            _merge_metadata_list(metadata, "contacts", list(hint.get("contacts", [])))
        return HouseholdProfileItem(
            id=_stable_id("activity", state.household_id, label.lower()),
            household_id=state.household_id,
            kind=HouseholdProfileKind.ACTIVITY,
            label=label,
            member_id=state.member_id,
            child_id=existing.child_id if existing is not None else None,
            metadata=metadata,
        )


class FlorenceGoogleSyncPersistenceService:
    """Persists Google connections and sync-derived review candidates."""

    def __init__(self, store: FlorenceStateDB):
        self.store = store

    def save_google_connection(self, connection: GoogleConnection) -> GoogleConnection:
        return self.store.upsert_google_connection(connection)

    def persist_sync_batch(self, batch: FlorenceGoogleSyncBatch) -> FlorenceGoogleSyncResult:
        result = build_google_import_candidates(batch)
        persisted = [self.store.upsert_imported_candidate(candidate) for candidate in result.candidates]
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
        context = _build_household_context(
            self.store,
            household_id=hydrated_connection.household_id,
            actor_member_id=hydrated_connection.member_id,
            channel_id=latest_onboarding.thread_id if latest_onboarding is not None else "dm",
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


class FlorenceHouseholdManagerService:
    """Generic Florence operating-state service for the household agent."""

    def __init__(self, store: FlorenceStateDB):
        self.store = store

    def upsert_work_item(self, work_item: HouseholdWorkItem) -> HouseholdWorkItem:
        return self.store.upsert_household_work_item(work_item)

    def upsert_routine(self, routine: HouseholdRoutine) -> HouseholdRoutine:
        return self.store.upsert_household_routine(routine)

    def upsert_meal(self, meal: HouseholdMeal) -> HouseholdMeal:
        return self.store.upsert_household_meal(meal)

    def upsert_shopping_item(self, item: HouseholdShoppingItem) -> HouseholdShoppingItem:
        return self.store.upsert_household_shopping_item(item)

    def get_manager_profile(self, household_id: str) -> dict[str, object]:
        household = self.store.get_household(household_id)
        if household is None:
            return {}
        raw = household.settings.get("manager_profile") if isinstance(household.settings, dict) else None
        return dict(raw) if isinstance(raw, dict) else {}

    def update_manager_profile(self, *, household_id: str, updates: dict[str, object]) -> dict[str, object]:
        household = self.store.get_household(household_id)
        if household is None:
            return {}
        settings = dict(household.settings) if isinstance(household.settings, dict) else {}
        profile = dict(settings.get("manager_profile")) if isinstance(settings.get("manager_profile"), dict) else {}
        profile.update(updates)
        settings["manager_profile"] = profile
        self.store.upsert_household(replace(household, settings=settings))
        return profile

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

    def record_reminder_feedback(
        self,
        *,
        household_id: str,
        feedback_text: str,
        member_id: str | None = None,
        channel_id: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, object]:
        cleaned = " ".join(feedback_text.split()).strip()
        if not cleaned:
            return self.get_manager_profile(household_id)
        current = self.get_manager_profile(household_id)
        feedback_items_raw = current.get("reminder_feedback")
        feedback_items = list(feedback_items_raw) if isinstance(feedback_items_raw, list) else []
        captured_at = (now or _utc_now()).isoformat()
        feedback_items.append(
            {
                "text": cleaned,
                "captured_at": captured_at,
                "member_id": member_id,
                "channel_id": channel_id,
            }
        )
        updates: dict[str, object] = {
            "nudge_preferences": cleaned,
            "nudge_preferences_override": cleaned,
            "nudge_preferences_last_updated_at": captured_at,
            "reminder_feedback": feedback_items[-80:],
        }
        profile = self.update_manager_profile(household_id=household_id, updates=updates)
        self.record_pilot_event(
            household_id=household_id,
            event_type="reminder_feedback_received",
            member_id=member_id,
            channel_id=channel_id,
            metadata={"text": cleaned},
            created_at=now,
        )
        return profile

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
        profile = self.get_manager_profile(household_id)
        operating_preferences = str(profile.get("operating_preferences") or "")
        default_owner = self.default_recipient_member_id(household_id)
        default_channel = self.default_dm_channel_id(household_id=household_id, member_id=default_owner)
        if default_owner is None:
            return []

        disable_morning = bool(re.search(r"\b(?:no|skip|disable)\s+morning\s+brief\b", operating_preferences, re.IGNORECASE))
        disable_evening = bool(re.search(r"\b(?:no|skip|disable)\s+evening\s+(?:check[- ]?in|brief)\b", operating_preferences, re.IGNORECASE))

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

        routine_specs = [
            {
                "kind": HouseholdBriefingKind.MORNING,
                "title": "Morning brief",
                "hour": morning_hour,
                "minute": morning_minute,
                "days": _local_schedule_days(text=operating_preferences, kind=HouseholdBriefingKind.MORNING),
                "disabled": disable_morning,
            },
            {
                "kind": HouseholdBriefingKind.EVENING,
                "title": "Evening check-in",
                "hour": evening_hour,
                "minute": evening_minute,
                "days": _local_schedule_days(text=operating_preferences, kind=HouseholdBriefingKind.EVENING),
                "disabled": disable_evening,
            },
        ]

        upserted: list[HouseholdRoutine] = []
        for spec in routine_specs:
            routine_id = _stable_id("routine", household_id, "briefing", spec["kind"].value)
            existing = self.store.get_household_routine(routine_id)
            metadata = {
                "automation_kind": "briefing",
                "brief_kind": spec["kind"].value,
                "local_time": f"{spec['hour']:02d}:{spec['minute']:02d}",
                "days": list(spec["days"]),
                "channel_id": default_channel,
            }
            cadence = (
                f"briefing on weekdays at {spec['hour']:02d}:{spec['minute']:02d} local"
                if spec["days"] == [0, 1, 2, 3, 4]
                else f"briefing at {spec['hour']:02d}:{spec['minute']:02d} local on days {','.join(str(day) for day in spec['days'])}"
            )
            if spec["disabled"]:
                if existing is None:
                    continue
                paused = replace(
                    existing,
                    title=spec["title"],
                    cadence=cadence,
                    status=HouseholdRoutineStatus.PAUSED,
                    owner_member_id=existing.owner_member_id or default_owner,
                    next_due_at=None,
                    metadata=metadata,
                )
                upserted.append(self.store.upsert_household_routine(paused))
                continue

            next_due = _next_due_local_schedule_iso(
                household_timezone=timezone_name,
                hour=int(spec["hour"]),
                minute=int(spec["minute"]),
                days=list(spec["days"]),
                now=current,
            )
            routine = HouseholdRoutine(
                id=routine_id,
                household_id=household_id,
                title=spec["title"],
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

    def summarize_tracking_state(self, *, household_id: str, now: datetime | None = None) -> str:
        current = now or _utc_now()
        work_items = [
            item
            for item in self.store.list_household_work_items(household_id=household_id)
            if item.status in {HouseholdWorkItemStatus.OPEN, HouseholdWorkItemStatus.IN_PROGRESS, HouseholdWorkItemStatus.BLOCKED}
        ]
        routines = self.store.list_household_routines(
            household_id=household_id,
            status=HouseholdRoutineStatus.ACTIVE,
        )
        nudges = [
            nudge
            for nudge in self.store.list_household_nudges(household_id=household_id)
            if nudge.status in {HouseholdNudgeStatus.SCHEDULED, HouseholdNudgeStatus.SENT}
        ]
        meals = [
            meal
            for meal in self.store.list_household_meals(
                household_id=household_id,
                status=HouseholdMealStatus.PLANNED,
            )
            if (_parse_iso_datetime(meal.scheduled_for) or current) >= current
        ]
        shopping_items = self.store.list_household_shopping_items(
            household_id=household_id,
            list_name="groceries",
            status=HouseholdShoppingItemStatus.NEEDED,
        )

        lines = ["Here is what I am actively tracking right now:"]
        lines.append(
            f"- Open tasks: {len(work_items)} | active routines: {len(routines)} | pending reminders: {len(nudges)} | planned meals: {len(meals)} | grocery items: {len(shopping_items)}"
        )

        if work_items:
            lines.append("Top open tasks:")
            for item in work_items[:5]:
                label = item.title
                if item.due_at:
                    label = f"{label} (due {item.due_at})"
                lines.append(f"- {label}")
        if nudges:
            lines.append("Upcoming reminders:")
            for nudge in nudges[:5]:
                label = nudge.message
                if nudge.scheduled_for:
                    label = f"{label} ({nudge.scheduled_for})"
                lines.append(f"- {label}")
        if meals:
            lines.append("Upcoming meals:")
            for meal in meals[:5]:
                lines.append(f"- {meal.title} ({meal.meal_type}, {meal.scheduled_for})")
        if shopping_items:
            lines.append("Top grocery items:")
            for item in shopping_items[:8]:
                label = item.title
                if item.quantity:
                    label = f"{label} x{item.quantity}"
                lines.append(f"- {label}")
        return "\n".join(lines)

    def summarize_pending_nudges(self, *, household_id: str, now: datetime | None = None) -> str:
        current = now or _utc_now()
        nudges = [
            nudge
            for nudge in self.store.list_household_nudges(household_id=household_id)
            if nudge.status in {HouseholdNudgeStatus.SCHEDULED, HouseholdNudgeStatus.SENT}
        ]
        if not nudges:
            return "You do not have any pending Florence reminders right now."
        lines = ["Here are your pending reminders:"]
        for nudge in nudges[:12]:
            scheduled = _parse_iso_datetime(nudge.scheduled_for)
            when = nudge.scheduled_for or "unscheduled"
            if scheduled is not None and scheduled <= current:
                when = f"due now ({scheduled.isoformat()})"
            lines.append(f"- {nudge.message} [{nudge.status.value}] ({when})")
        return "\n".join(lines)
