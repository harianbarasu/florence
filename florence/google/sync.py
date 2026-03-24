"""Continuous Google sync orchestration for Florence candidates."""

from __future__ import annotations

import hashlib
import re
from email.utils import parseaddr
from dataclasses import dataclass, field

from florence.contracts import (
    CandidateState,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdContext,
    ImportedCandidate,
)
from florence.google.types import GmailSyncItem, ParentCalendarSyncItem
from florence.relevance import (
    CandidateDecision,
    CandidateDecisionKind,
    build_gmail_candidate_decision,
    build_parent_calendar_candidate_decision,
)

_PLATFORM_DOMAIN_HINTS = {
    "parentsquare": "ParentSquare",
    "brightwheel": "Brightwheel",
    "konstella": "Konstella",
    "classdojo": "ClassDojo",
    "remind": "Remind",
    "sportsengine": "SportsEngine",
    "teamsnap": "TeamSnap",
    "heja": "Heja",
    "crossbar": "Crossbar",
}

_ACTIVITY_LABEL_HINTS = {
    "soccer": "Soccer",
    "baseball": "Baseball",
    "basketball": "Basketball",
    "softball": "Softball",
    "tennis": "Tennis",
    "swim": "Swim",
    "swimming": "Swim",
    "dance": "Dance",
    "gymnastics": "Gymnastics",
    "piano": "Piano",
    "violin": "Violin",
    "guitar": "Guitar",
    "drums": "Drums",
    "choir": "Choir",
    "band": "Band",
    "theater": "Theater",
    "drama": "Drama",
    "robotics": "Robotics",
    "chess": "Chess",
    "karate": "Karate",
    "taekwondo": "Taekwondo",
}

_SCHOOL_LABEL_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z'&.-]*(?:\s+[A-Z][A-Za-z'&.-]*){0,4}\s+(?:School|Elementary|Preschool|Daycare|Academy|Charter|Campus|Camp))\b"
)


def _clean_label(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split()).strip(" ,.;:-")
    return normalized or None


def _sorted_unique(values: set[str]) -> list[str]:
    return sorted(value for value in values if value)


def _sender_name_and_domain(from_address: str) -> tuple[str | None, str | None]:
    display_name, email = parseaddr(from_address)
    cleaned_name = _clean_label(display_name)
    cleaned_email = email.strip().lower()
    domain = cleaned_email.split("@", 1)[1] if "@" in cleaned_email else None
    return cleaned_name, domain


def _collect_platform_labels(*texts: str) -> set[str]:
    lowered = " ".join(texts).lower()
    return {
        label
        for hint, label in _PLATFORM_DOMAIN_HINTS.items()
        if re.search(rf"\b{re.escape(hint)}\b", lowered)
    }


def _collect_school_labels(*texts: str) -> set[str]:
    joined = "\n".join(texts)
    results = {
        match.group(1).strip()
        for match in _SCHOOL_LABEL_PATTERN.finditer(joined)
        if _clean_label(match.group(1))
    }
    return {value for value in results if value}


def _collect_activity_labels(*texts: str) -> set[str]:
    lowered = " ".join(texts).lower()
    return {
        label
        for hint, label in _ACTIVITY_LABEL_HINTS.items()
        if re.search(rf"\b{re.escape(hint)}\b", lowered)
    }


def _build_school_hint_entry(
    label: str,
    *,
    domains: set[str] | None = None,
    platforms: set[str] | None = None,
    contacts: set[str] | None = None,
) -> dict[str, object]:
    return {
        "label": label,
        "domains": _sorted_unique(domains or set()),
        "platforms": _sorted_unique(platforms or set()),
        "contacts": _sorted_unique(contacts or set()),
    }


def _build_activity_hint_entry(
    label: str,
    *,
    locations: set[str] | None = None,
    contacts: set[str] | None = None,
) -> dict[str, object]:
    return {
        "label": label,
        "locations": _sorted_unique(locations or set()),
        "contacts": _sorted_unique(contacts or set()),
    }


def build_google_grounding_hints(batch: FlorenceGoogleSyncBatch) -> dict[str, object]:
    schools: dict[str, dict[str, set[str]]] = {}
    activities: dict[str, dict[str, set[str]]] = {}
    contacts: set[str] = set()
    locations: set[str] = set()

    def ensure_school(label: str) -> dict[str, set[str]]:
        entry = schools.get(label)
        if entry is None:
            entry = {"domains": set(), "platforms": set(), "contacts": set()}
            schools[label] = entry
        return entry

    def ensure_activity(label: str) -> dict[str, set[str]]:
        entry = activities.get(label)
        if entry is None:
            entry = {"locations": set(), "contacts": set()}
            activities[label] = entry
        return entry

    for item in batch.gmail_items:
        sender_name, sender_domain = _sender_name_and_domain(item.from_address)
        raw_text = "\n".join(
            part for part in (item.subject, item.snippet, item.body_text, item.attachment_text) if part
        )
        platform_labels = _collect_platform_labels(item.from_address, raw_text)
        school_labels = _collect_school_labels(item.from_address, raw_text)
        activity_labels = _collect_activity_labels(raw_text)

        if sender_name:
            contacts.add(sender_name)
        if sender_domain and any(token in sender_domain for token in ("school", "academy", "charter", ".edu", "k12")):
            school_labels.add(sender_name) if sender_name and sender_name.endswith(("School", "Academy", "Camp")) else None

        for school_label in school_labels:
            entry = ensure_school(school_label)
            if sender_domain:
                entry["domains"].add(sender_domain)
            entry["platforms"].update(platform_labels)
            if sender_name:
                entry["contacts"].add(sender_name)

        for activity_label in activity_labels:
            entry = ensure_activity(activity_label)
            if sender_name:
                entry["contacts"].add(sender_name)

    for item in batch.calendar_items:
        raw_text = "\n".join(part for part in (item.title, item.description, item.location, item.calendar_summary) if part)
        school_labels = _collect_school_labels(raw_text)
        activity_labels = _collect_activity_labels(raw_text)
        location = _clean_label(item.location)
        if location:
            locations.add(location)

        for school_label in school_labels:
            ensure_school(school_label)

        for activity_label in activity_labels:
            entry = ensure_activity(activity_label)
            if location:
                entry["locations"].add(location)

    return {
        "schools": [
            _build_school_hint_entry(
                label,
                domains=entry["domains"],
                platforms=entry["platforms"],
                contacts=entry["contacts"],
            )
            for label, entry in sorted(schools.items())
        ],
        "activities": [
            _build_activity_hint_entry(
                label,
                locations=entry["locations"],
                contacts=entry["contacts"],
            )
            for label, entry in sorted(activities.items())
        ],
        "contacts": _sorted_unique(contacts),
        "locations": _sorted_unique(locations),
    }


def merge_google_grounding_hints(existing: dict[str, object] | None, new_hints: dict[str, object]) -> dict[str, object]:
    merged_schools: dict[str, dict[str, set[str]]] = {}
    merged_activities: dict[str, dict[str, set[str]]] = {}
    merged_contacts: set[str] = set()
    merged_locations: set[str] = set()

    def load_school(entry: object) -> None:
        if not isinstance(entry, dict):
            return
        label = _clean_label(str(entry.get("label") or ""))
        if not label:
            return
        bucket = merged_schools.setdefault(label, {"domains": set(), "platforms": set(), "contacts": set()})
        for key in ("domains", "platforms", "contacts"):
            values = entry.get(key)
            if isinstance(values, list):
                bucket[key].update(_clean_label(str(value)) for value in values if _clean_label(str(value)))

    def load_activity(entry: object) -> None:
        if not isinstance(entry, dict):
            return
        label = _clean_label(str(entry.get("label") or ""))
        if not label:
            return
        bucket = merged_activities.setdefault(label, {"locations": set(), "contacts": set()})
        for key in ("locations", "contacts"):
            values = entry.get(key)
            if isinstance(values, list):
                bucket[key].update(_clean_label(str(value)) for value in values if _clean_label(str(value)))

    for source in (existing or {}, new_hints):
        schools = source.get("schools") if isinstance(source, dict) else None
        if isinstance(schools, list):
            for entry in schools:
                load_school(entry)
        activities = source.get("activities") if isinstance(source, dict) else None
        if isinstance(activities, list):
            for entry in activities:
                load_activity(entry)
        contacts = source.get("contacts") if isinstance(source, dict) else None
        if isinstance(contacts, list):
            merged_contacts.update(_clean_label(str(value)) for value in contacts if _clean_label(str(value)))
        locations = source.get("locations") if isinstance(source, dict) else None
        if isinstance(locations, list):
            merged_locations.update(_clean_label(str(value)) for value in locations if _clean_label(str(value)))

    return {
        "schools": [
            _build_school_hint_entry(
                label,
                domains=entry["domains"],
                platforms=entry["platforms"],
                contacts=entry["contacts"],
            )
            for label, entry in sorted(merged_schools.items())
        ],
        "activities": [
            _build_activity_hint_entry(
                label,
                locations=entry["locations"],
                contacts=entry["contacts"],
            )
            for label, entry in sorted(merged_activities.items())
        ],
        "contacts": _sorted_unique(merged_contacts),
        "locations": _sorted_unique(merged_locations),
    }


@dataclass(slots=True)
class FlorenceGoogleSyncBatch:
    connection: GoogleConnection
    context: HouseholdContext
    gmail_items: list[GmailSyncItem] = field(default_factory=list)
    calendar_items: list[ParentCalendarSyncItem] = field(default_factory=list)

    @property
    def context_timezone(self) -> str:
        raw = self.connection.metadata.get("primary_calendar_timezone")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        return "America/Los_Angeles"


@dataclass(slots=True)
class FlorenceGoogleSyncResult:
    candidates: list[ImportedCandidate] = field(default_factory=list)
    skipped_count: int = 0

    @property
    def quarantined_count(self) -> int:
        return sum(1 for candidate in self.candidates if candidate.state == CandidateState.QUARANTINED)

    @property
    def pending_review_count(self) -> int:
        return sum(1 for candidate in self.candidates if candidate.state == CandidateState.PENDING_REVIEW)


def _stable_candidate_id(
    household_id: str,
    member_id: str,
    source_kind: GoogleSourceKind,
    source_identifier: str,
) -> str:
    raw = f"{household_id}:{member_id}:{source_kind.value}:{source_identifier}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:20]
    return f"cand_{digest}"


def _review_state_for_context(context: HouseholdContext) -> CandidateState:
    if context.is_grounded_for_google_matching:
        return CandidateState.PENDING_REVIEW
    return CandidateState.QUARANTINED


def _default_confirmation_question(decision: CandidateDecision) -> str:
    subject = decision.title or "this item"
    return decision.confirmation_question or f"Should I add {subject} to your household plan?"


def _gmail_candidate_summary(item: GmailSyncItem, decision: CandidateDecision) -> str:
    if decision.summary:
        return decision.summary

    bits = [
        item.from_address.strip() or None,
        (item.snippet or "").strip() or None,
    ]
    return " - ".join(bit for bit in bits if bit) or (decision.title or "Gmail candidate")


def _calendar_candidate_summary(item: ParentCalendarSyncItem, decision: CandidateDecision) -> str:
    if decision.summary:
        return decision.summary

    bits = [
        item.calendar_summary or "Google Calendar",
        item.starts_at.isoformat(),
        (item.description or "").strip() or None,
    ]
    return " - ".join(bit for bit in bits if bit)


def _build_imported_candidate(
    *,
    connection: GoogleConnection,
    context: HouseholdContext,
    source_kind: GoogleSourceKind,
    source_identifier: str,
    title: str,
    summary: str,
    decision: CandidateDecision,
    source_metadata: dict[str, object],
) -> ImportedCandidate:
    return ImportedCandidate(
        id=_stable_candidate_id(
            connection.household_id,
            connection.member_id,
            source_kind,
            source_identifier,
        ),
        household_id=connection.household_id,
        member_id=connection.member_id,
        source_kind=source_kind,
        source_identifier=source_identifier,
        title=title,
        summary=summary,
        state=_review_state_for_context(context),
        confidence_bps=decision.confidence_bps,
        requires_confirmation=True,
        metadata={
            "review_channel_type": ChannelType.PARENT_DM.value,
            "google_connection_id": connection.id,
            "connected_email": connection.email,
            "confirmation_question": _default_confirmation_question(decision),
            "source_requires_detail_confirmation": decision.requires_confirmation,
            "proposed_fields": decision.proposed_fields or {},
            "raw_metadata": decision.raw_metadata,
            **source_metadata,
        },
    )


def build_google_import_candidates(batch: FlorenceGoogleSyncBatch) -> FlorenceGoogleSyncResult:
    """Classify synced Gmail and Calendar items into Florence review candidates."""
    result = FlorenceGoogleSyncResult()

    for item in batch.gmail_items:
        decision = build_gmail_candidate_decision(item, batch.context_timezone, context=batch.context)
        if decision.kind != CandidateDecisionKind.CANDIDATE:
            result.skipped_count += 1
            continue

        source_identifier = f"gmail:{item.gmail_message_id}"
        result.candidates.append(
            _build_imported_candidate(
                connection=batch.connection,
                context=batch.context,
                source_kind=GoogleSourceKind.GMAIL,
                source_identifier=source_identifier,
                title=decision.title or item.subject or "Untitled Gmail candidate",
                summary=_gmail_candidate_summary(item, decision),
                decision=decision,
                source_metadata={
                    "gmail_message_id": item.gmail_message_id,
                    "gmail_thread_id": item.thread_id,
                    "from_address": item.from_address,
                    "received_at": item.received_at.isoformat() if item.received_at is not None else None,
                },
            )
        )

    for item in batch.calendar_items:
        decision = build_parent_calendar_candidate_decision(item, context=batch.context)
        if decision.kind != CandidateDecisionKind.CANDIDATE:
            result.skipped_count += 1
            continue

        source_identifier = f"google_calendar:{item.google_event_id}"
        result.candidates.append(
            _build_imported_candidate(
                connection=batch.connection,
                context=batch.context,
                source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
                source_identifier=source_identifier,
                title=decision.title or item.title or "Untitled calendar event",
                summary=_calendar_candidate_summary(item, decision),
                decision=decision,
                source_metadata={
                    "google_event_id": item.google_event_id,
                    "calendar_summary": item.calendar_summary,
                    "html_link": item.html_link,
                    "starts_at": item.starts_at.isoformat(),
                    "ends_at": item.ends_at.isoformat(),
                    "all_day": item.all_day,
                },
            )
        )

    return result
