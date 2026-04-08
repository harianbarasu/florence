"""Deterministic Google import triage for Florence."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parseaddr
from enum import StrEnum

from florence.contracts import HouseholdContext
from florence.google.types import GmailSyncItem, ParentCalendarSyncItem
from florence.relevance.common import (
    ALL_DAY_HINTS,
    AMBIGUITY_HINTS,
    CHILD_ACTIVITY_HINTS,
    LOGISTICS_HINTS,
    PERSONAL_CALENDAR_HINTS,
    PROMOTIONAL_HINTS,
    SCHOOL_SENDER_HINTS,
    count_hint_hits,
)
from florence.relevance.temporal import (
    ParsedExplicitDate,
    ParsedTime,
    ParsedTimeRange,
    parse_explicit_date,
    parse_single_time,
    parse_time_range,
)

class CandidateDecisionKind(StrEnum):
    CANDIDATE = "candidate"
    SKIP = "skip"


@dataclass(slots=True)
class CandidateDecision:
    kind: CandidateDecisionKind
    title: str | None = None
    summary: str | None = None
    proposed_fields: dict[str, object] | None = None
    confidence_bps: int | None = None
    requires_confirmation: bool = False
    confirmation_question: str | None = None
    reason: str | None = None
    raw_metadata: dict[str, object] = field(default_factory=dict)


def clamp_confidence_bps(value: int, minimum: int = 3_500, maximum: int = 9_800) -> int:
    return max(minimum, min(maximum, round(value)))


def cleanup_title(raw: str) -> str:
    return (
        raw.replace("[", " [")
        .replace("]", "] ")
        .replace("  ", " ")
        .strip()
    )


def compact_text(raw: str, max_length: int = 300) -> str:
    normalized = " ".join(raw.split())
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 1].rstrip()}…"


def _cleanup_gmail_title(raw: str) -> str:
    title = cleanup_title(raw)
    for prefix in ("re:", "fw:", "fwd:"):
        if title.lower().startswith(prefix):
            title = title[len(prefix):].strip()
    return title.strip(" ,.;:-")


def _normalized_values(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = " ".join(value.split()).strip().lower()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _count_known_hits(source: str, values: list[str]) -> int:
    hits = 0
    lowered = source.lower()
    for value in _normalized_values(values):
        pattern = re.escape(value)
        if re.fullmatch(r"[a-z0-9]+", value):
            pattern = rf"\b{pattern}\b"
        if re.search(pattern, lowered):
            hits += 1
    return hits


def _sender_domain(from_address: str) -> str:
    _, email = parseaddr(from_address)
    lowered = email.strip().lower()
    return lowered.split("@", 1)[1] if "@" in lowered else ""


def _format_parsed_time(value: ParsedTime | None) -> str | None:
    if value is None:
        return None
    return f"{value.hours:02d}:{value.minutes:02d}"


def _temporal_evidence_payload(
    *,
    date_match: ParsedExplicitDate | None,
    time_range: ParsedTimeRange | None,
    single_time: ParsedTime | None,
) -> dict[str, object]:
    payload: dict[str, object] = {}
    if date_match is not None:
        payload["date_match"] = {
            "text": date_match.match,
            "date": date_match.value.isoformat(),
        }
    if time_range is not None:
        payload["time_range"] = {
            "start": _format_parsed_time(time_range.start),
            "end": _format_parsed_time(time_range.end),
        }
    if single_time is not None:
        payload["single_time"] = _format_parsed_time(single_time)
    return payload


def _build_gmail_candidate_decision_heuristic(
    item: GmailSyncItem,
    time_zone: str,
    *,
    context: HouseholdContext | None = None,
    now: datetime | None = None,
) -> CandidateDecision:
    subject = _cleanup_gmail_title(item.subject) or "Untitled Gmail candidate"
    snippet = (item.snippet or "").strip()
    body_text = (item.body_text or "").strip()
    attachment_text = (item.attachment_text or "").strip()
    text = "\n".join(part for part in (subject, snippet, body_text, attachment_text) if part).strip()
    source_text = "\n".join(part for part in (item.from_address, subject, snippet, body_text, attachment_text) if part).strip()
    lowered = text.lower()
    lowered_source = source_text.lower()
    sender_lower = item.from_address.lower()
    sender_domain = _sender_domain(item.from_address)

    child_terms = list(context.visible_child_names) + list(context.child_aliases) if context is not None else []
    school_terms = list(context.school_labels) if context is not None else []
    activity_terms = list(context.activity_labels) if context is not None else []
    contact_terms = list(context.contact_names) if context is not None else []
    platform_terms = list(context.school_platforms) if context is not None else []
    location_terms = list(context.location_labels) if context is not None else []
    school_domain_hits = (
        1
        if context is not None and sender_domain and sender_domain.lower() in {domain.lower() for domain in context.school_domains}
        else 0
    )
    platform_hits = _count_known_hits(source_text, platform_terms)
    known_school_hits = _count_known_hits(source_text, school_terms)
    known_activity_hits = _count_known_hits(source_text, activity_terms)
    known_child_hits = _count_known_hits(source_text, child_terms)
    known_contact_hits = _count_known_hits(source_text, contact_terms)
    known_location_hits = _count_known_hits(source_text, location_terms)
    anchor_hits = (
        school_domain_hits
        + platform_hits
        + known_school_hits
        + known_activity_hits
        + known_child_hits
        + known_contact_hits
        + known_location_hits
    )
    sender_looks_school = any(hint in sender_lower for hint in SCHOOL_SENDER_HINTS)
    sender_looks_school = sender_looks_school or school_domain_hits > 0 or platform_hits > 0 or known_school_hits > 0
    logistics_hits = count_hint_hits(lowered, LOGISTICS_HINTS)
    activity_hint_hits = count_hint_hits(lowered, CHILD_ACTIVITY_HINTS)
    all_day_hits = count_hint_hits(lowered, ALL_DAY_HINTS)
    promotional_hits = count_hint_hits(lowered_source, PROMOTIONAL_HINTS)
    date_match = parse_explicit_date(text, time_zone, now=now or item.received_at)
    time_range = parse_time_range(text)
    single_time = None if time_range else parse_single_time(text)
    has_scheduling_evidence = bool(date_match or time_range or single_time or all_day_hits > 0)
    if promotional_hits > 0 and anchor_hits == 0:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="promotional_noise")

    strong_anchor = (
        anchor_hits >= 2
        or (known_child_hits > 0 and (known_activity_hits > 0 or known_school_hits > 0 or known_contact_hits > 0))
    )
    if sender_looks_school:
        looks_relevant = strong_anchor or logistics_hits > 0 or has_scheduling_evidence or activity_hint_hits > 0
    else:
        looks_relevant = anchor_hits > 0 and (logistics_hits > 0 or has_scheduling_evidence or activity_hint_hits > 0)
    if not looks_relevant:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="not_household_logistics")

    proposed_fields: dict[str, object] = {"title": subject}
    reason_tags: list[str] = []
    if sender_looks_school:
        reason_tags.append("school_source")
    if anchor_hits > 0:
        reason_tags.append("household_anchor")
    if logistics_hits > 0:
        reason_tags.append("logistics_signal")
    if activity_hint_hits > 0:
        reason_tags.append("activity_signal")
    if has_scheduling_evidence:
        reason_tags.append("schedule_signal")

    confidence_bps = 7_200
    if sender_looks_school and anchor_hits > 0:
        confidence_bps = 7_800
    elif not has_scheduling_evidence:
        confidence_bps = 6_800

    requires_confirmation = True
    confirmation_question: str | None = f"This looks relevant. Should I add or update anything from {subject}?"

    if count_hint_hits(lowered, AMBIGUITY_HINTS) > 0:
        confirmation_question = f"The schedule for {subject} looks conditional. Which date or time applies?"
        reason_tags.append("ambiguous_schedule")

    if not date_match:
        confidence_bps = min(confidence_bps, 6_500)
    elif not time_range and not single_time and all_day_hits <= 0:
        confidence_bps = min(confidence_bps, 6_800)

    if requires_confirmation:
        confidence_bps = max(confidence_bps, 6_500)

    return CandidateDecision(
        kind=CandidateDecisionKind.CANDIDATE,
        title=subject,
        proposed_fields=proposed_fields,
        confidence_bps=clamp_confidence_bps(confidence_bps, minimum=5_000),
        requires_confirmation=requires_confirmation,
        confirmation_question=confirmation_question,
        raw_metadata={
            "classifier": "gmail_heuristics_v2",
            "reason_tags": reason_tags,
            "anchor_hits": anchor_hits,
            "sender_looks_school": sender_looks_school,
            "temporal_evidence": _temporal_evidence_payload(
                date_match=date_match,
                time_range=time_range,
                single_time=single_time,
            ),
        },
    )


def build_gmail_candidate_decision(
    item: GmailSyncItem,
    time_zone: str,
    *,
    context: HouseholdContext | None = None,
    now: datetime | None = None,
) -> CandidateDecision:
    return _build_gmail_candidate_decision_heuristic(
        item,
        time_zone,
        context=context,
        now=now,
    )


def build_parent_calendar_candidate_decision(
    item: ParentCalendarSyncItem,
    *,
    context: HouseholdContext | None = None,
) -> CandidateDecision:
    title = _cleanup_gmail_title(item.title) or "Untitled calendar event"
    description = (item.description or "").strip()
    location = (item.location or "").strip()
    lowered = f"{title} {description} {location}".lower()

    logistics_hits = count_hint_hits(lowered, LOGISTICS_HINTS)
    activity_hits = count_hint_hits(lowered, CHILD_ACTIVITY_HINTS)
    personal_hits = count_hint_hits(lowered, PERSONAL_CALENDAR_HINTS)
    child_name_hits = sum(1 for name in item.family_member_names if name.strip() and name.strip().lower() in lowered)
    child_terms = list(context.visible_child_names) + list(context.child_aliases) if context is not None else []
    known_activity_hits = _count_known_hits(lowered, list(context.activity_labels) if context is not None else [])
    known_child_hits = _count_known_hits(lowered, child_terms)
    known_school_hits = _count_known_hits(lowered, list(context.school_labels) if context is not None else [])
    known_location_hits = _count_known_hits(lowered, list(context.location_labels) if context is not None else [])
    known_contact_hits = _count_known_hits(lowered, list(context.contact_names) if context is not None else [])
    family_signal_hits = (
        child_name_hits
        + activity_hits
        + known_activity_hits
        + known_child_hits
        + known_school_hits
        + known_location_hits
        + known_contact_hits
    )
    likely_child_logistics = logistics_hits > 0 or family_signal_hits > 0

    if not likely_child_logistics:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="not_child_or_family_logistics")

    if personal_hits > (logistics_hits + family_signal_hits) and family_signal_hits == 0:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="looks_personal_not_family")

    summary_bits = [
        item.calendar_summary or "Parent calendar",
        item.starts_at.isoformat(),
        description or None,
    ]
    summary = compact_text(" · ".join(bit for bit in summary_bits if bit), 300)
    confidence_bps = min(
        9_200,
        5_600
        + logistics_hits * 500
        + activity_hits * 450
        + child_name_hits * 700
        + known_activity_hits * 550
        + known_child_hits * 500
        + known_school_hits * 350
        + known_location_hits * 450
        + known_contact_hits * 300,
    )

    return CandidateDecision(
        kind=CandidateDecisionKind.CANDIDATE,
        title=title,
        summary=summary,
        proposed_fields={
            "title": title,
            "description": description or None,
            "location": location or None,
            "starts_at": item.starts_at.isoformat(),
            "ends_at": item.ends_at.isoformat(),
            "timezone": item.timezone,
            "all_day": item.all_day,
        },
        confidence_bps=confidence_bps,
        raw_metadata={
            "classifier": "parent_calendar_heuristics_v1",
            "known_activity_hits": known_activity_hits,
            "known_location_hits": known_location_hits,
            "family_signal_hits": family_signal_hits,
        },
    )
