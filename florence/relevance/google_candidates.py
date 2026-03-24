"""Gmail and Calendar candidate scoring for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum

from florence.google.types import GmailSyncItem, ParentCalendarSyncItem
from florence.relevance.common import (
    ALL_DAY_HINTS,
    AMBIGUITY_HINTS,
    CHILD_ACTIVITY_HINTS,
    LOGISTICS_HINTS,
    PERSONAL_CALENDAR_HINTS,
    SCHOOL_SENDER_HINTS,
    count_hint_hits,
)
from florence.relevance.temporal import (
    add_days,
    parse_explicit_date,
    parse_single_time,
    parse_time_range,
    zoned_datetime_to_utc,
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
    should_auto_handoff: bool = False
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


def build_gmail_candidate_decision(
    item: GmailSyncItem,
    time_zone: str,
    *,
    now: datetime | None = None,
) -> CandidateDecision:
    subject = _cleanup_gmail_title(item.subject) or "Untitled Gmail candidate"
    snippet = (item.snippet or "").strip()
    body_text = (item.body_text or "").strip()
    attachment_text = (item.attachment_text or "").strip()
    text = "\n".join(part for part in (subject, snippet, body_text, attachment_text) if part).strip()
    lowered = text.lower()
    sender_lower = item.from_address.lower()

    sender_looks_school = any(hint in sender_lower for hint in SCHOOL_SENDER_HINTS)
    logistics_hits = count_hint_hits(lowered, LOGISTICS_HINTS)
    ambiguity_hits = count_hint_hits(lowered, AMBIGUITY_HINTS)
    all_day_hits = count_hint_hits(lowered, ALL_DAY_HINTS)
    date_match = parse_explicit_date(text, time_zone, now=now or item.received_at)
    time_range = parse_time_range(text)
    single_time = None if time_range else parse_single_time(text)
    has_scheduling_evidence = bool(date_match or time_range or single_time)

    looks_relevant = (
        (logistics_hits > 0 or has_scheduling_evidence)
        if sender_looks_school
        else (logistics_hits > 0 and has_scheduling_evidence)
    )
    if not looks_relevant:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="not_school_logistics")

    proposed_fields: dict[str, object] = {"title": subject}
    reasons: list[str] = []
    if sender_looks_school:
        reasons.append("school_sender")
    if logistics_hits > 0:
        reasons.append("logistics_keywords")
    if date_match:
        reasons.append("date")
    if time_range or single_time:
        reasons.append("time")
    if all_day_hits > 0:
        reasons.append("all_day_hint")

    confidence_bps = 4_500
    confidence_bps += 2_000 if sender_looks_school else 500
    confidence_bps += min(logistics_hits, 2) * 1_000
    confidence_bps += 1_000 if date_match else 0
    confidence_bps += 1_000 if time_range else 700 if single_time else 0

    requires_confirmation = False
    confirmation_question: str | None = None

    if ambiguity_hits > 0:
        requires_confirmation = True
        confirmation_question = f"The schedule for {subject} looks conditional. Which date or time applies?"
        reasons.append("ambiguous_schedule")

    if date_match and time_range:
        start = zoned_datetime_to_utc(date_match.value, time_range.start.hours, time_range.start.minutes, time_zone)
        end = zoned_datetime_to_utc(date_match.value, time_range.end.hours, time_range.end.minutes, time_zone)
        proposed_fields.update(
            timezone=time_zone,
            starts_at=start.isoformat(),
            ends_at=end.isoformat(),
            all_day=False,
        )
    elif date_match and single_time:
        start = zoned_datetime_to_utc(date_match.value, single_time.hours, single_time.minutes, time_zone)
        end = start + timedelta(hours=1)
        proposed_fields.update(
            timezone=time_zone,
            starts_at=start.isoformat(),
            ends_at=end.isoformat(),
            all_day=False,
        )
    elif date_match and all_day_hits > 0:
        start = zoned_datetime_to_utc(date_match.value, 0, 0, time_zone)
        end = zoned_datetime_to_utc(add_days(date_match.value, 1), 0, 0, time_zone)
        proposed_fields.update(
            timezone=time_zone,
            starts_at=start.isoformat(),
            ends_at=end.isoformat(),
            all_day=True,
        )
    elif not date_match:
        requires_confirmation = True
        confirmation_question = confirmation_question or f"What day should I put {subject} on the Florence family calendar?"
        confidence_bps -= 1_500
    else:
        requires_confirmation = True
        confirmation_question = confirmation_question or f"What time should I put for {subject}?"
        confidence_bps -= 700

    if requires_confirmation:
        confidence_bps = max(confidence_bps, 6_500)

    return CandidateDecision(
        kind=CandidateDecisionKind.CANDIDATE,
        title=subject,
        proposed_fields=proposed_fields,
        confidence_bps=clamp_confidence_bps(confidence_bps, minimum=5_000),
        requires_confirmation=requires_confirmation,
        confirmation_question=confirmation_question,
        should_auto_handoff=confidence_bps >= 6_500,
        raw_metadata={
            "classifier": "gmail_heuristics_v1",
            "classification_reasons": reasons,
            "sender_looks_school": sender_looks_school,
            "logistics_hits": logistics_hits,
            "ambiguity_hits": ambiguity_hits,
            "attachment_count": item.attachment_count,
        },
    )


def build_parent_calendar_candidate_decision(item: ParentCalendarSyncItem) -> CandidateDecision:
    title = _cleanup_gmail_title(item.title) or "Untitled calendar event"
    description = (item.description or "").strip()
    location = (item.location or "").strip()
    lowered = f"{title} {description} {location}".lower()

    logistics_hits = count_hint_hits(lowered, LOGISTICS_HINTS)
    activity_hits = count_hint_hits(lowered, CHILD_ACTIVITY_HINTS)
    personal_hits = count_hint_hits(lowered, PERSONAL_CALENDAR_HINTS)
    child_name_hits = sum(1 for name in item.family_member_names if name.strip() and name.strip().lower() in lowered)
    likely_child_logistics = logistics_hits > 0 or activity_hits > 0 or child_name_hits > 0

    if not likely_child_logistics:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="not_child_or_family_logistics")

    if personal_hits > logistics_hits and child_name_hits == 0:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="looks_personal_not_family")

    summary_bits = [
        item.calendar_summary or "Parent calendar",
        item.starts_at.isoformat(),
        description or None,
    ]
    summary = compact_text(" · ".join(bit for bit in summary_bits if bit), 300)
    confidence_bps = min(8_900, 5_600 + logistics_hits * 500 + activity_hits * 450 + child_name_hits * 700)

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
        should_auto_handoff=child_name_hits > 0 or activity_hits > 0 or logistics_hits >= 2,
        raw_metadata={
            "classifier": "parent_calendar_heuristics_v1",
            "logistics_hits": logistics_hits,
            "activity_hits": activity_hits,
            "personal_hits": personal_hits,
            "child_name_hits": child_name_hits,
            "calendar_event_id": item.google_event_id,
            "calendar_summary": item.calendar_summary,
            "html_link": item.html_link,
        },
    )
