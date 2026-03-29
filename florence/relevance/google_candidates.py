"""Gmail and Calendar candidate scoring for Florence."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from enum import StrEnum
from typing import Any

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
    add_days,
    parse_explicit_date,
    parse_single_time,
    parse_time_range,
    zoned_datetime_to_utc,
)


logger = logging.getLogger(__name__)


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


def _response_output_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    if isinstance(response, dict):
        candidate = response.get("output_text")
        if isinstance(candidate, str):
            return candidate.strip()
    return ""


def _gmail_relevance_model() -> str:
    return os.getenv("FLORENCE_GMAIL_RELEVANCE_MODEL", "gpt-5.4").strip() or "gpt-5.4"


def _gmail_relevance_fast_model() -> str:
    return os.getenv("FLORENCE_GMAIL_RELEVANCE_FAST_MODEL", "gpt-5.4-mini").strip() or "gpt-5.4-mini"


def _gmail_relevance_client():
    if os.getenv("FLORENCE_GMAIL_RELEVANCE_DISABLE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return None
    api_key = (
        os.getenv("FLORENCE_GMAIL_RELEVANCE_OPENAI_API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
    )
    if not api_key:
        return None
    base_url = os.getenv("FLORENCE_GMAIL_RELEVANCE_OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
    from openai import OpenAI

    return OpenAI(api_key=api_key, base_url=base_url)


def _json_load_object(raw: str) -> dict[str, Any] | None:
    normalized = raw.strip()
    if normalized.startswith("```"):
        normalized = re.sub(r"^```(?:json)?\s*", "", normalized)
        normalized = re.sub(r"\s*```$", "", normalized)
    try:
        payload = json.loads(normalized)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_llm_datetime(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _normalize_llm_proposed_fields(value: Any, *, default_title: str) -> dict[str, object]:
    normalized: dict[str, object] = {"title": default_title}
    if not isinstance(value, dict):
        return normalized

    candidate_title = _cleanup_gmail_title(str(value.get("title") or "").strip()) or default_title
    normalized["title"] = candidate_title

    starts_at = _normalize_llm_datetime(value.get("starts_at"))
    if starts_at:
        normalized["starts_at"] = starts_at
    ends_at = _normalize_llm_datetime(value.get("ends_at"))
    if ends_at:
        normalized["ends_at"] = ends_at

    timezone_value = str(value.get("timezone") or "").strip()
    if timezone_value:
        normalized["timezone"] = timezone_value

    if "all_day" in value:
        normalized["all_day"] = bool(value.get("all_day"))

    location = str(value.get("location") or "").strip()
    if location:
        normalized["location"] = location

    description = compact_text(str(value.get("description") or "").strip(), max_length=800)
    if description:
        normalized["description"] = description

    return normalized


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _gmail_relevance_prompt() -> str:
    return (
        "You classify Gmail messages for Florence, a family house manager. "
        "Decide whether this email is relevant household logistics that Florence should surface for parent review. "
        "Be strict about skipping promotions, newsletters, podcasts, shopping, marketing, and generic news unless they are clearly tied to the family's known context. "
        "Known children, schools, activities, contacts, platforms, and locations are strong evidence. "
        "If the email is relevant but the date or time is ambiguous or incomplete, still return candidate=true with requires_confirmation=true. "
        "Return JSON only with keys: kind, reason, title, summary, confidence_bps, requires_confirmation, confirmation_question, signals, proposed_fields. "
        "kind must be candidate or skip. reason must be a short snake_case label. "
        "signals must be a short array of evidence tags like known_contact, school_domain, schedule_change, no_class, family_day, promotion. "
        "proposed_fields must be an object or null. Allowed proposed field keys are title, starts_at, ends_at, timezone, all_day, location, description. "
        "If you emit starts_at or ends_at, use ISO 8601 with timezone; UTC is preferred. Do not invent facts."
    )


def _gmail_relevance_payload(
    item: GmailSyncItem,
    *,
    time_zone: str,
    context: HouseholdContext | None,
    now: datetime | None,
) -> dict[str, object]:
    subject = _cleanup_gmail_title(item.subject) or "Untitled Gmail candidate"
    return {
        "time_zone": time_zone,
        "now_utc": (now or item.received_at).isoformat() if (now or item.received_at) is not None else None,
        "household_context": {
            "children": list(context.visible_child_names) if context is not None else [],
            "child_aliases": list(context.child_aliases) if context is not None else [],
            "schools": list(context.school_labels) if context is not None else [],
            "school_domains": list(context.school_domains) if context is not None else [],
            "school_platforms": list(context.school_platforms) if context is not None else [],
            "activities": list(context.activity_labels) if context is not None else [],
            "contacts": list(context.contact_names) if context is not None else [],
            "locations": list(context.location_labels) if context is not None else [],
        },
        "email": {
            "from_address": item.from_address,
            "subject": subject,
            "snippet": compact_text(item.snippet or "", max_length=500),
            "body_text": compact_text(item.body_text or "", max_length=3_000),
            "attachment_text": compact_text(item.attachment_text or "", max_length=2_500),
            "attachment_count": item.attachment_count,
            "received_at": item.received_at.isoformat() if item.received_at is not None else None,
        },
    }


def _decision_from_llm_payload(
    *,
    parsed: dict[str, Any],
    subject: str,
    time_zone: str,
    classifier: str,
    model: str,
) -> CandidateDecision | None:
    kind_raw = str(parsed.get("kind") or "").strip().lower()
    if kind_raw not in {CandidateDecisionKind.CANDIDATE.value, CandidateDecisionKind.SKIP.value}:
        return None

    reason = str(parsed.get("reason") or "").strip() or ("llm_candidate" if kind_raw == "candidate" else "llm_skip")
    confidence_value = _coerce_int(parsed.get("confidence_bps"))
    confidence_bps = clamp_confidence_bps(confidence_value, minimum=1_000) if confidence_value is not None else None
    signals = parsed.get("signals")
    signal_list = [str(signal).strip() for signal in signals if str(signal).strip()] if isinstance(signals, list) else []

    if kind_raw == CandidateDecisionKind.SKIP.value:
        return CandidateDecision(
            kind=CandidateDecisionKind.SKIP,
            reason=reason,
            confidence_bps=confidence_bps,
            raw_metadata={
                "classifier": classifier,
                "model": model,
                "signals": signal_list,
            },
        )

    title = _cleanup_gmail_title(str(parsed.get("title") or "").strip()) or subject
    summary = compact_text(str(parsed.get("summary") or "").strip(), max_length=300) or None
    requires_confirmation = bool(parsed.get("requires_confirmation"))
    confirmation_question = compact_text(str(parsed.get("confirmation_question") or "").strip(), max_length=220) or None
    proposed_fields = _normalize_llm_proposed_fields(parsed.get("proposed_fields"), default_title=title)
    if "timezone" not in proposed_fields and (proposed_fields.get("starts_at") or proposed_fields.get("ends_at")):
        proposed_fields["timezone"] = time_zone
    if requires_confirmation and not confirmation_question:
        confirmation_question = f"Should I add {title} to your household plan?"

    return CandidateDecision(
        kind=CandidateDecisionKind.CANDIDATE,
        title=title,
        summary=summary,
        proposed_fields=proposed_fields,
        confidence_bps=clamp_confidence_bps(confidence_bps or 7_000, minimum=5_000),
        requires_confirmation=requires_confirmation,
        confirmation_question=confirmation_question,
        should_auto_handoff=(confidence_bps or 7_000) >= 6_500,
        reason=reason,
        raw_metadata={
            "classifier": classifier,
            "model": model,
            "signals": signal_list,
        },
    )


def _run_gmail_candidate_decision_llm(
    item: GmailSyncItem,
    *,
    model: str,
    classifier: str,
    context: HouseholdContext | None = None,
    time_zone: str,
    now: datetime | None = None,
) -> CandidateDecision | None:
    client = _gmail_relevance_client()
    if client is None:
        return None

    subject = _cleanup_gmail_title(item.subject) or "Untitled Gmail candidate"
    payload = _gmail_relevance_payload(item, time_zone=time_zone, context=context, now=now)

    try:
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": _gmail_relevance_prompt()}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": json.dumps(payload, ensure_ascii=True)}],
                },
            ],
            max_output_tokens=900,
        )
    except Exception:
        logger.exception("Gmail relevance LLM call failed for %s using %s", item.gmail_message_id, model)
        return None

    parsed = _json_load_object(_response_output_text(response))
    if parsed is None:
        logger.warning("Gmail relevance LLM returned non-JSON for %s using %s", item.gmail_message_id, model)
        return None
    decision = _decision_from_llm_payload(
        parsed=parsed,
        subject=subject,
        time_zone=time_zone,
        classifier=classifier,
        model=model,
    )
    if decision is None:
        logger.warning("Gmail relevance LLM returned invalid payload for %s using %s", item.gmail_message_id, model)
    return decision


def _should_escalate_gmail_llm_decision(decision: CandidateDecision) -> bool:
    if decision.kind == CandidateDecisionKind.CANDIDATE:
        return True
    if decision.requires_confirmation:
        return True
    confidence = decision.confidence_bps or 0
    signals = [
        str(signal).strip().lower()
        for signal in decision.raw_metadata.get("signals", [])
        if str(signal).strip()
    ]
    if confidence < 6_500:
        return True
    if confidence >= 8_500:
        return False
    interesting_signal_tokens = (
        "known_",
        "school",
        "activity",
        "camp",
        "class",
        "schedule",
        "pickup",
        "dropoff",
        "family_day",
        "no_class",
        "cancel",
    )
    return any(any(token in signal for token in interesting_signal_tokens) for signal in signals)


def _build_gmail_candidate_decision_llm(
    item: GmailSyncItem,
    time_zone: str,
    *,
    context: HouseholdContext | None = None,
    now: datetime | None = None,
) -> CandidateDecision | None:
    fast_model = _gmail_relevance_fast_model()
    fast_decision = _run_gmail_candidate_decision_llm(
        item,
        model=fast_model,
        classifier="gmail_llm_fast_v1",
        context=context,
        time_zone=time_zone,
        now=now,
    )
    if fast_decision is None:
        return None

    deep_model = _gmail_relevance_model()
    if deep_model == fast_model or not _should_escalate_gmail_llm_decision(fast_decision):
        return fast_decision

    deep_decision = _run_gmail_candidate_decision_llm(
        item,
        model=deep_model,
        classifier="gmail_llm_deep_v1",
        context=context,
        time_zone=time_zone,
        now=now,
    )
    if deep_decision is None:
        return fast_decision
    deep_decision.raw_metadata["triage_model"] = fast_model
    deep_decision.raw_metadata["triage_classifier"] = fast_decision.raw_metadata.get("classifier")
    deep_decision.raw_metadata["triage_reason"] = fast_decision.reason
    deep_decision.raw_metadata["triage_confidence_bps"] = fast_decision.confidence_bps
    return deep_decision


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

    sender_looks_school = any(hint in sender_lower for hint in SCHOOL_SENDER_HINTS)
    sender_looks_school = sender_looks_school or school_domain_hits > 0 or platform_hits > 0 or known_school_hits > 0
    logistics_hits = count_hint_hits(lowered, LOGISTICS_HINTS)
    ambiguity_hits = count_hint_hits(lowered, AMBIGUITY_HINTS)
    all_day_hits = count_hint_hits(lowered, ALL_DAY_HINTS)
    promotional_hits = count_hint_hits(lowered_source, PROMOTIONAL_HINTS)
    date_match = parse_explicit_date(text, time_zone, now=now or item.received_at)
    time_range = parse_time_range(text)
    single_time = None if time_range else parse_single_time(text)
    has_scheduling_evidence = bool(date_match or time_range or single_time)
    context_signal_hits = (
        school_domain_hits
        + platform_hits
        + known_school_hits
        + known_activity_hits
        + known_child_hits
        + known_contact_hits
        + known_location_hits
    )
    strong_household_anchor_hits = (
        school_domain_hits
        + platform_hits
        + known_school_hits
        + known_activity_hits
        + known_child_hits
        + known_contact_hits
        + known_location_hits
    )

    has_household_anchor = context_signal_hits > 0
    if promotional_hits > 0 and strong_household_anchor_hits == 0:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="promotional_noise")
    if sender_looks_school:
        looks_relevant = bool(
            has_scheduling_evidence
            or (logistics_hits > 0 and strong_household_anchor_hits > 0)
            or strong_household_anchor_hits >= 2
            or (known_contact_hits > 0 and (logistics_hits > 0 or has_scheduling_evidence))
        )
    else:
        looks_relevant = bool(
            has_household_anchor
            and (
                has_scheduling_evidence
                or logistics_hits > 0
                or (
                    known_contact_hits > 0
                    and (known_child_hits > 0 or known_activity_hits > 0 or known_school_hits > 0)
                )
            )
        )
    if not looks_relevant:
        return CandidateDecision(kind=CandidateDecisionKind.SKIP, reason="not_school_logistics")

    proposed_fields: dict[str, object] = {"title": subject}
    reasons: list[str] = []
    if sender_looks_school:
        reasons.append("school_sender")
    if school_domain_hits > 0:
        reasons.append("known_school_domain")
    if platform_hits > 0:
        reasons.append("known_school_platform")
    if known_school_hits > 0:
        reasons.append("known_school_label")
    if known_activity_hits > 0:
        reasons.append("known_activity")
    if known_child_hits > 0:
        reasons.append("known_child")
    if known_contact_hits > 0:
        reasons.append("known_contact")
    if known_location_hits > 0:
        reasons.append("known_location")
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
    confidence_bps += school_domain_hits * 1_200
    confidence_bps += platform_hits * 600
    confidence_bps += min(known_school_hits, 1) * 900
    confidence_bps += min(known_activity_hits, 2) * 700
    confidence_bps += min(known_child_hits, 2) * 600
    confidence_bps += min(known_contact_hits, 1) * 500
    confidence_bps += min(known_location_hits, 1) * 300
    confidence_bps -= min(promotional_hits, 2) * 600

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
            "school_domain_hits": school_domain_hits,
            "platform_hits": platform_hits,
            "known_school_hits": known_school_hits,
            "known_activity_hits": known_activity_hits,
            "known_child_hits": known_child_hits,
            "known_contact_hits": known_contact_hits,
            "known_location_hits": known_location_hits,
            "context_signal_hits": context_signal_hits,
            "logistics_hits": logistics_hits,
            "ambiguity_hits": ambiguity_hits,
            "attachment_count": item.attachment_count,
            "sender_domain": sender_domain or None,
            "promotional_hits": promotional_hits,
        },
    )


def build_gmail_candidate_decision(
    item: GmailSyncItem,
    time_zone: str,
    *,
    context: HouseholdContext | None = None,
    now: datetime | None = None,
) -> CandidateDecision:
    llm_decision = _build_gmail_candidate_decision_llm(
        item,
        time_zone,
        context=context,
        now=now,
    )
    if llm_decision is not None:
        return llm_decision
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
        should_auto_handoff=child_name_hits > 0 or activity_hits > 0 or logistics_hits >= 2,
        raw_metadata={
            "classifier": "parent_calendar_heuristics_v1",
            "logistics_hits": logistics_hits,
            "activity_hits": activity_hits,
            "personal_hits": personal_hits,
            "child_name_hits": child_name_hits,
            "known_activity_hits": known_activity_hits,
            "known_child_hits": known_child_hits,
            "known_school_hits": known_school_hits,
            "known_location_hits": known_location_hits,
            "known_contact_hits": known_contact_hits,
            "calendar_event_id": item.google_event_id,
            "calendar_summary": item.calendar_summary,
            "html_link": item.html_link,
        },
    )
