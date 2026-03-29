"""Source-sharing helpers for Google-backed Florence household context."""

from __future__ import annotations

import re
from dataclasses import dataclass
from email.utils import parseaddr

from florence.contracts import (
    GoogleSourceKind,
    HouseholdSourceMatcherKind,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    ImportedCandidate,
)

_CONSUMER_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "aol.com",
}

_KNOWN_FAMILY_SIGNAL_FIELDS = (
    "school_domain_hits",
    "platform_hits",
    "known_school_hits",
    "known_activity_hits",
    "known_child_hits",
    "known_contact_hits",
    "known_location_hits",
    "child_name_hits",
)

_KNOWN_FAMILY_SIGNAL_TOKENS = (
    "known_school",
    "known_activity",
    "known_child",
    "known_contact",
    "known_location",
    "school_domain",
    "school_platform",
    "school_sender",
    "family_day",
    "no_class",
)


@dataclass(slots=True)
class SourceMatcherSpec:
    matcher_kind: HouseholdSourceMatcherKind
    matcher_value: str
    label: str


@dataclass(slots=True)
class CandidateSourceProfile:
    source_kind: GoogleSourceKind
    label: str
    matchers: tuple[SourceMatcherSpec, ...]
    default_shared: bool


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).split()).strip(" ,.;:-")
    return normalized or None


def _normalize_match_value(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    return cleaned.lower() if cleaned is not None else None


def _match_term(text: str, term: str) -> bool:
    normalized_text = f" {text.lower()} "
    normalized_term = term.lower().strip()
    if not normalized_term:
        return False
    if re.fullmatch(r"[a-z0-9]+", normalized_term):
        return re.search(rf"\b{re.escape(normalized_term)}\b", normalized_text) is not None
    return normalized_term in normalized_text


def _gmail_profile_from_address(from_address: str, *, default_shared: bool) -> CandidateSourceProfile | None:
    display_name, email = parseaddr(from_address)
    clean_name = _clean_text(display_name)
    clean_email = _normalize_match_value(email)
    if clean_name is None and clean_email is None:
        return None

    domain = clean_email.split("@", 1)[1] if clean_email and "@" in clean_email else None
    label_bits = [bit for bit in (clean_name, domain or clean_email) if bit]
    label = " / ".join(label_bits) or (clean_email or clean_name or "this source")

    matchers: list[SourceMatcherSpec] = []
    if clean_email:
        matchers.append(
            SourceMatcherSpec(
                matcher_kind=HouseholdSourceMatcherKind.GMAIL_FROM_ADDRESS,
                matcher_value=clean_email,
                label=clean_name or clean_email,
            )
        )
    if clean_name:
        matchers.append(
            SourceMatcherSpec(
                matcher_kind=HouseholdSourceMatcherKind.GMAIL_SENDER_NAME,
                matcher_value=clean_name.lower(),
                label=clean_name,
            )
        )
    if domain and domain not in _CONSUMER_EMAIL_DOMAINS:
        matchers.append(
            SourceMatcherSpec(
                matcher_kind=HouseholdSourceMatcherKind.GMAIL_FROM_DOMAIN,
                matcher_value=domain,
                label=domain,
            )
        )

    return CandidateSourceProfile(
        source_kind=GoogleSourceKind.GMAIL,
        label=label,
        matchers=tuple(matchers),
        default_shared=default_shared,
    )


def _calendar_profile(summary: str | None, *, default_shared: bool) -> CandidateSourceProfile | None:
    clean_summary = _clean_text(summary)
    if clean_summary is None:
        return None
    return CandidateSourceProfile(
        source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
        label=clean_summary,
        matchers=(
            SourceMatcherSpec(
                matcher_kind=HouseholdSourceMatcherKind.GOOGLE_CALENDAR_SUMMARY,
                matcher_value=clean_summary.lower(),
                label=clean_summary,
            ),
        ),
        default_shared=default_shared,
    )


def _candidate_has_known_family_signals(candidate: ImportedCandidate) -> bool:
    raw_metadata = candidate.metadata.get("raw_metadata")
    if isinstance(raw_metadata, dict):
        for field in _KNOWN_FAMILY_SIGNAL_FIELDS:
            value = raw_metadata.get(field)
            if isinstance(value, int) and value > 0:
                return True
        for key in ("signals", "classification_reasons"):
            signals = raw_metadata.get(key)
            if isinstance(signals, list):
                lowered = [str(signal).strip().lower() for signal in signals if str(signal).strip()]
                if any(any(token in signal for token in _KNOWN_FAMILY_SIGNAL_TOKENS) for signal in lowered):
                    return True
    if candidate.source_kind == GoogleSourceKind.GOOGLE_CALENDAR:
        summary = str(candidate.metadata.get("calendar_summary") or "").strip().lower()
        if "family" in summary:
            return True
    return False


def build_candidate_source_profile(candidate: ImportedCandidate) -> CandidateSourceProfile | None:
    default_shared = _candidate_has_known_family_signals(candidate)
    if candidate.source_kind == GoogleSourceKind.GMAIL:
        return _gmail_profile_from_address(
            str(candidate.metadata.get("from_address") or ""),
            default_shared=default_shared,
        )
    if candidate.source_kind == GoogleSourceKind.GOOGLE_CALENDAR:
        return _calendar_profile(
            str(candidate.metadata.get("calendar_summary") or ""),
            default_shared=default_shared,
        )
    return None


def candidate_matches_source_rule(candidate: ImportedCandidate, rule: HouseholdSourceRule) -> bool:
    profile = build_candidate_source_profile(candidate)
    if profile is None or profile.source_kind != rule.source_kind:
        return False
    for matcher in profile.matchers:
        if matcher.matcher_kind == rule.matcher_kind and matcher.matcher_value == rule.matcher_value:
            return True
    return False


def build_rules_for_candidate(
    candidate: ImportedCandidate,
    *,
    visibility: HouseholdSourceVisibility,
    created_by_member_id: str | None = None,
) -> tuple[HouseholdSourceRule, ...]:
    profile = build_candidate_source_profile(candidate)
    if profile is None:
        return ()

    rules: list[HouseholdSourceRule] = []
    for matcher in profile.matchers:
        if visibility == HouseholdSourceVisibility.PRIVATE and matcher.matcher_kind in {
            HouseholdSourceMatcherKind.GMAIL_FROM_DOMAIN,
            HouseholdSourceMatcherKind.GMAIL_SENDER_NAME,
        }:
            continue
        rules.append(
            HouseholdSourceRule(
                id=_source_rule_id(
                    candidate.household_id,
                    profile.source_kind,
                    matcher.matcher_kind,
                    matcher.matcher_value,
                ),
                household_id=candidate.household_id,
                source_kind=profile.source_kind,
                matcher_kind=matcher.matcher_kind,
                matcher_value=matcher.matcher_value,
                visibility=visibility,
                label=matcher.label,
                created_by_member_id=created_by_member_id,
                metadata={
                    "source_label": profile.label,
                    "created_from_candidate_id": candidate.id,
                },
            )
        )
    return tuple(rules)


def build_source_rule_prompt(candidate: ImportedCandidate) -> str | None:
    profile = build_candidate_source_profile(candidate)
    if profile is None:
        return None
    return (
        f"Future items from {profile.label} are not classified yet. "
        "Reply share to treat future items from this source as household-shared, "
        "or private to keep future items from this source private."
    )


def request_matches_shared_gmail_rule(
    rules: list[HouseholdSourceRule],
    *,
    sender: str | None,
    query: str | None,
    subject: str | None,
) -> bool:
    haystack = " ".join(part for part in (sender, query, subject) if part).strip().lower()
    if not haystack:
        return False
    for rule in rules:
        if rule.source_kind != GoogleSourceKind.GMAIL or rule.visibility != HouseholdSourceVisibility.SHARED:
            continue
        if _match_term(haystack, rule.matcher_value):
            return True
        if rule.label and _match_term(haystack, rule.label):
            return True
    return False


def _source_rule_id(
    household_id: str,
    source_kind: GoogleSourceKind,
    matcher_kind: HouseholdSourceMatcherKind,
    matcher_value: str,
) -> str:
    import hashlib

    raw = f"{household_id}:{source_kind.value}:{matcher_kind.value}:{matcher_value}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:20]
    return f"srcrule_{digest}"
