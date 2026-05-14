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


def _gmail_profile_from_address(from_address: str) -> CandidateSourceProfile | None:
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
    )


def _account_matcher(
    *,
    email: str | None,
    matcher_kind: HouseholdSourceMatcherKind,
) -> SourceMatcherSpec | None:
    clean_email = _normalize_match_value(email)
    if clean_email is None or "@" not in clean_email:
        return None
    return SourceMatcherSpec(
        matcher_kind=matcher_kind,
        matcher_value=clean_email,
        label=clean_email,
    )


def _with_account_matcher(
    profile: CandidateSourceProfile | None,
    *,
    email: str | None,
    matcher_kind: HouseholdSourceMatcherKind,
) -> CandidateSourceProfile | None:
    account_matcher = _account_matcher(email=email, matcher_kind=matcher_kind)
    if profile is None:
        if account_matcher is None:
            return None
        source_kind = (
            GoogleSourceKind.GMAIL
            if matcher_kind == HouseholdSourceMatcherKind.GMAIL_CONNECTED_ACCOUNT
            else GoogleSourceKind.GOOGLE_CALENDAR
        )
        return CandidateSourceProfile(
            source_kind=source_kind,
            label=account_matcher.label,
            matchers=(account_matcher,),
        )
    if account_matcher is None:
        return profile
    return CandidateSourceProfile(
        source_kind=profile.source_kind,
        label=profile.label,
        matchers=(*profile.matchers, account_matcher),
    )


def _calendar_profile(summary: str | None) -> CandidateSourceProfile | None:
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
    )


def build_candidate_source_profile(candidate: ImportedCandidate) -> CandidateSourceProfile | None:
    if candidate.source_kind == GoogleSourceKind.GMAIL:
        return _with_account_matcher(
            _gmail_profile_from_address(str(candidate.metadata.get("from_address") or "")),
            email=str(candidate.metadata.get("connected_email") or ""),
            matcher_kind=HouseholdSourceMatcherKind.GMAIL_CONNECTED_ACCOUNT,
        )
    if candidate.source_kind == GoogleSourceKind.GOOGLE_CALENDAR:
        return _with_account_matcher(
            _calendar_profile(str(candidate.metadata.get("calendar_summary") or "")),
            email=str(candidate.metadata.get("connected_email") or ""),
            matcher_kind=HouseholdSourceMatcherKind.GOOGLE_CALENDAR_CONNECTED_ACCOUNT,
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
        if matcher.matcher_kind in {
            HouseholdSourceMatcherKind.GMAIL_CONNECTED_ACCOUNT,
            HouseholdSourceMatcherKind.GOOGLE_CALENDAR_CONNECTED_ACCOUNT,
        }:
            continue
        if visibility in {HouseholdSourceVisibility.PRIVATE, HouseholdSourceVisibility.IGNORED} and matcher.matcher_kind in {
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


def build_account_source_rule(
    *,
    household_id: str,
    source_kind: GoogleSourceKind,
    email: str,
    visibility: HouseholdSourceVisibility,
    created_by_member_id: str | None = None,
    label: str | None = None,
    metadata: dict[str, object] | None = None,
) -> HouseholdSourceRule | None:
    clean_email = _normalize_match_value(email)
    if clean_email is None or "@" not in clean_email:
        return None
    matcher_kind = (
        HouseholdSourceMatcherKind.GMAIL_CONNECTED_ACCOUNT
        if source_kind == GoogleSourceKind.GMAIL
        else HouseholdSourceMatcherKind.GOOGLE_CALENDAR_CONNECTED_ACCOUNT
    )
    return HouseholdSourceRule(
        id=_source_rule_id(household_id, source_kind, matcher_kind, clean_email),
        household_id=household_id,
        source_kind=source_kind,
        matcher_kind=matcher_kind,
        matcher_value=clean_email,
        visibility=visibility,
        label=label or clean_email,
        created_by_member_id=created_by_member_id,
        metadata={
            "source_label": label or clean_email,
            "policy_scope": "connected_google_account",
            **(metadata or {}),
        },
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


def request_matches_shared_calendar_rule(
    rules: list[HouseholdSourceRule],
    *,
    query: str | None,
    calendar_summary: str | None,
) -> bool:
    haystack = " ".join(part for part in (calendar_summary, query) if part).strip().lower()
    if not haystack:
        return False
    for rule in rules:
        if rule.source_kind != GoogleSourceKind.GOOGLE_CALENDAR or rule.visibility != HouseholdSourceVisibility.SHARED:
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
