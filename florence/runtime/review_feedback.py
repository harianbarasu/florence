"""Deterministic feedback parsing for surfaced Florence review prompts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum


class ReviewFeedbackKind(StrEnum):
    IGNORE_SOURCE = "ignore_source"
    IGNORE_ITEM_TYPE = "ignore_item_type"
    ALREADY_HANDLED = "already_handled"
    STALE = "stale"
    WRONG_DETAILS = "wrong_details"
    DUPLICATE = "duplicate"
    TOO_NOISY = "too_noisy"
    WRONG_TIMING = "wrong_timing"
    PRIVATE_ONLY = "private_only"
    ALWAYS_SHARE = "always_share"
    ALWAYS_SURFACE = "always_surface"
    LESS_PROACTIVE = "less_proactive"
    MORE_PROACTIVE = "more_proactive"
    DISABLE_MODULE = "disable_module"


@dataclass(frozen=True, slots=True)
class ParsedReviewFeedback:
    kind: ReviewFeedbackKind
    target_index: int | None = None
    raw_text: str = ""
    module_hint: str | None = None


_INDEXED_REPLY_RE = re.compile(r"^\s*(?P<index>\d{1,2})[\).:\-\s]+(?P<body>.+?)\s*$")
_MODULE_DISABLE_RE = re.compile(
    r"\b(?:disable|turn\s+off|stop|skip|no)\s+"
    r"(?P<module>morning\s+brief|pickup\s+check|school\s+triage|evening\s+check(?:-?in)?|"
    r"weekend\s+preview|weekly\s+brief|meal(?:s)?|shopping|grocery|briefs?)\b"
)


def parse_review_feedback(text: str | None) -> ParsedReviewFeedback | None:
    """Parse obvious parent feedback for a surfaced review item.

    This intentionally stays conservative. Ambiguous corrections such as
    "yes, but it starts at 3:30" still flow to Hermes with review context.
    """

    raw_text = " ".join(str(text or "").split()).strip()
    if not raw_text:
        return None
    target_index = None
    body = raw_text
    index_match = _INDEXED_REPLY_RE.match(raw_text)
    if index_match is not None:
        try:
            target_index = int(index_match.group("index"))
            body = index_match.group("body").strip()
        except ValueError:
            target_index = None
    normalized = _normalize_feedback_text(body)
    if not normalized:
        return None

    module_match = _MODULE_DISABLE_RE.search(normalized)
    if module_match is not None:
        return ParsedReviewFeedback(
            kind=ReviewFeedbackKind.DISABLE_MODULE,
            target_index=target_index,
            raw_text=raw_text,
            module_hint=module_match.group("module"),
        )

    if _has_any(
        normalized,
        (
            "ignore sender",
            "ignore this sender",
            "ignore source",
            "ignore this source",
            "ignore these",
            "ignore those",
            "dont show those",
            "dont show these",
            "do not show those",
            "do not show these",
            "dont send those",
            "do not send those",
            "never show those",
            "never show these",
            "stop showing those",
            "stop showing these",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.IGNORE_SOURCE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "ignore this type",
            "ignore this kind",
            "ignore these types",
            "ignore these kinds",
            "dont show this type",
            "dont show this kind",
            "do not show this type",
            "do not show this kind",
            "stop surfacing this type",
            "stop flagging this type",
            "never flag this type",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.IGNORE_ITEM_TYPE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "already handled",
            "handled already",
            "already done",
            "done already",
            "taken care of",
            "already took care",
            "i handled it",
            "i already handled",
            "we handled it",
            "we already handled",
            "already responded",
            "responded already",
            "i already responded",
            "read and responded",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.ALREADY_HANDLED, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "too late",
            "stale",
            "already passed",
            "already happened",
            "in the past",
            "past already",
            "missed it",
            "not relevant anymore",
            "no longer relevant",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.STALE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "wrong date",
            "wrong time",
            "wrong details",
            "details are wrong",
            "date is wrong",
            "time is wrong",
            "not the right date",
            "not the right time",
            "incorrect date",
            "incorrect time",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.WRONG_DETAILS, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "duplicate",
            "same thing",
            "same item",
            "already surfaced",
            "you already showed",
            "you already flagged",
            "dont show duplicates",
            "do not show duplicates",
            "collapse duplicates",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.DUPLICATE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "too noisy",
            "noise",
            "low value",
            "not useful",
            "not worth surfacing",
            "not worth a text",
            "dont flag this unless important",
            "do not flag this unless important",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.TOO_NOISY, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "wrong timing",
            "bad timing",
            "tell me sooner",
            "too late in the day",
            "send this earlier",
            "morning is better",
            "morning of is better",
            "night before is better",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.WRONG_TIMING, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "private only",
            "keep private",
            "keep it private",
            "just for me",
            "my thread only",
            "dont share",
            "do not share",
            "not shared",
        ),
    ) or normalized == "private":
        return ParsedReviewFeedback(ReviewFeedbackKind.PRIVATE_ONLY, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "always share",
            "share this source",
            "share this sender",
            "make this shared",
            "shared household",
            "family context",
            "always include",
        ),
    ) or normalized in {"share", "shared"}:
        return ParsedReviewFeedback(ReviewFeedbackKind.ALWAYS_SHARE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "this matters",
            "these matter",
            "always surface",
            "always flag this",
            "always flag these",
            "always tell me",
            "make sure you flag",
            "dont miss these",
            "do not miss these",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.ALWAYS_SURFACE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "less proactive",
            "too proactive",
            "too many texts",
            "too many messages",
            "too wordy",
            "too verbose",
            "shorter updates",
            "keep updates shorter",
            "less wordy",
            "fewer texts",
            "fewer messages",
            "quiet down",
            "stop nudging",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.LESS_PROACTIVE, target_index=target_index, raw_text=raw_text)

    if _has_any(
        normalized,
        (
            "more proactive",
            "tell me sooner",
            "flag these sooner",
            "send more",
            "more reminders",
        ),
    ):
        return ParsedReviewFeedback(ReviewFeedbackKind.MORE_PROACTIVE, target_index=target_index, raw_text=raw_text)

    return None


def _normalize_feedback_text(text: str) -> str:
    normalized = text.lower().replace("’", "'")
    normalized = normalized.replace("don't", "dont").replace("can't", "cant")
    normalized = re.sub(r"[^a-z0-9@._+\-\s]", " ", normalized)
    return " ".join(normalized.split()).strip()


def _has_any(text: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)
