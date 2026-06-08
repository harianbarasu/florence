"""Need-to-know filtering for connected household sources."""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from florence.models import SourceDecision, SourceItem, SourcePreference, SourcePreferenceKind, SourceTriage
from florence.timekeeper import ensure_utc


ACTION_KEYWORDS = {
    "permission slip",
    "deadline",
    "due",
    "rsvp",
    "register",
    "signup",
    "sign up",
    "early dismissal",
    "delayed opening",
    "delayed start",
    "late start",
    "no school",
    "school closed",
    "school closure",
    "pickup",
    "pick up",
    "dropoff",
    "drop off",
    "appointment",
    "conference",
    "concert",
    "practice",
    "game",
    "field trip",
    "bring",
    "pack",
    "payment",
    "pay",
}

HIGH_SIGNAL_NO_TIME_KEYWORDS = {
    "delayed opening",
    "delayed start",
    "early dismissal",
    "late start",
    "no school",
    "school closed",
    "school closure",
}

LOW_SIGNAL_KEYWORDS = {
    "newsletter",
    "weekly update",
    "promotion",
    "sale",
    "coupon",
    "digest",
    "recap",
    "spirit wear",
    "fundraiser",
}

EMOTIONAL_SUPPORT_KEYWORDS = {
    "overwhelmed",
    "drowning",
    "exhausted",
    "not great",
    "losing it",
}


class NeedToKnowPolicy:
    """Classify source items before they interrupt parents."""

    def __init__(
        self,
        *,
        stale_grace: timedelta = timedelta(hours=2),
        urgent_window: timedelta = timedelta(hours=36),
        planning_window: timedelta = timedelta(days=7),
        no_time_recency_window: timedelta | None = None,
    ) -> None:
        self.stale_grace = stale_grace
        self.urgent_window = urgent_window
        self.planning_window = planning_window
        self.no_time_recency_window = no_time_recency_window or urgent_window

    def classify(
        self,
        item: SourceItem,
        *,
        now_utc: datetime,
        preferences: list[SourcePreference] | None = None,
    ) -> SourceTriage:
        now = ensure_utc(now_utc)
        event_at = ensure_utc(item.event_at_utc) if item.event_at_utc else None
        observed_at = ensure_utc(item.observed_at_utc)
        text = f"{item.title}\n{item.body}\n{item.sender or ''}".lower()

        if event_at is not None and event_at < now - self.stale_grace:
            return SourceTriage(
                decision=SourceDecision.SUPPRESS,
                reason="event_is_in_the_past",
                priority=0,
            )

        preference = _matching_preference(text, preferences or [])
        requested_surface = False
        if preference is not None:
            if preference.preference == SourcePreferenceKind.MUTE:
                return SourceTriage(
                    decision=SourceDecision.STORE_ONLY,
                    reason="household_muted_source",
                    priority=1,
                )
            requested_surface = True

        action_score = _keyword_score(text, ACTION_KEYWORDS)
        low_signal_score = _keyword_score(text, LOW_SIGNAL_KEYWORDS)
        support_score = _keyword_score(text, EMOTIONAL_SUPPORT_KEYWORDS)
        high_signal_no_time_score = _keyword_score(text, HIGH_SIGNAL_NO_TIME_KEYWORDS)

        if event_at is None:
            if observed_at < now - self.no_time_recency_window:
                return SourceTriage(
                    decision=SourceDecision.STORE_ONLY,
                    reason="source_observed_too_old_without_due_time",
                    priority=max(0, action_score - low_signal_score),
                )
            if high_signal_no_time_score > 0:
                return SourceTriage(
                    decision=SourceDecision.SURFACE,
                    reason="high_signal_without_known_due_time",
                    priority=75 + high_signal_no_time_score,
                    suggested_title=_clean_title(item.title),
                )
            if action_score >= 2:
                return SourceTriage(
                    decision=SourceDecision.SURFACE,
                    reason="actionable_without_known_due_time",
                    priority=60 + action_score,
                    suggested_title=_clean_title(item.title),
                )
            if requested_surface:
                return SourceTriage(
                    decision=SourceDecision.SURFACE,
                    reason="household_requested_source",
                    priority=95,
                    suggested_title=_clean_title(item.title),
                )
            return SourceTriage(
                decision=SourceDecision.STORE_ONLY,
                reason="no_due_time_and_not_actionable",
                priority=max(0, action_score - low_signal_score),
            )

        delta = event_at - now
        if delta <= self.urgent_window and action_score > 0:
            return SourceTriage(
                decision=SourceDecision.SURFACE,
                reason="urgent_actionable_source",
                priority=90 + action_score + support_score,
                suggested_title=_clean_title(item.title),
                suggested_due_at_utc=event_at,
            )

        if delta <= self.planning_window and action_score >= 2:
            return SourceTriage(
                decision=SourceDecision.SURFACE,
                reason="upcoming_actionable_source",
                priority=70 + action_score,
                suggested_title=_clean_title(item.title),
                suggested_due_at_utc=event_at,
            )

        if requested_surface:
            return SourceTriage(
                decision=SourceDecision.SURFACE,
                reason="household_requested_source",
                priority=95,
                suggested_title=_clean_title(item.title),
                suggested_due_at_utc=event_at,
            )

        if low_signal_score > action_score:
            return SourceTriage(
                decision=SourceDecision.STORE_ONLY,
                reason="low_signal_source",
                priority=5,
            )

        return SourceTriage(
            decision=SourceDecision.STORE_ONLY,
            reason="useful_context_not_interrupt_worthy",
            priority=20 + action_score,
        )


def _keyword_score(text: str, keywords: set[str]) -> int:
    score = 0
    for keyword in keywords:
        if re.search(rf"\b{re.escape(keyword)}\b", text):
            score += 1
    return score


def _clean_title(title: str) -> str:
    compact = " ".join(title.strip().split())
    return compact[:120] if compact else "Household item"


def _matching_preference(text: str, preferences: list[SourcePreference]) -> SourcePreference | None:
    for preference in preferences:
        for phrase in _phrase_variants(preference.phrase):
            if phrase and re.search(rf"\b{re.escape(phrase)}\b", text):
                return preference
    return None


def _phrase_variants(phrase: str) -> set[str]:
    normalized = " ".join(phrase.strip().lower().split())
    variants = {normalized}
    if not normalized:
        return set()
    if normalized.endswith("ies") and len(normalized) > 4:
        variants.add(normalized[:-3] + "y")
    elif _can_singularize(normalized):
        variants.add(normalized[:-1])
    else:
        variants.add(normalized + "s")
        if len(normalized) > 2 and normalized.endswith("y") and normalized[-2] not in "aeiou":
            variants.add(normalized[:-1] + "ies")
    return variants


def _can_singularize(phrase: str) -> bool:
    if not phrase.endswith("s") or len(phrase) <= 3:
        return False
    return not phrase.endswith(("ss", "us", "is", "ics", "news"))
