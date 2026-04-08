"""Deterministic onboarding intake for Florence."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from florence.onboarding.parsing import extract_child_names, split_entries, split_labels
from florence.onboarding.state import OnboardingStage, OnboardingState


@dataclass(slots=True)
class OnboardingIntakeResult:
    parent_name: str | None = None
    google_connected: bool = False
    child_names: list[str] = field(default_factory=list)
    child_updates: list[dict[str, Any]] = field(default_factory=list)
    ignore_message: bool = False


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split()).strip()
    return cleaned or None


def _clean_list(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        return None
    values: list[str] = []
    for item in value:
        cleaned = _clean_text(item)
        if cleaned:
            values.append(cleaned)
    return values


def _looks_like_google_connected(text: str) -> bool:
    return bool(re.search(r"\b(done|connected|finished|complete|i connected)\b", text, re.IGNORECASE))


def _looks_like_conversational_request(text: str) -> bool:
    normalized = " ".join(text.split()).strip()
    if not normalized:
        return False
    if "?" in normalized and len(normalized.split()) > 1:
        return True
    if re.search(
        r"\b(?:help figuring out|help with|can you help|could you help|i need help|while this is syncing)\b",
        normalized,
        re.IGNORECASE,
    ):
        return True
    return bool(
        re.search(
            r"^(?:can|could|would|what|when|where|who|why|how|show|check|find|plan|help|remind|review|list|share|send|post)\b",
            normalized,
            re.IGNORECASE,
        )
    )


def _looks_like_child_detail_reply(stage: OnboardingStage, text: str) -> bool:
    normalized = " ".join(text.split()).strip()
    if not normalized or _looks_like_conversational_request(normalized):
        return False
    lowered = normalized.lower()
    if stage == OnboardingStage.COLLECT_CHILD_AGE:
        if any(char.isdigit() for char in normalized):
            return True
        if any(marker in lowered for marker in ("year old", "years old", "turning", "turns", "month old", "months old")):
            return True
        return True
    if stage == OnboardingStage.COLLECT_CHILD_SCHOOL:
        return True
    if stage == OnboardingStage.COLLECT_CHILD_ACTIVITIES:
        return True
    return True


class FlorenceOnboardingIntakeService:
    """Interpret freeform onboarding replies into structured state updates."""

    def __init__(self, *, model: str = "gpt-4o-mini"):
        self.model = model

    def parse(self, *, state: OnboardingState, text: str) -> OnboardingIntakeResult:
        return self._fallback_parse(state=state, text=text)

    def _fallback_parse(self, *, state: OnboardingState, text: str) -> OnboardingIntakeResult:
        raw_text = str(text or "")
        cleaned = _clean_text(text) or ""
        if not cleaned:
            return OnboardingIntakeResult(ignore_message=True)
        if re.fullmatch(r"(?:ok|okay|got it|thanks|thank you|👍|🙏|cool|sounds good)", cleaned, re.IGNORECASE):
            return OnboardingIntakeResult(ignore_message=True)
        if state.stage == OnboardingStage.COLLECT_PARENT_NAME:
            return OnboardingIntakeResult(parent_name=cleaned)
        if state.stage == OnboardingStage.COLLECT_CHILD_NAMES:
            if _looks_like_conversational_request(cleaned):
                return OnboardingIntakeResult(
                    google_connected=_looks_like_google_connected(cleaned),
                    ignore_message=True,
                )
            return OnboardingIntakeResult(
                google_connected=_looks_like_google_connected(cleaned),
                child_names=extract_child_names(split_entries(raw_text)),
            )
        if state.stage in {
            OnboardingStage.COLLECT_CHILD_AGE,
            OnboardingStage.COLLECT_CHILD_SCHOOL,
            OnboardingStage.COLLECT_CHILD_ACTIVITIES,
        } and not _looks_like_child_detail_reply(state.stage, cleaned):
            return OnboardingIntakeResult(
                google_connected=_looks_like_google_connected(cleaned),
                ignore_message=True,
            )
        update: dict[str, Any] = {}
        if state.current_child_name:
            update["name"] = state.current_child_name
        if state.stage == OnboardingStage.COLLECT_CHILD_AGE:
            update["age"] = cleaned
        elif state.stage == OnboardingStage.COLLECT_CHILD_SCHOOL:
            update["school"] = cleaned
        elif state.stage == OnboardingStage.COLLECT_CHILD_ACTIVITIES:
            update["activities"] = split_labels(cleaned)
        return OnboardingIntakeResult(
            google_connected=_looks_like_google_connected(cleaned),
            child_updates=[update] if update else [],
        )
