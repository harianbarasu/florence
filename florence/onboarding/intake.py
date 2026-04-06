"""LLM-backed onboarding intake for Florence."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

from agent.auxiliary_client import call_llm, extract_content_or_reasoning
from florence.onboarding.parsing import extract_child_names, split_entries, split_labels
from florence.onboarding.state import OnboardingStage, OnboardingState

logger = logging.getLogger(__name__)


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
    word_count = len(normalized.split())
    if stage == OnboardingStage.COLLECT_CHILD_AGE:
        if any(char.isdigit() for char in normalized):
            return True
        if any(marker in lowered for marker in ("year old", "years old", "turning", "turns", "month old", "months old")):
            return True
        return word_count <= 3
    if stage == OnboardingStage.COLLECT_CHILD_SCHOOL:
        if lowered in {"not yet", "n/a", "na"}:
            return True
        return word_count <= 8
    if stage == OnboardingStage.COLLECT_CHILD_ACTIVITIES:
        if lowered.startswith("none"):
            return True
        return word_count <= 12
    return True


def _configured_api_key() -> str | None:
    raw = (
        os.getenv("FLORENCE_ONBOARDING_OPENAI_API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
    )
    if not raw:
        return None
    if raw == "no-key-required":
        return None
    return raw


class FlorenceOnboardingIntakeService:
    """Interpret freeform onboarding replies into structured state updates."""

    def __init__(self, *, model: str = "gpt-4o-mini"):
        self.model = model

    def parse(self, *, state: OnboardingState, text: str) -> OnboardingIntakeResult:
        parsed = self._parse_with_llm(state=state, text=text)
        if parsed is not None:
            return parsed
        return self._fallback_parse(state=state, text=text)

    def _parse_with_llm(self, *, state: OnboardingState, text: str) -> OnboardingIntakeResult | None:
        current_child = state.current_child_name or ""
        api_key = _configured_api_key()
        if api_key is None:
            return None
        try:
            response = call_llm(
                task="onboarding",
                provider="custom",
                model=self.model,
                base_url=os.getenv("FLORENCE_ONBOARDING_OPENAI_BASE_URL", "https://api.openai.com/v1").strip(),
                api_key=api_key,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Extract structured onboarding facts from the user's iMessage reply for Florence. "
                            "Return JSON only with keys: parent_name, google_connected, child_names, child_updates, ignore_message. "
                            "child_updates must be a list of objects with keys: name, age, school, activities. "
                            "Use current_child_name when the user answers a child question without repeating the name. "
                            "If the user says none for activities, return an empty list for activities. "
                            "If the message is only acknowledgement or reaction with no new facts, set ignore_message=true. "
                            "Only return facts that are explicit or very high confidence."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "stage": state.stage.value,
                                "current_child_name": current_child,
                                "known_child_names": state.child_names,
                                "google_connected_already": state.google_connected,
                                "message_text": text,
                            }
                        ),
                    },
                ],
                temperature=0,
                max_tokens=400,
            )
        except Exception:
            logger.exception("Florence onboarding intake LLM failed")
            return None

        content = extract_content_or_reasoning(response)
        if not content:
            return None
        try:
            data = json.loads(content)
        except Exception:
            logger.warning("Florence onboarding intake returned invalid JSON: %s", content[:200])
            return None
        return self._coerce_result(data)

    def _coerce_result(self, data: object) -> OnboardingIntakeResult:
        if not isinstance(data, dict):
            return OnboardingIntakeResult()
        child_names = _clean_list(data.get("child_names")) or []
        child_updates: list[dict[str, Any]] = []
        raw_updates = data.get("child_updates")
        if isinstance(raw_updates, list):
            for raw in raw_updates:
                if not isinstance(raw, dict):
                    continue
                name = _clean_text(raw.get("name"))
                age = _clean_text(raw.get("age"))
                school = _clean_text(raw.get("school"))
                activities = _clean_list(raw.get("activities"))
                if not any(value is not None for value in (name, age, school, activities)):
                    continue
                update: dict[str, Any] = {}
                if name is not None:
                    update["name"] = name
                if age is not None:
                    update["age"] = age
                if school is not None:
                    update["school"] = school
                if activities is not None:
                    update["activities"] = activities
                child_updates.append(update)
        return OnboardingIntakeResult(
            parent_name=_clean_text(data.get("parent_name")),
            google_connected=bool(data.get("google_connected")),
            child_names=child_names,
            child_updates=child_updates,
            ignore_message=bool(data.get("ignore_message")),
        )

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
