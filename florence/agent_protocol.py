"""Structured proposal envelope for agent backends.

Hermes should reason freely, but Florence owns household state changes. This
module extracts a small, explicit proposal block from an agent reply so the
service can validate it before writing memory or creating pending actions.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from florence.models import MemoryKind, SourcePreferenceKind


FENCE_RE = re.compile(r"```(?:florence|florence-json)\s*(.*?)```", re.IGNORECASE | re.DOTALL)
LINE_RE = re.compile(r"^FLORENCE_PROPOSAL:\s*(\{.*\})\s*$", re.IGNORECASE | re.MULTILINE)
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
PHONE_LIKE_RE = re.compile(
    r"(?<!\w)(?:"
    r"\+\d[\d\s().-]{7,}\d"
    r"|(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}"
    r")(?!\w)"
)
MAX_PROPOSAL_PAYLOAD_CHARS = 4096
MAX_MEMORY_PROPOSAL_TEXT_CHARS = 240
MAX_MEMORY_PROPOSAL_SUBJECT_CHARS = 120


@dataclass(frozen=True, slots=True)
class AgentActionProposal:
    action_type: str
    summary: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class AgentMemoryProposal:
    kind: MemoryKind
    text: str
    subject: str | None
    confidence: float


@dataclass(frozen=True, slots=True)
class AgentSourcePreferenceProposal:
    preference: SourcePreferenceKind
    phrase: str


@dataclass(frozen=True, slots=True)
class AgentProposalBundle:
    reply_text: str
    actions: list[AgentActionProposal]
    memories: list[AgentMemoryProposal]
    source_preferences: list[AgentSourcePreferenceProposal]
    rejected_proposal_count: int = 0


def extract_agent_proposals(reply: str) -> AgentProposalBundle:
    payloads: list[str] = []

    def _collect_fence(match: re.Match[str]) -> str:
        payloads.append(match.group(1))
        return ""

    def _collect_line(match: re.Match[str]) -> str:
        payloads.append(match.group(1))
        return ""

    stripped = FENCE_RE.sub(_collect_fence, reply)
    stripped = LINE_RE.sub(_collect_line, stripped)
    stripped = "\n".join(line.rstrip() for line in stripped.splitlines()).strip()

    actions: list[AgentActionProposal] = []
    memories: list[AgentMemoryProposal] = []
    source_preferences: list[AgentSourcePreferenceProposal] = []
    rejected_proposal_count = 0
    if payloads:
        parsed = _json_object(payloads[0])
        if parsed is None:
            rejected_proposal_count += 1
            parsed = {}
        actions.extend(_action_proposals(parsed.get("actions")))
        parsed_memories, rejected_memories = _memory_proposals(parsed.get("memories"))
        parsed_source_preferences, rejected_source_preferences = _source_preference_proposals(
            parsed.get("source_preferences")
        )
        memories.extend(parsed_memories)
        source_preferences.extend(parsed_source_preferences)
        rejected_proposal_count += rejected_memories + rejected_source_preferences

    return AgentProposalBundle(
        reply_text=stripped,
        actions=actions[:3],
        memories=memories[:5],
        source_preferences=source_preferences[:5],
        rejected_proposal_count=rejected_proposal_count,
    )


def _json_object(value: str) -> dict[str, Any] | None:
    if len(value) > MAX_PROPOSAL_PAYLOAD_CHARS:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _action_proposals(value: object) -> list[AgentActionProposal]:
    if not isinstance(value, list):
        return []
    proposals: list[AgentActionProposal] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        action_type = str(item.get("type") or item.get("action_type") or "").strip()
        if not action_type:
            continue
        payload = item.get("payload")
        payload = dict(payload) if isinstance(payload, dict) else {}
        if action_type == "create_reminder":
            _copy_top_level(item, payload, "title")
            _copy_top_level(item, payload, "due_at_utc")
        summary = str(item.get("summary") or "").strip()
        proposals.append(
            AgentActionProposal(
                action_type=action_type,
                summary=summary,
                payload=payload,
            )
        )
    return proposals


def _memory_proposals(value: object) -> tuple[list[AgentMemoryProposal], int]:
    if not isinstance(value, list):
        return [], 0
    proposals: list[AgentMemoryProposal] = []
    rejected = 0
    for item in value:
        if not isinstance(item, dict):
            continue
        text = " ".join(str(item.get("text") or "").strip().split())
        if not text:
            rejected += 1
            continue
        if len(text) > MAX_MEMORY_PROPOSAL_TEXT_CHARS:
            rejected += 1
            continue
        if _contains_contact_detail(text):
            rejected += 1
            continue
        raw_kind = str(item.get("kind") or MemoryKind.FACT.value).strip().lower()
        try:
            kind = MemoryKind(raw_kind)
        except ValueError:
            kind = MemoryKind.FACT
        confidence = _confidence(item.get("confidence"))
        subject = item.get("subject")
        proposals.append(
            AgentMemoryProposal(
                kind=kind,
                text=text,
                subject=(
                    str(subject).strip()[:MAX_MEMORY_PROPOSAL_SUBJECT_CHARS]
                    if subject not in (None, "")
                    and not _contains_contact_detail(str(subject))
                    else None
                ),
                confidence=confidence,
            )
        )
    return proposals, rejected


def _source_preference_proposals(value: object) -> tuple[list[AgentSourcePreferenceProposal], int]:
    if not isinstance(value, list):
        return [], 0
    proposals: list[AgentSourcePreferenceProposal] = []
    rejected = 0
    for item in value:
        if not isinstance(item, dict):
            continue
        raw_preference = str(item.get("preference") or "").strip().lower()
        try:
            preference = SourcePreferenceKind(raw_preference)
        except ValueError:
            rejected += 1
            continue
        phrase = _preference_phrase(item.get("phrase"))
        if not phrase:
            rejected += 1
            continue
        if _contains_contact_detail(phrase):
            rejected += 1
            continue
        proposals.append(
            AgentSourcePreferenceProposal(
                preference=preference,
                phrase=phrase,
            )
        )
    return proposals, rejected


def _copy_top_level(source: dict[str, object], payload: dict[str, object], key: str) -> None:
    if key not in payload and source.get(key) not in (None, ""):
        payload[key] = source[key]


def _preference_phrase(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip(" .").lower().split())[:120]


def _contains_contact_detail(value: str) -> bool:
    lowered = value.lower()
    return (
        "[phone number]" in lowered
        or EMAIL_RE.search(value) is not None
        or PHONE_LIKE_RE.search(value) is not None
    )


def _confidence(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.6
    return max(0.1, min(parsed, 0.9))
