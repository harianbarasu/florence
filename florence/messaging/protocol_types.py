"""Shared reply types for Florence messaging protocols."""

from __future__ import annotations

from dataclasses import dataclass, field


CANDIDATE_REVIEW_PROMPT_KIND = "candidate_review_prompt"


@dataclass(slots=True)
class FlorenceProtocolReply:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = ()
    reply_metadata: dict[str, object] = field(default_factory=dict)
    group_announcement: str | None = None
    consumed: bool = False
