"""Shared reply types for Florence messaging protocols."""

from __future__ import annotations

from dataclasses import dataclass, field


CANDIDATE_REVIEW_PROMPT_KIND = "candidate_review_prompt"
HOUSEHOLD_NUDGE_PROMPT_KIND = "household_nudge_prompt"
GOOGLE_CONNECT_PROMPT_KIND = "google_connect_prompt"
PENDING_ACTION_TYPE_KEY = "pending_action_type"
PENDING_ACTION_TARGET_ID_KEY = "pending_action_target_id"
PENDING_ACTION_TARGET_KIND_KEY = "pending_action_target_kind"


def build_candidate_review_prompt_metadata(candidate_id: str) -> dict[str, object]:
    return {
        "protocol_kind": CANDIDATE_REVIEW_PROMPT_KIND,
        PENDING_ACTION_TYPE_KEY: "candidate_review",
        PENDING_ACTION_TARGET_KIND_KEY: "imported_candidate",
        PENDING_ACTION_TARGET_ID_KEY: candidate_id,
    }


def build_household_nudge_metadata(nudge_id: str) -> dict[str, object]:
    return {
        "protocol_kind": HOUSEHOLD_NUDGE_PROMPT_KIND,
        PENDING_ACTION_TYPE_KEY: "household_nudge",
        PENDING_ACTION_TARGET_KIND_KEY: "household_nudge",
        PENDING_ACTION_TARGET_ID_KEY: nudge_id,
    }


def build_google_connect_prompt_metadata() -> dict[str, object]:
    return {
        "protocol_kind": GOOGLE_CONNECT_PROMPT_KIND,
        PENDING_ACTION_TYPE_KEY: "google_connect",
        PENDING_ACTION_TARGET_KIND_KEY: "google_connection",
    }


@dataclass(slots=True)
class FlorenceProtocolReply:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = ()
    reply_metadata: dict[str, object] = field(default_factory=dict)
    group_announcement: str | None = None
    consumed: bool = False
