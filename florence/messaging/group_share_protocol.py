"""Private-DM to household-group promotion protocol for Florence ingress."""

from __future__ import annotations

import re

from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.runtime.chat import _GROUP_SHARE_EXECUTE_SENTINEL


_SCHEDULE_FEED_PATTERN = re.compile(r"(?:\bwebcal://|\bhttps?://\S+\.ics(?:\?|$)|\b\S+\.ics(?:\?|$))", re.IGNORECASE)


def _looks_like_schedule_feed_link(text: str) -> bool:
    normalized = " ".join(str(text or "").split()).strip()
    if not normalized:
        return False
    return _SCHEDULE_FEED_PATTERN.search(normalized) is not None


def _looks_like_explicit_group_share_request(text: str) -> bool:
    normalized = " ".join(str(text or "").split()).strip().lower()
    if not normalized:
        return False
    if normalized in {"share that", "share it", "send it", "post it"}:
        return True
    return (
        re.search(r"\b(?:share|send|post)\b", normalized) is not None
        and any(phrase in normalized for phrase in ("group", "parent group", "family", "everyone", "parents"))
    )

class FlorenceGroupShareProtocol:
    """DM-only explicit promotion from a private parent thread into the household group."""

    def __init__(
        self,
        *,
        group_share_service,
    ) -> None:
        self.group_share_service = group_share_service

    def handle_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        current_provider: str,
        current_message_id: str,
        text: str,
    ) -> FlorenceProtocolReply | None:
        if _looks_like_schedule_feed_link(text) and not _looks_like_explicit_group_share_request(text):
            return None

        latest_assistant = self.group_share_service.channel_log.latest_assistant_message(channel_id=channel_id)
        decision = self.group_share_service.household_chat_service.compose_operator_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            kind="group_share_turn",
            payload={
                "user_message": text,
                "latest_assistant_protocol_kind": (
                    str((latest_assistant.metadata or {}).get("protocol_kind") or "").strip()
                    if latest_assistant is not None
                    else ""
                ),
            },
        )
        if decision != _GROUP_SHARE_EXECUTE_SENTINEL:
            return None

        share_result = self.group_share_service.handle_explicit_share_request(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            current_provider=current_provider,
            current_message_id=current_message_id,
        )
        if share_result is None:
            return None

        return FlorenceProtocolReply(
            reply_text=share_result.reply_text,
            group_announcement=share_result.group_announcement,
            consumed=True,
        )
