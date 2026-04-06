"""Private-DM to household-group promotion protocol for Florence ingress."""

from __future__ import annotations

import re

from florence.messaging.protocol_types import FlorenceProtocolReply


def _looks_like_group_share_request(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:share|send|post)\b.*\b(?:group|family|parent group|everyone)\b|^(?:share|send it|post it)\b",
            text.strip(),
            re.IGNORECASE,
        )
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
        if not _looks_like_group_share_request(text):
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
