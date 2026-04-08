"""Private-DM to household-group promotion protocol for Florence ingress."""

from __future__ import annotations

from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.runtime.chat import _GROUP_SHARE_EXECUTE_SENTINEL

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
