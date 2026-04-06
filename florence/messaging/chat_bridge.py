"""Bridge between Florence messaging protocols and Hermes household chat."""

from __future__ import annotations

from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.onboarding import build_google_connected_syncing_message_sequence
from florence.runtime.chat import FlorenceHouseholdChatService


class FlorenceHouseholdChatBridge:
    """Adapt channel history and sync state into Hermes household chat calls."""

    def __init__(
        self,
        *,
        household_chat_service: FlorenceHouseholdChatService,
        channel_log: FlorenceChannelLog,
    ) -> None:
        self.household_chat_service = household_chat_service
        self.channel_log = channel_log

    def respond_as_protocol(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        history_limit: int = 24,
    ) -> FlorenceProtocolReply | None:
        history = self.channel_log.conversation_history(channel_id=channel_id, limit=history_limit)
        reply = self.household_chat_service.respond(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            message_text=message_text,
            conversation_history=history,
        )
        if reply is None or not reply.text.strip():
            return None
        return FlorenceProtocolReply(reply_text=reply.text, consumed=True)

    def handle_setup_sync_waiting_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        user_message: str,
        data_dependent: bool,
    ) -> FlorenceProtocolReply:
        if data_dependent:
            waiting_reply = self._compose_sync_waiting_reply(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=member_id,
                user_message=user_message,
                data_dependent=True,
            )
            if waiting_reply is not None:
                return FlorenceProtocolReply(reply_text=waiting_reply, consumed=True)

        chat_result = self.respond_as_protocol(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=member_id,
            message_text=self._sync_contextual_message(user_message),
        )
        if chat_result is not None:
            return chat_result
        messages = build_google_connected_syncing_message_sequence()
        return FlorenceProtocolReply(
            reply_text=messages[0] if messages else None,
            reply_messages=messages,
            consumed=True,
        )

    def _compose_sync_waiting_reply(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        user_message: str,
        data_dependent: bool,
    ) -> str | None:
        reply = self.household_chat_service.compose_sync_waiting_reply(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            conversation_history=self.channel_log.conversation_history(channel_id=channel_id),
            data_dependent=data_dependent,
        )
        if reply is not None and reply.strip():
            return reply.strip()
        return None

    @staticmethod
    def _sync_contextual_message(user_message: str) -> str:
        return (
            "Context for this turn: the first Gmail and Calendar sync is still running. "
            "If the user asks for sync status, answer briefly that it is still running and Florence will text when the first pass is ready. "
            "If the user asks for information that depends on synced inbox or calendar data, say that it is still syncing. "
            "If the user seems to be continuing onboarding, keep the answer concise so they can finish setup in this thread. "
            "For everything else, help normally.\n\n"
            f"User message: {user_message}"
        )
