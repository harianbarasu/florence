"""Household-group lane routing for Florence messaging ingress."""

from __future__ import annotations

from florence.contracts import ChannelMessageRole
from florence.messaging.chat_bridge import FlorenceHouseholdChatBridge
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.ingress_types import (
    FlorenceMessagingIngressResult,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.state import FlorenceStateDB


class FlorenceGroupRouter:
    """Route household group turns through a Hermes intro decision and normal chat."""

    def __init__(
        self,
        *,
        store: FlorenceStateDB,
        channel_log: FlorenceChannelLog,
        chat_bridge: FlorenceHouseholdChatBridge,
    ) -> None:
        self.store = store
        self.channel_log = channel_log
        self.chat_bridge = chat_bridge

    def handle_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        if resolved.member_id is None:
            return FlorenceMessagingIngressResult(consumed=False)

        onboarding_sessions = self.store.list_member_onboarding_sessions(
            household_id=resolved.household_id,
            member_id=resolved.member_id,
        )
        latest = onboarding_sessions[0] if onboarding_sessions else None
        history = self.channel_log.recent_messages(channel_id=resolved.channel_id, limit=8)
        prior_assistant_messages = [message for message in history[:-1] if message.role == ChannelMessageRole.ASSISTANT]
        if (
            latest is not None
            and latest.is_complete
            and not prior_assistant_messages
        ):
            intro_result = self.chat_bridge.handle_group_intro_turn(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                member_id=resolved.member_id,
                user_message=resolved.message.body,
            )
            if intro_result is not None:
                return self._result_from_protocol_reply(intro_result)

        chat_result = self.chat_bridge.respond_as_protocol(
            household_id=resolved.household_id,
            channel_id=resolved.channel_id,
            actor_member_id=resolved.member_id,
            message_text=resolved.message.body,
        )
        if chat_result is not None:
            return self._result_from_protocol_reply(chat_result)
        return FlorenceMessagingIngressResult(consumed=False)

    @staticmethod
    def _result_from_protocol_reply(reply: FlorenceProtocolReply) -> FlorenceMessagingIngressResult:
        return FlorenceMessagingIngressResult(
            reply_text=reply.reply_text,
            reply_messages=reply.reply_messages,
            group_announcement=reply.group_announcement,
            consumed=reply.consumed,
        )
