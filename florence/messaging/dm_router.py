"""DM lane routing for Florence messaging ingress."""

from __future__ import annotations

import logging

from florence.messaging.chat_bridge import FlorenceHouseholdChatBridge
from florence.messaging.ingress_types import (
    FlorenceMessagingIngressResult,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.protocol_types import FlorenceProtocolReply

logger = logging.getLogger(__name__)


def _require_member_id(member_id: str | None) -> str:
    if member_id is None or not member_id.strip():
        raise ValueError("member_id_required_for_dm")
    return member_id


class FlorenceDmRouter:
    """Route parent DM turns through Florence protocol lanes before Hermes."""

    def __init__(
        self,
        *,
        onboarding_service,
        group_share_protocol,
        review_protocol,
        setup_protocol,
        reminder_protocol,
        chat_bridge: FlorenceHouseholdChatBridge,
    ) -> None:
        self.onboarding_service = onboarding_service
        self.group_share_protocol = group_share_protocol
        self.review_protocol = review_protocol
        self.setup_protocol = setup_protocol
        self.reminder_protocol = reminder_protocol
        self.chat_bridge = chat_bridge

    def handle_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        text = resolved.message.body.strip()
        if not text:
            return FlorenceMessagingIngressResult(consumed=True)
        member_id = _require_member_id(resolved.member_id)
        session = self.onboarding_service.get_or_create_session(
            household_id=resolved.household_id,
            member_id=member_id,
            thread_id=resolved.thread_id,
        )

        group_share_result = self.group_share_protocol.handle_turn(
            household_id=resolved.household_id,
            channel_id=resolved.channel_id,
            actor_member_id=resolved.member_id,
            current_provider=resolved.message.provider,
            current_message_id=resolved.message.message_id,
            text=text,
        )
        if group_share_result is not None:
            return self._result_from_protocol_reply(group_share_result)

        review_prompt, review_prompt_text = self.review_protocol.current_prompt(
            household_id=resolved.household_id,
            channel_id=resolved.channel_id,
            member_id=member_id,
            session=session,
        )
        if review_prompt is not None:
            review_result = self.review_protocol.handle_turn(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                member_id=member_id,
                text=text,
                prompt=review_prompt,
                rendered_prompt_text=review_prompt_text,
            )
            if review_result is not None:
                return self._result_from_protocol_reply(review_result)

        if not session.is_complete:
            return self._result_from_protocol_reply(self.setup_protocol.handle_incomplete_turn(
                household_id=resolved.household_id,
                member_id=member_id,
                channel_id=resolved.channel_id,
                thread_id=resolved.thread_id,
                session=session,
                text=text,
            ))

        reminder_result = self.reminder_protocol.handle_turn(
            household_id=resolved.household_id,
            member_id=member_id,
            channel_id=resolved.channel_id,
            thread_id=resolved.thread_id,
            text=text,
            respond_with_household_chat=lambda message_text: self.chat_bridge.respond_as_protocol(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                actor_member_id=resolved.member_id,
                message_text=message_text,
            ),
        )
        if reminder_result is not None:
            return self._result_from_protocol_reply(reminder_result)

        chat_result = self.chat_bridge.respond_as_protocol(
            household_id=resolved.household_id,
            channel_id=resolved.channel_id,
            actor_member_id=resolved.member_id,
            message_text=resolved.message.body,
        )
        if chat_result is not None:
            return self._result_from_protocol_reply(chat_result)
        logger.warning(
            "Household chat returned no reply for household_id=%s channel_id=%s",
            resolved.household_id,
            resolved.channel_id,
        )
        return FlorenceMessagingIngressResult(consumed=False)

    @staticmethod
    def _result_from_protocol_reply(reply: FlorenceProtocolReply) -> FlorenceMessagingIngressResult:
        return FlorenceMessagingIngressResult(
            reply_text=reply.reply_text,
            reply_messages=reply.reply_messages,
            reply_metadata=reply.reply_metadata,
            group_announcement=reply.group_announcement,
            consumed=reply.consumed,
        )
