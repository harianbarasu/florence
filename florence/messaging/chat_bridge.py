"""Bridge between Florence messaging protocols and Hermes household chat."""

from __future__ import annotations

import logging

from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.onboarding import build_google_connected_syncing_message_sequence
from florence.runtime.chat import (
    FlorenceHouseholdChatService,
    _GROUP_INTRO_NO_ACTION_SENTINEL,
    _GROUP_INTRO_SHOW_SENTINEL,
    _GROUP_SHARE_EXECUTE_SENTINEL,
    _GROUP_SHARE_NO_ACTION_SENTINEL,
    _ONBOARDING_CONTEXTUAL_CHAT_SENTINEL,
    _ONBOARDING_NO_REPLY_SENTINEL,
    _ONBOARDING_SYNC_WAITING_SENTINEL,
    _REVIEW_NO_ACTION_SENTINEL,
    _REVIEW_SHOW_PROMPT_SENTINEL,
)

logger = logging.getLogger(__name__)

_GROUP_INTRO_REPLY_TEXT = (
    "I’m in. This group is the main household thread, so ask me what matters this week, what changed on the kids' schedule, or what we should handle next. Use DM when something should stay private."
)


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
        if self._looks_like_protocol_sentinel(reply.text):
            logger.warning(
                "Suppressing leaked Florence protocol sentinel in normal chat reply household_id=%s channel_id=%s text=%s",
                household_id,
                channel_id,
                reply.text.strip(),
            )
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

    def handle_group_intro_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        user_message: str,
    ) -> FlorenceProtocolReply | None:
        reply = self.household_chat_service.compose_operator_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=member_id,
            kind="group_intro_turn",
            payload={"user_message": user_message},
            conversation_history=self.channel_log.conversation_history(channel_id=channel_id),
        )
        if reply is None or not reply.strip():
            return None
        normalized = reply.strip()
        if normalized == _GROUP_INTRO_SHOW_SENTINEL:
            return FlorenceProtocolReply(reply_text=_GROUP_INTRO_REPLY_TEXT, consumed=True)
        if self._looks_like_agent_failure(normalized):
            logger.warning(
                "Ignoring group_intro_turn operator failure reply and falling back to normal group chat"
            )
        return None

    def handle_setup_onboarding_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        payload: dict[str, object],
    ) -> FlorenceProtocolReply | None:
        if hasattr(self.household_chat_service, "compose_onboarding_turn"):
            reply_messages = self.household_chat_service.compose_onboarding_turn(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=member_id,
                payload=payload,
                conversation_history=self.channel_log.conversation_history(channel_id=channel_id),
            )
        else:
            reply = self.household_chat_service.compose_operator_message(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=member_id,
                kind="onboarding_turn",
                payload=payload,
                conversation_history=self.channel_log.conversation_history(channel_id=channel_id),
            )
            reply_messages = (reply.strip(),) if reply is not None and reply.strip() else None
        if not reply_messages:
            return None
        normalized = tuple(message.strip() for message in reply_messages if message and message.strip())
        if not normalized:
            return None
        special_outcome = self._handle_setup_onboarding_special_outcome(
            household_id=household_id,
            channel_id=channel_id,
            member_id=member_id,
            payload=payload,
            outcome=normalized[0],
        )
        if special_outcome is not None:
            return special_outcome
        if self._looks_like_agent_failure(normalized[0]):
            logger.warning(
                "Ignoring onboarding_turn operator failure reply and falling back to setup fallback handling"
            )
            return None
        return FlorenceProtocolReply(
            reply_text=normalized[0],
            reply_messages=normalized,
            consumed=True,
        )

    def _handle_setup_onboarding_special_outcome(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        payload: dict[str, object],
        outcome: str,
    ) -> FlorenceProtocolReply | None:
        user_message = str(payload.get("user_message") or "").strip()
        if outcome == _ONBOARDING_NO_REPLY_SENTINEL:
            return FlorenceProtocolReply(consumed=True)
        if outcome == _ONBOARDING_SYNC_WAITING_SENTINEL:
            return self.handle_setup_sync_waiting_turn(
                household_id=household_id,
                channel_id=channel_id,
                member_id=member_id,
                user_message=user_message,
                data_dependent=True,
            )
        if outcome == _ONBOARDING_CONTEXTUAL_CHAT_SENTINEL:
            return self.handle_setup_sync_waiting_turn(
                household_id=household_id,
                channel_id=channel_id,
                member_id=member_id,
                user_message=user_message,
                data_dependent=False,
            )
        return None

    def _compose_sync_waiting_reply(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        user_message: str,
        data_dependent: bool,
    ) -> str | None:
        reply = self.household_chat_service.compose_operator_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            kind="sync_waiting",
            payload={
                "user_message": user_message,
                "data_dependent": data_dependent,
            },
            conversation_history=self.channel_log.conversation_history(channel_id=channel_id),
        )
        if reply is not None and reply.strip():
            normalized = reply.strip()
            if not self._looks_like_agent_failure(normalized):
                return normalized
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

    @staticmethod
    def _looks_like_agent_failure(reply_text: str) -> bool:
        normalized = " ".join(reply_text.split()).strip().lower()
        return normalized.startswith("api call failed after ")

    @staticmethod
    def _looks_like_protocol_sentinel(reply_text: str) -> bool:
        normalized = str(reply_text or "").strip()
        return normalized in {
            _ONBOARDING_SYNC_WAITING_SENTINEL,
            _ONBOARDING_CONTEXTUAL_CHAT_SENTINEL,
            _ONBOARDING_NO_REPLY_SENTINEL,
            _REVIEW_SHOW_PROMPT_SENTINEL,
            _REVIEW_NO_ACTION_SENTINEL,
            _GROUP_SHARE_EXECUTE_SENTINEL,
            _GROUP_SHARE_NO_ACTION_SENTINEL,
            _GROUP_INTRO_SHOW_SENTINEL,
            _GROUP_INTRO_NO_ACTION_SENTINEL,
        }
