"""Reminder and Google-done protocol for Florence DM ingress."""

from __future__ import annotations

import json
from typing import Callable

from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.pending_actions import latest_assistant_protocol_message, latest_pending_action
from florence.messaging.protocol_types import (
    GOOGLE_CONNECT_PROMPT_KIND,
    HOUSEHOLD_NUDGE_PROMPT_KIND,
    FlorenceProtocolReply,
    latest_active_pending_action,
    pending_action_to_model_context,
)


class FlorenceReminderProtocol:
    """Reminder and Google-done follow-up handling for DM turns."""

    def __init__(
        self,
        channel_log: FlorenceChannelLog,
        household_manager_service,
        onboarding_service,
    ) -> None:
        self.channel_log = channel_log
        self.household_manager_service = household_manager_service
        self.onboarding_service = onboarding_service

    def handle_turn(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        thread_id: str,
        text: str,
        respond_with_household_chat: Callable[[str], FlorenceProtocolReply | None],
    ) -> FlorenceProtocolReply | None:
        google_done_result = self._handle_google_done_followup(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            text=text,
            channel_id=channel_id,
            respond_with_household_chat=respond_with_household_chat,
        )
        if google_done_result is not None:
            return google_done_result

        return None

    def is_reply_armed(self, *, channel_id: str) -> bool:
        return self._active_nudge_id(channel_id=channel_id) is not None

    def build_chat_followup_context(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        text: str,
    ) -> str | None:
        active_nudge_id = self._active_nudge_id(channel_id=channel_id)
        if active_nudge_id is None:
            return None
        nudge = self.household_manager_service.store.get_household_nudge(active_nudge_id)
        if nudge is None or nudge.household_id != household_id:
            return None
        if nudge.recipient_member_id and nudge.recipient_member_id != member_id:
            return None
        active_action = latest_active_pending_action(
            self.channel_log.recent_messages(channel_id=channel_id, limit=8)
        )
        pending_action_context = (
            pending_action_to_model_context(active_action)
            if active_action is not None
            else None
        )
        return (
            "Context for this turn: there is one currently surfaced reminder/nudge in this DM.\n"
            "Only that reminder is actionable right now.\n"
            "Use household_apply_nudge_action with the exact nudge_id if the user is completing, snoozing, or otherwise updating it.\n"
            "When calling household_apply_nudge_action, include pending_action_id from Active pending action if present.\n"
            "Interpret the whole message yourself, including short done/snooze/later replies; Florence no longer resolves those deterministically here.\n"
            "Do not mutate reminder state for vague acknowledgements unless the user clearly means this exact reminder.\n"
            f"Active pending action: {json.dumps(pending_action_context, ensure_ascii=True)}\n"
            f"Active nudge: {self._render_nudge_context(nudge)}\n"
            f"User reply: {text}"
        )

    def _handle_google_done_followup(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        text: str,
        channel_id: str,
        respond_with_household_chat: Callable[[str], FlorenceProtocolReply | None],
    ) -> FlorenceProtocolReply | None:
        if not (
            " ".join(text.strip().lower().split()) == "done"
            and self._google_connect_reply_is_armed(channel_id=channel_id)
        ):
            return None

        onboarding_reply = self.onboarding_service.handle_google_done_followup(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            continue_with_household_chat=lambda message_text: self._continue_with_household_chat(
                message_text=message_text,
                respond_with_household_chat=respond_with_household_chat,
            ),
        )
        return FlorenceProtocolReply(
            reply_text=onboarding_reply.reply_text,
            reply_messages=onboarding_reply.reply_messages,
            consumed=True,
        )

    def _google_connect_reply_is_armed(self, *, channel_id: str) -> bool:
        return latest_assistant_protocol_message(
            self.channel_log,
            channel_id=channel_id,
            protocol_kind=GOOGLE_CONNECT_PROMPT_KIND,
        ) is not None

    @staticmethod
    def _continue_with_household_chat(
        *,
        message_text: str,
        respond_with_household_chat: Callable[[str], FlorenceProtocolReply | None],
    ) -> tuple[str | None, tuple[str, ...]] | None:
        chat_result = respond_with_household_chat(message_text)
        if chat_result is None:
            return None
        reply_messages = chat_result.reply_messages or ((chat_result.reply_text,) if chat_result.reply_text else ())
        return chat_result.reply_text, reply_messages

    def _active_nudge_id(self, *, channel_id: str) -> str | None:
        active = latest_pending_action(
            self.channel_log,
            channel_id=channel_id,
            action_type="household_nudge",
            target_kind="household_nudge",
        )
        if active is None:
            return None
        protocol_kind = str(active.metadata.get("protocol_kind") or "").strip()
        if protocol_kind and protocol_kind != HOUSEHOLD_NUDGE_PROMPT_KIND:
            return None
        nudge_id = str(active.action.target_id or "").strip()
        return nudge_id or None

    @staticmethod
    def _render_nudge_context(nudge) -> str:
        bits = [f'nudge_id="{nudge.id}"', f'status="{nudge.status.value}"']
        if nudge.message:
            bits.append(f'message="{nudge.message}"')
        bits.append(f'target_kind="{nudge.target_kind.value}"')
        if nudge.target_id:
            bits.append(f'target_id="{nudge.target_id}"')
        if nudge.scheduled_for:
            bits.append(f'scheduled_for="{nudge.scheduled_for}"')
        return "{ " + ", ".join(bits) + " }"
