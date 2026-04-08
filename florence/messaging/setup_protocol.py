"""Onboarding and first-sync protocol for Florence DM ingress."""

from __future__ import annotations

import re
from typing import Callable

from florence.messaging.protocol_types import FlorenceProtocolReply


def _looks_like_acknowledgement(text: str) -> bool:
    normalized = " ".join(text.strip().lower().split())
    return normalized in {
        "ok",
        "okay",
        "sounds good",
        "sgtm",
        "got it",
        "cool",
        "nice",
        "great",
        "perfect",
        "thanks",
        "thank you",
        "awesome",
        "works for me",
        "understood",
        "roger",
        "👍",
        "🙏",
    }


def _looks_like_synced_data_request(text: str) -> bool:
    normalized = " ".join(text.split()).strip()
    if not normalized:
        return False
    return bool(
        re.search(
            r"\b(?:calendar|email|emails|inbox|gmail)\b|\b(?:check|show|find|pull|search|look\s+up|look\s+in|what(?:'s| is)\s+(?:on|in))\b.*\b(?:schedule|scheduled)\b",
            normalized,
            re.IGNORECASE,
        )
    )


class FlorenceSetupProtocol:
    """Onboarding and first-sync protocol handling before normal Hermes-first chat."""

    def __init__(
        self,
        *,
        onboarding_service,
        on_complete: Callable[[str, str, str], None],
        handle_sync_waiting_turn: Callable[[str, str, str, str, bool], FlorenceProtocolReply],
    ) -> None:
        self.onboarding_service = onboarding_service
        self.on_complete = on_complete
        self.handle_sync_waiting_turn = handle_sync_waiting_turn

    def handle_incomplete_turn(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        thread_id: str,
        session,
        text: str,
    ) -> FlorenceProtocolReply:
        onboarding_result = self._record_onboarding_reply(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel_id,
            thread_id=thread_id,
            previous_stage=session.stage,
            text=text,
        )
        if onboarding_result is not None:
            return onboarding_result
        if session.google_connected:
            if _looks_like_acknowledgement(text):
                return FlorenceProtocolReply(consumed=True)
            data_dependent = _looks_like_synced_data_request(text)
            return self.handle_sync_waiting_turn(
                household_id,
                channel_id,
                member_id,
                text,
                data_dependent,
            )
        return self._repeat_onboarding_prompt(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )

    def _record_onboarding_reply(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        thread_id: str,
        previous_stage,
        text: str,
    ) -> FlorenceProtocolReply | None:
        transition = self.onboarding_service.record_user_reply(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            text=text,
        )
        if not transition.changed:
            return None
        if transition.state.is_complete:
            self.on_complete(household_id, member_id, channel_id)
        return self._messages_reply(
            self.onboarding_service.get_transition_messages(
                transition,
                previous_stage=previous_stage,
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            )
        )

    def _repeat_onboarding_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> FlorenceProtocolReply:
        return self._messages_reply(self.onboarding_service.get_prompt_messages(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        ))

    @staticmethod
    def _messages_reply(messages: tuple[str, ...]) -> FlorenceProtocolReply:
        return FlorenceProtocolReply(
            reply_text=messages[0] if messages else None,
            reply_messages=messages,
            consumed=True,
        )
