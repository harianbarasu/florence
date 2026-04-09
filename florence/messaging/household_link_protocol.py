"""Pending household-link confirmation protocol for Florence DM ingress."""

from __future__ import annotations

from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import (
    HOUSEHOLD_LINK_PROMPT_KIND,
    HOUSEHOLD_LINK_PROMPT_ROLE_KEY,
    PENDING_ACTION_TARGET_ID_KEY,
    build_household_link_prompt_metadata,
    FlorenceProtocolReply,
)


_YES_RESPONSES = {
    "yes",
    "y",
    "yeah",
    "yep",
    "sure",
    "ok",
    "okay",
    "sounds good",
    "do it",
}
_NO_RESPONSES = {
    "no",
    "n",
    "nope",
    "nah",
    "not now",
    "don't",
    "do not",
}


def _normalize_confirmation(text: str) -> str | None:
    normalized = " ".join(text.strip().lower().split())
    if not normalized:
        return None
    if normalized in _YES_RESPONSES:
        return "yes"
    if normalized in _NO_RESPONSES:
        return "no"
    return None


class FlorenceHouseholdLinkProtocol:
    """Consent/confirmation protocol for linking two parents into one household."""

    def __init__(
        self,
        *,
        channel_log: FlorenceChannelLog,
        household_link_service,
    ) -> None:
        self.channel_log = channel_log
        self.household_link_service = household_link_service

    def handle_turn(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        sender_handle: str,
        text: str,
    ) -> FlorenceProtocolReply | None:
        inviter_request = self.household_link_service.find_pending_request_for_inviting_member(
            household_id=household_id,
            inviting_member_id=member_id,
        )
        if inviter_request is not None:
            return self._handle_inviting_confirmation(
                channel_id=channel_id,
                member_id=member_id,
                request=inviter_request,
                text=text,
            )

        invited_request = self.household_link_service.find_active_phone_link_request(
            invited_phone=sender_handle,
        )
        if invited_request is not None:
            return self._handle_invited_confirmation(
                channel_id=channel_id,
                member_id=member_id,
                request=invited_request,
                text=text,
            )
        return None

    def _handle_invited_confirmation(
        self,
        *,
        channel_id: str,
        member_id: str,
        request,
        text: str,
    ) -> FlorenceProtocolReply:
        decision = _normalize_confirmation(text)
        metadata = dict(request.metadata) if isinstance(request.metadata, dict) else {}
        invite_was_sent = bool(str(metadata.get("invited_message_sent_at") or "").strip())
        armed_request_id = self._armed_request_id(channel_id=channel_id, role="invited")
        if decision is None or (armed_request_id != request.id and not invite_was_sent):
            inviter = self.household_link_service.store.get_member(request.inviting_member_id)
            return FlorenceProtocolReply(
                reply_text=self.household_link_service.build_invited_confirmation_prompt(
                    request,
                    inviting_member_name=inviter.display_name if inviter is not None else None,
                ),
                reply_metadata=build_household_link_prompt_metadata(request.id, role="invited"),
                consumed=True,
            )
        if decision == "yes":
            result = self.household_link_service.accept_from_invited(
                request_id=request.id,
                invited_member_id=member_id,
            )
            return FlorenceProtocolReply(reply_text=result.reply_text, consumed=True)
        result = self.household_link_service.decline_from_invited(
            request_id=request.id,
            invited_member_id=member_id,
        )
        return FlorenceProtocolReply(reply_text=result.reply_text, consumed=True)

    def _handle_inviting_confirmation(
        self,
        *,
        channel_id: str,
        member_id: str,
        request,
        text: str,
    ) -> FlorenceProtocolReply:
        decision = _normalize_confirmation(text)
        if self._armed_request_id(channel_id=channel_id, role="inviting") != request.id or decision is None:
            return FlorenceProtocolReply(
                reply_text=self.household_link_service.build_inviting_confirmation_prompt(request),
                reply_metadata=build_household_link_prompt_metadata(request.id, role="inviting"),
                consumed=True,
            )
        if decision == "yes":
            result = self.household_link_service.accept_from_inviting_member(
                request_id=request.id,
                inviting_member_id=member_id,
            )
            return FlorenceProtocolReply(reply_text=result.reply_text, consumed=True)
        result = self.household_link_service.cancel_from_inviting_member(
            request_id=request.id,
            inviting_member_id=member_id,
        )
        return FlorenceProtocolReply(reply_text=result.reply_text, consumed=True)

    def _armed_request_id(self, *, channel_id: str, role: str) -> str | None:
        latest_assistant = self.channel_log.latest_assistant_message(channel_id=channel_id, limit=8)
        if latest_assistant is None:
            return None
        if latest_assistant.metadata.get("protocol_kind") != HOUSEHOLD_LINK_PROMPT_KIND:
            return None
        if str(latest_assistant.metadata.get(HOUSEHOLD_LINK_PROMPT_ROLE_KEY) or "").strip() != role:
            return None
        request_id = str(latest_assistant.metadata.get(PENDING_ACTION_TARGET_ID_KEY) or "").strip()
        return request_id or None
