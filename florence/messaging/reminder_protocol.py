"""Reminder and Google-done protocol for Florence DM ingress."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Callable

from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import FlorenceProtocolReply


def _looks_like_google_done_prompt(text: str) -> bool:
    lowered = " ".join(text.split()).lower()
    return "reply done" in lowered and any(
        token in lowered
        for token in (
            "google",
            "connect",
            "connected",
            "link",
            "gmail",
            "calendar",
            "email",
        )
    )


def _looks_like_reminder_feedback(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:too many reminders|fewer reminders|less proactive|more proactive|more reminders|too early|too late|nudge me less|nudge me more|stop pinging so much)\b",
            text,
            re.IGNORECASE,
        )
    )


def _looks_like_done_for_reminder(text: str) -> bool:
    return bool(
        re.search(
            r"^(?:done|handled|completed|finished|got it|took care of it)\b",
            text.strip(),
            re.IGNORECASE,
        )
    )


def _looks_like_snooze_request(text: str) -> bool:
    lowered = text.lower()
    return "snooze" in lowered or "remind me later" in lowered or "later" == lowered.strip()


def _parse_snooze_deadline(text: str, *, now: datetime | None = None) -> datetime:
    base = now or datetime.now(timezone.utc)
    match = re.search(r"\b(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)\b", text, re.IGNORECASE)
    if match:
        quantity = max(1, int(match.group(1)))
        unit = match.group(2).lower()
        if unit.startswith("m"):
            return base + timedelta(minutes=quantity)
        if unit.startswith("h"):
            return base + timedelta(hours=quantity)
        return base + timedelta(days=quantity)
    lowered = text.lower()
    if "tomorrow morning" in lowered:
        target = (base + timedelta(days=1)).astimezone(timezone.utc)
        return datetime(target.year, target.month, target.day, 14, 0, tzinfo=timezone.utc)
    if "tomorrow" in lowered:
        return base + timedelta(days=1)
    if "tonight" in lowered:
        return base + timedelta(hours=4)
    return base + timedelta(hours=2)


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
        if _looks_like_reminder_feedback(text):
            self.household_manager_service.record_reminder_feedback(
                household_id=household_id,
                feedback_text=text,
                member_id=member_id,
                channel_id=channel_id,
            )
            return FlorenceProtocolReply(
                reply_text=(
                    "Understood. I updated your reminder style and will adjust future nudges accordingly. "
                    "You can ask me to show reminders anytime."
                ),
                consumed=True,
            )

        google_done_result = self._handle_google_done_followup(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            text=text,
            latest_assistant_message_body=self.channel_log.latest_assistant_message_body(channel_id=channel_id),
            respond_with_household_chat=respond_with_household_chat,
        )
        if google_done_result is not None:
            return google_done_result

        if _looks_like_done_for_reminder(text):
            reminder_reply = self.household_manager_service.complete_actionable_nudge(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel_id,
            )
            if reminder_reply is None:
                return None
            return FlorenceProtocolReply(
                reply_text=reminder_reply.reply_text,
                consumed=True,
            )

        if _looks_like_snooze_request(text):
            now = datetime.now(timezone.utc)
            snooze_until = _parse_snooze_deadline(text, now=now).astimezone(timezone.utc)
            reminder_reply = self.household_manager_service.snooze_actionable_nudge(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel_id,
                scheduled_for=snooze_until,
                now=now,
            )
            if reminder_reply is None:
                return None
            return FlorenceProtocolReply(
                reply_text=reminder_reply.reply_text,
                consumed=True,
            )

        return None

    def _handle_google_done_followup(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        text: str,
        latest_assistant_message_body: str | None,
        respond_with_household_chat: Callable[[str], FlorenceProtocolReply | None],
    ) -> FlorenceProtocolReply | None:
        if not (
            _looks_like_done_for_reminder(text)
            and latest_assistant_message_body is not None
            and _looks_like_google_done_prompt(latest_assistant_message_body)
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
