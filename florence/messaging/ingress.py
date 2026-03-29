"""Transport-agnostic Florence DM and household-group ingress."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone

from florence.contracts import (
    ChannelMessage,
    ChannelMessageRole,
    HouseholdSourceVisibility,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdWorkItemStatus,
)
from florence.messaging.types import FlorenceInboundMessage
from florence.onboarding import (
    OnboardingPrompt,
    OnboardingStage,
    build_onboarding_ready_message_sequence,
    build_google_connect_message_sequence,
    build_web_onboarding_handoff_sequence,
    extract_child_names,
    split_entries,
    split_labels,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.onboarding_links import FlorenceOnboardingLinkService
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    FlorenceGoogleAccountLinkService,
    FlorenceHouseholdManagerService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)
_REVIEW_CONFIRMATION_SUFFIX = "Reply yes to confirm it, no if it is wrong, or skip for later."


@dataclass(slots=True)
class FlorenceResolvedInboundMessage:
    household_id: str
    member_id: str | None
    channel_id: str
    thread_id: str
    message: FlorenceInboundMessage

    @property
    def is_group(self) -> bool:
        return self.message.is_group_chat


@dataclass(slots=True)
class FlorenceMessagingIngressResult:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = field(default_factory=tuple)
    group_announcement: str | None = None
    consumed: bool = False


def _looks_like_yes(text: str) -> bool:
    return bool(re.search(r"^(?:yes|yep|yeah|sure|confirm|add it|do it)\b", text.strip(), re.IGNORECASE))


def _looks_like_no(text: str) -> bool:
    return bool(re.search(r"^(?:no|nope|nah|reject|wrong)\b", text.strip(), re.IGNORECASE))


def _looks_like_skip(text: str) -> bool:
    return bool(re.search(r"^(?:skip|later|not now)\b", text.strip(), re.IGNORECASE))


def _looks_like_share_source(text: str) -> bool:
    return bool(re.search(r"\b(?:share|shared|always share|future share)\b", text.strip(), re.IGNORECASE))


def _looks_like_private_source(text: str) -> bool:
    return bool(re.search(r"\b(?:private|keep private|don't share|do not share)\b", text.strip(), re.IGNORECASE))


def _looks_like_review_request(text: str) -> bool:
    return bool(re.search(r"\b(review|imports?|gmail|calendar|candidates?)\b", text, re.IGNORECASE))


def _looks_like_candidate_review_prompt(text: str) -> bool:
    normalized = text.strip()
    return "Imported item:" in normalized and _REVIEW_CONFIRMATION_SUFFIX in normalized


def _looks_like_google_connected(text: str) -> bool:
    return bool(re.search(r"\b(done|connected|finished|complete|i connected)\b", text, re.IGNORECASE))


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


def _looks_like_schedule_question(text: str) -> bool:
    normalized = " ".join(text.split()).lower()
    if not normalized:
        return False
    if re.search(
        r"\b(hours?|open|close|best time|tickets?|parking|weather|cost|price|directions?)\b",
        normalized,
    ):
        return False
    if re.search(r"\b(family|kids?|household|calendar|schedule|plan|coming up|happening)\b", normalized):
        return True
    if re.search(r"\bwhat(?:'s| is| do)\s+(?:on|happening|coming up|scheduled)\b", normalized):
        return True
    if re.search(r"\b(today|tomorrow|this week|next week)\b", normalized) and re.search(r"\b(we|our)\b", normalized):
        return True
    return False


def _looks_like_tracking_request(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:what\s+are\s+you\s+tracking|what\s+are\s+you\s+managing|show\s+tracking|what\s+do\s+you\s+have)\b",
            text,
            re.IGNORECASE,
        )
    )


def _looks_like_reminder_list_request(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:show|list|what\s+are)\s+(?:my\s+)?(?:reminders|nudges|follow[- ]?ups)\b",
            text,
            re.IGNORECASE,
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


def _parse_optional_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _require_member_id(member_id: str | None) -> str:
    if member_id is None or not member_id.strip():
        raise ValueError("member_id_required_for_dm")
    return member_id


def _stable_transport_message_id(provider: str, message_id: str) -> str:
    digest = hashlib.sha256(f"{provider}:{message_id}".encode("utf-8")).hexdigest()[:20]
    return f"chatmsg_{digest}"


def _assistant_message_id(channel_id: str, body: str) -> str:
    digest = hashlib.sha256(f"{channel_id}:{body}:{time.time_ns()}".encode("utf-8")).hexdigest()[:20]
    return f"assistant_{digest}"


class FlorenceMessagingIngressService:
    """Routes normalized transport messages into Florence onboarding and chat flows."""

    def __init__(
        self,
        store: FlorenceStateDB,
        onboarding_service: FlorenceOnboardingSessionService,
        candidate_review_service: FlorenceCandidateReviewService,
        query_service: FlorenceHouseholdQueryService,
        *,
        google_account_link_service: FlorenceGoogleAccountLinkService | None = None,
        onboarding_link_service: FlorenceOnboardingLinkService | None = None,
        household_chat_service: FlorenceHouseholdChatService | None = None,
        household_manager_service: FlorenceHouseholdManagerService | None = None,
    ):
        self.store = store
        self.onboarding_service = onboarding_service
        self.candidate_review_service = candidate_review_service
        self.query_service = query_service
        self.google_account_link_service = google_account_link_service
        self.onboarding_link_service = onboarding_link_service
        self.household_chat_service = household_chat_service
        self.household_manager_service = household_manager_service or FlorenceHouseholdManagerService(store)

    def handle_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        if resolved.message.is_from_me:
            return FlorenceMessagingIngressResult(consumed=False)

        inbound_message_id = _stable_transport_message_id(resolved.message.provider, resolved.message.message_id)
        if self.store.get_channel_message(inbound_message_id) is not None:
            logger.info(
                "Ignoring duplicate inbound message provider=%s message_id=%s channel_id=%s",
                resolved.message.provider,
                resolved.message.message_id,
                resolved.channel_id,
            )
            return FlorenceMessagingIngressResult(consumed=True)

        self._append_inbound_message(resolved)

        if resolved.is_group:
            result = self._handle_group_message(resolved)
        else:
            result = self._handle_dm_message(resolved)

        reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
        for body in reply_messages:
            self.append_assistant_message(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                body=body,
                metadata={
                    "provider": resolved.message.provider,
                    "transport_thread_id": resolved.thread_id,
                    "transport_reply_to": resolved.message.message_id,
                },
            )

        return result

    def append_assistant_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        body: str,
        metadata: dict[str, object] | None = None,
    ) -> ChannelMessage:
        return self.store.append_channel_message(
            ChannelMessage(
                id=_assistant_message_id(channel_id, body),
                household_id=household_id,
                channel_id=channel_id,
                sender_role=ChannelMessageRole.ASSISTANT,
                body=body,
                metadata=metadata or {},
                created_at=time.time(),
            )
        )

    def _render_onboarding_prompt_messages(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        prompt: OnboardingPrompt | None,
        include_intro: bool = False,
    ) -> tuple[str, ...]:
        if prompt is None:
            return ()
        intro: tuple[str, ...] = (
            (
                "Hi, I'm Florence.",
                "I help run the household with you by learning the family map first, then keeping up with reminders, logistics, school noise, and schedule changes.",
            )
            if include_intro
            else ()
        )
        if prompt.stage == OnboardingStage.CONNECT_GOOGLE and self.google_account_link_service is not None:
            link = self.google_account_link_service.build_connect_link(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            )
            return intro + build_google_connect_message_sequence(link.url)
        if prompt.stage == OnboardingStage.CONNECT_GOOGLE:
            return intro + build_google_connect_message_sequence()
        return intro + (prompt.text,)

    def _render_web_onboarding_messages(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        include_intro: bool,
    ) -> tuple[str, ...]:
        if self.onboarding_link_service is None:
            return ()
        link = self.onboarding_link_service.build_link(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        return build_web_onboarding_handoff_sequence(link.url, include_intro=include_intro)

    def _channel_has_assistant_history(self, *, channel_id: str) -> bool:
        return any(
            message.sender_role == ChannelMessageRole.ASSISTANT and message.body.strip()
            for message in self.store.list_channel_messages(channel_id=channel_id, limit=8)
        )

    @staticmethod
    def _result_with_messages(
        messages: tuple[str, ...],
        *,
        group_announcement: str | None = None,
        consumed: bool = True,
    ) -> FlorenceMessagingIngressResult:
        return FlorenceMessagingIngressResult(
            reply_text=messages[0] if messages else None,
            reply_messages=messages,
            group_announcement=group_announcement,
            consumed=consumed,
        )

    def _append_inbound_message(self, resolved: FlorenceResolvedInboundMessage) -> None:
        body = resolved.message.body.strip()
        if not body:
            return
        self.store.append_channel_message(
            ChannelMessage(
                id=_stable_transport_message_id(resolved.message.provider, resolved.message.message_id),
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                sender_role=ChannelMessageRole.USER,
                sender_member_id=resolved.member_id,
                body=body,
                metadata={
                    "provider": resolved.message.provider,
                    "event_type": resolved.message.event_type,
                    "transport_thread_id": resolved.thread_id,
                    "transport_message_id": resolved.message.message_id,
                    "reply_to_message_id": resolved.message.reply_to_message_id,
                    "sent_at": resolved.message.sent_at,
                    **resolved.message.metadata,
                },
                created_at=time.time(),
            )
        )

    def _is_candidate_review_reply_armed(self, *, channel_id: str, review_prompt_text: str) -> bool:
        history = self.store.list_channel_messages(channel_id=channel_id, limit=8)
        latest_assistant = next(
            (
                message
                for message in reversed(history)
                if message.sender_role == ChannelMessageRole.ASSISTANT
            ),
            None,
        )
        if latest_assistant is None:
            return False

        latest_body = latest_assistant.body.strip()
        if not latest_body:
            return False
        if latest_body == review_prompt_text.strip():
            return True
        return _looks_like_candidate_review_prompt(latest_body)

    def _latest_assistant_message_body(self, *, channel_id: str) -> str | None:
        history = self.store.list_channel_messages(channel_id=channel_id, limit=8)
        latest_assistant = next(
            (
                message
                for message in reversed(history)
                if message.sender_role == ChannelMessageRole.ASSISTANT and message.body.strip()
            ),
            None,
        )
        return latest_assistant.body.strip() if latest_assistant is not None else None

    def _handle_dm_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        text = resolved.message.body.strip()
        member_id = _require_member_id(resolved.member_id)
        session = self.onboarding_service.get_or_create_session(
            household_id=resolved.household_id,
            member_id=member_id,
            thread_id=resolved.thread_id,
        )

        review_prompt = None
        if session.is_grounded_for_google_matching:
            review_prompt = self.candidate_review_service.build_next_review_prompt(
                household_id=resolved.household_id,
                member_id=member_id,
            )
            if review_prompt is not None:
                review_reply_armed = self._is_candidate_review_reply_armed(
                    channel_id=resolved.channel_id,
                    review_prompt_text=review_prompt.text,
                )
                if review_reply_armed and (_looks_like_share_source(text) or _looks_like_private_source(text)):
                    visibility = (
                        HouseholdSourceVisibility.PRIVATE
                        if _looks_like_private_source(text)
                        else HouseholdSourceVisibility.SHARED
                    )
                    updated_candidate = self.candidate_review_service.set_candidate_source_visibility(
                        candidate_id=review_prompt.candidate.id,
                        visibility=visibility,
                        created_by_member_id=member_id,
                    )
                    source_label = str(
                        updated_candidate.metadata.get("source_rule_label")
                        or updated_candidate.metadata.get("source_visibility_label")
                        or "this source"
                    )
                    prefix = (
                        f"Understood. I’ll keep future items from {source_label} private to your review queue."
                        if visibility == HouseholdSourceVisibility.PRIVATE
                        else f"Understood. I’ll treat future items from {source_label} as shared household context."
                    )
                    if _looks_like_yes(text):
                        result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                        suffix = (
                            f" Confirmed. I added {result.event.title} to the family plan."
                            if result.event
                            else " Confirmed."
                        )
                        return FlorenceMessagingIngressResult(
                            reply_text=f"{prefix}{suffix}",
                            group_announcement=result.group_announcement,
                            consumed=True,
                        )
                    if _looks_like_no(text):
                        self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                        return FlorenceMessagingIngressResult(
                            reply_text=f"{prefix} I left this item out.",
                            consumed=True,
                        )
                    return FlorenceMessagingIngressResult(
                        reply_text=f"{prefix} Reply yes if you want me to add this item too.",
                        consumed=True,
                    )
                if review_reply_armed and _looks_like_yes(text):
                    result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                    return FlorenceMessagingIngressResult(
                        reply_text=f"Confirmed. I added {result.event.title} to the family plan." if result.event else "Confirmed.",
                        group_announcement=result.group_announcement,
                        consumed=True,
                    )
                if review_reply_armed and _looks_like_no(text):
                    self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                    return FlorenceMessagingIngressResult(reply_text="Rejected. I will leave it out.", consumed=True)
                if review_reply_armed and _looks_like_skip(text):
                    return FlorenceMessagingIngressResult(reply_text="Okay. I will leave it in your review queue for later.", consumed=True)
                if _looks_like_review_request(text):
                    return FlorenceMessagingIngressResult(reply_text=review_prompt.text, consumed=True)

        if not session.is_complete:
            if self.onboarding_link_service is not None:
                return self._result_with_messages(
                    self._render_web_onboarding_messages(
                        household_id=resolved.household_id,
                        member_id=member_id,
                        thread_id=resolved.thread_id,
                        include_intro=not self._channel_has_assistant_history(channel_id=resolved.channel_id),
                    )
                )
            return self._handle_onboarding_message(resolved, session.stage, text)

        if review_prompt is not None:
            review_reply_armed = self._is_candidate_review_reply_armed(
                channel_id=resolved.channel_id,
                review_prompt_text=review_prompt.text,
            )
            if review_reply_armed and (_looks_like_share_source(text) or _looks_like_private_source(text)):
                visibility = (
                    HouseholdSourceVisibility.PRIVATE
                    if _looks_like_private_source(text)
                    else HouseholdSourceVisibility.SHARED
                )
                updated_candidate = self.candidate_review_service.set_candidate_source_visibility(
                    candidate_id=review_prompt.candidate.id,
                    visibility=visibility,
                    created_by_member_id=member_id,
                )
                source_label = str(
                    updated_candidate.metadata.get("source_rule_label")
                    or updated_candidate.metadata.get("source_visibility_label")
                    or "this source"
                )
                prefix = (
                    f"Understood. I’ll keep future items from {source_label} private to your review queue."
                    if visibility == HouseholdSourceVisibility.PRIVATE
                    else f"Understood. I’ll treat future items from {source_label} as shared household context."
                )
                if _looks_like_yes(text):
                    result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                    suffix = (
                        f" Confirmed. I added {result.event.title} to the family plan."
                        if result.event
                        else " Confirmed."
                    )
                    return FlorenceMessagingIngressResult(
                        reply_text=f"{prefix}{suffix}",
                        group_announcement=result.group_announcement,
                        consumed=True,
                    )
                if _looks_like_no(text):
                    self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                    return FlorenceMessagingIngressResult(
                        reply_text=f"{prefix} I left this item out.",
                        consumed=True,
                    )
                return FlorenceMessagingIngressResult(
                    reply_text=f"{prefix} Reply yes if you want me to add this item too.",
                    consumed=True,
                )
            if review_reply_armed and _looks_like_yes(text):
                result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                return FlorenceMessagingIngressResult(
                    reply_text=f"Confirmed. I added {result.event.title} to the family plan." if result.event else "Confirmed.",
                    group_announcement=result.group_announcement,
                    consumed=True,
                )
            if review_reply_armed and _looks_like_no(text):
                self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                return FlorenceMessagingIngressResult(reply_text="Rejected. I will leave it out.", consumed=True)
            if review_reply_armed and _looks_like_skip(text):
                return FlorenceMessagingIngressResult(reply_text="Okay. I will leave it in your review queue for later.", consumed=True)
            if _looks_like_review_request(text):
                return FlorenceMessagingIngressResult(reply_text=review_prompt.text, consumed=True)

        if _looks_like_reminder_feedback(text):
            self.household_manager_service.record_reminder_feedback(
                household_id=resolved.household_id,
                feedback_text=text,
                member_id=member_id,
                channel_id=resolved.channel_id,
            )
            return FlorenceMessagingIngressResult(
                reply_text=(
                    "Understood. I updated your reminder style and will adjust future nudges accordingly. "
                    "You can ask me to show reminders anytime."
                ),
                consumed=True,
            )

        pending_nudges = self.household_manager_service.list_pending_nudges(
            household_id=resolved.household_id,
            recipient_member_id=member_id,
            channel_id=resolved.channel_id,
        )
        sent_nudges = [nudge for nudge in pending_nudges if nudge.status == HouseholdNudgeStatus.SENT]
        if sent_nudges:
            min_dt = datetime.min.replace(tzinfo=timezone.utc)
            actionable_nudge = max(
                sent_nudges,
                key=lambda nudge: _parse_optional_iso(nudge.sent_at) or _parse_optional_iso(nudge.scheduled_for) or min_dt,
            )
        else:
            actionable_nudge = pending_nudges[0] if pending_nudges else None

        latest_assistant_body = self._latest_assistant_message_body(channel_id=resolved.channel_id)
        if (
            _looks_like_done_for_reminder(text)
            and latest_assistant_body is not None
            and _looks_like_google_done_prompt(latest_assistant_body)
        ):
            member_connections = self.store.list_google_connections(
                household_id=resolved.household_id,
                member_id=member_id,
            )
            if member_connections:
                if self.household_chat_service is not None:
                    history = self.store.list_channel_messages(channel_id=resolved.channel_id, limit=24)
                    reply = self.household_chat_service.respond(
                        household_id=resolved.household_id,
                        channel_id=resolved.channel_id,
                        actor_member_id=resolved.member_id,
                        message_text="My Google account is connected now. Continue with the inbox or calendar lookup you just offered.",
                        conversation_history=history[:-1] if history else None,
                    )
                    if reply is not None and reply.text.strip():
                        return FlorenceMessagingIngressResult(reply_text=reply.text, consumed=True)
                return self._result_with_messages(build_onboarding_ready_message_sequence())

            if self.google_account_link_service is not None:
                link = self.google_account_link_service.build_connect_link(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                )
                return self._result_with_messages(
                    (
                        "I still don’t see your Google account connected yet.",
                        link.url,
                        "Once Google says you're connected, come back here and text done.",
                    )
                )
            return FlorenceMessagingIngressResult(
                reply_text="I still don’t see your Google account connected yet.",
                consumed=True,
            )

        if _looks_like_done_for_reminder(text):
            if actionable_nudge is None:
                return FlorenceMessagingIngressResult(
                    reply_text="I don’t see an active reminder to mark done right now.",
                    consumed=True,
                )
            now = datetime.now(timezone.utc)
            self.household_manager_service.acknowledge_nudge(
                nudge_id=actionable_nudge.id,
                acknowledged_at=now,
            )
            completed_work_item_title: str | None = None
            if (
                actionable_nudge.target_kind == HouseholdNudgeTargetKind.WORK_ITEM
                and actionable_nudge.target_id
            ):
                work_item = self.store.get_household_work_item(actionable_nudge.target_id)
                if work_item is not None and work_item.status not in {
                    HouseholdWorkItemStatus.DONE,
                    HouseholdWorkItemStatus.CANCELLED,
                }:
                    updated_work_item = replace(
                        work_item,
                        status=HouseholdWorkItemStatus.DONE,
                        completed_at=now.isoformat(),
                    )
                    self.household_manager_service.upsert_work_item(updated_work_item)
                    completed_work_item_title = updated_work_item.title
            self.household_manager_service.record_pilot_event(
                household_id=resolved.household_id,
                event_type="reminder_done",
                member_id=member_id,
                channel_id=resolved.channel_id,
                metadata={
                    "nudge_id": actionable_nudge.id,
                    "target_kind": actionable_nudge.target_kind.value,
                    "target_id": actionable_nudge.target_id,
                    "marked_work_item_done": bool(completed_work_item_title),
                },
                created_at=now,
            )
            if completed_work_item_title:
                return FlorenceMessagingIngressResult(
                    reply_text=f'Done. I marked "{completed_work_item_title}" complete and stopped that reminder.',
                    consumed=True,
                )
            return FlorenceMessagingIngressResult(
                reply_text="Done. I marked that reminder complete.",
                consumed=True,
            )

        if _looks_like_snooze_request(text):
            if actionable_nudge is None:
                return FlorenceMessagingIngressResult(
                    reply_text="I don’t see an active reminder to snooze right now.",
                    consumed=True,
                )
            now = datetime.now(timezone.utc)
            snooze_until = _parse_snooze_deadline(text, now=now).astimezone(timezone.utc)
            updated_nudge = self.household_manager_service.snooze_nudge(
                nudge_id=actionable_nudge.id,
                scheduled_for=snooze_until,
                snoozed_at=now,
            )
            self.household_manager_service.record_pilot_event(
                household_id=resolved.household_id,
                event_type="reminder_snoozed",
                member_id=member_id,
                channel_id=resolved.channel_id,
                metadata={
                    "nudge_id": actionable_nudge.id,
                    "target_kind": actionable_nudge.target_kind.value,
                    "target_id": actionable_nudge.target_id,
                    "snoozed_until": (updated_nudge.scheduled_for if updated_nudge else snooze_until.isoformat()),
                },
                created_at=now,
            )
            until_text = (updated_nudge.scheduled_for if updated_nudge else snooze_until.isoformat()).replace("T", " ").replace("+00:00", "Z")
            return FlorenceMessagingIngressResult(
                reply_text=f"Okay, snoozed. I’ll remind you again around {until_text}.",
                consumed=True,
            )

        if self.household_chat_service is not None:
            history = self.store.list_channel_messages(channel_id=resolved.channel_id, limit=24)
            reply = self.household_chat_service.respond(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                actor_member_id=resolved.member_id,
                message_text=resolved.message.body,
                conversation_history=history[:-1] if history else None,
            )
            if reply is not None and reply.text.strip():
                return FlorenceMessagingIngressResult(reply_text=reply.text, consumed=True)
            logger.warning(
                "Household chat returned no reply for household_id=%s channel_id=%s",
                resolved.household_id,
                resolved.channel_id,
            )

        if _looks_like_schedule_question(text):
            return FlorenceMessagingIngressResult(
                reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
                consumed=True,
            )

        if _looks_like_tracking_request(text):
            return FlorenceMessagingIngressResult(
                reply_text=self.query_service.summarize_tracking_state(household_id=resolved.household_id),
                consumed=True,
            )

        if _looks_like_reminder_list_request(text):
            return FlorenceMessagingIngressResult(
                reply_text=self.query_service.summarize_pending_nudges(household_id=resolved.household_id),
                consumed=True,
            )

        return self._result_with_messages(build_onboarding_ready_message_sequence())

    def _handle_onboarding_message(
        self,
        resolved: FlorenceResolvedInboundMessage,
        stage: OnboardingStage,
        text: str,
    ) -> FlorenceMessagingIngressResult:
        member_id = _require_member_id(resolved.member_id)
        if stage == OnboardingStage.COLLECT_PARENT_NAME and text:
            transition = self.onboarding_service.record_parent_name(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                display_name=text,
            )
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                    include_intro=True,
                )
            )

        if stage == OnboardingStage.COLLECT_HOUSEHOLD_MEMBERS:
            transition = self.onboarding_service.record_household_members(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                household_members=split_entries(text),
            )
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.CONNECT_GOOGLE:
            if _looks_like_google_connected(text):
                transition = self.onboarding_service.record_google_connected(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                )
                return self._result_with_messages(
                    self._render_onboarding_prompt_messages(
                        household_id=resolved.household_id,
                        member_id=member_id,
                        thread_id=resolved.thread_id,
                        prompt=transition.prompt,
                    )
                )
            prompt = self.onboarding_service.get_prompt(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
            )
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=prompt,
                )
            )

        if stage == OnboardingStage.COLLECT_CHILD_NAMES:
            entries = split_entries(text)
            transition = self.onboarding_service.record_child_names(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                child_names=extract_child_names(entries),
                child_details=entries,
            )
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.COLLECT_SCHOOL_BASICS:
            transition = self.onboarding_service.record_school_basics(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                school_labels=split_labels(text),
            )
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.COLLECT_ACTIVITY_BASICS:
            transition = self.onboarding_service.record_activity_basics(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                activity_labels=split_labels(text),
            )
            if transition.state.is_complete:
                return self._result_with_messages(build_onboarding_ready_message_sequence())
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.COLLECT_HOUSEHOLD_OPERATIONS:
            transition = self.onboarding_service.record_household_operations(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                household_operations=split_entries(text),
            )
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.COLLECT_NUDGE_PREFERENCES:
            transition = self.onboarding_service.record_nudge_preferences(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                nudge_preferences=text,
            )
            if transition.state.is_complete:
                self._record_onboarding_completion(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    channel_id=resolved.channel_id,
                )
                return self._result_with_messages(build_onboarding_ready_message_sequence())
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.COLLECT_OPERATING_PREFERENCES:
            transition = self.onboarding_service.record_operating_preferences(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                operating_preferences=text,
            )
            if transition.state.is_complete:
                self._record_onboarding_completion(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    channel_id=resolved.channel_id,
                )
                return self._result_with_messages(build_onboarding_ready_message_sequence())
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.ACTIVATE_GROUP:
            return self._result_with_messages(build_onboarding_ready_message_sequence())

        prompt = self.onboarding_service.get_prompt(
            household_id=resolved.household_id,
            member_id=member_id,
            thread_id=resolved.thread_id,
        )
        return self._result_with_messages(
            self._render_onboarding_prompt_messages(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                prompt=prompt,
            )
        )

    def _handle_group_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        if resolved.member_id is None:
            if _looks_like_schedule_question(resolved.message.body):
                return FlorenceMessagingIngressResult(
                    reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
                    consumed=True,
                )
            return FlorenceMessagingIngressResult(consumed=False)

        onboarding_sessions = self.store.list_member_onboarding_sessions(
            household_id=resolved.household_id,
            member_id=resolved.member_id,
        )
        latest = onboarding_sessions[0] if onboarding_sessions else None
        if latest is not None and latest.is_complete and not latest.group_channel_id:
            transition = self.onboarding_service.record_group_activated(
                household_id=resolved.household_id,
                member_id=resolved.member_id,
                thread_id=latest.thread_id,
                group_channel_id=resolved.thread_id,
            )
            return FlorenceMessagingIngressResult(
                reply_text=(
                    "I’m in. Ask me things like what’s on the kids’ schedule next week, check connected email for school or camp updates, or plan dinners and groceries."
                    if transition.state.is_complete
                    else None
                ),
                consumed=True,
            )

        if self.household_chat_service is not None:
            history = self.store.list_channel_messages(channel_id=resolved.channel_id, limit=24)
            reply = self.household_chat_service.respond(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                actor_member_id=resolved.member_id,
                message_text=resolved.message.body,
                conversation_history=history[:-1] if history else None,
            )
            if reply is not None and reply.text.strip():
                return FlorenceMessagingIngressResult(reply_text=reply.text, consumed=True)

        if _looks_like_schedule_question(resolved.message.body):
            return FlorenceMessagingIngressResult(
                reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
                consumed=True,
            )

        return FlorenceMessagingIngressResult(consumed=False)

    def _record_onboarding_completion(self, *, household_id: str, member_id: str, channel_id: str) -> None:
        try:
            self.household_manager_service.ensure_briefing_routines(household_id=household_id)
            self.household_manager_service.record_pilot_event(
                household_id=household_id,
                event_type="onboarding_complete",
                member_id=member_id,
                channel_id=channel_id,
            )
        except Exception:
            logger.exception("Failed to finalize onboarding completion hooks for household_id=%s", household_id)
