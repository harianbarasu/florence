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
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdWorkItemStatus,
)
from florence.messaging.types import FlorenceInboundMessage
from florence.onboarding import (
    OnboardingPrompt,
    OnboardingStage,
    build_google_connect_message_sequence,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    FlorenceGoogleAccountLinkService,
    FlorenceHouseholdManagerService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


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


def _looks_like_review_request(text: str) -> bool:
    return bool(re.search(r"\b(review|imports?|gmail|calendar|candidates?)\b", text, re.IGNORECASE))


def _looks_like_google_connected(text: str) -> bool:
    return bool(re.search(r"\b(done|connected|finished|complete|i connected)\b", text, re.IGNORECASE))


def _looks_like_schedule_question(text: str) -> bool:
    return bool(re.search(r"\b(today|this week|coming up|schedule|happening)\b", text, re.IGNORECASE))


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


def _onboarding_ready_messages() -> tuple[str, ...]:
    return (
        "Perfect. I have enough context to start acting like your household manager.",
        "You can ask me to plan, remind, research, coordinate, and stay on top of the family's logistics here.",
    )


def _split_names(text: str) -> list[str]:
    normalized = re.sub(r"\b(?:and|&)\b", ",", text, flags=re.IGNORECASE)
    return [part.strip(" .,!?:;") for part in normalized.split(",") if part.strip(" .,!?:;")]


def _split_entries(text: str) -> list[str]:
    if "\n" in text:
        return [part.strip(" .,!?:;") for part in text.splitlines() if part.strip(" .,!?:;")]
    if ";" in text:
        return [part.strip(" .,!?:;") for part in text.split(";") if part.strip(" .,!?:;")]
    return _split_names(text)


def _split_labels(text: str) -> list[str]:
    if re.search(r"^\s*none\b", text, re.IGNORECASE):
        return []
    return _split_names(text)


def _extract_child_names(entries: list[str]) -> list[str]:
    child_names: list[str] = []
    for entry in entries:
        head = re.split(r"\s*(?:-|:|\(|,)\s*", entry, maxsplit=1)[0]
        cleaned = " ".join(head.split()).strip(" .,!?:;")
        if cleaned:
            child_names.append(cleaned)
    return child_names


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
        household_chat_service: FlorenceHouseholdChatService | None = None,
        household_manager_service: FlorenceHouseholdManagerService | None = None,
    ):
        self.store = store
        self.onboarding_service = onboarding_service
        self.candidate_review_service = candidate_review_service
        self.query_service = query_service
        self.google_account_link_service = google_account_link_service
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
                if _looks_like_yes(text):
                    result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                    return FlorenceMessagingIngressResult(
                        reply_text=f"Confirmed. I added {result.event.title} to the family plan." if result.event else "Confirmed.",
                        group_announcement=result.group_announcement,
                        consumed=True,
                    )
                if _looks_like_no(text):
                    self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                    return FlorenceMessagingIngressResult(reply_text="Rejected. I will leave it out.", consumed=True)
                if _looks_like_skip(text):
                    return FlorenceMessagingIngressResult(reply_text="Okay. I will leave it in your review queue for later.", consumed=True)
                if _looks_like_review_request(text):
                    return FlorenceMessagingIngressResult(reply_text=review_prompt.text, consumed=True)

        if not session.is_complete:
            return self._handle_onboarding_message(resolved, session.stage, text)

        if review_prompt is not None:
            if _looks_like_yes(text):
                result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                return FlorenceMessagingIngressResult(
                    reply_text=f"Confirmed. I added {result.event.title} to the family plan." if result.event else "Confirmed.",
                    group_announcement=result.group_announcement,
                    consumed=True,
                )
            if _looks_like_no(text):
                self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                return FlorenceMessagingIngressResult(reply_text="Rejected. I will leave it out.", consumed=True)
            if _looks_like_skip(text):
                return FlorenceMessagingIngressResult(reply_text="Okay. I will leave it in your review queue for later.", consumed=True)
            if _looks_like_review_request(text):
                return FlorenceMessagingIngressResult(reply_text=review_prompt.text, consumed=True)

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

        return FlorenceMessagingIngressResult(
            reply_text="I’m set up. You can ask me to plan, research, draft, review imports, or help with household logistics here or in the family group.",
            consumed=True,
        )

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
                household_members=_split_entries(text),
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
            entries = _split_entries(text)
            transition = self.onboarding_service.record_child_names(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                child_names=_extract_child_names(entries),
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
                school_labels=_split_labels(text),
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
                activity_labels=_split_labels(text),
            )
            if transition.state.is_complete:
                return self._result_with_messages(_onboarding_ready_messages())
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
                household_operations=_split_entries(text),
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
                return self._result_with_messages(_onboarding_ready_messages())
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
                return self._result_with_messages(_onboarding_ready_messages())
            return self._result_with_messages(
                self._render_onboarding_prompt_messages(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                    prompt=transition.prompt,
                )
            )

        if stage == OnboardingStage.ACTIVATE_GROUP:
            return self._result_with_messages(_onboarding_ready_messages())

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
                    "I’m in. Ask me to plan, research, summarize, or help with household logistics here, and I can still review imported school and calendar items in DM."
                    if transition.state.is_complete
                    else None
                ),
                consumed=True,
            )

        if _looks_like_schedule_question(resolved.message.body):
            return FlorenceMessagingIngressResult(
                reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
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
