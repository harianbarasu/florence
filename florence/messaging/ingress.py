"""Transport-agnostic Florence DM and household-group ingress."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field

from florence.contracts import ChannelMessage, ChannelMessageRole
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


def _onboarding_ready_messages() -> tuple[str, ...]:
    return (
        "Perfect. I have enough household context to help now.",
        "You can keep asking me here, and you can add me to the family group later if you want shared help there too.",
    )


def _split_names(text: str) -> list[str]:
    normalized = re.sub(r"\b(?:and|&)\b", ",", text, flags=re.IGNORECASE)
    return [part.strip(" .,!?:;") for part in normalized.split(",") if part.strip(" .,!?:;")]


def _split_labels(text: str) -> list[str]:
    if re.search(r"^\s*none\b", text, re.IGNORECASE):
        return []
    return _split_names(text)


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
    ):
        self.store = store
        self.onboarding_service = onboarding_service
        self.candidate_review_service = candidate_review_service
        self.query_service = query_service
        self.google_account_link_service = google_account_link_service
        self.household_chat_service = household_chat_service

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
        if prompt.stage == OnboardingStage.CONNECT_GOOGLE and self.google_account_link_service is not None:
            link = self.google_account_link_service.build_connect_link(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            )
            return build_google_connect_message_sequence(link.url, include_intro=include_intro)
        if prompt.stage == OnboardingStage.CONNECT_GOOGLE:
            return build_google_connect_message_sequence(include_intro=include_intro)
        return (prompt.text,)

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
            transition = self.onboarding_service.record_child_names(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                child_names=_split_names(text),
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
