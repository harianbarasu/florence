"""First-party Florence app chat services."""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass

from florence.contracts import (
    AppChatMessage,
    AppChatMessageRole,
    AppChatScope,
    AppChatThread,
    Channel,
    ChannelType,
    Household,
    Member,
    MemberRole,
)
from florence.runtime.chat import FlorenceHouseholdChatService, FlorenceHouseholdChatReply
from florence.runtime.resolver import household_name_from_display_name
from florence.runtime.services import (
    CandidateReviewResult,
    FlorenceCandidateReviewService,
    FlorenceGoogleAccountLinkService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def _looks_like_yes(text: str) -> bool:
    return bool(re.search(r"^(?:yes|yep|yeah|sure|confirm|add it|do it)\b", text.strip(), re.IGNORECASE))


def _looks_like_no(text: str) -> bool:
    return bool(re.search(r"^(?:no|nope|nah|reject|wrong)\b", text.strip(), re.IGNORECASE))


def _looks_like_skip(text: str) -> bool:
    return bool(re.search(r"^(?:skip|later|not now)\b", text.strip(), re.IGNORECASE))


def _looks_like_review_request(text: str) -> bool:
    return bool(re.search(r"\b(review|imports?|gmail|calendar|candidates?)\b", text, re.IGNORECASE))


def _looks_like_schedule_question(text: str) -> bool:
    return bool(re.search(r"\b(today|this week|coming up|schedule|happening)\b", text, re.IGNORECASE))


def _split_names(text: str) -> list[str]:
    normalized = re.sub(r"\b(?:and|&)\b", ",", text, flags=re.IGNORECASE)
    return [part.strip(" .,!?:;") for part in normalized.split(",") if part.strip(" .,!?:;")]


def _split_labels(text: str) -> list[str]:
    if re.search(r"^\s*none\b", text, re.IGNORECASE):
        return []
    return _split_names(text)


@dataclass(slots=True)
class FlorenceAppBootstrapResult:
    household: Household
    member: Member
    private_thread: AppChatThread
    shared_thread: AppChatThread
    assistant_message: AppChatMessage | None


@dataclass(slots=True)
class FlorenceAppChatTurnResult:
    thread: AppChatThread
    user_message: AppChatMessage
    assistant_message: AppChatMessage | None


class FlorenceAppChatService:
    """Owns app-native household and private Florence chat flows."""

    provider = "florence_app"

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        onboarding_service: FlorenceOnboardingSessionService,
        candidate_review_service: FlorenceCandidateReviewService,
        query_service: FlorenceHouseholdQueryService,
        household_chat_service: FlorenceHouseholdChatService | None = None,
        google_account_link_service: FlorenceGoogleAccountLinkService | None = None,
    ):
        self.store = store
        self.onboarding_service = onboarding_service
        self.candidate_review_service = candidate_review_service
        self.query_service = query_service
        self.household_chat_service = household_chat_service
        self.google_account_link_service = google_account_link_service

    def bootstrap_parent(
        self,
        *,
        parent_name: str,
        household_name: str | None = None,
        timezone: str = "America/Los_Angeles",
    ) -> FlorenceAppBootstrapResult:
        normalized_parent = " ".join(parent_name.split()).strip() or "Parent"
        household = self.store.upsert_household(
            Household(
                id=_new_id("hh"),
                name=(" ".join(household_name.split()).strip() if household_name and household_name.strip() else household_name_from_display_name(normalized_parent)),
                timezone=timezone,
            )
        )
        member = self.store.upsert_member(
            Member(
                id=_new_id("mem"),
                household_id=household.id,
                display_name=normalized_parent,
                role=MemberRole.ADMIN,
            )
        )
        private_thread = self._ensure_private_thread(household_id=household.id, member_id=member.id)
        shared_thread = self._ensure_shared_thread(household_id=household.id)
        transition = self.onboarding_service.record_parent_name(
            household_id=household.id,
            member_id=member.id,
            thread_id=private_thread.channel.id,
            display_name=normalized_parent,
        )
        assistant_message = None
        if transition.prompt is not None:
            assistant_message = self._append_message(
                household_id=household.id,
                channel_id=private_thread.channel.id,
                role=AppChatMessageRole.ASSISTANT,
                body=transition.prompt.text,
            )
        return FlorenceAppBootstrapResult(
            household=household,
            member=member,
            private_thread=private_thread,
            shared_thread=shared_thread,
            assistant_message=assistant_message,
        )

    def list_threads(self, *, household_id: str, member_id: str) -> list[AppChatThread]:
        threads: list[AppChatThread] = []
        shared = self._ensure_shared_thread(household_id=household_id)
        threads.append(shared)
        threads.append(self._ensure_private_thread(household_id=household_id, member_id=member_id))
        return threads

    def list_messages(self, *, channel_id: str, limit: int = 50) -> list[AppChatMessage]:
        return self.store.list_app_chat_messages(channel_id=channel_id, limit=limit)

    def append_assistant_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        body: str,
    ) -> AppChatMessage:
        return self._append_message(
            household_id=household_id,
            channel_id=channel_id,
            role=AppChatMessageRole.ASSISTANT,
            body=body,
        )

    def send_message(
        self,
        *,
        household_id: str,
        member_id: str,
        scope: AppChatScope,
        text: str,
    ) -> FlorenceAppChatTurnResult:
        thread = (
            self._ensure_shared_thread(household_id=household_id)
            if scope == AppChatScope.SHARED
            else self._ensure_private_thread(household_id=household_id, member_id=member_id)
        )
        user_message = self._append_message(
            household_id=household_id,
            channel_id=thread.channel.id,
            role=AppChatMessageRole.USER,
            body=text,
            sender_member_id=member_id,
        )
        assistant_text = (
            self._handle_shared_chat(household_id=household_id, member_id=member_id, thread=thread, text=text)
            if scope == AppChatScope.SHARED
            else self._handle_private_chat(household_id=household_id, member_id=member_id, thread=thread, text=text)
        )
        assistant_message = None
        if assistant_text and assistant_text.strip():
            assistant_message = self._append_message(
                household_id=household_id,
                channel_id=thread.channel.id,
                role=AppChatMessageRole.ASSISTANT,
                body=assistant_text,
            )
        return FlorenceAppChatTurnResult(
            thread=thread,
            user_message=user_message,
            assistant_message=assistant_message,
        )

    def _handle_private_chat(
        self,
        *,
        household_id: str,
        member_id: str,
        thread: AppChatThread,
        text: str,
    ) -> str:
        session = self.onboarding_service.get_or_create_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread.channel.id,
        )

        if session.is_grounded_for_google_matching:
            review_prompt = self.candidate_review_service.build_next_review_prompt(
                household_id=household_id,
                member_id=member_id,
            )
            if review_prompt is not None:
                handled = self._handle_review_turn(text=text, prompt=review_prompt)
                if handled is not None:
                    return handled

        if not session.is_complete:
            stage = session.stage
            if stage.value == "connect_google":
                if self.google_account_link_service is not None:
                    link = self.google_account_link_service.build_connect_link(
                        household_id=household_id,
                        member_id=member_id,
                        thread_id=thread.channel.id,
                    )
                    return f"Connect Google here:\n{link.url}\n\nWhen you finish, come back here and say done."
                if text.strip():
                    transition = self.onboarding_service.record_google_connected(
                        household_id=household_id,
                        member_id=member_id,
                        thread_id=thread.channel.id,
                    )
                    return transition.prompt.text if transition.prompt else "Google marked as connected."

            if stage.value == "collect_child_names":
                transition = self.onboarding_service.record_child_names(
                    household_id=household_id,
                    member_id=member_id,
                    thread_id=thread.channel.id,
                    child_names=_split_names(text),
                )
                return transition.prompt.text if transition.prompt else "Got it."

            if stage.value == "collect_school_basics":
                transition = self.onboarding_service.record_school_basics(
                    household_id=household_id,
                    member_id=member_id,
                    thread_id=thread.channel.id,
                    school_labels=_split_labels(text),
                )
                return transition.prompt.text if transition.prompt else "Got it."

            if stage.value == "collect_activity_basics":
                transition = self.onboarding_service.record_activity_basics(
                    household_id=household_id,
                    member_id=member_id,
                    thread_id=thread.channel.id,
                    activity_labels=_split_labels(text),
                )
                if transition.state.group_channel_id:
                    return "You’re set up. Use the shared household chat as the main Florence conversation. I’ll keep this private chat for Google review and clarifications."
                auto_complete = self.onboarding_service.record_group_activated(
                    household_id=household_id,
                    member_id=member_id,
                    thread_id=thread.channel.id,
                    group_channel_id=self._ensure_shared_thread(household_id=household_id).channel.id,
                )
                _ = auto_complete
                return "You’re set up. Use the shared household chat as the main Florence conversation. I’ll keep this private chat for Google review and clarifications."

            prompt = self.onboarding_service.get_prompt(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread.channel.id,
            )
            return prompt.text if prompt else "Let’s keep going."

        review_prompt = self.candidate_review_service.build_next_review_prompt(
            household_id=household_id,
            member_id=member_id,
        )
        if review_prompt is not None:
            handled = self._handle_review_turn(text=text, prompt=review_prompt)
            if handled is not None:
                return handled

        if _looks_like_schedule_question(text):
            return self.query_service.summarize_upcoming_events(household_id=household_id)
        return "Use the shared household chat for normal Florence conversations. I’ll keep this private chat for imports, setup, and clarifications."

    def _handle_shared_chat(
        self,
        *,
        household_id: str,
        member_id: str,
        thread: AppChatThread,
        text: str,
    ) -> str:
        sessions = self.store.list_member_onboarding_sessions(household_id=household_id, member_id=member_id)
        if not any(session.is_complete for session in sessions):
            return "Finish setup in your private Florence chat first, then come back here for the shared household conversation."

        prior_messages = self.store.list_app_chat_messages(channel_id=thread.channel.id, limit=24)
        history = prior_messages[:-1] if prior_messages and prior_messages[-1].sender_role == AppChatMessageRole.USER else prior_messages
        reply: FlorenceHouseholdChatReply | None = None
        if self.household_chat_service is not None:
            reply = self.household_chat_service.respond(
                household_id=household_id,
                channel_id=thread.channel.id,
                actor_member_id=member_id,
                message_text=text,
                conversation_history=history,
            )
        if reply is not None and reply.text.strip():
            return reply.text
        if _looks_like_schedule_question(text):
            return self.query_service.summarize_upcoming_events(household_id=household_id)
        return "I’m here. Ask me about the family plan, something you shared, or what I should keep track of for the household."

    def _handle_review_turn(self, *, text: str, prompt) -> str | None:
        if _looks_like_yes(text):
            result: CandidateReviewResult = self.candidate_review_service.confirm_candidate(candidate_id=prompt.candidate.id)
            return f"Confirmed. I added {result.event.title} to the shared household state." if result.event else "Confirmed."
        if _looks_like_no(text):
            self.candidate_review_service.reject_candidate(candidate_id=prompt.candidate.id)
            return "Rejected. I will leave it out."
        if _looks_like_skip(text):
            return "Okay. I’ll leave it in your review queue for later."
        if _looks_like_review_request(text):
            return prompt.text
        return None

    def _ensure_shared_thread(self, *, household_id: str) -> AppChatThread:
        provider_channel_id = f"shared:{household_id}"
        channel = self.store.get_channel_by_provider_id(
            provider=self.provider,
            provider_channel_id=provider_channel_id,
        )
        if channel is None or channel.household_id != household_id:
            channel = self.store.upsert_channel(
                Channel(
                    id=_new_id("chan"),
                    household_id=household_id,
                    provider=self.provider,
                    provider_channel_id=provider_channel_id,
                    channel_type=ChannelType.APP_CHAT,
                    title="Household chat",
                    metadata={"scope": AppChatScope.SHARED.value},
                )
            )
        return AppChatThread(channel=channel, scope=AppChatScope.SHARED)

    def _ensure_private_thread(self, *, household_id: str, member_id: str) -> AppChatThread:
        provider_channel_id = f"private:{household_id}:{member_id}"
        channel = self.store.get_channel_by_provider_id(
            provider=self.provider,
            provider_channel_id=provider_channel_id,
        )
        if channel is None or channel.household_id != household_id:
            member = self.store.get_member(member_id)
            channel = self.store.upsert_channel(
                Channel(
                    id=_new_id("chan"),
                    household_id=household_id,
                    provider=self.provider,
                    provider_channel_id=provider_channel_id,
                    channel_type=ChannelType.APP_CHAT,
                    title=f"{member.display_name if member is not None else 'Parent'} private chat",
                    metadata={
                        "scope": AppChatScope.PRIVATE.value,
                        "member_id": member_id,
                    },
                )
            )
        return AppChatThread(channel=channel, scope=AppChatScope.PRIVATE, member_id=member_id)

    def _append_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        role: AppChatMessageRole,
        body: str,
        sender_member_id: str | None = None,
    ) -> AppChatMessage:
        message = AppChatMessage(
            id=_new_id("appmsg"),
            household_id=household_id,
            channel_id=channel_id,
            sender_role=role,
            sender_member_id=sender_member_id,
            body=body,
            created_at=time.time(),
        )
        self.store.append_app_chat_message(message)
        return message
