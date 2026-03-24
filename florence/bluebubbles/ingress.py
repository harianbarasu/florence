"""Deterministic BlueBubbles ingress for Florence DM and group flows."""

from __future__ import annotations

import re
from dataclasses import dataclass

from florence.bluebubbles.adapter import BlueBubblesInboundMessage
from florence.onboarding import OnboardingStage
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    FlorenceGoogleAccountLinkService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class FlorenceResolvedBlueBubblesMessage:
    household_id: str
    member_id: str | None
    thread_id: str
    message: BlueBubblesInboundMessage

    @property
    def is_group(self) -> bool:
        return self.message.is_group_chat


@dataclass(slots=True)
class FlorenceIngressResult:
    reply_text: str | None = None
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


class FlorenceBlueBubblesIngressService:
    """Routes BlueBubbles DMs/groups into Florence onboarding and review flows."""

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

    def handle_message(self, resolved: FlorenceResolvedBlueBubblesMessage) -> FlorenceIngressResult:
        if resolved.message.is_from_me:
            return FlorenceIngressResult(consumed=False)
        if resolved.is_group:
            return self._handle_group_message(resolved)
        return self._handle_dm_message(resolved)

    def _handle_dm_message(self, resolved: FlorenceResolvedBlueBubblesMessage) -> FlorenceIngressResult:
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
                    return FlorenceIngressResult(
                        reply_text=f"Confirmed. I added {result.event.title} to the family plan." if result.event else "Confirmed.",
                        group_announcement=result.group_announcement,
                        consumed=True,
                    )
                if _looks_like_no(text):
                    self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                    return FlorenceIngressResult(reply_text="Rejected. I will leave it out.", consumed=True)
                if _looks_like_skip(text):
                    return FlorenceIngressResult(reply_text="Okay. I will leave it in your review queue for later.", consumed=True)
                if _looks_like_review_request(text):
                    return FlorenceIngressResult(reply_text=review_prompt.text, consumed=True)

        if not session.is_complete:
            return self._handle_onboarding_message(resolved, session, text)

        if review_prompt is not None:
            if _looks_like_yes(text):
                result = self.candidate_review_service.confirm_candidate(candidate_id=review_prompt.candidate.id)
                return FlorenceIngressResult(
                    reply_text=f"Confirmed. I added {result.event.title} to the family plan." if result.event else "Confirmed.",
                    group_announcement=result.group_announcement,
                    consumed=True,
                )
            if _looks_like_no(text):
                self.candidate_review_service.reject_candidate(candidate_id=review_prompt.candidate.id)
                return FlorenceIngressResult(reply_text="Rejected. I will leave it out.", consumed=True)
            if _looks_like_skip(text):
                return FlorenceIngressResult(reply_text="Okay. I will leave it in your review queue for later.", consumed=True)
            if _looks_like_review_request(text):
                return FlorenceIngressResult(reply_text=review_prompt.text, consumed=True)

        if _looks_like_schedule_question(text):
            return FlorenceIngressResult(
                reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
                consumed=True,
            )

        return FlorenceIngressResult(
            reply_text="I’m set up. Ask me to review imports here, or ask in the family group what is happening this week.",
            consumed=True,
        )

    def _handle_onboarding_message(
        self,
        resolved: FlorenceResolvedBlueBubblesMessage,
        session,
        text: str,
    ) -> FlorenceIngressResult:
        member_id = _require_member_id(resolved.member_id)
        stage = session.stage
        if stage == OnboardingStage.COLLECT_PARENT_NAME and text:
            transition = self.onboarding_service.record_parent_name(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                display_name=text,
            )
            return FlorenceIngressResult(reply_text=transition.prompt.text if transition.prompt else None, consumed=True)

        if stage == OnboardingStage.CONNECT_GOOGLE:
            if _looks_like_google_connected(text):
                transition = self.onboarding_service.record_google_connected(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                )
                return FlorenceIngressResult(reply_text=transition.prompt.text if transition.prompt else None, consumed=True)
            if self.google_account_link_service is not None:
                link = self.google_account_link_service.build_connect_link(
                    household_id=resolved.household_id,
                    member_id=member_id,
                    thread_id=resolved.thread_id,
                )
                return FlorenceIngressResult(
                    reply_text=f"Connect Google here:\n{link.url}\n\nWhen you finish, reply done here.",
                    consumed=True,
                )
            prompt = self.onboarding_service.get_prompt(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
            )
            return FlorenceIngressResult(reply_text=prompt.text if prompt else None, consumed=True)

        if stage == OnboardingStage.COLLECT_CHILD_NAMES:
            transition = self.onboarding_service.record_child_names(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                child_names=_split_names(text),
            )
            return FlorenceIngressResult(reply_text=transition.prompt.text if transition.prompt else None, consumed=True)

        if stage == OnboardingStage.COLLECT_SCHOOL_BASICS:
            transition = self.onboarding_service.record_school_basics(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                school_labels=_split_labels(text),
            )
            return FlorenceIngressResult(reply_text=transition.prompt.text if transition.prompt else None, consumed=True)

        if stage == OnboardingStage.COLLECT_ACTIVITY_BASICS:
            transition = self.onboarding_service.record_activity_basics(
                household_id=resolved.household_id,
                member_id=member_id,
                thread_id=resolved.thread_id,
                activity_labels=_split_labels(text),
            )
            return FlorenceIngressResult(reply_text=transition.prompt.text if transition.prompt else None, consumed=True)

        if stage == OnboardingStage.ACTIVATE_GROUP:
            return FlorenceIngressResult(
                reply_text="Add me to the family group and send a message there. I will use that first group thread as the household chat.",
                consumed=True,
            )

        prompt = self.onboarding_service.get_prompt(
            household_id=resolved.household_id,
            member_id=member_id,
            thread_id=resolved.thread_id,
        )
        return FlorenceIngressResult(reply_text=prompt.text if prompt else None, consumed=True)

    def _handle_group_message(self, resolved: FlorenceResolvedBlueBubblesMessage) -> FlorenceIngressResult:
        if resolved.member_id is None:
            if _looks_like_schedule_question(resolved.message.body):
                return FlorenceIngressResult(
                    reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
                    consumed=True,
                )
            return FlorenceIngressResult(consumed=False)

        onboarding_sessions = self.store.list_member_onboarding_sessions(
            household_id=resolved.household_id,
            member_id=resolved.member_id,
        )
        latest = onboarding_sessions[0] if onboarding_sessions else None
        if latest is not None and latest.stage == OnboardingStage.ACTIVATE_GROUP and not latest.group_channel_id:
            transition = self.onboarding_service.record_group_activated(
                household_id=resolved.household_id,
                member_id=resolved.member_id,
                thread_id=latest.thread_id,
                group_channel_id=resolved.thread_id,
            )
            return FlorenceIngressResult(
                reply_text=(
                    "I’m in. Ask me what is happening this week, or I can start reviewing imported school and calendar items in DM."
                    if transition.state.is_complete
                    else None
                ),
                consumed=True,
            )

        if _looks_like_schedule_question(resolved.message.body):
            return FlorenceIngressResult(
                reply_text=self.query_service.summarize_upcoming_events(household_id=resolved.household_id),
                consumed=True,
            )

        if self.household_chat_service is not None:
            reply = self.household_chat_service.respond(
                household_id=resolved.household_id,
                channel_id=resolved.thread_id,
                actor_member_id=resolved.member_id,
                message_text=resolved.message.body,
            )
            if reply is not None and reply.text.strip():
                return FlorenceIngressResult(reply_text=reply.text, consumed=True)

        return FlorenceIngressResult(consumed=False)
