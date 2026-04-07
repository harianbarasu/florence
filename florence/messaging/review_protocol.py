"""Candidate review protocol for Florence DM ingress."""

from __future__ import annotations

import logging
import re

from florence.contracts import HouseholdSourceVisibility
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import CANDIDATE_REVIEW_PROMPT_KIND, FlorenceProtocolReply

logger = logging.getLogger(__name__)


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


def _looks_like_review_queue_request(text: str) -> bool:
    normalized = " ".join(text.split()).strip()
    if not normalized:
        return False
    return bool(
        re.search(
            r"\b(?:review\s+(?:imports?|queue|candidates?)|pending\s+(?:imports?|candidates?)|show\s+(?:imports?|queue|candidates?)|check\s+(?:imports?|queue|candidates?)|anything\s+to\s+review|what(?:'s| is)\s+(?:pending|in\s+the\s+review\s+queue))\b",
            normalized,
            re.IGNORECASE,
        )
    )


def _looks_like_candidate_review_prompt(text: str, *, confirmation_suffix: str) -> bool:
    return confirmation_suffix in text.strip()


class FlorenceCandidateReviewProtocol:
    """Review queue handling for DM protocol turns."""

    def __init__(
        self,
        *,
        channel_log: FlorenceChannelLog,
        candidate_review_service,
        household_chat_service,
        confirmation_suffix: str,
    ) -> None:
        self.channel_log = channel_log
        self.candidate_review_service = candidate_review_service
        self.household_chat_service = household_chat_service
        self.confirmation_suffix = confirmation_suffix

    def current_prompt(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        session,
    ) -> tuple[object | None, str | None]:
        if not session.is_grounded_for_google_matching:
            return (None, None)
        prompt = self.candidate_review_service.build_next_dm_review_prompt(
            household_id=household_id,
            member_id=member_id,
        )
        if prompt is None:
            return (None, None)
        return (
            prompt,
            self._render_review_prompt_text(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=member_id,
                prompt=prompt,
            ),
        )

    def handle_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        text: str,
        prompt,
        rendered_prompt_text: str | None = None,
    ) -> FlorenceProtocolReply | None:
        prompt_text = rendered_prompt_text or self._render_review_prompt_text(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=member_id,
            prompt=prompt,
        )
        review_reply_armed = self._is_candidate_review_reply_armed(
            channel_id=channel_id,
            review_prompt_text=prompt_text,
        )
        if review_reply_armed and (_looks_like_share_source(text) or _looks_like_private_source(text)):
            source_visibility = (
                HouseholdSourceVisibility.PRIVATE
                if _looks_like_private_source(text)
                else HouseholdSourceVisibility.SHARED
            )
            resolution = "confirm" if _looks_like_yes(text) else "reject" if _looks_like_no(text) else None
            review_reply = self.candidate_review_service.apply_review_response(
                candidate_id=prompt.candidate.id,
                member_id=member_id,
                source_visibility=source_visibility,
                resolution=resolution,
            )
            return FlorenceProtocolReply(
                reply_text=review_reply.reply_text,
                group_announcement=review_reply.group_announcement,
                consumed=True,
            )
        if review_reply_armed and _looks_like_yes(text):
            review_reply = self.candidate_review_service.apply_review_response(
                candidate_id=prompt.candidate.id,
                member_id=member_id,
                resolution="confirm",
            )
            return FlorenceProtocolReply(
                reply_text=review_reply.reply_text,
                group_announcement=review_reply.group_announcement,
                consumed=True,
            )
        if review_reply_armed and _looks_like_no(text):
            review_reply = self.candidate_review_service.apply_review_response(
                candidate_id=prompt.candidate.id,
                member_id=member_id,
                resolution="reject",
            )
            return FlorenceProtocolReply(reply_text=review_reply.reply_text, consumed=True)
        if review_reply_armed and _looks_like_skip(text):
            review_reply = self.candidate_review_service.apply_review_response(
                candidate_id=prompt.candidate.id,
                member_id=member_id,
                resolution="skip",
            )
            return FlorenceProtocolReply(reply_text=review_reply.reply_text, consumed=True)
        if _looks_like_review_queue_request(text):
            return FlorenceProtocolReply(
                reply_text=prompt_text,
                reply_metadata={"protocol_kind": CANDIDATE_REVIEW_PROMPT_KIND},
                consumed=True,
            )
        return None

    def _render_review_prompt_text(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        prompt,
    ) -> str:
        try:
            rendered = self.household_chat_service.compose_operator_message(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                kind="review_prompt",
                payload={
                    "candidate": {
                        "title": str(getattr(prompt.candidate, "title", "") or "").strip(),
                        "summary": str(getattr(prompt.candidate, "summary", "") or "").strip(),
                        "state": str(getattr(prompt.candidate, "state", "") or "").strip(),
                        "confirmation_question": str(getattr(prompt.candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                    },
                    "source_prompt": prompt.source_prompt,
                },
            )
            if rendered is not None and rendered.strip():
                return rendered.strip()
        except Exception:
            logger.exception(
                "Florence review prompt composition failed household_id=%s channel_id=%s candidate_id=%s",
                household_id,
                channel_id,
                getattr(prompt.candidate, "id", ""),
            )
        return prompt.text

    def _is_candidate_review_reply_armed(self, *, channel_id: str, review_prompt_text: str) -> bool:
        latest_assistant = self.channel_log.latest_assistant_message(channel_id=channel_id, limit=8)
        if latest_assistant is None:
            return False
        latest_body = latest_assistant.body.strip()
        if not latest_body:
            return False
        if latest_assistant.metadata.get("protocol_kind") == CANDIDATE_REVIEW_PROMPT_KIND:
            return True
        if latest_body == review_prompt_text.strip():
            return True
        return _looks_like_candidate_review_prompt(latest_body, confirmation_suffix=self.confirmation_suffix)
