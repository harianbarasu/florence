"""Candidate review protocol for Florence DM ingress."""

from __future__ import annotations

import json
import logging

from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    PENDING_ACTION_TARGET_ID_KEY,
    build_candidate_review_prompt_metadata,
    FlorenceProtocolReply,
)
from florence.runtime.chat import _REVIEW_SHOW_PROMPT_SENTINEL

logger = logging.getLogger(__name__)


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
        prompt_armed = self._is_candidate_review_reply_armed(
            channel_id=channel_id,
            review_prompt_text=prompt_text,
        )
        decision = self.household_chat_service.compose_operator_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=member_id,
            kind="review_queue_turn",
            payload={
                "user_message": text,
                "prompt_armed": prompt_armed,
                "rendered_prompt_text": prompt_text,
                "candidate": {
                    "id": getattr(prompt.candidate, "id", ""),
                    "title": getattr(prompt.candidate, "title", ""),
                    "summary": getattr(prompt.candidate, "summary", ""),
                    "confirmation_question": (getattr(prompt.candidate, "metadata", {}) or {}).get("confirmation_question"),
                },
            },
        )
        if decision == _REVIEW_SHOW_PROMPT_SENTINEL:
            return FlorenceProtocolReply(
                reply_text=prompt_text,
                reply_metadata=build_candidate_review_prompt_metadata(prompt.candidate.id),
                consumed=True,
            )
        return None

    def is_reply_armed(
        self,
        *,
        channel_id: str,
        review_prompt_text: str,
    ) -> bool:
        return self._is_candidate_review_reply_armed(
            channel_id=channel_id,
            review_prompt_text=review_prompt_text,
        )

    def build_chat_followup_context(
        self,
        *,
        text: str,
        prompt,
    ) -> str:
        candidate = prompt.candidate
        metadata = dict(getattr(candidate, "metadata", {}) or {})
        review_item = {
            "candidate_id": getattr(candidate, "id", ""),
            "title": getattr(candidate, "title", ""),
            "summary": getattr(candidate, "summary", ""),
            "confirmation_question": metadata.get("confirmation_question"),
            "proposed_fields": metadata.get("proposed_fields"),
            "source_provenance": metadata.get("source_provenance"),
            "temporal_evidence": (metadata.get("raw_metadata") or {}).get("temporal_evidence"),
            "source_visibility": metadata.get("source_visibility"),
            "source_rule_label": metadata.get("source_rule_label"),
        }
        return (
            "Context for this turn: there is one currently surfaced private review item in this DM.\n"
            "Treat only this item as review-actionable right now. Do not act on any other hidden review items.\n"
            "If the user's reply is clearly about this item, use household_apply_candidate_review with this exact "
            "candidate_id to confirm, reject, skip, set source_visibility, or confirm with corrected fields.\n"
            "For imported Gmail items, treat source_provenance as the primary evidence. Florence may preserve light proposed_fields, but do not trust Gmail-derived times/dates unless they are clearly supported by the raw source.\n"
            "If the user is clarifying or correcting details, keep the same item in focus instead of treating it as a new request.\n"
            "Interpret the whole message yourself, including short yes/no/share/private replies; Florence no longer resolves those deterministically here.\n"
            "If the user's message is not actually about this review item, ignore the review item and just help normally.\n\n"
            f"Active review item:\n{json.dumps(review_item, ensure_ascii=True)}\n\n"
            f"User reply: {text.strip()}"
        )

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
        return self.confirmation_suffix in latest_body

    def active_candidate_id(self, *, channel_id: str) -> str | None:
        latest_assistant = self.channel_log.latest_assistant_message(channel_id=channel_id, limit=8)
        if latest_assistant is None:
            return None
        if latest_assistant.metadata.get("protocol_kind") != CANDIDATE_REVIEW_PROMPT_KIND:
            return None
        candidate_id = str(latest_assistant.metadata.get(PENDING_ACTION_TARGET_ID_KEY) or "").strip()
        return candidate_id or None
