"""Candidate review protocol for Florence DM ingress."""

from __future__ import annotations

import json
import logging

from florence.contracts import CandidateState
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.pending_actions import (
    active_pending_target_ids,
    latest_assistant_protocol_message,
    latest_pending_action,
)
from florence.messaging.protocol_sentinels import REVIEW_SHOW_PROMPT_SENTINEL
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    PENDING_ACTION_TARGET_ID_KEY,
    PENDING_ACTION_TARGET_IDS_KEY,
    build_candidate_review_prompt_metadata,
    FlorenceProtocolReply,
    latest_active_pending_action,
    pending_action_to_model_context,
)

logger = logging.getLogger(__name__)


class FlorenceCandidateReviewProtocol:
    """Review queue handling for DM protocol turns."""

    def __init__(
        self,
        *,
        channel_log: FlorenceChannelLog,
        candidate_review_service,
        household_chat_service,
    ) -> None:
        self.channel_log = channel_log
        self.candidate_review_service = candidate_review_service
        self.household_chat_service = household_chat_service

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
        active_candidate_ids = self.active_candidate_ids(channel_id=channel_id)
        prompt = None
        if active_candidate_ids:
            active_candidates = [
                self.candidate_review_service.store.get_imported_candidate(candidate_id)
                for candidate_id in active_candidate_ids
            ]
            active_candidates = [
                candidate
                for candidate in active_candidates
                if (
                    candidate is not None
                    and candidate.household_id == household_id
                    and candidate.member_id == member_id
                    and candidate.state == CandidateState.PENDING_REVIEW
                )
            ]
            if active_candidates:
                prompt = self.candidate_review_service.build_dm_review_batch_prompt(
                    household_id=household_id,
                    member_id=member_id,
                    candidate_ids=[candidate.id for candidate in active_candidates],
                )
        if prompt is None:
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
                "review_items": [
                    {
                        "id": getattr(candidate, "id", ""),
                        "title": getattr(candidate, "title", ""),
                        "summary": getattr(candidate, "summary", ""),
                        "confirmation_question": (getattr(candidate, "metadata", {}) or {}).get("confirmation_question"),
                        "candidate_scope": (getattr(candidate, "metadata", {}) or {}).get("candidate_scope"),
                    }
                    for candidate in (getattr(prompt, "candidates", ()) or (getattr(prompt, "candidate", None),))
                    if candidate is not None
                ],
                "candidate": {
                    "id": getattr(prompt.candidate, "id", ""),
                    "title": getattr(prompt.candidate, "title", ""),
                    "summary": getattr(prompt.candidate, "summary", ""),
                    "confirmation_question": (getattr(prompt.candidate, "metadata", {}) or {}).get("confirmation_question"),
                    "candidate_scope": (getattr(prompt.candidate, "metadata", {}) or {}).get("candidate_scope"),
                },
            },
        )
        if decision == REVIEW_SHOW_PROMPT_SENTINEL:
            return FlorenceProtocolReply(
                reply_text=prompt_text,
                reply_metadata=build_candidate_review_prompt_metadata(
                    prompt.candidate.id,
                    candidate_ids=self._prompt_target_ids(prompt),
                ),
                consumed=True,
            )
        return None

    def is_reply_armed(
        self,
        *,
        channel_id: str,
    ) -> bool:
        return self._is_candidate_review_reply_armed(
            channel_id=channel_id,
        )

    def build_chat_followup_context(
        self,
        *,
        channel_id: str,
        text: str,
        prompt,
    ) -> str:
        active_action = latest_active_pending_action(
            self.channel_log.recent_messages(channel_id=channel_id, limit=8)
        )
        pending_action_context = (
            pending_action_to_model_context(active_action)
            if active_action is not None
            else None
        )
        candidates = tuple(getattr(prompt, "candidates", ()) or ())
        if len(candidates) > 1:
            review_items = []
            for index, candidate in enumerate(candidates, start=1):
                metadata = dict(getattr(candidate, "metadata", {}) or {})
                review_items.append(
                    {
                        "index": index,
                        "candidate_id": getattr(candidate, "id", ""),
                        "title": getattr(candidate, "title", ""),
                        "summary": getattr(candidate, "summary", ""),
                        "confirmation_question": metadata.get("confirmation_question"),
                        "proposed_fields": metadata.get("proposed_fields"),
                        "source_provenance": metadata.get("source_provenance"),
                        "temporal_evidence": (metadata.get("raw_metadata") or {}).get("temporal_evidence"),
                        "related_candidate_ids": metadata.get("review_group_candidate_ids"),
                        "source_visibility": metadata.get("source_visibility"),
                        "source_rule_label": metadata.get("source_rule_label"),
                        "candidate_scope": metadata.get("candidate_scope"),
                    }
                )
            return (
                "Context for this turn: there is one currently surfaced private review batch in this DM.\n"
                "Treat only the numbered review items listed here as actionable right now. Do not act on any other hidden review items.\n"
                "If the user replies with numbered decisions like 1 yes, 2 no, 3 skip, apply household_apply_candidate_review to those exact candidate_ids.\n"
                "You may handle multiple numbered items from one user reply.\n"
                "If the user does not use numbers but clearly refers to exactly one listed item by title, sender, place, child, or date, treat that as the targeted review item instead of asking them to restate it mechanically.\n"
                "A reply like 'Zimmi is correct but it's at 7 PM EST' should be treated as confirm-with-correction for the uniquely referenced Zimmi item.\n"
                "If a listed item includes related_candidate_ids, that visible line represents a grouped recurring series, so one confirm/reject/skip should apply to the whole grouped item.\n"
                "If the user says plain yes, no, or skip without a number and multiple items are active, ask which number they mean instead of guessing.\n"
                "If a review item has candidate_scope private_parent, a confirm should keep it in this parent's private lane, not promote it to shared household state.\n"
                "When calling household_apply_candidate_review, include pending_action_id from Active pending action if present.\n"
                "Interpret the whole message yourself, including short numbered replies and corrections.\n"
                "If the user's message is not actually about this review batch, ignore the review batch and just help normally.\n\n"
                f"Active pending action:\n{json.dumps(pending_action_context, ensure_ascii=True)}\n\n"
                f"Active review batch:\n{json.dumps(review_items, ensure_ascii=True)}\n\n"
                f"User reply: {text.strip()}"
            )

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
            "related_candidate_ids": metadata.get("review_group_candidate_ids"),
            "source_visibility": metadata.get("source_visibility"),
            "source_rule_label": metadata.get("source_rule_label"),
            "candidate_scope": metadata.get("candidate_scope"),
        }
        return (
            "Context for this turn: there is one currently surfaced private review item in this DM.\n"
            "Treat only this item as review-actionable right now. Do not act on any other hidden review items.\n"
            "If the user's reply is clearly about this item, use household_apply_candidate_review with this exact "
            "candidate_id to confirm, reject, skip, set source_visibility, or confirm with corrected fields.\n"
            "When calling household_apply_candidate_review, include pending_action_id from Active pending action if present.\n"
            "If candidate_scope is private_parent, a confirm should keep it in this parent's private lane, not promote it to shared household state.\n"
            "For imported Gmail items, treat source_provenance as the primary evidence. Florence may preserve light proposed_fields, but do not trust Gmail-derived times/dates unless they are clearly supported by the raw source.\n"
            "If the user is clarifying or correcting details, keep the same item in focus instead of treating it as a new request.\n"
            "Interpret the whole message yourself, including short yes/no/share/private replies; Florence no longer resolves those deterministically here.\n"
            "If the user's message is not actually about this review item, ignore the review item and just help normally.\n\n"
            f"Active pending action:\n{json.dumps(pending_action_context, ensure_ascii=True)}\n\n"
            f"Active review item:\n{json.dumps(review_item, ensure_ascii=True)}\n\n"
            f"User reply: {text.strip()}"
        )

    @staticmethod
    def _prompt_target_ids(prompt) -> list[str]:
        normalized: list[str] = []
        for candidate in tuple(getattr(prompt, "candidates", ()) or ()):
            metadata = dict(getattr(candidate, "metadata", {}) or {})
            group_ids = [
                str(candidate_id).strip()
                for candidate_id in list(metadata.get("review_group_candidate_ids") or [])
                if str(candidate_id).strip()
            ]
            if not group_ids:
                group_ids = [str(getattr(candidate, "id", "") or "").strip()]
            for candidate_id in group_ids:
                if candidate_id and candidate_id not in normalized:
                    normalized.append(candidate_id)
        return normalized

    def _render_review_prompt_text(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        prompt,
    ) -> str:
        if self._prompt_should_use_raw_text(prompt):
            return prompt.text
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
                        "candidate_scope": str(getattr(prompt.candidate, "metadata", {}).get("candidate_scope") or "").strip(),
                    },
                    "items": [
                        {
                            "title": str(getattr(candidate, "title", "") or "").strip(),
                            "summary": str(getattr(candidate, "summary", "") or "").strip(),
                            "confirmation_question": str(getattr(candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                            "candidate_scope": str(getattr(candidate, "metadata", {}).get("candidate_scope") or "").strip(),
                        }
                        for candidate in tuple(getattr(prompt, "candidates", ()) or ())
                    ],
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

    @staticmethod
    def _prompt_should_use_raw_text(prompt) -> bool:
        candidates = tuple(getattr(prompt, "candidates", ()) or ())
        if len(candidates) != 1:
            return False
        candidate = candidates[0]
        metadata = dict(getattr(candidate, "metadata", {}) or {})
        group_ids = [
            str(candidate_id).strip()
            for candidate_id in list(metadata.get("review_group_candidate_ids") or [])
            if str(candidate_id).strip()
        ]
        summary = str(getattr(candidate, "summary", "") or "").strip()
        title = str(getattr(candidate, "title", "") or "").strip()
        is_calendar_candidate = str(getattr(candidate, "source_kind", "") or "") == "google_calendar"
        return bool(group_ids and len(group_ids) > 1) or (
            is_calendar_candidate and bool(summary) and summary != title
        )

    def _is_candidate_review_reply_armed(self, *, channel_id: str) -> bool:
        return latest_assistant_protocol_message(
            self.channel_log,
            channel_id=channel_id,
            protocol_kind=CANDIDATE_REVIEW_PROMPT_KIND,
        ) is not None

    def active_candidate_id(self, *, channel_id: str) -> str | None:
        candidate_ids = self.active_candidate_ids(channel_id=channel_id)
        return candidate_ids[0] if candidate_ids else None

    def active_candidate_ids(self, *, channel_id: str) -> list[str]:
        active = latest_pending_action(
            self.channel_log,
            channel_id=channel_id,
            protocol_kind=CANDIDATE_REVIEW_PROMPT_KIND,
            action_type="candidate_review",
            target_kind="imported_candidate",
        )
        candidate_ids = active_pending_target_ids(active)
        if candidate_ids:
            return candidate_ids
        latest_assistant = latest_assistant_protocol_message(
            self.channel_log,
            channel_id=channel_id,
            protocol_kind=CANDIDATE_REVIEW_PROMPT_KIND,
        )
        if latest_assistant is None:
            return []
        raw_candidate_ids = latest_assistant.metadata.get(PENDING_ACTION_TARGET_IDS_KEY)
        if not isinstance(raw_candidate_ids, (list, tuple)):
            raw_candidate_ids = []
        candidate_ids = [
            str(candidate_id).strip()
            for candidate_id in list(raw_candidate_ids)
            if str(candidate_id).strip()
        ]
        if candidate_ids:
            return candidate_ids
        candidate_id = str(latest_assistant.metadata.get(PENDING_ACTION_TARGET_ID_KEY) or "").strip()
        return [candidate_id] if candidate_id else []
