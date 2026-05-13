"""Candidate review protocol for Florence DM ingress."""

from __future__ import annotations

import json
import logging

from florence.contracts import CandidateState, HouseholdSourceVisibility
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
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.review_feedback import (
    ParsedReviewFeedback,
    ReviewFeedbackKind,
    parse_review_feedback,
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
        deterministic_reply = self._handle_deterministic_feedback(
            household_id=household_id,
            channel_id=channel_id,
            member_id=member_id,
            text=text,
            prompt=prompt,
            prompt_armed=prompt_armed,
        )
        if deterministic_reply is not None:
            return deterministic_reply
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
                "Review mini-playbook: suppress stale, already-handled, duplicate, and noisy email/import items; surface only actionable household items.\n"
                "If the user replies with numbered decisions like 1 yes, 2 no, 3 skip, apply household_apply_candidate_review to those exact candidate_ids.\n"
                "You may handle multiple numbered items from one user reply.\n"
                "If the user does not use numbers but clearly refers to exactly one listed item by title, sender, place, child, or date, treat that as the targeted review item instead of asking them to restate it mechanically.\n"
                "A reply like 'Zimmi is correct but it's at 7 PM EST' should be treated as confirm-with-correction for the uniquely referenced Zimmi item.\n"
                "If a listed item includes related_candidate_ids, that visible line represents a grouped recurring series, so one confirm/reject/skip should apply to the whole grouped item.\n"
                "If the user says plain yes, no, or skip without a number and multiple items are active, ask which number they mean instead of guessing.\n"
                "If a review item has candidate_scope private_parent, a confirm should keep it in this parent's private lane, not promote it to shared household state.\n"
                "When calling household_apply_candidate_review, include pending_action_id from Active pending action if present.\n"
                "Obvious feedback like already handled, too late, private only, always share this source, or ignore this sender is handled deterministically before this Hermes fallback.\n"
                "Interpret the whole remaining message yourself, including corrections.\n"
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
            "Review mini-playbook: suppress stale, already-handled, duplicate, and noisy email/import items; surface only actionable household items.\n"
            "If the user's reply is clearly about this item, use household_apply_candidate_review with this exact "
            "candidate_id to confirm, reject, skip, set source_visibility, or confirm with corrected fields.\n"
            "When calling household_apply_candidate_review, include pending_action_id from Active pending action if present.\n"
            "If candidate_scope is private_parent, a confirm should keep it in this parent's private lane, not promote it to shared household state.\n"
            "For imported Gmail items, treat source_provenance as the primary evidence. Florence may preserve light proposed_fields, but do not trust Gmail-derived times/dates unless they are clearly supported by the raw source.\n"
            "If the user is clarifying or correcting details, keep the same item in focus instead of treating it as a new request.\n"
            "Obvious feedback like already handled, too late, private only, always share this source, or ignore this sender is handled deterministically before this Hermes fallback.\n"
            "Interpret the whole remaining message yourself, including short yes/no replies and corrections.\n"
            "If the user's message is not actually about this review item, ignore the review item and just help normally.\n\n"
            f"Active pending action:\n{json.dumps(pending_action_context, ensure_ascii=True)}\n\n"
            f"Active review item:\n{json.dumps(review_item, ensure_ascii=True)}\n\n"
            f"User reply: {text.strip()}"
        )

    def _handle_deterministic_feedback(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        text: str,
        prompt,
        prompt_armed: bool,
    ) -> FlorenceProtocolReply | None:
        if not prompt_armed:
            return None
        feedback = parse_review_feedback(text)
        if feedback is None:
            return None
        candidates = tuple(getattr(prompt, "candidates", ()) or (getattr(prompt, "candidate", None),))
        candidates = tuple(candidate for candidate in candidates if candidate is not None)
        if not candidates:
            return None
        target_candidate = self._feedback_target_candidate(feedback=feedback, candidates=candidates)
        if target_candidate is None:
            return None
        candidate_ids = self._candidate_action_ids(target_candidate)
        active_action = latest_active_pending_action(
            self.channel_log.recent_messages(channel_id=channel_id, limit=8)
        )
        review_context = {
            "resolution_source": "deterministic_feedback",
            "feedback_kind": feedback.kind.value,
            "actor_member_id": member_id,
            "channel_id": channel_id,
            "pending_action_id": active_action.id if active_action is not None else None,
            "pending_action_message_id": active_action.message_id if active_action is not None else None,
        }
        if feedback.kind in {
            ReviewFeedbackKind.IGNORE_SOURCE,
            ReviewFeedbackKind.IGNORE_ITEM_TYPE,
            ReviewFeedbackKind.ALREADY_HANDLED,
            ReviewFeedbackKind.STALE,
            ReviewFeedbackKind.WRONG_DETAILS,
            ReviewFeedbackKind.DUPLICATE,
            ReviewFeedbackKind.TOO_NOISY,
            ReviewFeedbackKind.WRONG_TIMING,
            ReviewFeedbackKind.PRIVATE_ONLY,
            ReviewFeedbackKind.ALWAYS_SHARE,
            ReviewFeedbackKind.ALWAYS_SURFACE,
        }:
            source_visibility = self._feedback_source_visibility(feedback.kind)
            reply = self.candidate_review_service.apply_feedback_response(
                candidate_id=getattr(target_candidate, "id", ""),
                member_id=member_id,
                candidate_ids=candidate_ids,
                feedback_kind=feedback.kind.value,
                source_visibility=source_visibility,
                user_text=text,
                review_context=review_context,
            )
            return FlorenceProtocolReply(
                reply_text=reply.reply_text,
                reply_metadata={
                    "review_feedback_kind": feedback.kind.value,
                    "review_feedback_candidate_ids": list(candidate_ids),
                    "source_visibility": source_visibility.value if source_visibility is not None else None,
                    "pending_action_id": active_action.id if active_action is not None else None,
                },
                group_announcement=reply.group_announcement,
                consumed=True,
            )
        if feedback.kind in {
            ReviewFeedbackKind.LESS_PROACTIVE,
            ReviewFeedbackKind.MORE_PROACTIVE,
            ReviewFeedbackKind.DISABLE_MODULE,
        }:
            preference = self._record_feedback_preference(
                household_id=household_id,
                channel_id=channel_id,
                member_id=member_id,
                feedback=feedback,
            )
            return FlorenceProtocolReply(
                reply_text="Got it. I’ll remember that preference for future Florence prompts.",
                reply_metadata={
                    "review_feedback_kind": feedback.kind.value,
                    "recorded_preference_id": preference.id,
                    "pending_action_id": active_action.id if active_action is not None else None,
                },
                consumed=True,
            )
        return None

    @staticmethod
    def _feedback_target_candidate(
        *,
        feedback: ParsedReviewFeedback,
        candidates: tuple[object, ...],
    ):
        if len(candidates) == 1:
            return candidates[0]
        if feedback.target_index is None:
            return None
        index = feedback.target_index - 1
        if index < 0 or index >= len(candidates):
            return None
        return candidates[index]

    @staticmethod
    def _candidate_action_ids(candidate) -> list[str]:
        metadata = dict(getattr(candidate, "metadata", {}) or {})
        group_ids = [
            str(candidate_id).strip()
            for candidate_id in list(metadata.get("review_group_candidate_ids") or [])
            if str(candidate_id).strip()
        ]
        return group_ids or [str(getattr(candidate, "id", "") or "").strip()]

    @staticmethod
    def _feedback_source_visibility(kind: ReviewFeedbackKind) -> HouseholdSourceVisibility | None:
        if kind == ReviewFeedbackKind.PRIVATE_ONLY:
            return HouseholdSourceVisibility.PRIVATE
        if kind == ReviewFeedbackKind.ALWAYS_SHARE:
            return HouseholdSourceVisibility.SHARED
        if kind == ReviewFeedbackKind.IGNORE_SOURCE:
            return HouseholdSourceVisibility.IGNORED
        return None

    def _record_feedback_preference(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str,
        feedback: ParsedReviewFeedback,
    ):
        if feedback.kind == ReviewFeedbackKind.LESS_PROACTIVE:
            label = "Florence proactivity"
            value = "Be less proactive with review prompts and only interrupt for clearly useful, timely items."
        elif feedback.kind == ReviewFeedbackKind.MORE_PROACTIVE:
            label = "Florence proactivity"
            value = "Be more proactive about flagging relevant household items sooner."
        else:
            module = feedback.module_hint or "requested module"
            label = f"Disabled module: {module}"
            value = f"Do not proactively run or suggest the {module} module unless a parent asks for it."
        return FlorenceHouseholdManagerService(self.candidate_review_service.store).record_preference(
            household_id=household_id,
            label=label,
            value=value,
            category="automation_boundary" if feedback.kind == ReviewFeedbackKind.DISABLE_MODULE else "operating_preference",
            recorded_by_member_id=member_id,
            channel_id=channel_id,
            metadata={
                "review_feedback_kind": feedback.kind.value,
                "raw_feedback_text": feedback.raw_text,
                "module_hint": feedback.module_hint,
            },
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
