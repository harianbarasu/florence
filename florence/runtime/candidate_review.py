"""Private DM review helpers for imported Google candidates."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from florence.contracts import (
    CandidateState,
    GoogleSourceKind,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdProfileKind,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    ImportedCandidate,
)
from florence.runtime.household_calendar_projection import FlorenceHouseholdCalendarProjectionService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.services import _stable_id
from florence.runtime.trust_policy import record_constitution_source_preference
from florence.source_rules import (
    build_candidate_source_profile,
    build_rules_for_candidate,
    candidate_matches_source_rule,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)

_SHARED_CANDIDATE_SCOPE = "shared_household"
_PRIVATE_CANDIDATE_SCOPE = "private_parent"
_EVENT_REASON_TAGS = frozenset({"schedule_signal", "activity_signal", "school_source"})
_RELEVANCE_RULE_CATEGORY = "relevance_rule"
_RELEVANCE_GROUP_ITEM_TYPES = frozenset({"activity", "school_admin", "travel"})
_HIGH_IMPORTANCE_HINTS = frozenset(
    {
        "case",
        "charge",
        "deadline",
        "dispute",
        "due today",
        "emergency",
        "important",
        "overdue",
        "payment",
        "refund",
        "requires action",
        "today",
        "urgent",
        "venmo",
    }
)
_FINANCIAL_RECORD_HINTS = frozenset(
    {
        "receipt",
        "payment",
        "invoice",
        "bill",
        "expense",
        "charge",
        "copay",
        "refund",
        "reimbursement",
        "statement",
        "premium",
        "deductible",
    }
)


def _candidate_scope(candidate: ImportedCandidate) -> str:
    metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
    scope = str(metadata.get("candidate_scope") or "").strip().lower()
    if scope == _PRIVATE_CANDIDATE_SCOPE:
        return _PRIVATE_CANDIDATE_SCOPE
    return _SHARED_CANDIDATE_SCOPE


@dataclass(slots=True)
class _CandidateReviewPrompt:
    candidate: ImportedCandidate
    text: str
    candidates: tuple[ImportedCandidate, ...] = ()
    source_prompt: str | None = None


@dataclass(slots=True)
class _CandidateReviewResult:
    candidate: ImportedCandidate
    event: HouseholdEvent | None = None
    work_item: HouseholdWorkItem | None = None
    events: tuple[HouseholdEvent, ...] = ()
    work_items: tuple[HouseholdWorkItem, ...] = ()
    group_announcement: str | None = None


@dataclass(slots=True)
class _CandidateReviewReply:
    reply_text: str
    group_announcement: str | None = None


class _SourceRuleService:
    """Matches and persists reusable Gmail/Calendar sharing rules."""

    def __init__(self, store: FlorenceStateDB):
        self.store = store

    def apply_candidate_policy(self, candidate: ImportedCandidate) -> ImportedCandidate:
        metadata = dict(candidate.metadata)
        matched_rule = self._match_rule(candidate)
        if matched_rule is not None:
            metadata["source_visibility"] = matched_rule.visibility.value
            metadata["source_rule_id"] = matched_rule.id
            metadata["source_rule_label"] = matched_rule.label or matched_rule.matcher_value
            metadata.pop("source_rule_prompt", None)
            if matched_rule.visibility == HouseholdSourceVisibility.IGNORED:
                metadata["suppressed_reason"] = "source_rule_ignored"
                metadata["suppressed_by_source_rule_id"] = matched_rule.id
                return replace(candidate, state=CandidateState.REJECTED, metadata=metadata)
            return replace(candidate, metadata=metadata)
        metadata.pop("source_rule_prompt", None)
        return replace(candidate, metadata=metadata)

    def set_candidate_visibility(
        self,
        *,
        candidate_id: str,
        visibility: HouseholdSourceVisibility,
        created_by_member_id: str | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> ImportedCandidate:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")

        rules = self._persist_rules(
            candidate,
            visibility=visibility,
            created_by_member_id=created_by_member_id,
            review_context=review_context,
        )
        metadata = dict(candidate.metadata)
        metadata["source_visibility"] = visibility.value
        metadata["source_rule_ids"] = [rule.id for rule in rules]
        metadata["source_rule_label"] = self.describe_candidate_source(candidate) or metadata.get("source_rule_label")
        metadata.pop("source_rule_prompt", None)
        updated = replace(candidate, metadata=metadata)
        self.store.upsert_imported_candidate(updated)
        return updated

    def describe_candidate_source(self, candidate: ImportedCandidate) -> str | None:
        profile = build_candidate_source_profile(candidate)
        return profile.label if profile is not None else None

    def build_candidate_source_prompt(self, candidate: ImportedCandidate) -> str | None:
        label = self.describe_candidate_source(candidate)
        if not label:
            return None
        return (
            f"For future items from {label}, you can say private only, "
            "always share this source, or ignore this sender."
        )

    def _match_rule(self, candidate: ImportedCandidate) -> HouseholdSourceRule | None:
        for rule in self.store.list_household_source_rules(
            household_id=candidate.household_id,
            source_kind=candidate.source_kind,
        ):
            if candidate_matches_source_rule(candidate, rule):
                return rule
        return None

    def _persist_rules(
        self,
        candidate: ImportedCandidate,
        *,
        visibility: HouseholdSourceVisibility,
        created_by_member_id: str | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> list[HouseholdSourceRule]:
        created: list[HouseholdSourceRule] = []
        for rule in build_rules_for_candidate(
            candidate,
            visibility=visibility,
            created_by_member_id=created_by_member_id,
        ):
            created.append(self.store.upsert_household_source_rule(rule))
        if created:
            try:
                profile = build_candidate_source_profile(candidate)
                record_constitution_source_preference(
                    self.store,
                    household_id=candidate.household_id,
                    visibility=visibility,
                    source_label=profile.label if profile is not None else "this source",
                    source_kind=created[0].source_kind.value,
                    matcher_kind=created[0].matcher_kind.value,
                    matcher_value=created[0].matcher_value,
                    rule_ids=[rule.id for rule in created],
                    member_id=created_by_member_id,
                    channel_id=str((review_context or {}).get("channel_id") or "").strip() or None,
                    trigger=str((review_context or {}).get("resolution_source") or "source_rule_feedback"),
                )
            except Exception:
                logger.exception(
                    "Failed to update household constitution source policy household_id=%s candidate_id=%s",
                    candidate.household_id,
                    candidate.id,
                )
        return created


class FlorenceCandidateReviewService:
    """Manages the DM-only review lifecycle for imported Google candidates."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        source_rule_service: _SourceRuleService | None = None,
        now_getter: Callable[[], datetime] | None = None,
    ):
        self.store = store
        self.source_rule_service = source_rule_service or _SourceRuleService(store)
        self._now_getter = now_getter or (lambda: datetime.now(timezone.utc))

    def release_quarantined_candidates(self, *, household_id: str, member_id: str) -> list[ImportedCandidate]:
        candidates = self.store.list_imported_candidates(
            household_id=household_id,
            member_id=member_id,
            state=CandidateState.QUARANTINED,
        )
        released: list[ImportedCandidate] = []
        for candidate in candidates:
            promoted = replace(candidate, state=CandidateState.PENDING_REVIEW)
            self.store.upsert_imported_candidate(promoted)
            released.append(promoted)
        return released

    def build_next_dm_review_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        candidate_filter: Callable[[ImportedCandidate], bool] | None = None,
    ) -> _CandidateReviewPrompt | None:
        return self.build_next_review_prompt(
            household_id=household_id,
            member_id=member_id,
            candidate_filter=candidate_filter,
        )

    def build_dm_review_batch_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        candidate_filter: Callable[[ImportedCandidate], bool] | None = None,
        limit: int = 3,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
    ) -> _CandidateReviewPrompt | None:
        return self.build_review_batch_prompt(
            household_id=household_id,
            member_id=member_id,
            candidate_filter=candidate_filter,
            limit=limit,
            candidate_ids=candidate_ids,
        )

    def build_next_review_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        candidate_filter: Callable[[ImportedCandidate], bool] | None = None,
    ) -> _CandidateReviewPrompt | None:
        candidates = self._list_pending_review_candidates(
            household_id=household_id,
            member_id=member_id,
            candidate_filter=candidate_filter,
        )
        candidate = candidates[0] if candidates else None
        if candidate is None:
            return None
        return self._build_display_review_prompt(candidate)

    def build_review_batch_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        candidate_filter: Callable[[ImportedCandidate], bool] | None = None,
        limit: int = 3,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
    ) -> _CandidateReviewPrompt | None:
        candidates = self._list_pending_review_candidates(
            household_id=household_id,
            member_id=member_id,
            candidate_filter=candidate_filter,
            candidate_ids=candidate_ids,
        )
        candidates = candidates[: max(1, int(limit or 1))]
        if not candidates:
            return None
        if len(candidates) == 1:
            return self._build_display_review_prompt(candidates[0])

        lines = ["I found a few things to review:"]
        for index, candidate in enumerate(candidates, start=1):
            title = " ".join(str(candidate.title or "").split()).strip() or "Untitled item"
            summary = " ".join(str(candidate.summary or "").split()).strip()
            emoji = "👤" if _candidate_scope(candidate) == _PRIVATE_CANDIDATE_SCOPE else "🏠"
            line = f"{index}. {emoji} {title}"
            if summary and summary != title:
                line = f"{line} — {summary}"
            lines.append(line)
        lines.append(
            "Reply with 1 yes, 2 no, 3 skip, or corrections like already handled, too late, private only, always share this source, or ignore this sender."
        )
        return _CandidateReviewPrompt(
            candidate=candidates[0],
            candidates=tuple(candidates),
            text="\n".join(lines),
            source_prompt=None,
        )

    def apply_review_response(
        self,
        *,
        candidate_id: str,
        member_id: str | None,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
        source_visibility: HouseholdSourceVisibility | None = None,
        resolution: str | None = None,
        overrides: dict[str, Any] | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> _CandidateReviewReply:
        normalized_candidate_ids = self._normalize_candidate_ids(candidate_ids, fallback=candidate_id)
        prefix: str | None = None
        if source_visibility is not None:
            updated_candidates = [
                self.set_candidate_source_visibility(
                    candidate_id=target_candidate_id,
                    visibility=source_visibility,
                    created_by_member_id=member_id,
                    review_context=review_context,
                )
                for target_candidate_id in normalized_candidate_ids
            ]
            updated_candidate = updated_candidates[0]
            source_label = str(
                updated_candidate.metadata.get("source_rule_label")
                or updated_candidate.metadata.get("source_visibility_label")
                or "this source"
            )
            prefix = (
                f"Understood. I’ll keep future items from {source_label} private to your review queue."
                if source_visibility == HouseholdSourceVisibility.PRIVATE
                else f"Understood. I’ll ignore future items from {source_label}."
                if source_visibility == HouseholdSourceVisibility.IGNORED
                else f"Understood. I’ll treat future items from {source_label} as shared household context."
            )

        if resolution == "confirm":
            if len(normalized_candidate_ids) > 1:
                result = self.confirm_candidate_group(
                    candidate_ids=normalized_candidate_ids,
                    overrides=overrides,
                    review_context=review_context,
                )
                confirmation_suffix = self._group_confirmation_suffix(result)
            else:
                result = self.confirm_candidate(
                    candidate_id=candidate_id,
                    overrides=overrides,
                    review_context=review_context,
                )
                confirmation_suffix = (
                    f" Confirmed. I’ll keep track of {result.work_item.title} just for you."
                    if result.work_item is not None
                    else f" Confirmed. I added {result.event.title} to the family plan."
                    if result.event is not None
                    else " Confirmed."
                )
            if prefix:
                return _CandidateReviewReply(
                    reply_text=f"{prefix}{confirmation_suffix}",
                    group_announcement=result.group_announcement,
                )
            return _CandidateReviewReply(
                reply_text=confirmation_suffix.strip(),
                group_announcement=result.group_announcement,
            )

        if resolution == "reject":
            if len(normalized_candidate_ids) > 1:
                self.reject_candidate_group(candidate_ids=normalized_candidate_ids)
                rejection_text = "Rejected. I will leave those items out."
            else:
                self.reject_candidate(candidate_id=candidate_id)
                rejection_text = "Rejected. I will leave it out."
            if prefix:
                return _CandidateReviewReply(reply_text=f"{prefix} {rejection_text}")
            return _CandidateReviewReply(reply_text=rejection_text)

        if resolution == "skip":
            return _CandidateReviewReply(reply_text="Okay. I will leave it in your review queue for later.")

        if prefix:
            return _CandidateReviewReply(reply_text=f"{prefix} Reply yes if you want me to add this item too.")

        raise ValueError("invalid_review_response")

    def apply_feedback_response(
        self,
        *,
        candidate_id: str,
        member_id: str | None,
        feedback_kind: str,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
        source_visibility: HouseholdSourceVisibility | None = None,
        user_text: str | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> _CandidateReviewReply:
        normalized_candidate_ids = self._normalize_candidate_ids(candidate_ids, fallback=candidate_id)
        updated_candidates: list[ImportedCandidate] = []
        if source_visibility is not None:
            for target_candidate_id in normalized_candidate_ids:
                try:
                    updated_candidates.append(
                        self.set_candidate_source_visibility(
                            candidate_id=target_candidate_id,
                            visibility=source_visibility,
                            created_by_member_id=member_id,
                            review_context=review_context,
                        )
                    )
                except ValueError:
                    continue

        if feedback_kind in {"private_only", "always_share"}:
            updated_candidate = updated_candidates[0] if updated_candidates else self.store.get_imported_candidate(candidate_id)
            source_label = "this source"
            if updated_candidate is not None:
                source_label = str(
                    updated_candidate.metadata.get("source_rule_label")
                    or updated_candidate.metadata.get("source_visibility_label")
                    or self.source_rule_service.describe_candidate_source(updated_candidate)
                    or "this source"
                )
            reply_text = (
                f"Got it. I’ll keep future items from {source_label} private to your review queue."
                if feedback_kind == "private_only"
                else f"Got it. I’ll treat future items from {source_label} as shared household context."
            )
            return _CandidateReviewReply(reply_text=f"{reply_text} This item is still in review if you want to add it.")

        if feedback_kind == "always_surface":
            recorded_types: list[str] = []
            for target_candidate_id in normalized_candidate_ids:
                candidate = self.store.get_imported_candidate(target_candidate_id)
                if candidate is None:
                    continue
                item_type = self._candidate_item_type(candidate)
                recorded_types.append(item_type)
                self._record_relevance_feedback_rule(
                    candidate,
                    feedback_kind=feedback_kind,
                    member_id=member_id,
                    user_text=user_text,
                    review_context=review_context,
                    item_type=item_type,
                )
            item_type_label = self._item_type_label(recorded_types[0] if recorded_types else "items")
            return _CandidateReviewReply(
                reply_text=f"Got it. I’ll make sure {item_type_label} like this get surfaced. This item is still in review if you want to add it."
            )

        close_reasons = {
            "ignore_source": "source_ignored_by_parent",
            "ignore_item_type": "item_type_ignored_by_parent",
            "already_handled": "already_handled_by_parent",
            "stale": "stale_by_parent_feedback",
            "wrong_details": "wrong_details_by_parent",
            "duplicate": "duplicate_by_parent",
            "too_noisy": "too_noisy_by_parent",
            "wrong_timing": "wrong_timing_by_parent",
        }
        suppressed_reason = close_reasons.get(feedback_kind)
        if suppressed_reason is None:
            raise ValueError("invalid_review_feedback")

        relevance_feedback_kinds = {"ignore_item_type", "too_noisy", "wrong_timing"}
        for target_candidate_id in normalized_candidate_ids:
            candidate = self.store.get_imported_candidate(target_candidate_id)
            if candidate is None:
                continue
            item_type = self._candidate_item_type(candidate)
            if feedback_kind in relevance_feedback_kinds:
                self._record_relevance_feedback_rule(
                    candidate,
                    feedback_kind=feedback_kind,
                    member_id=member_id,
                    user_text=user_text,
                    review_context=review_context,
                    item_type=item_type,
                )
            metadata = dict(candidate.metadata)
            feedback_metadata = {
                "kind": feedback_kind,
                "member_id": member_id,
                "user_text": " ".join(str(user_text or "").split()).strip() or None,
                "item_type": item_type,
            }
            if source_visibility is not None:
                feedback_metadata["source_visibility"] = source_visibility.value
            normalized_review_context = self._normalize_review_context(review_context)
            if normalized_review_context is not None:
                feedback_metadata["review_context"] = normalized_review_context
            metadata["review_feedback"] = {
                key: value
                for key, value in feedback_metadata.items()
                if value is not None
            }
            metadata["review_feedback_kind"] = feedback_kind
            metadata["suppressed_reason"] = suppressed_reason
            self.store.upsert_imported_candidate(
                replace(candidate, state=CandidateState.REJECTED, metadata=metadata)
            )

        if feedback_kind == "ignore_source":
            source_label = "that source"
            candidate = self.store.get_imported_candidate(candidate_id)
            if candidate is not None:
                source_label = str(
                    candidate.metadata.get("source_rule_label")
                    or self.source_rule_service.describe_candidate_source(candidate)
                    or "that source"
                )
            return _CandidateReviewReply(
                reply_text=f"Got it. I’ll ignore future items from {source_label} and leave this one out."
            )
        if feedback_kind == "ignore_item_type":
            candidate = self.store.get_imported_candidate(candidate_id)
            item_type_label = self._item_type_label(self._candidate_item_type(candidate)) if candidate is not None else "items"
            return _CandidateReviewReply(
                reply_text=f"Got it. I’ll suppress future {item_type_label} like this and leave this one out."
            )
        if feedback_kind == "already_handled":
            return _CandidateReviewReply(reply_text="Got it. I marked that as already handled and left it out.")
        if feedback_kind == "stale":
            return _CandidateReviewReply(reply_text="Got it. I marked that as too late and left it out.")
        if feedback_kind == "duplicate":
            return _CandidateReviewReply(reply_text="Got it. I marked that as a duplicate and left it out.")
        if feedback_kind == "too_noisy":
            return _CandidateReviewReply(
                reply_text="Got it. I’ll be stricter about low-value items like this and leave this one out."
            )
        if feedback_kind == "wrong_timing":
            return _CandidateReviewReply(
                reply_text="Got it. I’ll adjust the timing for items like this and leave this one out."
            )
        return _CandidateReviewReply(
            reply_text="Got it. I left that item out. Send the corrected details if you want me to track it."
        )

    def set_candidate_source_visibility(
        self,
        *,
        candidate_id: str,
        visibility: HouseholdSourceVisibility,
        created_by_member_id: str | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> ImportedCandidate:
        return self.source_rule_service.set_candidate_visibility(
            candidate_id=candidate_id,
            visibility=visibility,
            created_by_member_id=created_by_member_id,
            review_context=review_context,
        )

    def confirm_candidate(
        self,
        *,
        candidate_id: str,
        overrides: dict[str, Any] | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> _CandidateReviewResult:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")

        if _candidate_scope(candidate) == _PRIVATE_CANDIDATE_SCOPE:
            work_item = self._candidate_to_private_work_item(
                candidate,
                overrides=overrides or {},
                review_context=review_context,
            )
            self.store.upsert_household_work_item(work_item)
            confirmed_metadata = dict(candidate.metadata)
            confirmed_metadata["confirmed_work_item_id"] = work_item.id
            normalized_review_context = self._normalize_review_context(review_context)
            if normalized_review_context is not None:
                confirmed_metadata["review_context"] = normalized_review_context
            confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
            self.store.upsert_imported_candidate(confirmed)
            return _CandidateReviewResult(
                candidate=confirmed,
                work_item=work_item,
                work_items=(work_item,),
            )

        event = self._candidate_to_event(
            candidate,
            overrides=overrides or {},
            review_context=review_context,
        )
        self.store.upsert_household_event(event)
        try:
            FlorenceHouseholdCalendarProjectionService(self.store).sync_household(
                household_id=candidate.household_id,
            )
        except Exception:
            logger.exception(
                "Florence household calendar projection sync failed after candidate confirmation household_id=%s candidate_id=%s",
                candidate.household_id,
                candidate_id,
            )
        confirmed_metadata = dict(candidate.metadata)
        confirmed_metadata["confirmed_event_id"] = event.id
        normalized_review_context = self._normalize_review_context(review_context)
        if normalized_review_context is not None:
            confirmed_metadata["review_context"] = normalized_review_context
        confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
        self.store.upsert_imported_candidate(confirmed)
        return _CandidateReviewResult(
            candidate=confirmed,
            event=event,
            events=(event,),
            group_announcement=self._build_group_announcement(event),
        )

    def confirm_candidate_group(
        self,
        *,
        candidate_ids: list[str] | tuple[str, ...],
        overrides: dict[str, Any] | None = None,
        review_context: dict[str, Any] | None = None,
    ) -> _CandidateReviewResult:
        normalized_candidate_ids = self._normalize_candidate_ids(candidate_ids)
        candidates = [
            self.store.get_imported_candidate(candidate_id)
            for candidate_id in normalized_candidate_ids
        ]
        candidates = [candidate for candidate in candidates if candidate is not None]
        if not candidates:
            raise ValueError("unknown_candidate")
        resolved_overrides = overrides or {}
        events: list[HouseholdEvent] = []
        work_items: list[HouseholdWorkItem] = []
        for candidate in candidates:
            if _candidate_scope(candidate) == _PRIVATE_CANDIDATE_SCOPE:
                work_item = self._candidate_to_private_work_item(
                    candidate,
                    overrides=resolved_overrides,
                    review_context=review_context,
                )
                self.store.upsert_household_work_item(work_item)
                confirmed_metadata = dict(candidate.metadata)
                confirmed_metadata["confirmed_work_item_id"] = work_item.id
                confirmed_metadata["confirmed_work_item_ids"] = [work_item.id]
                normalized_review_context = self._normalize_review_context(review_context)
                if normalized_review_context is not None:
                    confirmed_metadata["review_context"] = normalized_review_context
                confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
                self.store.upsert_imported_candidate(confirmed)
                work_items.append(work_item)
                continue
            event = self._candidate_to_event(
                candidate,
                overrides=resolved_overrides,
                review_context=review_context,
            )
            self.store.upsert_household_event(event)
            confirmed_metadata = dict(candidate.metadata)
            confirmed_metadata["confirmed_event_id"] = event.id
            confirmed_metadata["confirmed_event_ids"] = [event.id]
            normalized_review_context = self._normalize_review_context(review_context)
            if normalized_review_context is not None:
                confirmed_metadata["review_context"] = normalized_review_context
            confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
            self.store.upsert_imported_candidate(confirmed)
            events.append(event)
        if events:
            try:
                FlorenceHouseholdCalendarProjectionService(self.store).sync_household(
                    household_id=candidates[0].household_id,
                )
            except Exception:
                logger.exception(
                    "Florence household calendar projection sync failed after grouped candidate confirmation household_id=%s candidate_ids=%s",
                    candidates[0].household_id,
                    normalized_candidate_ids,
                )
        primary_candidate = self.store.get_imported_candidate(candidates[0].id) or candidates[0]
        return _CandidateReviewResult(
            candidate=primary_candidate,
            event=events[0] if events else None,
            work_item=work_items[0] if work_items else None,
            events=tuple(events),
            work_items=tuple(work_items),
            group_announcement=self._build_group_announcement(events[0]) if len(events) == 1 else None,
        )

    def reject_candidate(self, *, candidate_id: str) -> _CandidateReviewResult:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")
        rejected = replace(candidate, state=CandidateState.REJECTED)
        self.store.upsert_imported_candidate(rejected)
        return _CandidateReviewResult(candidate=rejected)

    def reject_candidate_group(self, *, candidate_ids: list[str] | tuple[str, ...]) -> None:
        for candidate_id in self._normalize_candidate_ids(candidate_ids):
            candidate = self.store.get_imported_candidate(candidate_id)
            if candidate is None:
                continue
            rejected = replace(candidate, state=CandidateState.REJECTED)
            self.store.upsert_imported_candidate(rejected)

    def _build_display_review_prompt(self, candidate: ImportedCandidate) -> _CandidateReviewPrompt:
        question = str(candidate.metadata.get("confirmation_question") or "Should I add this?")
        title = " ".join(str(candidate.title or "").split()).strip()
        summary = " ".join(str(candidate.summary or "").split()).strip()
        lines = [title or summary]
        if summary and summary != title:
            lines.append(summary)
        lines.append(question)
        if _candidate_scope(candidate) == _PRIVATE_CANDIDATE_SCOPE:
            lines.append("This would stay private to your own Florence thread.")
        source_prompt = self.source_rule_service.build_candidate_source_prompt(candidate)
        if source_prompt:
            lines.append(source_prompt)
        lines.append(
            "Reply yes if I should keep track of it, no if it's wrong, skip for later, or corrections like already handled, too late, private only, always share this source, or ignore this sender."
        )
        return _CandidateReviewPrompt(
            candidate=candidate,
            candidates=(candidate,),
            text="\n".join(line for line in lines if line),
            source_prompt=source_prompt,
        )

    def _list_pending_review_candidates(
        self,
        *,
        household_id: str,
        member_id: str,
        candidate_filter: Callable[[ImportedCandidate], bool] | None = None,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[ImportedCandidate]:
        candidates = self.store.list_imported_candidates(
            household_id=household_id,
            member_id=member_id,
            state=CandidateState.PENDING_REVIEW,
        )
        reviewable_candidates: list[ImportedCandidate] = []
        for candidate in candidates:
            if not self.is_candidate_reviewable_now(candidate=candidate):
                continue
            relevance_skip_reason = self.candidate_relevance_skip_reason(candidate)
            if relevance_skip_reason is not None:
                self.suppress_candidate_for_relevance(
                    candidate,
                    reason=relevance_skip_reason,
                    trigger="build_review_prompt",
                )
                continue
            reviewable_candidates.append(candidate)
        candidates = reviewable_candidates
        if candidate_ids is not None:
            normalized_ids = [
                str(candidate_id).strip()
                for candidate_id in list(candidate_ids)
                if str(candidate_id).strip()
            ]
            allowed_ids = set(normalized_ids)
            ordering = {candidate_id: index for index, candidate_id in enumerate(normalized_ids)}
            candidates = [
                candidate
                for candidate in candidates
                if candidate.id in allowed_ids
            ]
            candidates.sort(key=lambda candidate: ordering.get(candidate.id, len(ordering)))
        if candidate_filter is not None:
            candidates = [candidate for candidate in candidates if candidate_filter(candidate)]
        return self._display_review_candidates(
            household_id=household_id,
            candidates=candidates,
        )

    def resolve_review_group_candidate_ids(
        self,
        *,
        household_id: str,
        member_id: str,
        candidate_id: str,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[str]:
        display_candidates = self._list_pending_review_candidates(
            household_id=household_id,
            member_id=member_id,
            candidate_ids=candidate_ids,
        )
        for display_candidate in display_candidates:
            group_candidate_ids = self._review_group_candidate_ids(display_candidate)
            if candidate_id == display_candidate.id or candidate_id in group_candidate_ids:
                return group_candidate_ids
        return [candidate_id]

    def is_candidate_reviewable_now(self, *, candidate: ImportedCandidate) -> bool:
        return not self._candidate_is_stale_review_item(candidate)

    def _candidate_is_stale_review_item(self, candidate: ImportedCandidate) -> bool:
        if not self._candidate_is_time_bound_event(candidate):
            return False
        local_start = self._candidate_local_start(candidate)
        if local_start is None:
            return False
        start_at, has_clock_time, all_day = local_start
        household = self.store.get_household(candidate.household_id)
        timezone_name = str(getattr(household, "timezone", "") or "").strip()
        local_now = self._utc_now().astimezone(self._tzinfo(timezone_name))
        if has_clock_time and not all_day:
            return start_at <= local_now
        return start_at.date() < local_now.date()

    def _candidate_is_time_bound_event(self, candidate: ImportedCandidate) -> bool:
        if self._candidate_looks_financial_record(candidate):
            return False
        if getattr(candidate, "source_kind", None) == GoogleSourceKind.GOOGLE_CALENDAR:
            return True

        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        proposed_fields = dict(metadata.get("proposed_fields") or {})
        if any(str(proposed_fields.get(key) or "").strip() for key in ("starts_at", "ends_at")):
            return True
        if bool(proposed_fields.get("all_day")):
            return True

        raw_metadata = dict(metadata.get("raw_metadata") or {})
        reason_tags = {
            str(tag).strip().lower()
            for tag in list(raw_metadata.get("reason_tags") or [])
            if str(tag).strip()
        }
        if "schedule_signal" not in reason_tags:
            return False
        return bool(
            reason_tags.intersection(_EVENT_REASON_TAGS - {"schedule_signal"})
            or "household_anchor" in reason_tags
            or _candidate_scope(candidate) == _PRIVATE_CANDIDATE_SCOPE
        )

    def _candidate_looks_financial_record(self, candidate: ImportedCandidate) -> bool:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        confirmation_question = str(metadata.get("confirmation_question") or "").strip()
        text = " ".join(
            bit
            for bit in (
                str(candidate.title or "").strip(),
                str(candidate.summary or "").strip(),
                confirmation_question,
            )
            if bit
        ).lower()
        return any(hint in text for hint in _FINANCIAL_RECORD_HINTS)

    def candidate_relevance_skip_reason(self, candidate: ImportedCandidate) -> str | None:
        """Return a durable household relevance reason for skipping this candidate."""

        if getattr(candidate, "state", None) != CandidateState.PENDING_REVIEW:
            return None
        if self.candidate_always_surface_reason(candidate) is not None:
            return None
        for rule in self._matching_relevance_rules(candidate):
            rule_kind = str(rule.metadata.get("rule_kind") or "").strip().lower()
            if rule_kind == "ignore_item_type":
                return "relevance_rule_ignored_item_type"
            if rule_kind == "too_noisy" and not self._candidate_is_high_importance(candidate):
                return "relevance_rule_too_noisy"
        return None

    def candidate_always_surface_reason(self, candidate: ImportedCandidate) -> str | None:
        for rule in self._matching_relevance_rules(candidate):
            if str(rule.metadata.get("rule_kind") or "").strip().lower() == "always_surface":
                return "relevance_rule_always_surface"
        return None

    def suppress_candidate_for_relevance(
        self,
        candidate: ImportedCandidate,
        *,
        reason: str,
        trigger: str | None = None,
    ) -> ImportedCandidate:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        metadata["suppressed_reason"] = reason
        metadata["review_feedback_kind"] = metadata.get("review_feedback_kind") or "relevance_rule"
        metadata["item_type"] = self._candidate_item_type(candidate)
        if trigger:
            metadata["relevance_policy_trigger"] = trigger
        suppressed = replace(candidate, state=CandidateState.REJECTED, metadata=metadata)
        self.store.upsert_imported_candidate(suppressed)
        return suppressed

    def _record_relevance_feedback_rule(
        self,
        candidate: ImportedCandidate,
        *,
        feedback_kind: str,
        member_id: str | None,
        user_text: str | None,
        review_context: dict[str, Any] | None,
        item_type: str | None = None,
    ) -> None:
        normalized_kind = str(feedback_kind or "").strip().lower()
        if normalized_kind not in {"ignore_item_type", "too_noisy", "wrong_timing", "always_surface"}:
            return
        resolved_item_type = item_type or self._candidate_item_type(candidate)
        item_type_label = self._item_type_label(resolved_item_type)
        value_by_kind = {
            "ignore_item_type": f"Suppress future {item_type_label} like this before prompting this parent.",
            "too_noisy": f"Only surface {item_type_label} like this when they are clearly important or urgent.",
            "wrong_timing": f"Adjust timing for {item_type_label} like this before prompting this parent.",
            "always_surface": f"Always surface important {item_type_label} like this to this parent.",
        }
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        normalized_review_context = self._normalize_review_context(review_context)
        rule_metadata: dict[str, object] = {
            "rule_kind": normalized_kind,
            "item_type": resolved_item_type,
            "source_kind": candidate.source_kind.value,
            "candidate_scope": _candidate_scope(candidate),
            "created_from_candidate_id": candidate.id,
            "created_from_candidate_title": candidate.title,
            "created_from_source_identifier": candidate.source_identifier,
            "raw_feedback_text": " ".join(str(user_text or "").split()).strip(),
            "review_context": normalized_review_context,
            "source_visibility": metadata.get("source_visibility"),
        }
        rule_metadata = {key: value for key, value in rule_metadata.items() if value not in (None, "")}
        FlorenceHouseholdManagerService(self.store).record_preference(
            household_id=candidate.household_id,
            label=f"Florence relevance rule: {normalized_kind}:{resolved_item_type}",
            value=value_by_kind[normalized_kind],
            category=_RELEVANCE_RULE_CATEGORY,
            member_id=member_id,
            recorded_by_member_id=member_id,
            metadata=rule_metadata,
        )

    def _matching_relevance_rules(self, candidate: ImportedCandidate) -> list[Any]:
        candidate_item_type = self._candidate_item_type(candidate)
        candidate_source_kind = getattr(candidate.source_kind, "value", str(candidate.source_kind))
        matched = []
        for item in self.store.list_household_profile_items(
            household_id=candidate.household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        ):
            metadata = dict(item.metadata) if isinstance(item.metadata, dict) else {}
            if str(metadata.get("category") or "").strip().lower() != _RELEVANCE_RULE_CATEGORY:
                continue
            if item.member_id is not None and item.member_id != candidate.member_id:
                continue
            rule_item_type = str(metadata.get("item_type") or "").strip().lower()
            if rule_item_type and rule_item_type != candidate_item_type:
                continue
            rule_source_kind = str(metadata.get("source_kind") or "").strip().lower()
            if rule_source_kind and rule_source_kind != str(candidate_source_kind).strip().lower():
                continue
            matched.append(item)
        return matched

    def _candidate_is_high_importance(self, candidate: ImportedCandidate) -> bool:
        if int(getattr(candidate, "confidence_bps", 0) or 0) >= 9_000:
            return True
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        if str(metadata.get("source_visibility") or "").strip().lower() == HouseholdSourceVisibility.SHARED.value:
            return True
        raw_metadata = dict(metadata.get("raw_metadata") or {})
        reason_tags = {
            str(tag).strip().lower()
            for tag in list(raw_metadata.get("reason_tags") or [])
            if str(tag).strip()
        }
        if {"urgent_signal", "payment_signal", "deadline_signal"}.intersection(reason_tags):
            return True
        text = self._candidate_search_text(candidate)
        return any(hint in text for hint in _HIGH_IMPORTANCE_HINTS)

    def _candidate_item_type(self, candidate: ImportedCandidate | None) -> str:
        if candidate is None:
            return "items"
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        explicit_type = str(metadata.get("item_type") or "").strip().lower()
        if explicit_type:
            return explicit_type
        raw_metadata = dict(metadata.get("raw_metadata") or {})
        explicit_raw_type = str(raw_metadata.get("item_type") or "").strip().lower()
        if explicit_raw_type:
            return explicit_raw_type
        reason_tags = {
            str(tag).strip().lower()
            for tag in list(raw_metadata.get("reason_tags") or [])
            if str(tag).strip()
        }
        text = self._candidate_search_text(candidate)
        if self._candidate_looks_financial_record(candidate) or any(
            hint in text for hint in ("venmo", "invoice", "payment", "bill", "refund", "charge")
        ):
            return "financial"
        if any(hint in text for hint in ("flight", "airport", "jfk", "lax", "travel", "trip", "boarding")):
            return "travel"
        if any(hint in text for hint in ("grocery", "groceries", "shopping", "meal", "dinner", "recipe")):
            return "meals_shopping"
        if "activity_signal" in reason_tags or any(
            hint in text for hint in ("open gym", "practice", "sports", "soccer", "music class", "booking")
        ):
            return "activity"
        if (
            bool(raw_metadata.get("sender_looks_school"))
            or "school_source" in reason_tags
            or any(
                hint in text
                for hint in (
                    "bloomz",
                    "bring",
                    "class",
                    "library",
                    "pack",
                    "school",
                    "stuffy",
                    "teacher",
                    "wear",
                    "wish",
                    "young minds",
                )
            )
        ):
            return "school_admin"
        if "schedule_signal" in reason_tags or self._candidate_is_time_bound_event(candidate):
            return "calendar"
        return "general"

    @staticmethod
    def _item_type_label(item_type: str) -> str:
        labels = {
            "activity": "activity items",
            "calendar": "calendar items",
            "financial": "financial items",
            "general": "items",
            "items": "items",
            "meals_shopping": "meal and shopping items",
            "school_admin": "school/admin items",
            "travel": "travel items",
        }
        return labels.get(str(item_type or "").strip().lower(), "items")

    @staticmethod
    def _candidate_search_text(candidate: ImportedCandidate) -> str:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        raw_metadata = dict(metadata.get("raw_metadata") or {})
        source_provenance = dict(metadata.get("source_provenance") or {})
        bits = [
            candidate.title,
            candidate.summary,
            metadata.get("confirmation_question"),
            metadata.get("from_address"),
            source_provenance.get("from_address"),
            source_provenance.get("subject"),
            raw_metadata.get("snippet"),
            raw_metadata.get("sender"),
        ]
        return " ".join(str(bit or "").lower() for bit in bits if str(bit or "").strip())

    def _candidate_local_date(self, candidate: ImportedCandidate) -> date | None:
        local_start = self._candidate_local_start(candidate)
        if local_start is not None:
            return local_start[0].date()
        return self._candidate_temporal_evidence_date(candidate)

    def _candidate_local_start(self, candidate: ImportedCandidate) -> tuple[datetime, bool, bool] | None:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        proposed_fields = dict(metadata.get("proposed_fields") or {})
        household = self.store.get_household(candidate.household_id)
        timezone_name = (
            str(proposed_fields.get("timezone") or "").strip()
            or str(getattr(household, "timezone", "") or "").strip()
        )
        tz = self._tzinfo(timezone_name)
        all_day = bool(proposed_fields.get("all_day"))

        parsed_start = self._parse_datetime(proposed_fields.get("starts_at"), timezone_name=timezone_name)
        if parsed_start is not None:
            return (parsed_start.astimezone(tz), True, all_day)

        parsed_end = self._parse_datetime(proposed_fields.get("ends_at"), timezone_name=timezone_name)
        if parsed_end is not None:
            return (parsed_end.astimezone(tz), True, all_day)

        candidate_date = self._candidate_temporal_evidence_date(candidate)
        if candidate_date is None:
            return None

        raw_metadata = dict(metadata.get("raw_metadata") or {})
        temporal_evidence = dict(raw_metadata.get("temporal_evidence") or {})
        clock_time = self._candidate_temporal_evidence_clock(temporal_evidence)
        if clock_time is not None:
            return (datetime.combine(candidate_date, clock_time, tzinfo=tz), True, all_day)
        return (datetime.combine(candidate_date, time.min, tzinfo=tz), False, all_day)

    @staticmethod
    def _candidate_temporal_evidence_clock(temporal_evidence: dict[str, Any]) -> time | None:
        time_range = dict(temporal_evidence.get("time_range") or {})
        raw_time = str(time_range.get("start") or temporal_evidence.get("single_time") or "").strip()
        if not raw_time:
            return None
        try:
            hour_text, minute_text = raw_time.split(":", 1)
            return time(hour=int(hour_text), minute=int(minute_text))
        except (TypeError, ValueError):
            logger.debug("Failed to parse candidate review clock=%s", raw_time)
            return None

    def _candidate_temporal_evidence_date(self, candidate: ImportedCandidate) -> date | None:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        raw_metadata = dict(metadata.get("raw_metadata") or {})
        temporal_evidence = dict(raw_metadata.get("temporal_evidence") or {})
        date_match = dict(temporal_evidence.get("date_match") or {})
        date_text = str(date_match.get("date") or "").strip()
        if date_text:
            try:
                return date.fromisoformat(date_text)
            except ValueError:
                logger.debug("Failed to parse candidate review date_match=%s", date_text)
        return None

    @staticmethod
    def _parse_datetime(value: object, *, timezone_name: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            logger.debug("Failed to parse candidate review datetime=%s", text)
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=FlorenceCandidateReviewService._tzinfo(timezone_name))
        return parsed

    @staticmethod
    def _tzinfo(timezone_name: str):
        try:
            return ZoneInfo(timezone_name) if timezone_name else timezone.utc
        except Exception:
            return timezone.utc

    def _utc_now(self) -> datetime:
        value = self._now_getter()
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _candidate_to_event(
        self,
        candidate: ImportedCandidate,
        *,
        overrides: dict[str, Any],
        review_context: dict[str, Any] | None = None,
    ) -> HouseholdEvent:
        proposed_fields = candidate.metadata.get("proposed_fields")
        base_fields = dict(proposed_fields) if isinstance(proposed_fields, dict) else {}
        event_fields = {**base_fields, **overrides}
        title = str(event_fields.get("title") or candidate.title).strip() or candidate.title
        starts_at = event_fields.get("starts_at")
        ends_at = event_fields.get("ends_at")
        status = (
            HouseholdEventStatus.CONFIRMED
            if isinstance(starts_at, str) and starts_at and isinstance(ends_at, str) and ends_at
            else HouseholdEventStatus.TENTATIVE
        )
        return HouseholdEvent(
            id=_stable_id("evt", candidate.household_id, candidate.id),
            household_id=candidate.household_id,
            title=title,
            starts_at=str(starts_at) if starts_at is not None else None,
            ends_at=str(ends_at) if ends_at is not None else None,
            timezone=str(event_fields.get("timezone")) if event_fields.get("timezone") is not None else None,
            all_day=bool(event_fields.get("all_day")),
            location=str(event_fields.get("location")) if event_fields.get("location") is not None else None,
            description=str(event_fields.get("description")) if event_fields.get("description") is not None else None,
            source_candidate_id=candidate.id,
            status=status,
            metadata=self._candidate_confirmation_metadata(
                candidate,
                proposed_fields=base_fields,
                overrides=overrides,
                review_context=review_context,
            ),
        )

    def _candidate_to_private_work_item(
        self,
        candidate: ImportedCandidate,
        *,
        overrides: dict[str, Any],
        review_context: dict[str, Any] | None = None,
    ) -> HouseholdWorkItem:
        proposed_fields = candidate.metadata.get("proposed_fields")
        base_fields = dict(proposed_fields) if isinstance(proposed_fields, dict) else {}
        item_fields = {**base_fields, **overrides}
        title = str(item_fields.get("title") or candidate.title).strip() or candidate.title
        description = str(item_fields.get("description") or candidate.summary).strip() or None
        starts_at = item_fields.get("starts_at")
        due_at = item_fields.get("due_at") or starts_at
        return HouseholdWorkItem(
            id=_stable_id("work", candidate.household_id, candidate.member_id, candidate.id),
            household_id=candidate.household_id,
            title=title,
            description=description,
            status=HouseholdWorkItemStatus.OPEN,
            owner_member_id=candidate.member_id,
            due_at=str(due_at) if due_at is not None else None,
            starts_at=str(starts_at) if starts_at is not None else None,
            metadata={
                "category": "private_import",
                **self._candidate_confirmation_metadata(
                    candidate,
                    proposed_fields=base_fields,
                    overrides=overrides,
                    review_context=review_context,
                ),
            },
        )

    def _candidate_confirmation_metadata(
        self,
        candidate: ImportedCandidate,
        *,
        proposed_fields: dict[str, Any],
        overrides: dict[str, Any],
        review_context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        confirmation: dict[str, Any] = {
            "source_candidate_id": candidate.id,
            "source_candidate_member_id": candidate.member_id,
            "source_kind": candidate.source_kind.value,
            "source_identifier": candidate.source_identifier,
            "candidate_scope": _candidate_scope(candidate),
            "candidate_summary": candidate.summary,
            "confirmed_at": self._utc_now().isoformat(),
            "source_provenance": metadata.get("source_provenance"),
            "candidate_raw_metadata": metadata.get("raw_metadata"),
            "proposed_fields": proposed_fields,
        }
        normalized_review_context = self._normalize_review_context(review_context)
        if normalized_review_context is not None:
            confirmation["review_context"] = normalized_review_context
        if overrides:
            confirmation["confirmation_overrides"] = dict(overrides)
        source_visibility = metadata.get("source_visibility")
        if source_visibility is not None:
            confirmation["source_visibility"] = source_visibility
        source_rule_label = metadata.get("source_rule_label")
        if source_rule_label is not None:
            confirmation["source_rule_label"] = source_rule_label
        return confirmation

    @staticmethod
    def _normalize_review_context(review_context: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(review_context, dict):
            return None
        normalized = {str(key): value for key, value in review_context.items() if value is not None}
        return normalized or None

    @staticmethod
    def _build_group_announcement(event: HouseholdEvent) -> str:
        bits = [f"Added to the family plan: {event.title}"]
        if event.starts_at:
            bits.append(f"at {event.starts_at}")
        if event.location:
            bits.append(f"at {event.location}")
        return " ".join(bits)

    @staticmethod
    def _normalize_candidate_ids(
        candidate_ids: list[str] | tuple[str, ...] | None,
        *,
        fallback: str | None = None,
    ) -> list[str]:
        normalized: list[str] = []
        for candidate_id in list(candidate_ids or []):
            cleaned = str(candidate_id).strip()
            if cleaned and cleaned not in normalized:
                normalized.append(cleaned)
        fallback_candidate_id = str(fallback or "").strip()
        if not normalized and fallback_candidate_id:
            normalized.append(fallback_candidate_id)
        return normalized

    def _display_review_candidates(
        self,
        *,
        household_id: str,
        candidates: list[ImportedCandidate],
    ) -> list[ImportedCandidate]:
        household = self.store.get_household(household_id)
        household_timezone = str(getattr(household, "timezone", "") or "").strip() or "America/Los_Angeles"
        grouped_candidates: list[list[ImportedCandidate]] = []
        grouped_by_key: dict[tuple[object, ...], list[ImportedCandidate]] = {}
        for candidate in candidates:
            group_key = self._review_group_key(candidate)
            if group_key is None:
                grouped_candidates.append([candidate])
                continue
            bucket = grouped_by_key.get(group_key)
            if bucket is None:
                bucket = [candidate]
                grouped_by_key[group_key] = bucket
                grouped_candidates.append(bucket)
            else:
                bucket.append(candidate)

        display_candidates: list[ImportedCandidate] = []
        for grouped in grouped_candidates:
            ordered_group = sorted(grouped, key=self._candidate_sort_key)
            representative = ordered_group[0]
            metadata = dict(representative.metadata) if isinstance(representative.metadata, dict) else {}
            metadata["review_group_candidate_ids"] = [candidate.id for candidate in ordered_group]
            metadata["review_group_size"] = len(ordered_group)
            display_candidates.append(
                replace(
                    representative,
                    summary=self._review_display_summary(
                        candidates=ordered_group,
                        household_timezone=household_timezone,
                    ),
                    metadata=metadata,
                )
            )
        return display_candidates

    def _review_display_summary(
        self,
        *,
        candidates: list[ImportedCandidate],
        household_timezone: str,
    ) -> str:
        candidate = candidates[0]
        if getattr(candidate, "source_kind", None) != GoogleSourceKind.GOOGLE_CALENDAR:
            summary = " ".join(str(candidate.summary or "").split()).strip()
            if len(candidates) <= 1:
                return summary
            item_type_label = self._item_type_label(self._candidate_item_type(candidate))
            if summary:
                return f"{len(candidates)} matching {item_type_label}: {summary}"
            return f"{len(candidates)} matching {item_type_label}."
        occurrences = [
            occurrence
            for occurrence in (
                self._calendar_occurrence(candidate, household_timezone=household_timezone)
                for candidate in candidates
            )
            if occurrence is not None
        ]
        if not occurrences:
            return " ".join(str(candidate.summary or "").split()).strip()
        if len(occurrences) == 1:
            return self._calendar_occurrence_label(occurrences[0])
        if self._occurrences_share_series_shape(occurrences):
            series_label = self._calendar_series_label(occurrences[0])
            dates_label = self._calendar_occurrence_dates_label(occurrences)
            return f"{series_label} — dates I found: {dates_label}"
        count = len(occurrences)
        labels = [self._calendar_occurrence_label(occurrence) for occurrence in occurrences[:2]]
        if count > 2:
            labels.append(f"{count - 2} more dates")
        return f"{count} dates: {'; '.join(label for label in labels if label)}"

    def _review_group_key(self, candidate: ImportedCandidate) -> tuple[object, ...] | None:
        calendar_group_key = self._calendar_review_group_key(candidate)
        if calendar_group_key is not None:
            return calendar_group_key
        item_type = self._candidate_item_type(candidate)
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        explicit_key = str(metadata.get("review_duplicate_key") or "").strip().lower()
        if item_type not in _RELEVANCE_GROUP_ITEM_TYPES and not explicit_key:
            return None
        normalized_text = explicit_key or self._candidate_duplicate_text(candidate)
        if not normalized_text:
            return None
        candidate_date = self._candidate_local_date(candidate)
        source_kind = getattr(candidate.source_kind, "value", str(candidate.source_kind))
        return (
            "candidate_duplicate",
            _candidate_scope(candidate),
            source_kind,
            item_type,
            candidate_date.isoformat() if candidate_date is not None else "",
            normalized_text,
        )

    @staticmethod
    def _candidate_duplicate_text(candidate: ImportedCandidate) -> str:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        source_provenance = dict(metadata.get("source_provenance") or {})
        raw_metadata = dict(metadata.get("raw_metadata") or {})
        text = str(
            source_provenance.get("subject")
            or raw_metadata.get("subject")
            or candidate.title
            or candidate.summary
            or ""
        ).strip().lower()
        text = re.sub(r"^(?:re|fw|fwd)\s*:\s*", "", text)
        text = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", "", text)
        text = re.sub(r"\b(?:mon|tue|wed|thu|fri|sat|sun)(?:day)?\b", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _calendar_review_group_key(self, candidate: ImportedCandidate) -> tuple[object, ...] | None:
        if getattr(candidate, "source_kind", None) != GoogleSourceKind.GOOGLE_CALENDAR:
            return None
        occurrence = self._calendar_occurrence(candidate, household_timezone="UTC")
        if occurrence is None:
            return None
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        source_provenance = dict(metadata.get("source_provenance") or {})
        recurring_event_id = str(
            metadata.get("recurring_event_id")
            or source_provenance.get("recurring_event_id")
            or ""
        ).strip()
        calendar_id = str(metadata.get("calendar_id") or "").strip()
        scope = _candidate_scope(candidate)
        title = " ".join(str(candidate.title or "").lower().split()).strip()
        location = " ".join(str(occurrence.get("location") or "").lower().split()).strip()
        if recurring_event_id:
            return ("calendar_recurring", scope, calendar_id, recurring_event_id)
        return (
            "calendar_series_fallback",
            scope,
            calendar_id,
            title,
            location,
            bool(occurrence["all_day"]),
            occurrence["local_start"].weekday(),
            occurrence["local_start"].hour,
            occurrence["local_start"].minute,
            occurrence["duration_minutes"],
        )

    def _calendar_occurrence(
        self,
        candidate: ImportedCandidate,
        *,
        household_timezone: str,
    ) -> dict[str, Any] | None:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        proposed_fields = dict(metadata.get("proposed_fields") or {})
        source_provenance = dict(metadata.get("source_provenance") or {})
        timezone_name = (
            str(proposed_fields.get("timezone") or "").strip()
            or str(metadata.get("timezone") or "").strip()
            or str(source_provenance.get("timezone") or "").strip()
            or household_timezone
        )
        starts_at = self._parse_datetime(proposed_fields.get("starts_at"), timezone_name=timezone_name)
        ends_at = self._parse_datetime(proposed_fields.get("ends_at"), timezone_name=timezone_name)
        if starts_at is None:
            return None
        source_zone = self._tzinfo(timezone_name)
        household_zone = self._tzinfo(household_timezone)
        source_start = starts_at.astimezone(source_zone)
        local_start = starts_at.astimezone(household_zone)
        source_end = ends_at.astimezone(source_zone) if ends_at is not None else None
        local_end = ends_at.astimezone(household_zone) if ends_at is not None else None
        location = str(proposed_fields.get("location") or source_provenance.get("location") or "").strip() or None
        return {
            "candidate_id": candidate.id,
            "title": str(candidate.title or "").strip(),
            "starts_at": starts_at,
            "ends_at": ends_at,
            "source_start": source_start,
            "source_end": source_end,
            "local_start": local_start,
            "local_end": local_end,
            "all_day": bool(proposed_fields.get("all_day")),
            "source_timezone": timezone_name,
            "source_tz_abbr": source_start.tzname() or timezone_name,
            "local_timezone": household_timezone,
            "local_tz_abbr": local_start.tzname() or household_timezone,
            "duration_minutes": int(((ends_at or starts_at) - starts_at).total_seconds() // 60),
            "location": location,
        }

    @staticmethod
    def _candidate_sort_key(candidate: ImportedCandidate) -> tuple[datetime, str]:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        proposed_fields = dict(metadata.get("proposed_fields") or {})
        timezone_name = str(proposed_fields.get("timezone") or "UTC").strip() or "UTC"
        starts_at = FlorenceCandidateReviewService._parse_datetime(
            proposed_fields.get("starts_at"),
            timezone_name=timezone_name,
        )
        return (starts_at or datetime.max.replace(tzinfo=timezone.utc), candidate.id)

    @staticmethod
    def _review_group_candidate_ids(candidate: ImportedCandidate) -> list[str]:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        group_ids = [
            str(candidate_id).strip()
            for candidate_id in list(metadata.get("review_group_candidate_ids") or [])
            if str(candidate_id).strip()
        ]
        return group_ids or [candidate.id]

    @staticmethod
    def _format_clock_label(value: datetime) -> str:
        hour = value.hour % 12 or 12
        return f"{hour}:{value.minute:02d} {'AM' if value.hour < 12 else 'PM'}"

    @staticmethod
    def _format_short_date_label(value: datetime) -> str:
        return f"{value.strftime('%b')} {value.day}"

    @staticmethod
    def _format_full_date_label(value: datetime) -> str:
        return f"{value.strftime('%a')}, {value.strftime('%b')} {value.day}"

    def _calendar_occurrence_label(self, occurrence: dict[str, Any]) -> str:
        if occurrence["all_day"]:
            return self._format_full_date_label(occurrence["local_start"])
        local_date = self._format_full_date_label(occurrence["local_start"])
        local_time = self._format_clock_label(occurrence["local_start"])
        if occurrence["local_timezone"] == occurrence["source_timezone"]:
            return f"{local_date} at {local_time}"
        source_time = self._format_clock_label(occurrence["source_start"])
        return (
            f"{local_date} at {local_time} {occurrence['local_tz_abbr']} "
            f"({source_time} {occurrence['source_tz_abbr']})"
        )

    def _occurrences_share_series_shape(self, occurrences: list[dict[str, Any]]) -> bool:
        if len(occurrences) < 2:
            return False
        first = occurrences[0]
        return all(
            occurrence["all_day"] == first["all_day"]
            and occurrence["local_start"].weekday() == first["local_start"].weekday()
            and (
                first["all_day"]
                or (
                    occurrence["local_start"].hour == first["local_start"].hour
                    and occurrence["local_start"].minute == first["local_start"].minute
                )
            )
            for occurrence in occurrences[1:]
        )

    def _calendar_series_label(self, occurrence: dict[str, Any]) -> str:
        weekday = occurrence["local_start"].strftime("%A")
        if occurrence["all_day"]:
            return f"{weekday}s"
        local_time = self._format_clock_label(occurrence["local_start"])
        if occurrence["local_timezone"] == occurrence["source_timezone"]:
            return f"{weekday}s at {local_time}"
        source_time = self._format_clock_label(occurrence["source_start"])
        return (
            f"{weekday}s at {local_time} {occurrence['local_tz_abbr']} "
            f"({source_time} {occurrence['source_tz_abbr']})"
        )

    def _calendar_occurrence_dates_label(self, occurrences: list[dict[str, Any]]) -> str:
        dates = [self._format_short_date_label(occurrence["local_start"]) for occurrence in occurrences]
        if len(dates) == 2:
            return f"{dates[0]} and {dates[1]}"
        if len(dates) == 3:
            return f"{dates[0]}, {dates[1]}, and {dates[2]}"
        if len(dates) > 3:
            return f"{dates[0]}, {dates[1]}, and {len(dates) - 2} more dates"
        return dates[0]

    def _group_confirmation_suffix(self, result: _CandidateReviewResult) -> str:
        if result.events:
            count = len(result.events)
            noun = "date" if count == 1 else "dates"
            title = result.events[0].title or "that event"
            return f" Confirmed. I added {count} {noun} for {title} to the family plan."
        if result.work_items:
            count = len(result.work_items)
            noun = "item" if count == 1 else "items"
            return f" Confirmed. I’ll keep track of {count} private {noun} for you."
        return " Confirmed."
