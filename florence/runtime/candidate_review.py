"""Private DM review helpers for imported Google candidates."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any

from florence.contracts import (
    CandidateState,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    ImportedCandidate,
)
from florence.runtime.household_calendar_projection import FlorenceHouseholdCalendarProjectionService
from florence.runtime.services import _stable_id
from florence.source_rules import (
    build_candidate_source_profile,
    build_rules_for_candidate,
    candidate_matches_source_rule,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _CandidateReviewPrompt:
    candidate: ImportedCandidate
    text: str
    source_prompt: str | None = None


@dataclass(slots=True)
class _CandidateReviewResult:
    candidate: ImportedCandidate
    event: HouseholdEvent | None = None
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
            return replace(candidate, metadata=metadata)
        metadata.pop("source_rule_prompt", None)
        return replace(candidate, metadata=metadata)

    def set_candidate_visibility(
        self,
        *,
        candidate_id: str,
        visibility: HouseholdSourceVisibility,
        created_by_member_id: str | None = None,
    ) -> ImportedCandidate:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")

        rules = self._persist_rules(candidate, visibility=visibility, created_by_member_id=created_by_member_id)
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
        _ = candidate
        return None

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
    ) -> list[HouseholdSourceRule]:
        created: list[HouseholdSourceRule] = []
        for rule in build_rules_for_candidate(
            candidate,
            visibility=visibility,
            created_by_member_id=created_by_member_id,
        ):
            created.append(self.store.upsert_household_source_rule(rule))
        return created


class FlorenceCandidateReviewService:
    """Manages the DM-only review lifecycle for imported Google candidates."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        source_rule_service: _SourceRuleService | None = None,
    ):
        self.store = store
        self.source_rule_service = source_rule_service or _SourceRuleService(store)

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

    def build_next_dm_review_prompt(self, *, household_id: str, member_id: str) -> _CandidateReviewPrompt | None:
        return self.build_next_review_prompt(household_id=household_id, member_id=member_id)

    def build_next_review_prompt(self, *, household_id: str, member_id: str) -> _CandidateReviewPrompt | None:
        return self._build_review_prompt(household_id=household_id, member_id=member_id)

    def apply_review_response(
        self,
        *,
        candidate_id: str,
        member_id: str | None,
        source_visibility: HouseholdSourceVisibility | None = None,
        resolution: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> _CandidateReviewReply:
        prefix: str | None = None
        if source_visibility is not None:
            updated_candidate = self.set_candidate_source_visibility(
                candidate_id=candidate_id,
                visibility=source_visibility,
                created_by_member_id=member_id,
            )
            source_label = str(
                updated_candidate.metadata.get("source_rule_label")
                or updated_candidate.metadata.get("source_visibility_label")
                or "this source"
            )
            prefix = (
                f"Understood. I’ll keep future items from {source_label} private to your review queue."
                if source_visibility == HouseholdSourceVisibility.PRIVATE
                else f"Understood. I’ll treat future items from {source_label} as shared household context."
            )

        if resolution == "confirm":
            result = self.confirm_candidate(candidate_id=candidate_id, overrides=overrides)
            if prefix:
                suffix = (
                    f" Confirmed. I added {result.event.title} to the family plan."
                    if result.event
                    else " Confirmed."
                )
                return _CandidateReviewReply(
                    reply_text=f"{prefix}{suffix}",
                    group_announcement=result.group_announcement,
                )
            return _CandidateReviewReply(
                reply_text=(
                    f"Confirmed. I added {result.event.title} to the family plan."
                    if result.event
                    else "Confirmed."
                ),
                group_announcement=result.group_announcement,
            )

        if resolution == "reject":
            self.reject_candidate(candidate_id=candidate_id)
            if prefix:
                return _CandidateReviewReply(reply_text=f"{prefix} I left this item out.")
            return _CandidateReviewReply(reply_text="Rejected. I will leave it out.")

        if resolution == "skip":
            return _CandidateReviewReply(reply_text="Okay. I will leave it in your review queue for later.")

        if prefix:
            return _CandidateReviewReply(reply_text=f"{prefix} Reply yes if you want me to add this item too.")

        raise ValueError("invalid_review_response")

    def set_candidate_source_visibility(
        self,
        *,
        candidate_id: str,
        visibility: HouseholdSourceVisibility,
        created_by_member_id: str | None = None,
    ) -> ImportedCandidate:
        return self.source_rule_service.set_candidate_visibility(
            candidate_id=candidate_id,
            visibility=visibility,
            created_by_member_id=created_by_member_id,
        )

    def confirm_candidate(
        self,
        *,
        candidate_id: str,
        overrides: dict[str, Any] | None = None,
    ) -> _CandidateReviewResult:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")

        event = self._candidate_to_event(candidate, overrides=overrides or {})
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
        confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
        self.store.upsert_imported_candidate(confirmed)
        return _CandidateReviewResult(
            candidate=confirmed,
            event=event,
            group_announcement=self._build_group_announcement(event),
        )

    def reject_candidate(self, *, candidate_id: str) -> _CandidateReviewResult:
        candidate = self.store.get_imported_candidate(candidate_id)
        if candidate is None:
            raise ValueError("unknown_candidate")
        rejected = replace(candidate, state=CandidateState.REJECTED)
        self.store.upsert_imported_candidate(rejected)
        return _CandidateReviewResult(candidate=rejected)

    def _build_review_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
    ) -> _CandidateReviewPrompt | None:
        candidates = self.store.list_imported_candidates(
            household_id=household_id,
            member_id=member_id,
            state=CandidateState.PENDING_REVIEW,
        )
        candidate = candidates[0] if candidates else None
        if candidate is None:
            return None
        question = str(candidate.metadata.get("confirmation_question") or "Should I add this?")
        title = " ".join(str(candidate.title or "").split()).strip()
        summary = " ".join(str(candidate.summary or "").split()).strip()
        lines = [title or summary]
        if summary and summary != title:
            lines.append(summary)
        lines.append(question)
        source_prompt = self.source_rule_service.build_candidate_source_prompt(candidate)
        if source_prompt:
            lines.append(source_prompt)
        lines.append("Reply yes if I should add it, no if it's wrong, or skip for later.")
        return _CandidateReviewPrompt(
            candidate=candidate,
            text="\n".join(line for line in lines if line),
            source_prompt=source_prompt,
        )

    def _candidate_to_event(self, candidate: ImportedCandidate, *, overrides: dict[str, Any]) -> HouseholdEvent:
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
            metadata={
                "source_kind": candidate.source_kind.value,
                "source_identifier": candidate.source_identifier,
                "candidate_summary": candidate.summary,
                "source_provenance": candidate.metadata.get("source_provenance"),
                "candidate_raw_metadata": candidate.metadata.get("raw_metadata"),
            },
        )

    @staticmethod
    def _build_group_announcement(event: HouseholdEvent) -> str:
        bits = [f"Added to the family plan: {event.title}"]
        if event.starts_at:
            bits.append(f"at {event.starts_at}")
        if event.location:
            bits.append(f"at {event.location}")
        return " ".join(bits)
