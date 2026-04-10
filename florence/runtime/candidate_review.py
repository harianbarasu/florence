"""Private DM review helpers for imported Google candidates."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from florence.contracts import (
    CandidateState,
    GoogleSourceKind,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
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

_SHARED_CANDIDATE_SCOPE = "shared_household"
_PRIVATE_CANDIDATE_SCOPE = "private_parent"
_EVENT_REASON_TAGS = frozenset({"schedule_signal", "activity_signal", "school_source"})
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
        lines.append("Reply with 1 yes, 2 no, 3 skip, or ask me about one of them.")
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
    ) -> _CandidateReviewReply:
        normalized_candidate_ids = self._normalize_candidate_ids(candidate_ids, fallback=candidate_id)
        prefix: str | None = None
        if source_visibility is not None:
            updated_candidates = [
                self.set_candidate_source_visibility(
                    candidate_id=target_candidate_id,
                    visibility=source_visibility,
                    created_by_member_id=member_id,
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
                else f"Understood. I’ll treat future items from {source_label} as shared household context."
            )

        if resolution == "confirm":
            if len(normalized_candidate_ids) > 1:
                result = self.confirm_candidate_group(
                    candidate_ids=normalized_candidate_ids,
                    overrides=overrides,
                )
                confirmation_suffix = self._group_confirmation_suffix(result)
            else:
                result = self.confirm_candidate(candidate_id=candidate_id, overrides=overrides)
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

        if _candidate_scope(candidate) == _PRIVATE_CANDIDATE_SCOPE:
            work_item = self._candidate_to_private_work_item(candidate, overrides=overrides or {})
            self.store.upsert_household_work_item(work_item)
            confirmed_metadata = dict(candidate.metadata)
            confirmed_metadata["confirmed_work_item_id"] = work_item.id
            confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
            self.store.upsert_imported_candidate(confirmed)
            return _CandidateReviewResult(
                candidate=confirmed,
                work_item=work_item,
                work_items=(work_item,),
            )

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
            events=(event,),
            group_announcement=self._build_group_announcement(event),
        )

    def confirm_candidate_group(
        self,
        *,
        candidate_ids: list[str] | tuple[str, ...],
        overrides: dict[str, Any] | None = None,
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
                work_item = self._candidate_to_private_work_item(candidate, overrides=resolved_overrides)
                self.store.upsert_household_work_item(work_item)
                confirmed_metadata = dict(candidate.metadata)
                confirmed_metadata["confirmed_work_item_id"] = work_item.id
                confirmed_metadata["confirmed_work_item_ids"] = [work_item.id]
                confirmed = replace(candidate, state=CandidateState.CONFIRMED, metadata=confirmed_metadata)
                self.store.upsert_imported_candidate(confirmed)
                work_items.append(work_item)
                continue
            event = self._candidate_to_event(candidate, overrides=resolved_overrides)
            self.store.upsert_household_event(event)
            confirmed_metadata = dict(candidate.metadata)
            confirmed_metadata["confirmed_event_id"] = event.id
            confirmed_metadata["confirmed_event_ids"] = [event.id]
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
        lines.append("Reply yes if I should keep track of it, no if it's wrong, or skip for later.")
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
        candidates = [
            candidate
            for candidate in candidates
            if self.is_candidate_reviewable_now(candidate=candidate)
        ]
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
        candidate_date = self._candidate_local_date(candidate)
        if candidate_date is None:
            return False
        household = self.store.get_household(candidate.household_id)
        timezone_name = str(getattr(household, "timezone", "") or "").strip()
        local_today = self._utc_now().astimezone(self._tzinfo(timezone_name)).date()
        return candidate_date < local_today

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

    def _candidate_local_date(self, candidate: ImportedCandidate) -> date | None:
        metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
        proposed_fields = dict(metadata.get("proposed_fields") or {})
        household = self.store.get_household(candidate.household_id)
        timezone_name = (
            str(proposed_fields.get("timezone") or "").strip()
            or str(getattr(household, "timezone", "") or "").strip()
        )
        tz = self._tzinfo(timezone_name)

        for key in ("starts_at", "ends_at"):
            parsed = self._parse_datetime(proposed_fields.get(key), timezone_name=timezone_name)
            if parsed is not None:
                return parsed.astimezone(tz).date()

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
        return datetime.now(timezone.utc)

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

    def _candidate_to_private_work_item(
        self,
        candidate: ImportedCandidate,
        *,
        overrides: dict[str, Any],
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
                "source_candidate_id": candidate.id,
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
            group_key = self._calendar_review_group_key(candidate)
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
            return " ".join(str(candidate.summary or "").split()).strip()
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
