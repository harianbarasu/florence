"""Continuous Google sync orchestration for Florence candidates."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from florence.contracts import (
    CandidateState,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdContext,
    ImportedCandidate,
)
from florence.google.types import GmailSyncItem, ParentCalendarSyncItem
from florence.relevance import (
    CandidateDecision,
    CandidateDecisionKind,
    build_gmail_candidate_decision,
    build_parent_calendar_candidate_decision,
)


@dataclass(slots=True)
class FlorenceGoogleSyncBatch:
    connection: GoogleConnection
    context: HouseholdContext
    gmail_items: list[GmailSyncItem] = field(default_factory=list)
    calendar_items: list[ParentCalendarSyncItem] = field(default_factory=list)

    @property
    def context_timezone(self) -> str:
        raw = self.connection.metadata.get("primary_calendar_timezone")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        return "America/Los_Angeles"


@dataclass(slots=True)
class FlorenceGoogleSyncResult:
    candidates: list[ImportedCandidate] = field(default_factory=list)
    skipped_count: int = 0

    @property
    def quarantined_count(self) -> int:
        return sum(1 for candidate in self.candidates if candidate.state == CandidateState.QUARANTINED)

    @property
    def pending_review_count(self) -> int:
        return sum(1 for candidate in self.candidates if candidate.state == CandidateState.PENDING_REVIEW)


def _stable_candidate_id(
    household_id: str,
    member_id: str,
    source_kind: GoogleSourceKind,
    source_identifier: str,
) -> str:
    raw = f"{household_id}:{member_id}:{source_kind.value}:{source_identifier}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:20]
    return f"cand_{digest}"


def _review_state_for_context(context: HouseholdContext) -> CandidateState:
    if context.is_grounded_for_google_matching:
        return CandidateState.PENDING_REVIEW
    return CandidateState.QUARANTINED


def _default_confirmation_question(decision: CandidateDecision) -> str:
    subject = decision.title or "this item"
    return decision.confirmation_question or f"Should I add {subject} to your household plan?"


def _gmail_candidate_summary(item: GmailSyncItem, decision: CandidateDecision) -> str:
    if decision.summary:
        return decision.summary

    bits = [
        item.from_address.strip() or None,
        (item.snippet or "").strip() or None,
    ]
    return " - ".join(bit for bit in bits if bit) or (decision.title or "Gmail candidate")


def _calendar_candidate_summary(item: ParentCalendarSyncItem, decision: CandidateDecision) -> str:
    if decision.summary:
        return decision.summary

    bits = [
        item.calendar_summary or "Google Calendar",
        item.starts_at.isoformat(),
        (item.description or "").strip() or None,
    ]
    return " - ".join(bit for bit in bits if bit)


def _build_imported_candidate(
    *,
    connection: GoogleConnection,
    context: HouseholdContext,
    source_kind: GoogleSourceKind,
    source_identifier: str,
    title: str,
    summary: str,
    decision: CandidateDecision,
    source_metadata: dict[str, object],
) -> ImportedCandidate:
    return ImportedCandidate(
        id=_stable_candidate_id(
            connection.household_id,
            connection.member_id,
            source_kind,
            source_identifier,
        ),
        household_id=connection.household_id,
        member_id=connection.member_id,
        source_kind=source_kind,
        source_identifier=source_identifier,
        title=title,
        summary=summary,
        state=_review_state_for_context(context),
        confidence_bps=decision.confidence_bps,
        requires_confirmation=True,
        metadata={
            "review_channel_type": ChannelType.PARENT_DM.value,
            "google_connection_id": connection.id,
            "connected_email": connection.email,
            "confirmation_question": _default_confirmation_question(decision),
            "source_requires_detail_confirmation": decision.requires_confirmation,
            "proposed_fields": decision.proposed_fields or {},
            "raw_metadata": decision.raw_metadata,
            **source_metadata,
        },
    )


def build_google_import_candidates(batch: FlorenceGoogleSyncBatch) -> FlorenceGoogleSyncResult:
    """Classify synced Gmail and Calendar items into Florence review candidates."""
    result = FlorenceGoogleSyncResult()

    for item in batch.gmail_items:
        decision = build_gmail_candidate_decision(item, batch.context_timezone)
        if decision.kind != CandidateDecisionKind.CANDIDATE:
            result.skipped_count += 1
            continue

        source_identifier = f"gmail:{item.gmail_message_id}"
        result.candidates.append(
            _build_imported_candidate(
                connection=batch.connection,
                context=batch.context,
                source_kind=GoogleSourceKind.GMAIL,
                source_identifier=source_identifier,
                title=decision.title or item.subject or "Untitled Gmail candidate",
                summary=_gmail_candidate_summary(item, decision),
                decision=decision,
                source_metadata={
                    "gmail_message_id": item.gmail_message_id,
                    "gmail_thread_id": item.thread_id,
                    "from_address": item.from_address,
                    "received_at": item.received_at.isoformat() if item.received_at is not None else None,
                },
            )
        )

    for item in batch.calendar_items:
        decision = build_parent_calendar_candidate_decision(item)
        if decision.kind != CandidateDecisionKind.CANDIDATE:
            result.skipped_count += 1
            continue

        source_identifier = f"google_calendar:{item.google_event_id}"
        result.candidates.append(
            _build_imported_candidate(
                connection=batch.connection,
                context=batch.context,
                source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
                source_identifier=source_identifier,
                title=decision.title or item.title or "Untitled calendar event",
                summary=_calendar_candidate_summary(item, decision),
                decision=decision,
                source_metadata={
                    "google_event_id": item.google_event_id,
                    "calendar_summary": item.calendar_summary,
                    "html_link": item.html_link,
                    "starts_at": item.starts_at.isoformat(),
                    "ends_at": item.ends_at.isoformat(),
                    "all_day": item.all_day,
                },
            )
        )

    return result
