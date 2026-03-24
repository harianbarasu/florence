from datetime import datetime, timezone

from florence.google import GmailSyncItem, ParentCalendarSyncItem
from florence.relevance import (
    CandidateDecisionKind,
    build_gmail_candidate_decision,
    build_parent_calendar_candidate_decision,
)


def test_gmail_candidate_detects_school_logistics_event():
    item = GmailSyncItem(
        gmail_message_id="gmail_123",
        thread_id="thread_123",
        from_address="teacher@school.edu",
        subject="Soccer practice update",
        snippet="Practice moves to Thursday 4pm to 5pm",
        body_text="Ava soccer practice is on September 18 from 4pm to 5pm.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.requires_confirmation is False
    assert decision.proposed_fields is not None
    assert decision.proposed_fields["title"] == "Soccer practice update"


def test_gmail_candidate_skips_unrelated_email():
    item = GmailSyncItem(
        gmail_message_id="gmail_124",
        thread_id="thread_124",
        from_address="news@example.com",
        subject="Weekend sale now on",
        snippet="Save 20 percent on patio furniture",
        body_text="Shop patio furniture and decor this weekend only.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    decision = build_gmail_candidate_decision(item, "America/Los_Angeles")

    assert decision.kind == CandidateDecisionKind.SKIP
    assert decision.reason == "not_school_logistics"


def test_parent_calendar_candidate_detects_child_activity():
    item = ParentCalendarSyncItem(
        google_event_id="event_123",
        title="Ava soccer practice",
        description="Weekly team practice on the north field",
        location="North field",
        html_link=None,
        starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Family calendar",
        family_member_names=["Ava"],
    )

    decision = build_parent_calendar_candidate_decision(item)

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.proposed_fields is not None
    assert decision.proposed_fields["title"] == "Ava soccer practice"


def test_parent_calendar_candidate_skips_personal_meeting():
    item = ParentCalendarSyncItem(
        google_event_id="event_124",
        title="Client meeting",
        description="Quarterly planning Zoom",
        location="Zoom",
        html_link=None,
        starts_at=datetime(2026, 9, 18, 16, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 18, 17, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Personal calendar",
        family_member_names=["Ava"],
    )

    decision = build_parent_calendar_candidate_decision(item)

    assert decision.kind == CandidateDecisionKind.SKIP
    assert decision.reason == "not_child_or_family_logistics"
