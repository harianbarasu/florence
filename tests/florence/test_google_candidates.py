from datetime import datetime, timezone

from florence.contracts import HouseholdContext
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
    assert decision.requires_confirmation is True
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
    assert decision.reason == "promotional_noise"


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


def test_gmail_candidate_uses_household_platform_and_child_alias_context():
    item = GmailSyncItem(
        gmail_message_id="gmail_126",
        thread_id="thread_126",
        from_address="updates@parentsquare.com",
        subject="Practice reminder",
        snippet="Aves soccer is Thursday from 4pm to 5pm",
        body_text="Please arrive early for soccer practice on September 18.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Ava"],
        child_aliases=["Aves"],
        school_platforms=["ParentSquare"],
        activity_labels=["Soccer"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["classifier"] == "gmail_heuristics_v2"
    assert decision.raw_metadata["anchor_hits"] >= 2
    assert "household_anchor" in decision.raw_metadata["reason_tags"]
    assert "schedule_signal" in decision.raw_metadata["reason_tags"]


def test_gmail_candidate_ignores_invalid_slash_dates_instead_of_crashing():
    item = GmailSyncItem(
        gmail_message_id="gmail_127",
        thread_id="thread_127",
        from_address="teacher@school.edu",
        subject="Soccer practice update",
        snippet="Schedule change",
        body_text="Ava soccer practice is 13/24 at 4pm.",
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
    assert decision.requires_confirmation is True
    assert decision.confirmation_question is not None


def test_gmail_candidate_parses_shared_meridiem_time_range_correctly():
    item = GmailSyncItem(
        gmail_message_id="gmail_127b",
        thread_id="thread_127b",
        from_address="teacher@school.edu",
        subject="Theo music class update",
        snippet="June 10 class is 3:30-4:15 PM",
        body_text="Theo music class is on June 10 from 3:30 to 4:15 PM.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo"],
        activity_labels=["Music"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.requires_confirmation is True
    assert decision.proposed_fields is not None
    assert decision.proposed_fields == {"title": "Theo music class update"}
    assert decision.raw_metadata["temporal_evidence"]["date_match"]["date"] == "2026-06-10"
    assert decision.raw_metadata["temporal_evidence"]["time_range"] == {
        "start": "15:30",
        "end": "16:15",
    }


def test_gmail_candidate_parses_single_side_meridiem_with_dash_range():
    item = GmailSyncItem(
        gmail_message_id="gmail_127c",
        thread_id="thread_127c",
        from_address="coach@school.edu",
        subject="Practice schedule",
        snippet="Thursday 3:30-4:15pm",
        body_text="Theo baseball practice is Thursday 3:30-4:15pm at North Field.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo"],
        activity_labels=["Baseball"],
        location_labels=["North Field"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.requires_confirmation is True
    assert decision.proposed_fields == {"title": "Practice schedule"}
    assert decision.raw_metadata["temporal_evidence"]["date_match"]["date"] == "2026-09-17"
    assert decision.raw_metadata["temporal_evidence"]["time_range"] == {
        "start": "15:30",
        "end": "16:15",
    }


def test_parent_calendar_candidate_uses_known_location_context():
    item = ParentCalendarSyncItem(
        google_event_id="event_126",
        title="Scrimmage",
        description="",
        location="North Field",
        html_link=None,
        starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Family calendar",
        family_member_names=[],
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Ava"],
        activity_labels=["Soccer"],
        location_labels=["North Field"],
    )

    decision = build_parent_calendar_candidate_decision(item, context=context)

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["known_location_hits"] == 1


def test_gmail_candidate_skips_newsletter_with_schedule_words_when_no_household_anchor():
    item = GmailSyncItem(
        gmail_message_id="gmail_128",
        thread_id="thread_128",
        from_address="pod@mail.scalablepod.com",
        subject="Scalable: Creators Want Their Red Carpet Moment Too",
        snippet="The schedule looks conditional. Which date or time applies?",
        body_text="Kaya & Jasmine plus little news. Episode drops Friday at 4pm.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo", "Violet"],
        school_labels=["Wish Community School", "Young Minds Preschool"],
        activity_labels=["Baseball", "Dance", "Music"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.SKIP
    assert decision.reason == "promotional_noise"


def test_gmail_candidate_accepts_non_school_sender_when_child_and_activity_match_context():
    item = GmailSyncItem(
        gmail_message_id="gmail_129",
        thread_id="thread_129",
        from_address="coach.jen@gmail.com",
        subject="Theo baseball practice moved",
        snippet="Practice is Thursday 5pm instead of Wednesday.",
        body_text="Theo baseball practice is on September 18 at 5pm.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo", "Violet"],
        activity_labels=["Baseball", "Dance"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["anchor_hits"] >= 2
    assert "household_anchor" in decision.raw_metadata["reason_tags"]
