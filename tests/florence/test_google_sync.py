from datetime import datetime, timezone

from florence.contracts import CandidateState, GoogleConnection, GoogleSourceKind, HouseholdContext
from florence.google import (
    FlorenceGoogleSyncBatch,
    GmailSyncItem,
    ParentCalendarSyncItem,
    build_google_grounding_hints,
    build_google_import_candidates,
)


def test_google_sync_quarantines_candidates_until_household_grounding_is_complete():
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Ava"],
        school_labels=[],
        activity_labels=[],
    )
    gmail_item = GmailSyncItem(
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

    result = build_google_import_candidates(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=context,
            gmail_items=[gmail_item],
        )
    )

    assert result.skipped_count == 0
    assert result.quarantined_count == 1
    assert result.pending_review_count == 0
    assert result.candidates[0].state == CandidateState.QUARANTINED
    assert result.candidates[0].requires_confirmation is True


def test_google_sync_moves_candidates_to_pending_review_once_grounded():
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Ava"],
        school_labels=["Roosevelt Elementary"],
        activity_labels=["Soccer"],
    )
    calendar_item = ParentCalendarSyncItem(
        google_event_id="event_123",
        title="Ava soccer practice",
        description="Weekly team practice on the north field",
        location="North field",
        html_link="https://calendar.google.com/event?eid=abc",
        starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Family calendar",
        family_member_names=["Ava"],
    )

    result = build_google_import_candidates(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=context,
            calendar_items=[calendar_item],
        )
    )

    assert result.skipped_count == 0
    assert result.pending_review_count == 1
    candidate = result.candidates[0]
    assert candidate.state == CandidateState.PENDING_REVIEW
    assert candidate.requires_confirmation is True
    assert candidate.metadata["review_channel_type"] == "parent_dm"
    assert candidate.metadata["google_event_id"] == "event_123"


def test_google_sync_skips_unrelated_items():
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Ava"],
        school_labels=["Roosevelt Elementary"],
        activity_labels=[],
    )
    gmail_item = GmailSyncItem(
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

    result = build_google_import_candidates(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=context,
            gmail_items=[gmail_item],
        )
    )

    assert result.skipped_count == 1
    assert result.candidates == []


def test_google_sync_keeps_known_contact_schedule_email_even_without_school_sender_keywords():
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Violet"],
        school_labels=["Young Minds Preschool"],
        activity_labels=["Musical Beginnings"],
        contact_names=["Linda"],
    )
    gmail_item = GmailSyncItem(
        gmail_message_id="gmail_126",
        thread_id="thread_126",
        from_address="Linda <linda@musicalbeginnings.com>",
        subject="Spring break and Family Day dates",
        snippet="No class April 1 and April 8.",
        body_text="For Violet's class: no class April 1 and April 8. Family Day May 6. Classes end July 1.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )

    result = build_google_import_candidates(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=context,
            gmail_items=[gmail_item],
        )
    )

    assert result.skipped_count == 0
    assert result.pending_review_count == 1
    assert result.candidates[0].title == "Spring break and Family Day dates"


def test_google_sync_skips_promotional_email_with_dates_and_schedule_words():
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Theo", "Violet"],
        school_labels=["Wish Community School"],
        activity_labels=["Baseball", "Musical Beginnings"],
    )
    gmail_item = GmailSyncItem(
        gmail_message_id="gmail_127",
        thread_id="thread_127",
        from_address="Kaya & Jasmine <pod@mail.scalablepod.com>",
        subject="Scalable: Creators Want Their Red Carpet Moment Too",
        snippet="The schedule for Scalable looks conditional. Which date or time applies?",
        body_text="Listen to the latest podcast episode and catch up on creator news, new brands, and NewFronts coverage.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )

    result = build_google_import_candidates(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=context,
            gmail_items=[gmail_item],
        )
    )

    assert result.skipped_count == 1
    assert result.candidates == []


def test_google_sync_extracts_grounding_hints_from_google_sources():
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Ava"],
    )

    hints = build_google_grounding_hints(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=context,
            gmail_items=[
                GmailSyncItem(
                    gmail_message_id="gmail_125",
                    thread_id="thread_125",
                    from_address="Ms. Kim <teacher@roosevelt.k12.ca.us>",
                    subject="Roosevelt Elementary soccer practice update",
                    snippet="ParentSquare reminder",
                    body_text="Ava soccer practice is on September 18 from 4pm to 5pm.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
            calendar_items=[
                ParentCalendarSyncItem(
                    google_event_id="event_125",
                    title="Soccer practice",
                    description="Team practice on the north field",
                    location="North Field",
                    html_link=None,
                    starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
                    ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
                    timezone="America/Los_Angeles",
                    all_day=False,
                    updated_at=None,
                    calendar_summary="Family calendar",
                    family_member_names=["Ava"],
                )
            ],
        )
    )

    school_hint = hints["schools"][0]
    activity_hint = hints["activities"][0]
    assert school_hint["label"] == "Roosevelt Elementary"
    assert school_hint["domains"] == ["roosevelt.k12.ca.us"]
    assert school_hint["platforms"] == ["ParentSquare"]
    assert school_hint["contacts"] == ["Ms. Kim"]
    assert activity_hint["label"] == "Soccer"
    assert activity_hint["locations"] == ["North Field"]
    assert hints["contacts"] == ["Ms. Kim"]
    assert hints["locations"] == ["North Field"]
