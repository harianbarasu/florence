from florence.google import (
    GoogleCalendarMetadata,
    build_gmail_sync_item,
    build_parent_calendar_sync_item,
)


def test_build_gmail_sync_item_extracts_headers_body_and_attachment_count():
    message = {
        "id": "gmail_123",
        "threadId": "thread_123",
        "snippet": "Practice moved to Thursday.",
        "internalDate": "1799726400000",
        "payload": {
            "mimeType": "multipart/alternative",
            "headers": [
                {"name": "From", "value": "teacher@school.edu"},
                {"name": "Subject", "value": "Soccer practice update"},
            ],
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": "QXZhIHByYWN0aWNlIG1vdmVkIHRvIFRodXJzZGF5IGF0IDQgcG0u"},
                },
                {
                    "mimeType": "application/pdf",
                    "filename": "schedule.pdf",
                    "body": {"attachmentId": "att_123"},
                },
            ],
        },
    }

    item = build_gmail_sync_item(message)

    assert item.gmail_message_id == "gmail_123"
    assert item.thread_id == "thread_123"
    assert item.from_address == "teacher@school.edu"
    assert item.subject == "Soccer practice update"
    assert item.body_text == "Ava practice moved to Thursday at 4 pm."
    assert item.attachment_count == 1
    assert item.attachment_text is None


def test_build_parent_calendar_sync_item_skips_cancelled_events():
    calendar = GoogleCalendarMetadata(
        id="primary",
        summary="Family calendar",
        timezone="America/Los_Angeles",
    )

    item = build_parent_calendar_sync_item(
        {
            "id": "event_123",
            "status": "cancelled",
            "summary": "Ava soccer practice",
        },
        calendar=calendar,
        family_member_names=["Ava"],
    )

    assert item is None


def test_build_parent_calendar_sync_item_maps_event_fields():
    calendar = GoogleCalendarMetadata(
        id="primary",
        summary="Family calendar",
        timezone="America/Los_Angeles",
    )

    item = build_parent_calendar_sync_item(
        {
            "id": "event_123",
            "summary": "Ava soccer practice",
            "description": "Weekly team practice",
            "location": "North field",
            "htmlLink": "https://calendar.google.com/event?eid=abc",
            "start": {"dateTime": "2026-09-18T16:00:00-07:00", "timeZone": "America/Los_Angeles"},
            "end": {"dateTime": "2026-09-18T17:00:00-07:00", "timeZone": "America/Los_Angeles"},
            "updated": "2026-09-10T08:15:00Z",
        },
        calendar=calendar,
        family_member_names=["Ava", "Noah"],
    )

    assert item is not None
    assert item.google_event_id == "event_123"
    assert item.title == "Ava soccer practice"
    assert item.calendar_summary == "Family calendar"
    assert item.family_member_names == ["Ava", "Noah"]
    assert item.all_day is False
