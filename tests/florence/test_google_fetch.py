import sys
import types

import florence.google.fetch as google_fetch
from florence.google import (
    GoogleCalendarMetadata,
    build_gmail_sync_item,
    list_recent_gmail_sync_items,
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


def test_list_recent_gmail_sync_items_extracts_text_html_and_pdf_attachments(monkeypatch):
    # Use module helpers to avoid hand-rolled base64 mistakes.
    html_attachment = "<html><body>Picture Day is Friday at 8am.</body></html>"
    inline_text_attachment = "Bring baseball glove and cleats."
    pdf_bytes = b"%PDF-1.7 fake"

    def _to_b64url_bytes(raw: bytes) -> str:
        import base64

        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    def _to_b64url_text(raw: str) -> str:
        return _to_b64url_bytes(raw.encode("utf-8"))

    monkeypatch.setattr(
        google_fetch,
        "_extract_pdf_attachment_text_with_gpt",
        lambda **_: "Practice moved to Tuesday at 4:30 PM at North Field.",
    )

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    def _fake_get(url, *, params=None, headers=None, timeout=None):  # noqa: ARG001
        if url.endswith("/messages"):
            return _FakeResponse({"messages": [{"id": "msg_1"}]})
        if url.endswith("/messages/msg_1"):
            return _FakeResponse(
                {
                    "id": "msg_1",
                    "threadId": "thread_1",
                    "snippet": "Snippet text",
                    "internalDate": "1799726400000",
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "coach@league.org"},
                            {"name": "Subject", "value": "Weekly update"},
                        ],
                        "parts": [
                            {
                                "mimeType": "text/plain",
                                "body": {"data": _to_b64url_text("Body says schedule changed.")},
                            },
                            {
                                "mimeType": "text/html",
                                "filename": "school.html",
                                "body": {"attachmentId": "att_html"},
                            },
                            {
                                "mimeType": "application/pdf",
                                "filename": "practice.pdf",
                                "body": {"attachmentId": "att_pdf"},
                            },
                            {
                                "mimeType": "text/plain",
                                "filename": "gear.txt",
                                "body": {"data": _to_b64url_text(inline_text_attachment)},
                            },
                        ],
                    },
                }
            )
        if url.endswith("/attachments/att_html"):
            return _FakeResponse({"data": _to_b64url_text(html_attachment)})
        if url.endswith("/attachments/att_pdf"):
            return _FakeResponse({"data": _to_b64url_bytes(pdf_bytes)})
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(google_fetch.httpx, "get", _fake_get)

    items = list_recent_gmail_sync_items(access_token="token", max_results=5)
    assert len(items) == 1
    item = items[0]
    assert item.attachment_count == 3
    assert item.attachment_text is not None
    assert "school.html: Picture Day is Friday at 8am." in item.attachment_text
    assert "practice.pdf: Practice moved to Tuesday at 4:30 PM at North Field." in item.attachment_text
    assert "gear.txt: Bring baseball glove and cleats." in item.attachment_text


def test_extract_pdf_attachment_text_with_gpt_uses_gpt_5_4_by_default(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeResponses:
        def create(self, **kwargs):
            calls["kwargs"] = kwargs
            return types.SimpleNamespace(output_text="Pick up forms by Thursday.")

    class _FakeOpenAI:
        def __init__(self, *, api_key, base_url):
            calls["api_key"] = api_key
            calls["base_url"] = base_url
            self.responses = _FakeResponses()

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("FLORENCE_GMAIL_PDF_MODEL", raising=False)
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))

    extracted = google_fetch._extract_pdf_attachment_text_with_gpt(
        pdf_bytes=b"%PDF fake",
        filename="forms.pdf",
    )

    assert extracted == "Pick up forms by Thursday."
    assert calls["api_key"] == "sk-test"
    assert calls["base_url"] == "https://api.openai.com/v1"
    assert isinstance(calls["kwargs"], dict)
    assert calls["kwargs"]["model"] == "gpt-5.4"
