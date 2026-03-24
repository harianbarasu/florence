import sys
import types

import florence.linq.media as linq_media
from florence.linq import parse_linq_payload


def test_enrich_linq_payload_with_media_text_appends_extracted_attachment_context(monkeypatch):
    payload = {
        "api_version": "v3",
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "chat_123", "is_group": False},
            "id": "msg_123",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [
                {"type": "text", "value": "Please check these."},
                {"type": "image", "url": "https://example.com/shot.png", "filename": "screenshot.png"},
                {"type": "file", "url": "https://example.com/form.pdf", "filename": "form.pdf"},
            ],
            "service": "iMessage",
        },
    }

    class _FakeResponse:
        def __init__(self, *, content: bytes, content_type: str):
            self.content = content
            self.headers = {"content-type": content_type}
            self.status_code = 200

        def raise_for_status(self):
            return None

    def _fake_get(url, *, headers=None, timeout=None):  # noqa: ARG001
        if url.endswith("shot.png"):
            return _FakeResponse(content=b"\x89PNGfake", content_type="image/png")
        if url.endswith("form.pdf"):
            return _FakeResponse(content=b"%PDF-fake", content_type="application/pdf")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(linq_media.httpx, "get", _fake_get)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class _FakeResponses:
        def create(self, **kwargs):
            content = kwargs["input"][1]["content"]
            if any(item.get("type") == "input_file" for item in content):
                return types.SimpleNamespace(output_text="Picture Day form due Thursday at 8am.")
            if any(item.get("type") == "input_image" for item in content):
                return types.SimpleNamespace(output_text="Flyer says baseball practice Tuesday 4:30 PM.")
            raise AssertionError("Unexpected response input payload")

    class _FakeOpenAI:
        def __init__(self, **kwargs):  # noqa: ARG002
            self.responses = _FakeResponses()

    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))

    changed = linq_media.enrich_linq_payload_with_media_text(payload, linq_api_key=None)
    assert changed is True

    inbound = parse_linq_payload(payload)
    assert "Media context extracted from attachments" in inbound.body
    assert "screenshot.png: Flyer says baseball practice Tuesday 4:30 PM." in inbound.body
    assert "form.pdf: Picture Day form due Thursday at 8am." in inbound.body


def test_enrich_linq_payload_with_media_text_no_media_parts_returns_false():
    payload = {
        "api_version": "v3",
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "chat_123", "is_group": False},
            "id": "msg_123",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "Hello Florence"}],
            "service": "iMessage",
        },
    }

    changed = linq_media.enrich_linq_payload_with_media_text(payload)
    assert changed is False
    inbound = parse_linq_payload(payload)
    assert inbound.body == "Hello Florence"
