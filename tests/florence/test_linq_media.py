import sys
import types

import florence.linq.media as linq_media
from florence.linq import parse_linq_payload
from florence.media.openai_extract import RenderedPdfPageImage
from florence.messaging.types import FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY


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
    monkeypatch.setattr(
        linq_media,
        "render_pdf_pages_to_images",
        lambda **kwargs: (
            RenderedPdfPageImage(
                page_number=1,
                mime_type="image/png",
                image_bytes=b"pdf-page",
                data_url="data:image/png;base64,cGRmLXBhZ2U=",
            ),
        ),
    )
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class _FakeResponses:
        def create(self, **kwargs):
            content = kwargs["input"][1]["content"]
            file_item = next((item for item in content if item.get("type") == "input_file"), None)
            if file_item is not None:
                assert file_item["file_data"].startswith("data:application/pdf;base64,")
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
    assert len(inbound.attachments) == 3
    assert inbound.attachments[0].kind == "image"
    assert inbound.attachments[0].data_url.startswith("data:image/png;base64,")
    assert inbound.attachments[1].kind == "pdf"
    assert inbound.attachments[2].kind == "image"
    assert inbound.attachments[2].filename == "form.pdf#page-1.png"
    assert inbound.attachments[2].data_url == "data:image/png;base64,cGRmLXBhZ2U="
    assert payload[FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY][1]["filename"] == "form.pdf"
    assert payload[FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY][2]["filename"] == "form.pdf#page-1.png"


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


def test_enrich_linq_payload_with_media_text_handles_fridge_style_images(monkeypatch):
    payload = {
        "api_version": "v3",
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "chat_123", "is_group": False},
            "id": "msg_456",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [
                {"type": "text", "value": "What can we make from this?"},
                {"type": "image", "url": "https://example.com/fridge.png", "filename": "fridge.png"},
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
        if url.endswith("fridge.png"):
            return _FakeResponse(content=b"\x89PNGfake", content_type="image/png")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(linq_media.httpx, "get", _fake_get)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class _FakeResponses:
        def create(self, **kwargs):
            prompt_text = kwargs["input"][0]["content"][0]["text"]
            assert "pantry, fridge, grocery, meal, or food photo" in prompt_text
            assert "permission slips, uniforms, materials to bring, follow-up actions" in prompt_text
            return types.SimpleNamespace(output_text="Visible items: eggs, spinach, tortillas, salsa.")

    class _FakeOpenAI:
        def __init__(self, **kwargs):  # noqa: ARG002
            self.responses = _FakeResponses()

    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))

    changed = linq_media.enrich_linq_payload_with_media_text(payload, linq_api_key=None)
    assert changed is True

    inbound = parse_linq_payload(payload)
    assert "fridge.png: Visible items: eggs, spinach, tortillas, salsa." in inbound.body
