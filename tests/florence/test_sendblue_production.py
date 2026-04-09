import json
import sys
import types

from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceRedisRuntimeConfig,
    FlorenceSendblueRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.contracts import Channel, ChannelType, Household
from florence.media.openai_extract import RenderedPdfPageImage
from florence.messaging.types import FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY
from florence.runtime import FlorenceEntrypointResult, FlorenceProductionService
import florence.sendblue.media as sendblue_media
from florence.server import _extract_sendblue_webhook_secret
from florence.state import FlorenceStateDB


class _FakeSendblueClient:
    def __init__(self):
        self.sent = []

    def verify_webhook_signature(self, *, secret_header):
        return True

    def send_text(self, *, thread_id, message, group_id=None, numbers=None):
        self.sent.append(
            {
                "thread_id": thread_id,
                "message": message,
                "group_id": group_id,
                "numbers": numbers,
            }
        )


def _build_settings(tmp_path):
    return FlorenceSettings(
        server=FlorenceServerRuntimeConfig(
            host="127.0.0.1",
            port=8081,
            public_base_url="https://florence.example.com",
            sync_interval_seconds=300.0,
            db_path=tmp_path / "florence.db",
        ),
        google=FlorenceGoogleRuntimeConfig(
            client_id=None,
            client_secret=None,
            redirect_uri=None,
            state_secret=None,
        ),
        linq=FlorenceLinqRuntimeConfig(
            api_key="linq-api-key",
            webhook_secret="linq-webhook-secret",
        ),
        sendblue=FlorenceSendblueRuntimeConfig(
            api_key_id="sb-key-id",
            api_secret_key="sb-secret",
            from_number="+15122164639",
            webhook_secret="sb-webhook-secret",
        ),
        hermes=FlorenceHermesRuntimeConfig(
            model="anthropic/claude-opus-4.6",
            max_iterations=4,
        ),
        redis=FlorenceRedisRuntimeConfig(url=None),
    )


def test_production_service_handles_sendblue_webhook(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.sendblue = _FakeSendblueClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_sendblue_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        ),
    )

    payload = {
        "content": "hello",
        "is_outbound": False,
        "status": "RECEIVED",
        "message_handle": "msg_123",
        "from_number": "+15555550123",
        "number": "+15555550123",
        "to_number": "+15122164639",
        "sendblue_number": "+15122164639",
        "service": "iMessage",
    }
    result = service.handle_sendblue_webhook(
        payload=payload,
        webhook_secret="sb-webhook-secret",
    )

    assert result.status_code == 200
    assert json.loads(result.body)["ok"] is True
    assert service.sendblue.sent[0]["thread_id"] == "+15122164639|+15555550123"
    assert service.sendblue.sent[0]["message"] == "Hi from Florence"
    store.close()


def test_production_service_strips_markdown_before_sending_sendblue_message(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.sendblue = _FakeSendblueClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_sendblue_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text="**Private note**\n\nCheck [camp email](https://example.com) tonight.",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        ),
    )

    payload = {
        "content": "hello",
        "is_outbound": False,
        "status": "RECEIVED",
        "message_handle": "msg_123",
        "from_number": "+15555550123",
        "number": "+15555550123",
        "to_number": "+15122164639",
        "sendblue_number": "+15122164639",
        "service": "iMessage",
    }
    result = service.handle_sendblue_webhook(
        payload=payload,
        webhook_secret="sb-webhook-secret",
    )

    assert result.status_code == 200
    assert json.loads(result.body)["ok"] is True
    assert service.sendblue.sent[0]["message"] == "Private note\n\nCheck camp email (https://example.com) tonight."
    store.close()


def test_production_service_scrubs_internal_ids_before_sending_sendblue_message(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.sendblue = _FakeSendblueClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_sendblue_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text='Review work_123 and check mem_abc before touching hh_123.',
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        ),
    )

    payload = {
        "content": "hello",
        "is_outbound": False,
        "status": "RECEIVED",
        "message_handle": "msg_123",
        "from_number": "+15555550123",
        "number": "+15555550123",
        "to_number": "+15122164639",
        "sendblue_number": "+15122164639",
        "service": "iMessage",
    }
    result = service.handle_sendblue_webhook(
        payload=payload,
        webhook_secret="sb-webhook-secret",
    )

    assert result.status_code == 200
    assert json.loads(result.body)["ok"] is True
    assert service.sendblue.sent[0]["message"] == "Review that item and check another parent before touching that item."
    store.close()


def test_server_accepts_sendblue_documented_secret_header():
    headers = {"sb-signing-secret": "sb-webhook-secret"}

    assert _extract_sendblue_webhook_secret(headers) == "sb-webhook-secret"


def test_production_service_sends_group_message_through_sendblue_group_endpoint(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|group:group_123456",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
            metadata={
                "group_id": "group_123456",
                "participant_handles": ["+15555550123", "+15555550124"],
                "sendblue_number": "+15122164639",
            },
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.sendblue = _FakeSendblueClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_sendblue_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_group_123",
        ),
    )

    payload = {
        "content": "hello group",
        "is_outbound": False,
        "status": "RECEIVED",
        "message_handle": "msg_group_123",
        "from_number": "+15555550123",
        "number": "+15555550123",
        "to_number": "+15122164639",
        "sendblue_number": "+15122164639",
        "group_id": "group_123456",
        "participants": ["+15555550123", "+15555550124", "+15122164639"],
        "service": "iMessage",
    }
    result = service.handle_sendblue_webhook(
        payload=payload,
        webhook_secret="sb-webhook-secret",
    )

    assert result.status_code == 200
    assert json.loads(result.body)["ok"] is True
    assert service.sendblue.sent[0]["thread_id"] == "+15122164639|group:group_123456"
    assert service.sendblue.sent[0]["group_id"] == "group_123456"
    assert service.sendblue.sent[0]["numbers"] is None
    assert service.sendblue.sent[0]["message"] == "Hi from Florence"
    store.close()


def test_production_service_enriches_sendblue_media_payload_before_entrypoint(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.sendblue = _FakeSendblueClient()

    class _FakeResponse:
        def __init__(self, *, content: bytes, content_type: str):
            self.content = content
            self.headers = {"content-type": content_type}

        def raise_for_status(self):
            return None

    def _fake_get(url, *, timeout=None):  # noqa: ARG001
        if url.endswith("form.pdf"):
            return _FakeResponse(content=b"%PDF-fake", content_type="application/pdf")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(sendblue_media.httpx, "get", _fake_get)
    monkeypatch.setattr(
        sendblue_media,
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
            assert file_item is not None
            assert file_item["file_data"].startswith("data:application/pdf;base64,")
            return types.SimpleNamespace(output_text="Permission slip due Friday at pickup.")

    class _FakeOpenAI:
        def __init__(self, **kwargs):  # noqa: ARG002
            self.responses = _FakeResponses()

    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))
    captured_payload: dict[str, object] = {}

    def _handle(payload):
        captured_payload.update(payload)
        return FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        )

    monkeypatch.setattr(service.entrypoints, "handle_sendblue_payload", _handle)

    payload = {
        "content": "Can you check this form?",
        "is_outbound": False,
        "status": "RECEIVED",
        "message_handle": "msg_media_123",
        "from_number": "+15555550123",
        "number": "+15555550123",
        "to_number": "+15122164639",
        "sendblue_number": "+15122164639",
        "service": "iMessage",
        "message_type": "file",
        "media_url": "https://example.com/form.pdf",
        "filename": "form.pdf",
    }

    result = service.handle_sendblue_webhook(
        payload=payload,
        webhook_secret="sb-webhook-secret",
    )

    assert result.status_code == 200
    assert "Media context extracted from attachments" in str(captured_payload["content"])
    assert "form.pdf: Permission slip due Friday at pickup." in str(captured_payload["content"])
    serialized = captured_payload[FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY]
    assert len(serialized) == 2
    assert serialized[0]["kind"] == "pdf"
    assert serialized[1]["kind"] == "image"
    assert serialized[1]["filename"] == "form.pdf#page-1.png"
    store.close()


def test_production_service_enriches_sendblue_image_payload_before_entrypoint(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.sendblue = _FakeSendblueClient()

    class _FakeResponse:
        def __init__(self, *, content: bytes, content_type: str):
            self.content = content
            self.headers = {"content-type": content_type}

        def raise_for_status(self):
            return None

    def _fake_get(url, *, timeout=None):  # noqa: ARG001
        if url.endswith("calendar.png"):
            return _FakeResponse(content=b"\x89PNG-fake", content_type="image/png")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(sendblue_media.httpx, "get", _fake_get)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class _FakeResponses:
        def create(self, **kwargs):
            content = kwargs["input"][1]["content"]
            image_item = next((item for item in content if item.get("type") == "input_image"), None)
            assert image_item is not None
            assert image_item["image_url"].startswith("data:image/png;base64,")
            system_text = kwargs["input"][0]["content"][0]["text"]
            assert "faithful OCR-style extraction" in system_text
            assert "row-by-row closures" in system_text
            return types.SimpleNamespace(
                output_text=(
                    "Young Minds school calendar: Mar 30-Apr 3, 2026 Spring Break; "
                    "May 25 Memorial Day; Jun 18 TK graduation."
                )
            )

    class _FakeOpenAI:
        def __init__(self, **kwargs):  # noqa: ARG002
            self.responses = _FakeResponses()

    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))
    captured_payload: dict[str, object] = {}

    def _handle(payload):
        captured_payload.update(payload)
        return FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        )

    monkeypatch.setattr(service.entrypoints, "handle_sendblue_payload", _handle)

    payload = {
        "content": "Can you look at this calendar screenshot?",
        "is_outbound": False,
        "status": "RECEIVED",
        "message_handle": "msg_media_img_123",
        "from_number": "+15555550123",
        "number": "+15555550123",
        "to_number": "+15122164639",
        "sendblue_number": "+15122164639",
        "service": "iMessage",
        "message_type": "file",
        "media_url": "https://example.com/calendar.png",
        "filename": "calendar.png",
    }

    result = service.handle_sendblue_webhook(
        payload=payload,
        webhook_secret="sb-webhook-secret",
    )

    assert result.status_code == 200
    assert "Media context extracted from attachments" in str(captured_payload["content"])
    assert "calendar.png: Young Minds school calendar:" in str(captured_payload["content"])
    assert captured_payload[FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY][0]["kind"] == "image"
    assert captured_payload[FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY][0]["data_url"].startswith("data:image/png;base64,")
    store.close()
