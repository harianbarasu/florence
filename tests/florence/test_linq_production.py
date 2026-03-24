import hashlib
import hmac
import json
import time

from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.contracts import Channel, ChannelType, Household
from florence.runtime import FlorenceEntrypointResult, FlorenceProductionService
from florence.state import FlorenceStateDB


class _FakeLinqClient:
    def __init__(self):
        self.sent = []

    def verify_webhook_signature(self, *, raw_body, timestamp, signature):
        return True

    def send_text(self, *, chat_id, message):
        self.sent.append({"chat_id": chat_id, "message": message})


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
        hermes=FlorenceHermesRuntimeConfig(
            model="anthropic/claude-opus-4.6",
            max_iterations=4,
        ),
    )


def test_production_service_handles_linq_webhook(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="chat_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_linq_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        ),
    )

    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "chat_123", "is_group": False},
            "id": "msg_123",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "hello"}],
            "service": "iMessage",
        },
    }
    raw_body = json.dumps(payload).encode("utf-8")
    result = service.handle_linq_webhook(
        payload=payload,
        raw_body=raw_body,
        webhook_signature="sig",
        webhook_timestamp=str(int(time.time())),
    )

    assert result.status_code == 200
    assert service.linq.sent[0]["chat_id"] == "chat_123"
    assert service.linq.sent[0]["message"] == "Hi from Florence"
    store.close()
