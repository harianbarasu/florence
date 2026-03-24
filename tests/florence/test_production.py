import json
import time
from urllib.parse import parse_qs, urlparse

from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.contracts import Channel, ChannelType, Household
from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
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
            client_id="google-client",
            client_secret="google-secret",
            redirect_uri="https://florence.example.com/v1/florence/google/callback",
            state_secret="state-secret",
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


def test_production_service_delivers_dm_reply_and_group_announcement(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Family group",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_linq_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            group_announcement="Added to the family plan: Ava soccer practice",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        ),
    )

    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "dm-thread-123", "is_group": False},
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
    assert service.linq.sent[0]["chat_id"] == "dm-thread-123"
    assert service.linq.sent[1]["chat_id"] == "group-thread-123"
    store.close()


def test_production_service_google_callback_sends_dm_follow_up(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service.entrypoints.onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
    )

    link = service.entrypoints.google_account_link_service.build_connect_link(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        now_ms=int(time.time() * 1000),
        nonce="nonce-123",
    )
    raw_state = parse_qs(urlparse(link.url).query)["state"][0]

    monkeypatch.setattr(
        "florence.runtime.services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.services.fetch_primary_google_calendar",
        lambda **_: GoogleCalendarMetadata(
            id="primary",
            summary="Family",
            timezone="America/Los_Angeles",
            access_role="owner",
        ),
    )
    monkeypatch.setattr("florence.runtime.services.list_recent_gmail_sync_items", lambda **_: [])
    monkeypatch.setattr("florence.runtime.services.list_recent_parent_calendar_sync_items", lambda **_: [])

    result = service.handle_google_callback(code="auth-code", state=raw_state)

    assert result.status_code == 200
    assert "Google connected" in result.body
    assert service.linq.sent
    assert service.linq.sent[0]["chat_id"] == "dm-thread-123"
    assert "children" in service.linq.sent[0]["message"].lower()
    store.close()


def test_production_service_first_dm_sends_onboarding_sequence_as_separate_messages(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()

    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "dm-thread-123", "is_group": False},
            "id": "msg_hello",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "Maya"}],
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
    assert [item["message"] for item in service.linq.sent[:5]] == [
        "Hi, I'm Florence.",
        "I help keep your household organized by keeping up with school emails, calendar invites, and schedule changes.",
        "First step: connect your Google account so I can start syncing Gmail and Calendar.",
        service.linq.sent[3]["message"],
        "When you're done, reply done here and I'll keep going.",
    ]
    assert service.linq.sent[3]["message"].startswith("https://accounts.google.com/")
    store.close()
