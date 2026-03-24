import time
from urllib.parse import parse_qs, urlparse

from florence.config import (
    FlorenceBlueBubblesRuntimeConfig,
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


class _FakeBlueBubblesClient:
    def __init__(self):
        self.sent = []

    def is_configured(self):
        return True

    def verify_webhook_secret(self, value):
        return value == "webhook-secret"

    def send_text(self, *, chat_guid, message, reply_to_guid=None):
        self.sent.append(
            {
                "chat_guid": chat_guid,
                "message": message,
                "reply_to_guid": reply_to_guid,
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
            client_id="google-client",
            client_secret="google-secret",
            redirect_uri="https://florence.example.com/v1/florence/google/callback",
            state_secret="state-secret",
        ),
        bluebubbles=FlorenceBlueBubblesRuntimeConfig(
            base_url="https://bb.example.com",
            password="bb-password",
            webhook_secret="webhook-secret",
        ),
        linq=FlorenceLinqRuntimeConfig(
            api_key=None,
            webhook_secret=None,
        ),
        hermes=FlorenceHermesRuntimeConfig(
            model="anthropic/claude-opus-4.6",
            max_iterations=4,
        ),
    )


def test_production_service_delivers_dm_reply_and_group_announcement(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="bluebubbles",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="bluebubbles",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Family group",
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.bluebubbles = _FakeBlueBubblesClient()
    monkeypatch.setattr(
        service.app,
        "handle_bluebubbles_payload",
        lambda payload: FlorenceEntrypointResult(
            reply_text="Hi from Florence",
            group_announcement="Added to the family plan: Ava soccer practice",
            consumed=True,
            household_id="hh_123",
            channel_id="chan_dm_123",
        ),
    )

    result = service.handle_bluebubbles_webhook(
        payload={
            "type": "new-message",
            "data": {
                "message": {
                    "guid": "msg_123",
                    "text": "hello",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123", "isGroup": False},
                "sender": {"address": "+15555550123"},
            },
        },
        webhook_secret="webhook-secret",
    )

    assert result.status_code == 200
    assert service.bluebubbles.sent[0]["chat_guid"] == "dm-thread-123"
    assert service.bluebubbles.sent[1]["chat_guid"] == "group-thread-123"
    store.close()


def test_production_service_google_callback_sends_dm_follow_up(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.bluebubbles = _FakeBlueBubblesClient()
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="bluebubbles",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service.app.onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
    )

    link = service.app.google_account_link_service.build_connect_link(
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
    assert service.bluebubbles.sent
    assert service.bluebubbles.sent[0]["chat_guid"] == "dm-thread-123"
    assert "children" in service.bluebubbles.sent[0]["message"].lower()
    store.close()
