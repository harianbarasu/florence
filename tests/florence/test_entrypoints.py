from urllib.parse import parse_qs, urlparse

from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
from florence.runtime import FlorenceAppService, FlorenceGoogleOauthConfig
from florence.state import FlorenceStateDB


def test_app_service_group_without_resolved_household_returns_dm_first_message(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    app = FlorenceAppService(store)

    result = app.handle_bluebubbles_payload(
        {
            "type": "new-message",
            "data": {
                "message": {
                    "guid": "msg_123",
                    "text": "Hi Florence",
                    "isFromMe": False,
                },
                "chat": {
                    "chatGuid": "iMessage;-;group123",
                    "participants": ["+15555550123", "+15555550124"],
                },
                "sender": {
                    "address": "+15555550123",
                },
            },
        }
    )

    assert result.consumed is True
    assert result.error == "unresolved_group_household"
    assert result.reply_text is not None
    store.close()


def test_app_service_dm_connect_stage_returns_google_link(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    app = FlorenceAppService(
        store,
        google_oauth=FlorenceGoogleOauthConfig(
            client_id="client-id",
            client_secret="client-secret",
            redirect_uri="https://example.com/callback",
            state_secret="state-secret",
        ),
    )

    app.handle_bluebubbles_payload(
        {
            "data": {
                "message": {
                    "guid": "msg_1",
                    "text": "Maya",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123"},
                "sender": {"address": "+15555550123"},
            }
        }
    )

    result = app.handle_bluebubbles_payload(
        {
            "data": {
                "message": {
                    "guid": "msg_2",
                    "text": "connect",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123"},
                "sender": {"address": "+15555550123"},
            }
        }
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "accounts.google.com" in result.reply_text
    store.close()


def test_app_service_google_callback_returns_next_prompt(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    app = FlorenceAppService(
        store,
        google_oauth=FlorenceGoogleOauthConfig(
            client_id="client-id",
            client_secret="client-secret",
            redirect_uri="https://example.com/callback",
            state_secret="state-secret",
        ),
    )
    app.handle_bluebubbles_payload(
        {
            "data": {
                "message": {
                    "guid": "msg_1",
                    "text": "Maya",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123"},
                "sender": {"address": "+15555550123"},
            }
        }
    )
    dm_result = app.handle_bluebubbles_payload(
        {
            "data": {
                "message": {
                    "guid": "msg_2",
                    "text": "connect",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123"},
                "sender": {"address": "+15555550123"},
            }
        }
    )
    raw_state = parse_qs(urlparse(dm_result.reply_text.splitlines()[1]).query)["state"][0]

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

    result = app.handle_google_oauth_callback(code="auth-code", state=raw_state)

    assert result.consumed is True
    assert result.reply_text is not None
    assert "children" in result.reply_text.lower()
    store.close()


def test_app_service_ignores_noisy_bluebubbles_event_types(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    app = FlorenceAppService(store)

    result = app.handle_bluebubbles_payload(
        {
            "type": "typing-indicator",
            "data": {
                "message": {
                    "guid": "msg_ignored",
                    "text": "",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123", "isGroup": False},
                "sender": {"address": "+15555550123"},
            },
        }
    )

    assert result.consumed is False
    assert result.reply_text is None
    store.close()


def test_app_service_ignores_partial_bluebubbles_payloads(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    app = FlorenceAppService(store)

    result = app.handle_bluebubbles_payload(
        {
            "type": "new-message",
            "data": {
                "message": {
                    "guid": "",
                    "text": "Hi Florence",
                    "isFromMe": False,
                },
                "chat": {"chatGuid": "dm-thread-123", "isGroup": False},
                "sender": {"address": "+15555550123"},
            },
        }
    )

    assert result.consumed is False
    assert result.error == "bluebubbles_message_id_required"
    store.close()
