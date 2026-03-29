from urllib.parse import parse_qs, urlparse

from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
from florence.onboarding import OnboardingVariant
from florence.runtime import FlorenceEntrypointService, FlorenceGoogleOauthConfig
from florence.state import FlorenceStateDB


def _linq_payload(*, message_id: str, text: str, chat_id: str, sender: str, is_group: bool) -> dict[str, object]:
    return {
        "api_version": "v3",
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "event_id": f"evt_{message_id}",
        "trace_id": f"trace_{message_id}",
        "data": {
            "chat": {
                "id": chat_id,
                "is_group": is_group,
                "participants": [{"handle": sender}],
            },
            "id": message_id,
            "direction": "inbound",
            "sender_handle": {"handle": sender, "is_me": False},
            "parts": [{"type": "text", "value": text}],
            "service": "iMessage",
        },
    }


def test_entrypoints_group_without_resolved_household_returns_dm_first_message(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = FlorenceEntrypointService(store)

    result = service.handle_linq_payload(
        _linq_payload(
            message_id="msg_123",
            text="Hi Florence",
            chat_id="group_123",
            sender="+15555550123",
            is_group=True,
        )
    )

    assert result.consumed is True
    assert result.error == "unresolved_group_household"
    assert result.reply_text is not None
    store.close()


def test_entrypoints_hybrid_onboarding_reaches_google_link_after_household_ops(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = FlorenceEntrypointService(
        store,
        google_oauth=FlorenceGoogleOauthConfig(
            client_id="client-id",
            client_secret="client-secret",
            redirect_uri="https://example.com/callback",
            state_secret="state-secret",
        ),
    )
    service.onboarding_service.variant_selector = lambda _household_id, _member_id: OnboardingVariant.HYBRID

    first = service.handle_linq_payload(
        _linq_payload(
            message_id="msg_1",
            text="Maya",
            chat_id="dm-thread-123",
            sender="+15555550123",
            is_group=False,
        )
    )
    service.onboarding_service.record_child_names(
        household_id=first.household_id or "",
        member_id=first.member_id or "",
        thread_id="dm-thread-123",
        child_names=["Ava"],
    )
    service.onboarding_service.record_school_basics(
        household_id=first.household_id or "",
        member_id=first.member_id or "",
        thread_id="dm-thread-123",
        school_labels=["Roosevelt Elementary"],
    )
    service.onboarding_service.record_activity_basics(
        household_id=first.household_id or "",
        member_id=first.member_id or "",
        thread_id="dm-thread-123",
        activity_labels=["Soccer"],
    )
    service.onboarding_service.record_household_operations(
        household_id=first.household_id or "",
        member_id=first.member_id or "",
        thread_id="dm-thread-123",
        household_operations=["school forms", "pickup planning"],
    )

    result = service.handle_linq_payload(
        _linq_payload(
            message_id="msg_2",
            text="what next?",
            chat_id="dm-thread-123",
            sender="+15555550123",
            is_group=False,
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert len(result.reply_messages) == 3
    assert result.reply_messages[0] == "You're almost ready. Connect your Google account so I can compare Gmail and Calendar against the household context you just gave me."
    assert result.reply_messages[1].startswith("https://accounts.google.com/")
    store.close()


def test_entrypoints_google_callback_returns_next_prompt(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = FlorenceEntrypointService(
        store,
        google_oauth=FlorenceGoogleOauthConfig(
            client_id="client-id",
            client_secret="client-secret",
            redirect_uri="https://example.com/callback",
            state_secret="state-secret",
        ),
    )
    service.onboarding_service.variant_selector = lambda _household_id, _member_id: OnboardingVariant.HYBRID

    first = service.handle_linq_payload(
        _linq_payload(
            message_id="msg_1",
            text="Maya",
            chat_id="dm-thread-123",
            sender="+15555550123",
            is_group=False,
        )
    )
    household_id = first.household_id or ""
    member_id = first.member_id or ""
    service.onboarding_service.record_child_names(
        household_id=household_id,
        member_id=member_id,
        thread_id="dm-thread-123",
        child_names=["Ava"],
    )
    service.onboarding_service.record_school_basics(
        household_id=household_id,
        member_id=member_id,
        thread_id="dm-thread-123",
        school_labels=["Roosevelt Elementary"],
    )
    service.onboarding_service.record_activity_basics(
        household_id=household_id,
        member_id=member_id,
        thread_id="dm-thread-123",
        activity_labels=["Soccer"],
    )
    service.onboarding_service.record_household_operations(
        household_id=household_id,
        member_id=member_id,
        thread_id="dm-thread-123",
        household_operations=["school forms", "pickup planning"],
    )
    link = service.google_account_link_service.build_connect_link(
        household_id=household_id,
        member_id=member_id,
        thread_id="dm-thread-123",
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

    result = service.handle_google_oauth_callback(code="auth-code", state=raw_state)

    assert result.consumed is True
    assert result.reply_text is not None
    assert "you're ready" in result.reply_text.lower()
    store.close()


def test_entrypoints_threads_household_chat_provider(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = FlorenceEntrypointService(
        store,
        household_chat_model="openai/gpt-5.4",
        household_chat_provider="custom",
    )

    assert service.household_chat_service is not None
    assert service.household_chat_service.provider == "custom"
    store.close()


def test_entrypoints_ignores_linq_delivery_events(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = FlorenceEntrypointService(store)

    result = service.handle_linq_payload(
        {
            "api_version": "v3",
            "webhook_version": "2026-02-03",
            "event_type": "message.delivered",
            "data": {
                "chat": {"id": "dm-thread-123", "is_group": False},
                "id": "msg_ignored",
                "direction": "outbound",
                "sender_handle": {"handle": "+15555550123", "is_me": True},
                "parts": [{"type": "text", "value": ""}],
                "service": "iMessage",
            },
        }
    )

    assert result.consumed is False
    assert result.reply_text is None
    store.close()


def test_entrypoints_ignores_partial_linq_payloads(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = FlorenceEntrypointService(store)

    result = service.handle_linq_payload(
        {
            "api_version": "v3",
            "webhook_version": "2026-02-03",
            "event_type": "message.received",
            "data": {
                "chat": {"id": "dm-thread-123", "is_group": False},
                "id": "",
                "direction": "inbound",
                "sender_handle": {"handle": "+15555550123", "is_me": False},
                "parts": [{"type": "text", "value": "Hi Florence"}],
                "service": "iMessage",
            },
        }
    )

    assert result.consumed is False
    assert result.error == "linq_message_id_required"
    store.close()
