from florence.config import FlorenceGoogleRuntimeConfig
from florence.contracts import ChannelType
from florence.messaging import FlorenceMessagingIngressResult
from florence.onboarding import OnboardingVariant
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime import FlorenceEntrypointService
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


def _build_entrypoints(store: FlorenceStateDB, **kwargs) -> FlorenceEntrypointService:
    kwargs.setdefault(
        "household_chat_service",
        FlorenceHouseholdChatService(
            store,
            model="openai/gpt-5.4",
            provider="custom",
        ),
    )
    return FlorenceEntrypointService(store, **kwargs)


def test_entrypoints_group_without_resolved_household_returns_dm_first_message(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = _build_entrypoints(store)

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


def test_entrypoints_hybrid_onboarding_offers_google_link_immediately_after_name(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = _build_entrypoints(
        store,
        google_oauth=FlorenceGoogleRuntimeConfig(
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
    assert first.consumed is True
    assert first.reply_text is not None
    assert first.reply_messages[0] == "Hi, I'm Florence."
    assert first.reply_messages[2] == (
        "Connect your Google account so I can pull the last 30 days of family email and calendar in the background while we keep going here."
    )
    assert first.reply_messages[3].startswith("https://accounts.google.com/")
    assert first.reply_messages[-1] == "What are your kids' names? Send all of them in one message, one per line or comma-separated."
    store.close()


def test_entrypoints_uses_injected_household_chat_service(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    household_chat_service = FlorenceHouseholdChatService(
        store,
        model="openai/gpt-5.4",
        provider="custom",
    )
    service = FlorenceEntrypointService(
        store,
        household_chat_service=household_chat_service,
    )

    assert service.household_chat_service is household_chat_service
    assert service.household_chat_service.provider == "custom"
    store.close()


def test_entrypoints_ignores_linq_delivery_events(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = _build_entrypoints(store)

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
    service = _build_entrypoints(store)

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


def test_entrypoints_sendblue_group_persists_group_id_on_channel(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = _build_entrypoints(store)

    first = service.handle_sendblue_payload(
        {
            "content": "Maya",
            "is_outbound": False,
            "status": "RECEIVED",
            "message_handle": "msg_dm_123",
            "from_number": "+15555550123",
            "number": "+15555550123",
            "to_number": "+15122164639",
            "sendblue_number": "+15122164639",
            "service": "iMessage",
        }
    )
    assert first.household_id is not None

    service.ingress.handle_message = lambda _resolved: FlorenceMessagingIngressResult(reply_text="Hi group", consumed=True)  # type: ignore[method-assign]

    result = service.handle_sendblue_payload(
        {
            "content": "hey Florence",
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
    )

    assert result.consumed is True
    channel = store.get_channel_by_provider_id(
        provider="sendblue",
        provider_channel_id="+15122164639|group:group_123456",
    )
    assert channel is not None
    assert channel.channel_type == ChannelType.HOUSEHOLD_GROUP
    assert channel.metadata["group_id"] == "group_123456"
    assert channel.metadata["sendblue_number"] == "+15122164639"
    store.close()
