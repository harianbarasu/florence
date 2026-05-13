import hashlib
import hmac
import json
import time

from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceRedisRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.contracts import (
    Channel,
    ChannelType,
    Household,
    IdentityKind,
    Member,
    MemberIdentity,
    MemberRole,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime import FlorenceEntrypointResult, FlorenceProductionService
from florence.state import FlorenceStateDB


class _FakeLinqClient:
    def __init__(self):
        self.sent = []

    def verify_webhook_signature(self, *, raw_body, timestamp, signature):
        return True

    def send_text(self, *, chat_id, message):
        self.sent.append({"chat_id": chat_id, "message": message})


class _FakeSessionDB:
    def get_messages_as_conversation(self, session_id):  # noqa: ARG002
        return []

    def get_session(self, session_id):  # noqa: ARG002
        return None


class _ReplyAgent:
    created = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id") or "session-test"
        _ReplyAgent.created.append(kwargs)

    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None, **kwargs):  # noqa: ARG002
        return {
            "final_response": "Hi from Hermes",
            "messages": [
                {"role": "assistant", "content": "Hi from Hermes"},
            ],
        }


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
        redis=FlorenceRedisRuntimeConfig(url=None),
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


def test_production_linq_dm_turn_records_full_reliability_trace(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_maya_phone",
            member_id="mem_123",
            kind=IdentityKind.PHONE,
            value="+15555550123",
            normalized_value="+15555550123",
        )
    )
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
    service.household_chat_service = FlorenceHouseholdChatService(
        store,
        model="test-model",
        max_iterations=2,
        provider="test",
        agent_factory=_ReplyAgent,
        session_db=_FakeSessionDB(),
    )
    onboarding = service.entrypoints.onboarding_service
    onboarding.apply_explicit_update(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="chat_123",
        parent_name="Maya",
        child_names=["Ava"],
    )
    onboarding.apply_explicit_update(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="chat_123",
        child_name="Ava",
        age="7",
        school="Roosevelt Elementary",
        activities=["Soccer"],
        google_connected=True,
    )

    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "chat_123", "is_group": False},
            "id": "msg_trace_123",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "What is going on today?"}],
            "service": "iMessage",
        },
    }
    result = service.handle_linq_webhook(
        payload=payload,
        raw_body=json.dumps(payload).encode("utf-8"),
        webhook_signature="sig",
        webhook_timestamp=str(int(time.time())),
    )

    event_types = {
        event.event_type: event
        for event in store.list_pilot_events(household_id="hh_123", limit=20)
    }
    turn_records = store.list_turn_records(household_id="hh_123")
    assert result.status_code == 200
    assert service.linq.sent == [{"chat_id": "chat_123", "message": "Hi from Hermes"}]
    assert "inbound_received" in {
        event.event_type
        for event in store.list_pilot_events(household_id="__unresolved_household__", limit=10)
    }
    assert event_types["identity_resolved"].metadata["resolved"] is True
    assert event_types["channel_resolved"].metadata["channel_id"] == "chan_dm_123"
    assert event_types["route_selected"].metadata["route"] == "dm"
    assert event_types["hermes_turn_started"].metadata["turn_id"] == turn_records[0]["id"]
    assert event_types["hermes_turn_completed"].metadata["final_response_present"] is True
    assert event_types["reply_generated"].metadata["reply_count"] == 1
    assert event_types["outbound_attempted"].metadata["delivery_kind"] == "reply"
    assert event_types["outbound_sent"].metadata["message_length"] == len("Hi from Hermes")
    assert turn_records[0]["trigger_kind"] == "inbound_dm"
    assert turn_records[0]["outcome"]["reply_text"] == "Hi from Hermes"
    store.close()


def test_production_linq_unresolved_group_records_resolution_failure(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "group_chat_123", "is_group": True},
            "id": "msg_unresolved_group",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "Hey Florence"}],
            "service": "iMessage",
        },
    }

    result = service.handle_linq_webhook(
        payload=payload,
        raw_body=json.dumps(payload).encode("utf-8"),
        webhook_signature="sig",
        webhook_timestamp=str(int(time.time())),
    )

    events = {
        event.event_type: event
        for event in store.list_pilot_events(household_id="__unresolved_household__", limit=10)
    }
    assert result.status_code == 200
    assert service.linq.sent == []
    assert json.loads(result.body)["error"] == "unresolved_group_household"
    assert events["inbound_received"].metadata["message_id"] == "msg_unresolved_group"
    assert events["identity_resolved"].metadata["resolved"] is False
    assert events["identity_resolved"].metadata["failure_reason"] == "unresolved_group_household"
    assert events["channel_resolved"].metadata["resolved"] is False
    assert events["channel_resolved"].metadata["provider_channel_id"] == "group_chat_123"
    store.close()


def test_production_service_strips_markdown_before_sending_linq_message(tmp_path, monkeypatch):
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
            reply_text=(
                "**Evening check-in**\n\n"
                "- **Tomorrow is light but underspecified:** no confirmed events.\n"
                "- **Suggested prep item:** set out bags tonight."
            ),
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
    assert service.linq.sent[0]["message"] == (
        "Evening check-in\n\n"
        "- Tomorrow is light but underspecified: no confirmed events.\n"
        "- Suggested prep item: set out bags tonight."
    )
    store.close()
