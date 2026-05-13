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
from florence.runtime import FlorenceProductionService
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.pilot_readiness import (
    PILOT_PRODUCT_SPINE,
    build_pilot_readiness_checklist,
    build_pilot_scenario_matrix,
    build_pilot_trace_report,
)
from florence.state import FlorenceStateDB


class _FakeLinqClient:
    def __init__(self):
        self.sent = []

    def verify_webhook_signature(self, *, raw_body, timestamp, signature):  # noqa: ARG002
        return True

    def send_text(self, *, chat_id, message):
        self.sent.append({"chat_id": chat_id, "message": message})


class _FakeSessionDB:
    def get_messages_as_conversation(self, session_id):  # noqa: ARG002
        return []

    def get_session(self, session_id):  # noqa: ARG002
        return None


class _ReplyAgent:
    def __init__(self, **kwargs):
        self.session_id = kwargs.get("session_id") or "session-test"

    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None, **kwargs):  # noqa: ARG002
        return {
            "final_response": "Action list:\n- Pack library books\n- Confirm pickup",
            "messages": [
                {"role": "assistant", "content": "Action list:\n- Pack library books\n- Confirm pickup"},
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


def _seed_completed_parent_dm(store: FlorenceStateDB) -> None:
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_123", household_id="hh_123", display_name="Maya", role=MemberRole.ADMIN))
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


def test_pilot_readiness_checklist_covers_spine_and_scenarios() -> None:
    checklist = build_pilot_readiness_checklist()
    scenarios = build_pilot_scenario_matrix()
    spine_ids = {item[0] for item in PILOT_PRODUCT_SPINE}

    assert {
        "parent_dm",
        "family_group_chat",
        "household_onboarding",
        "google_oauth_data_connection",
        "school_email_calendar_ingestion",
        "calendar_reminder_creation",
        "daily_weekly_briefings",
        "source_review_rules",
        "delivery_reliability",
        "admin_debug_visibility",
    }.issubset(spine_ids)
    assert len(checklist) >= 9
    assert {check.status for check in checklist} == {"pass"}
    assert all(check.evidence for check in checklist)
    assert len(scenarios) >= 12
    assert all(scenario.required_assertions for scenario in scenarios)
    assert all("trace" in scenario.required_assertions for scenario in scenarios)
    assert all(scenario.evidence_tests for scenario in scenarios)


def test_pilot_trace_report_links_inbound_to_delivery_outcome(tmp_path) -> None:
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    _seed_completed_parent_dm(store)
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
    service.entrypoints.onboarding_service.apply_explicit_update(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="chat_123",
        parent_name="Maya",
        child_names=["Ava"],
    )
    service.entrypoints.onboarding_service.apply_explicit_update(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="chat_123",
        child_name="Ava",
        age="7",
        school="Roosevelt Elementary",
        google_connected=True,
    )
    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "chat_123", "is_group": False},
            "id": "msg_pilot_trace",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "What needs action?"}],
            "service": "iMessage",
        },
    }

    result = service.handle_linq_webhook(
        payload=payload,
        raw_body=json.dumps(payload).encode("utf-8"),
        webhook_signature="sig",
        webhook_timestamp=str(int(time.time())),
    )
    report = build_pilot_trace_report(
        store,
        household_id="hh_123",
        correlation_id="linq:msg_pilot_trace",
        message_id="msg_pilot_trace",
    )

    assert result.status_code == 200
    assert service.linq.sent == [{"chat_id": "chat_123", "message": "Action list:\n- Pack library books\n- Confirm pickup"}]
    assert report.summary["received"] is True
    assert report.summary["routed"] is True
    assert report.summary["hermes_started"] is True
    assert report.summary["hermes_completed"] is True
    assert report.summary["reply_generated"] is True
    assert report.summary["outbound_attempted"] is True
    assert report.summary["outbound_sent"] is True
    assert report.summary["turn_count"] == 1
    assert not report.summary["failures"]
    assert {event["metadata"]["correlation_id"] for event in report.events} == {"linq:msg_pilot_trace"}
    store.close()


def test_pilot_trace_report_explains_unresolved_group_miss(tmp_path) -> None:
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
    report = build_pilot_trace_report(
        store,
        household_id="__unresolved_household__",
        correlation_id="linq:msg_unresolved_group",
        message_id="msg_unresolved_group",
    )

    assert result.status_code == 200
    assert service.linq.sent == []
    assert report.summary["received"] is True
    assert report.summary["routed"] is False
    assert report.summary["outbound_sent"] is False
    assert "unresolved_group_household" in report.summary["failures"]
    event_types = {event["event_type"] for event in report.events}
    assert {"inbound_received", "identity_resolved", "channel_resolved"}.issubset(event_types)
    store.close()
