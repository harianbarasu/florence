import json
import threading
import time
from dataclasses import replace
from urllib.parse import parse_qs, urlparse

from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.contracts import (
    CandidateState,
    Channel,
    ChannelType,
    GoogleSourceKind,
    Household,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    ImportedCandidate,
    Member,
    MemberRole,
)
from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
from florence.onboarding import OnboardingVariant
from florence.runtime import FlorenceEntrypointResult, FlorenceProductionService
from florence.state import FlorenceStateDB


class _FakeLinqClient:
    def __init__(self):
        self.sent = []

    def verify_webhook_signature(self, *, raw_body, timestamp, signature):
        return True

    def send_text(self, *, chat_id, message):
        self.sent.append({"chat_id": chat_id, "message": message})


class _FakeBriefingChatService:
    def __init__(self):
        self.calls = []

    def compose_brief(self, *, household_id, channel_id, actor_member_id, brief_kind):
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "brief_kind": brief_kind.value,
            }
        )
        return "Morning brief: soccer bag, lunch order, and pickup timing are all on deck."


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
    service.entrypoints.onboarding_service.variant_selector = lambda _household_id, _member_id: OnboardingVariant.HYBRID
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
    service.entrypoints.onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )
    service.entrypoints.onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        child_names=["Ava"],
    )
    service.entrypoints.onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        school_labels=["Roosevelt Elementary"],
    )
    service.entrypoints.onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        activity_labels=["Soccer"],
    )
    service.entrypoints.onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        household_operations=["school forms", "pickup planning"],
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
    assert "reminders and nudges" in service.linq.sent[0]["message"].lower()
    store.close()


def test_production_service_google_callback_keeps_onboarding_prompt_separate_from_review(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.entrypoints.onboarding_service.variant_selector = lambda _household_id, _member_id: OnboardingVariant.HYBRID
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
    service.entrypoints.onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )
    service.entrypoints.onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        child_names=["Ava"],
    )
    service.entrypoints.onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        school_labels=["Roosevelt Elementary"],
    )
    service.entrypoints.onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        activity_labels=["Soccer"],
    )
    service.entrypoints.onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        household_operations=["school forms", "pickup planning"],
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail_123",
            title="Pending review candidate",
            summary="Needs confirmation.",
            state=CandidateState.PENDING_REVIEW,
            requires_confirmation=True,
            metadata={"confirmation_question": "Should I add this?"},
        )
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
    assert len(service.linq.sent) == 1
    assert "How proactive should I be with reminders and nudges?" in service.linq.sent[0]["message"]
    assert "Imported item:" not in service.linq.sent[0]["message"]
    store.close()


def test_production_service_first_dm_sends_onboarding_sequence_as_separate_messages(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.entrypoints.onboarding_service.variant_selector = lambda _household_id, _member_id: OnboardingVariant.HYBRID
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
    assert [item["message"] for item in service.linq.sent] == [
        "Hi, I'm Florence.",
        "I help run the household with you by learning the family map first, then keeping up with reminders, logistics, school noise, and schedule changes.",
        "Start with the kids I should know about: first name plus grade or age if helpful. One per line or comma-separated is fine.",
    ]
    store.close()


def test_production_service_returns_500_when_linq_webhook_processing_fails(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    monkeypatch.setattr(
        service.entrypoints,
        "handle_linq_payload",
        lambda payload: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "dm-thread-123", "is_group": False},
            "id": "msg_err",
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

    assert result.status_code == 500
    assert json.loads(result.body) == {"ok": False, "error": "internal_linq_webhook_error"}
    assert service.linq.sent == []
    store.close()


def test_production_service_ignores_duplicate_linq_message_ids(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()

    payload = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "dm-thread-123", "is_group": False},
            "id": "msg_dup_123",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "Maya"}],
            "service": "iMessage",
        },
    }
    raw_body = json.dumps(payload).encode("utf-8")

    first = service.handle_linq_webhook(
        payload=payload,
        raw_body=raw_body,
        webhook_signature="sig",
        webhook_timestamp=str(int(time.time())),
    )
    sent_count_after_first = len(service.linq.sent)
    second = service.handle_linq_webhook(
        payload=payload,
        raw_body=raw_body,
        webhook_signature="sig",
        webhook_timestamp=str(int(time.time())),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(service.linq.sent) == sent_count_after_first
    store.close()


def test_production_service_serializes_webhook_processing_per_chat(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()

    counter_lock = threading.Lock()
    state = {"calls": 0, "active": 0, "max_active": 0}
    second_entered = threading.Event()

    def fake_handle(_payload):
        with counter_lock:
            state["calls"] += 1
            call_number = state["calls"]
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
        # If calls can overlap for the same chat, this first call waits and the
        # second call will enter and set the event. With per-chat locking that
        # second call cannot enter until the first exits.
        if call_number == 1:
            second_entered.wait(timeout=0.2)
        else:
            second_entered.set()
        time.sleep(0.03)
        with counter_lock:
            state["active"] -= 1
        return FlorenceEntrypointResult(consumed=True)

    monkeypatch.setattr(service.entrypoints, "handle_linq_payload", fake_handle)

    payload_one = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "dm-thread-serial-1", "is_group": False},
            "id": "msg_serial_1",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "first"}],
            "service": "iMessage",
        },
    }
    payload_two = {
        "webhook_version": "2026-02-03",
        "event_type": "message.received",
        "data": {
            "chat": {"id": "dm-thread-serial-1", "is_group": False},
            "id": "msg_serial_2",
            "direction": "inbound",
            "sender_handle": {"handle": "+15555550123", "is_me": False},
            "parts": [{"type": "text", "value": "second"}],
            "service": "iMessage",
        },
    }
    raw_one = json.dumps(payload_one).encode("utf-8")
    raw_two = json.dumps(payload_two).encode("utf-8")

    start = threading.Event()
    results = []

    def run(payload, raw_body):
        start.wait(timeout=1)
        results.append(
            service.handle_linq_webhook(
                payload=payload,
                raw_body=raw_body,
                webhook_signature="sig",
                webhook_timestamp=str(int(time.time())),
            )
        )

    t1 = threading.Thread(target=run, args=(payload_one, raw_one))
    t2 = threading.Thread(target=run, args=(payload_two, raw_two))
    t1.start()
    t2.start()
    start.set()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert len(results) == 2
    assert all(result.status_code == 200 for result in results)
    assert state["max_active"] == 1
    store.close()


def test_production_service_run_sync_pass_sends_due_household_nudges_without_google_activity(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
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
    service.entrypoints.onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )
    nudge = service.household_manager_service.schedule_nudge(
        household_id="hh_123",
        message="Taco night is tomorrow. Make sure groceries are in.",
        scheduled_for="2026-03-24T12:00:00+00:00",
    )

    result = service.run_sync_pass()

    assert result["nudges_sent"] == 1
    assert result["nudges"] == 1
    assert result["briefings_sent"] == 0
    assert service.linq.sent == [
        {"chat_id": "dm-thread-123", "message": "Taco night is tomorrow. Make sure groceries are in."}
    ]
    stored_nudge = store.get_household_nudge(nudge.id)
    assert stored_nudge is not None
    assert stored_nudge.status.value == "sent"
    assert stored_nudge.sent_at is not None
    store.close()


def test_production_service_run_sync_pass_sends_due_household_briefing(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    service.entrypoints.household_chat_service = _FakeBriefingChatService()
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={"manager_profile": {"operating_preferences": "Weekday morning brief at 6:45."}},
        )
    )
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
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
    service.entrypoints.onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )
    routines = service.household_manager_service.ensure_briefing_routines(household_id="hh_123")
    morning = next(routine for routine in routines if routine.metadata.get("brief_kind") == "morning")
    store.upsert_household_routine(
        replace(
            morning,
            status=HouseholdRoutineStatus.ACTIVE,
            next_due_at="2026-03-24T00:00:00+00:00",
        )
    )

    result = service.run_sync_pass()

    assert result["briefings_sent"] == 1
    assert service.linq.sent == [
        {
            "chat_id": "dm-thread-123",
            "message": "Morning brief: soccer bag, lunch order, and pickup timing are all on deck.",
        }
    ]
    updated = store.get_household_routine(morning.id)
    assert updated is not None
    assert updated.last_completed_at is not None
    events = store.list_pilot_events(household_id="hh_123", event_type="briefing_sent")
    assert len(events) == 1
    store.close()
