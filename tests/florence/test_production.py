import json
import threading
import time
from dataclasses import replace
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import florence.runtime.production as production_module
from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceRedisRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.contracts import (
    CandidateState,
    Channel,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdEvent,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    ImportedCandidate,
    Member,
    MemberRole,
)
from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
from florence.onboarding import build_onboarding_ready_syncing_message_sequence
from florence.runtime import FlorenceEntrypointResult, FlorenceProductionService
from florence.runtime import FlorenceHouseholdManagerService
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
        self.sync_waiting_text = (
            "Google is connected. I’m pulling in the first pass in the background and I’ll text you here when it’s ready."
        )

    def compose_briefing_routine_plan(self, *, household_id, channel_id, actor_member_id, operating_preferences=None):
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "kind": "briefing_routine_plan",
                "operating_preferences": list(operating_preferences or []),
            }
        )
        return [
            {"kind": "morning", "enabled": True, "hour": 6, "minute": 45, "days": [0, 1, 2, 3, 4]},
            {"kind": "evening", "enabled": True, "hour": 20, "minute": 15, "days": [0, 1, 2, 3, 4]},
            {"kind": "weekly", "enabled": True, "hour": 17, "minute": 30, "days": [6]},
        ]

    def compose_brief(self, *, household_id, channel_id, actor_member_id, brief_kind):
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "brief_kind": brief_kind.value,
            }
        )
        if brief_kind.value == "weekly":
            return "Weekly preview: science fair, pickup swap, and dinner coverage need attention."
        return "Morning brief: soccer bag, lunch order, and pickup timing are all on deck."

    def compose_operator_message(
        self,
        *,
        household_id,
        channel_id,
        actor_member_id,
        kind,
        payload=None,
        conversation_history=None,
    ):
        payload = dict(payload or {})
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "kind": kind,
                "payload": payload,
            }
        )
        if kind == "activation_brief":
            return "A few things look important from your recent email and calendar, including Science fair reminder. I can dig into any of them if you want."
        if kind == "sync_update_brief":
            current_sync = dict(payload.get("current_sync") or {})
            candidates = current_sync.get("candidates") if isinstance(current_sync.get("candidates"), list) else []
            headline = "another household update"
            if candidates:
                headline = str(candidates[0].get("title") or headline).strip() or headline
            return f"I finished another sync pass and {headline} looks like the main thing to double-check. Ask what changed if you want the short version."
        if kind == "group_promotion":
            return "Household update: Science fair reminder looks important from recent email and calendar."
        if kind == "review_prompt":
            return "This looks like a household item to double-check. Reply yes if I should add it, no if it's wrong, or skip for later."
        return self.sync_waiting_text


def _simulate_test_onboarding_turn(
    onboarding_service,
    *,
    household_id,
    channel_id,
    actor_member_id,
    payload=None,
):
    if actor_member_id is None:
        return None
    payload = dict(payload or {})
    thread_id = str(payload.get("thread_id") or "").strip()
    channel = onboarding_service.store.get_channel(channel_id)
    if not thread_id and channel is not None and channel.provider_channel_id:
        thread_id = channel.provider_channel_id
    if not thread_id:
        return None
    previous_session = onboarding_service.get_or_create_session(
        household_id=household_id,
        member_id=actor_member_id,
        thread_id=thread_id,
    )
    user_message = str(payload.get("user_message") or "").strip()
    stage = str(payload.get("stage") or previous_session.stage.value)

    if stage == "collect_parent_name" and user_message:
        transition = onboarding_service.record_parent_name(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            display_name=user_message,
        )
    elif stage == "collect_child_names" and user_message:
        child_names = [
            part.strip(" .,!?:;")
            for part in user_message.replace("\n", ",").split(",")
            if part.strip(" .,!?:;")
        ]
        if not child_names:
            return onboarding_service.get_prompt_messages(
                household_id=household_id,
                member_id=actor_member_id,
                thread_id=thread_id,
            )
        transition = onboarding_service.record_child_names(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            child_names=child_names,
        )
    elif stage == "collect_child_age" and user_message:
        transition = onboarding_service.apply_explicit_update(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            age=user_message,
        )
    elif stage == "collect_child_school" and user_message:
        transition = onboarding_service.apply_explicit_update(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            school=user_message,
        )
    elif stage == "collect_child_activities":
        transition = onboarding_service.apply_explicit_update(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            activities=[] if user_message.lower().startswith("none") else ([user_message] if user_message else []),
        )
    else:
        return onboarding_service.get_prompt_messages(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
        )

    if transition.state.is_complete:
        FlorenceHouseholdManagerService(onboarding_service.store).finalize_onboarding_completion(
            household_id=household_id,
            member_id=actor_member_id,
            channel_id=channel_id,
        )
    return onboarding_service.get_transition_messages(
        transition,
        previous_stage=previous_session.stage,
        household_id=household_id,
        member_id=actor_member_id,
        thread_id=thread_id,
    )


class _OnboardingSequenceChatService:
    def __init__(self, reply_text: str = "I can keep planning with you here."):
        self.reply_text = reply_text
        self.onboarding_service = None

    def bind_onboarding_service(self, onboarding_service) -> None:
        self.onboarding_service = onboarding_service

    def respond(self, **_kwargs):
        return SimpleNamespace(text=self.reply_text)

    def compose_onboarding_turn(
        self,
        *,
        household_id,
        channel_id,
        actor_member_id,
        payload=None,
        conversation_history=None,
    ):
        _ = conversation_history
        if self.onboarding_service is None or actor_member_id is None:
            return None
        return _simulate_test_onboarding_turn(
            self.onboarding_service,
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            payload=payload,
        )

    def compose_operator_message(self, *, household_id, channel_id, actor_member_id, kind, payload=None, conversation_history=None):
        if kind == "onboarding_turn":
            messages = self.compose_onboarding_turn(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                payload=payload,
                conversation_history=conversation_history,
            )
            return messages[0] if messages else None
        return self.reply_text


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
        redis=FlorenceRedisRuntimeConfig(url=None),
    )


def _complete_child_profile(onboarding_service, *, household_id: str, member_id: str, thread_id: str, age: str = "7", school: str = "Roosevelt Elementary", activities: str = "Soccer"):
    onboarding_service.apply_explicit_update(
        household_id=household_id,
        member_id=member_id,
        thread_id=thread_id,
        age=age,
    )
    onboarding_service.apply_explicit_update(
        household_id=household_id,
        member_id=member_id,
        thread_id=thread_id,
        school=school,
    )
    onboarding_service.apply_explicit_update(
        household_id=household_id,
        member_id=member_id,
        thread_id=thread_id,
        activities=[] if activities.lower().startswith("none") else [activities],
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
    service.household_chat_service = _FakeBriefingChatService()
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
    service.entrypoints.onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        child_names=["Ava"],
    )
    _complete_child_profile(
        service.entrypoints.onboarding_service,
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
        "florence.runtime.google_services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.google_services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.google_services.list_google_calendars",
        lambda **_: [
            GoogleCalendarMetadata(
                id="primary",
                summary="Family",
                timezone="America/Los_Angeles",
                access_role="owner",
                primary=True,
            )
        ],
    )
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_items", lambda **_: [])
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])
    launched: list[dict[str, object]] = []
    monkeypatch.setattr(service, "_launch_google_sync_job", lambda **kwargs: launched.append(kwargs))

    result = service.handle_google_callback(code="auth-code", state=raw_state)

    assert result.status_code == 200
    assert "Go back to your Messages conversation" in result.body
    assert service.linq.sent
    assert service.linq.sent[0]["chat_id"] == "dm-thread-123"
    assert [message["message"] for message in service.linq.sent] == list(build_onboarding_ready_syncing_message_sequence())
    onboarding_events = store.list_pilot_events(household_id="hh_123", event_type="onboarding_complete")
    assert len(onboarding_events) == 1
    assert len(launched) == 1
    assert str(launched[0]["connection_id"]).startswith("gconn_")
    assert launched[0]["thread_id"] == "dm-thread-123"
    assert launched[0]["notify_when_finished"] is True
    store.close()


def test_production_service_run_google_sync_queue_once_acknowledges_stale_deleted_connection(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)

    claimed = SimpleNamespace(
        job=SimpleNamespace(
            connection_id="gconn_deleted",
            thread_id="dm-thread-123",
            notify_when_finished=True,
            attempt=1,
        ),
        raw_payload='{"connection_id":"gconn_deleted"}',
    )

    class _FakeQueue:
        configured = True

        def __init__(self):
            self.acknowledged = []
            self.retried = []

        def claim(self, *, timeout_seconds=None):
            return claimed

        def acknowledge(self, item):
            self.acknowledged.append(item)

        def retry(self, item):
            self.retried.append(item)

    fake_queue = _FakeQueue()
    service.google_sync_queue = fake_queue

    def _raise_stale(**kwargs):
        raise production_module._StaleGoogleSyncJobError("gconn_deleted")

    monkeypatch.setattr(service, "process_google_sync_job", _raise_stale)

    handled = service.run_google_sync_queue_once()

    assert handled is True
    assert fake_queue.acknowledged == [claimed]
    assert fake_queue.retried == []
    store.close()


def test_production_service_google_callback_keeps_onboarding_prompt_separate_from_review(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.household_chat_service = _FakeBriefingChatService()
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
    service.entrypoints.onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        child_names=["Ava"],
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
        "florence.runtime.google_services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.google_services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.google_services.list_google_calendars",
        lambda **_: [
            GoogleCalendarMetadata(
                id="primary",
                summary="Family",
                timezone="America/Los_Angeles",
                access_role="owner",
                primary=True,
            )
        ],
    )
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_items", lambda **_: [])
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])
    launched: list[dict[str, object]] = []
    monkeypatch.setattr(service, "_launch_google_sync_job", lambda **kwargs: launched.append(kwargs))

    result = service.handle_google_callback(code="auth-code", state=raw_state)

    assert result.status_code == 200
    assert len(service.linq.sent) == 1
    assert service.linq.sent[0]["message"] == (
        "Google is connected. I’m pulling in the first pass in the background and I’ll text you here when it’s ready."
    )
    assert all("Imported item:" not in message["message"] for message in service.linq.sent)
    assert launched[0]["notify_when_finished"] is True
    store.close()


def test_production_service_google_callback_sends_shared_calendar_link_when_projection_is_available(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.household_chat_service = _FakeBriefingChatService()
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
    service.entrypoints.onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        child_names=["Ava"],
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
        "florence.runtime.google_services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.google_services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.google_services.list_google_calendars",
        lambda **_: [
            GoogleCalendarMetadata(
                id="primary",
                summary="Family",
                timezone="America/Los_Angeles",
                access_role="owner",
                primary=True,
            )
        ],
    )
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_items", lambda **_: [])
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])
    monkeypatch.setattr(
        service.household_calendar_projection_service,
        "ensure_projection",
        lambda **_: {
            "calendar_id": "cal_shared_123",
            "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
            "status": "active",
        },
    )
    monkeypatch.setattr(
        service.household_calendar_projection_service,
        "sync_household",
        lambda **_: {
            "calendar_id": "cal_shared_123",
            "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
            "status": "active",
        },
    )
    launched: list[dict[str, object]] = []
    monkeypatch.setattr(service, "_launch_google_sync_job", lambda **kwargs: launched.append(kwargs))

    result = service.handle_google_callback(code="auth-code", state=raw_state)

    assert result.status_code == 200
    assert len(service.linq.sent) == 2
    assert service.linq.sent[0]["message"] == (
        "Google is connected. I’m pulling in the first pass in the background and I’ll text you here when it’s ready."
    )
    assert service.linq.sent[1]["message"] == (
        "Your shared household calendar is here:\n"
        "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123\n"
        "I’ll keep confirmed shared events synced there."
    )
    household = store.get_household("hh_123")
    assert household is not None
    projection = household.settings["shared_google_calendar_projection"]
    assert projection["calendar_link_shared_member_ids"] == ["mem_123"]
    assert len(launched) == 1
    store.close()


def test_production_service_google_callback_only_sends_shared_calendar_link_once_per_member(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "shared_google_calendar_projection": {
                    "calendar_id": "cal_shared_123",
                    "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
                    "status": "active",
                    "calendar_link_shared_member_ids": ["mem_123"],
                }
            },
        )
    )
    service = FlorenceProductionService(settings, store=store)
    service.household_chat_service = _FakeBriefingChatService()
    service.linq = _FakeLinqClient()
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
    service.entrypoints.onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        child_names=["Ava"],
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
        "florence.runtime.google_services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.google_services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.google_services.list_google_calendars",
        lambda **_: [
            GoogleCalendarMetadata(
                id="primary",
                summary="Family",
                timezone="America/Los_Angeles",
                access_role="owner",
                primary=True,
            )
        ],
    )
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_items", lambda **_: [])
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])
    monkeypatch.setattr(
        service.household_calendar_projection_service,
        "ensure_projection",
        lambda **_: {
            "calendar_id": "cal_shared_123",
            "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
            "status": "active",
            "calendar_link_shared_member_ids": ["mem_123"],
        },
    )
    monkeypatch.setattr(
        service.household_calendar_projection_service,
        "sync_household",
        lambda **_: {
            "calendar_id": "cal_shared_123",
            "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
            "status": "active",
            "calendar_link_shared_member_ids": ["mem_123"],
        },
    )
    monkeypatch.setattr(service, "_launch_google_sync_job", lambda **kwargs: None)

    result = service.handle_google_callback(code="auth-code", state=raw_state)

    assert result.status_code == 200
    assert len(service.linq.sent) == 1
    assert service.linq.sent[0]["message"] == (
        "Google is connected. I’m pulling in the first pass in the background and I’ll text you here when it’s ready."
    )
    store.close()


def test_process_google_sync_job_skips_already_nudged_review_candidates(tmp_path, monkeypatch):
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
    service.entrypoints.onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
    )
    connection = store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
            metadata={
                "primary_calendar_id": "primary",
                "primary_calendar_summary": "Family",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    candidate = store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_123",
            title="Young Minds invoice",
            summary="Invoice due for Violet.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add Young Minds invoice due for Violet to your household plan?",
                "review_nudged_at": "2026-03-30T03:30:00Z",
            },
        )
    )

    class _FakeSyncWorkerService:
        def __init__(self, *_args, **_kwargs):
            pass

        def sync_connection(self, connection_id, **_kwargs):
            assert connection_id == "gconn_123"
            return SimpleNamespace(
                connection=connection,
                sync_result=SimpleNamespace(candidates=[candidate]),
            )

    monkeypatch.setattr("florence.runtime.production.FlorenceGoogleSyncWorkerService", _FakeSyncWorkerService)

    service.process_google_sync_job(
        connection_id="gconn_123",
        thread_id="dm-thread-123",
        notify_when_finished=False,
        raise_on_error=True,
    )

    assert service.linq.sent == []
    store.close()


def test_process_google_sync_job_prefers_household_group_for_activation_brief(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    service.household_chat_service = _FakeBriefingChatService()
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
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )
    service.entrypoints.onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
    )
    connection = store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={
                "last_gmail_item_count": 9,
                "last_calendar_item_count": 3,
                "primary_calendar_id": "primary",
                "primary_calendar_summary": "Family",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
        title="Science fair reminder",
        summary="Science fair is Friday.",
        state=CandidateState.PENDING_REVIEW,
    )

    class _FakeSyncWorkerService:
        def __init__(self, *_args, **_kwargs):
            pass

        def sync_connection(self, connection_id, **_kwargs):
            assert connection_id == "gconn_123"
            return SimpleNamespace(
                connection=connection,
                sync_result=SimpleNamespace(candidates=[candidate]),
            )

    monkeypatch.setattr("florence.runtime.production.FlorenceGoogleSyncWorkerService", _FakeSyncWorkerService)
    monkeypatch.setattr(service, "_nudge_for_new_pending_candidates", lambda **_kwargs: False)

    service.process_google_sync_job(
        connection_id="gconn_123",
        thread_id="dm-thread-123",
        notify_when_finished=True,
        raise_on_error=True,
    )

    assert len(service.linq.sent) == 1
    assert service.linq.sent[0]["chat_id"] == "group-thread-123"
    assert "A few things look important from your recent email and calendar" in service.linq.sent[0]["message"]
    assert "Science fair reminder" in service.linq.sent[0]["message"]
    stored_messages = store.list_channel_messages(channel_id="chan_group_123")
    assert len(stored_messages) == 1
    assert "promotion_kind" not in stored_messages[0].metadata
    assert "promotable_group_message" not in stored_messages[0].metadata
    updated_connection = store.get_google_connection("gconn_123")
    assert updated_connection is not None
    assert updated_connection.metadata["initial_sync_activation_brief_sent_at"]
    assert updated_connection.metadata["initial_sync_activation_brief_channel_id"] == "chan_group_123"
    assert updated_connection.metadata["last_sync_brief_kind"] == "activation"
    assert service.household_chat_service.calls[0]["channel_id"] == "chan_group_123"
    store.close()


def test_production_service_process_google_sync_job_does_not_repeat_activation_brief(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_123", household_id="hh_123", display_name="Maya", role=MemberRole.ADMIN))
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
    initial_connection = store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={
                "last_gmail_item_count": 9,
                "last_calendar_item_count": 3,
                "primary_calendar_id": "primary",
                "primary_calendar_summary": "Family",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
        title="Science fair reminder",
        summary="Science fair is Friday.",
        state=CandidateState.PENDING_REVIEW,
    )

    class _FakeSyncWorkerService:
        def __init__(self, *_args, **_kwargs):
            pass

        def sync_connection(self, connection_id, **_kwargs):
            assert connection_id == "gconn_123"
            return SimpleNamespace(
                connection=store.get_google_connection(connection_id) or initial_connection,
                sync_result=SimpleNamespace(candidates=[candidate]),
            )

    monkeypatch.setattr("florence.runtime.production.FlorenceGoogleSyncWorkerService", _FakeSyncWorkerService)
    monkeypatch.setattr(service, "_nudge_for_new_pending_candidates", lambda **_kwargs: False)

    service.process_google_sync_job(
        connection_id="gconn_123",
        thread_id="dm-thread-123",
        notify_when_finished=True,
        raise_on_error=True,
    )
    service.process_google_sync_job(
        connection_id="gconn_123",
        thread_id="dm-thread-123",
        notify_when_finished=True,
        raise_on_error=True,
    )

    assert len(service.linq.sent) == 1
    store.close()


def test_production_service_process_google_sync_job_sends_sync_update_brief_after_activation(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    service.household_chat_service = _FakeBriefingChatService()
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_123", household_id="hh_123", display_name="Maya", role=MemberRole.ADMIN))
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
    initial_connection = store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={
                "last_gmail_item_count": 9,
                "last_calendar_item_count": 3,
                "last_candidate_count": 1,
                "primary_calendar_id": "primary",
                "primary_calendar_summary": "Family",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    first_candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
        title="Science fair reminder",
        summary="Science fair is Friday.",
        state=CandidateState.PENDING_REVIEW,
    )
    second_candidate = ImportedCandidate(
        id="cand_456",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
        source_identifier="google_calendar:event_456",
        title="Dentist appointment moved",
        summary="Pickup timing may need a tweak.",
        state=CandidateState.PENDING_REVIEW,
    )
    sync_results = [
        (
            {
                "last_gmail_item_count": 9,
                "last_calendar_item_count": 3,
                "last_candidate_count": 1,
            },
            [first_candidate],
        ),
        (
            {
                "last_gmail_item_count": 2,
                "last_calendar_item_count": 1,
                "last_candidate_count": 1,
            },
            [second_candidate],
        ),
    ]

    call_index = {"value": 0}

    class _FakeSyncWorkerService:
        def __init__(self, *_args, **_kwargs):
            pass

        def sync_connection(self, connection_id, **_kwargs):
            assert connection_id == "gconn_123"
            metadata_update, candidates = sync_results[call_index["value"]]
            call_index["value"] += 1
            current = store.get_google_connection(connection_id) or initial_connection
            updated = store.upsert_google_connection(
                replace(
                    current,
                    metadata={
                        **dict(current.metadata),
                        **metadata_update,
                    },
                )
            )
            return SimpleNamespace(
                connection=updated,
                sync_result=SimpleNamespace(candidates=candidates),
            )

    monkeypatch.setattr("florence.runtime.production.FlorenceGoogleSyncWorkerService", _FakeSyncWorkerService)
    monkeypatch.setattr(service, "_nudge_for_new_pending_candidates", lambda **_kwargs: False)

    service.process_google_sync_job(
        connection_id="gconn_123",
        thread_id="dm-thread-123",
        notify_when_finished=True,
        raise_on_error=True,
    )
    service.process_google_sync_job(
        connection_id="gconn_123",
        thread_id="dm-thread-123",
        notify_when_finished=True,
        raise_on_error=True,
    )

    assert len(service.linq.sent) == 2
    assert "A few things look important from your recent email and calendar" in service.linq.sent[0]["message"]
    assert "I finished another sync pass" in service.linq.sent[1]["message"]
    assert "Dentist appointment moved" in service.linq.sent[1]["message"]
    sync_update_calls = [
        call
        for call in service.household_chat_service.calls
        if call["kind"] == "sync_update_brief"
    ]
    assert len(sync_update_calls) == 1
    assert sync_update_calls[0]["payload"]["previous_sync"]["candidate_titles"] == ["Science fair reminder"]
    assert sync_update_calls[0]["payload"]["current_sync"]["candidate_count"] == 1
    updated_connection = store.get_google_connection("gconn_123")
    assert updated_connection is not None
    assert updated_connection.metadata["last_sync_update_brief_sent_at"]
    assert updated_connection.metadata["last_sync_brief_kind"] == "update"
    store.close()


def test_production_service_first_dm_sends_onboarding_sequence_as_separate_messages(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    service.household_chat_service = _OnboardingSequenceChatService()
    service.household_chat_service.bind_onboarding_service(service.entrypoints.onboarding_service)

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
    assert len(service.linq.sent) == 6
    assert service.linq.sent[0]["message"] == "Hi, I'm Florence."
    assert service.linq.sent[1]["message"] == (
        "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise."
    )
    assert service.linq.sent[2]["message"] == "Connect your Google account so I can pull up to the last year of family email and calendar in the background while we keep going here."
    assert service.linq.sent[3]["message"].startswith("https://accounts.google.com/")
    assert service.linq.sent[4]["message"] == "Once Google says you're connected, come right back here. You can also keep answering my questions while it runs."
    assert service.linq.sent[5]["message"] == "What are your kids' names? Send all of them in one message, one per line or comma-separated."
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
    service.household_chat_service = _FakeBriefingChatService()
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
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


def test_production_service_run_sync_pass_prefers_household_group_for_briefing(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    fake_chat = _FakeBriefingChatService()
    service.household_chat_service = fake_chat
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
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
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
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
            "chat_id": "group-thread-123",
            "message": "Morning brief: soccer bag, lunch order, and pickup timing are all on deck.",
        }
    ]
    assert fake_chat.calls[0]["channel_id"] == "chan_group_123"
    store.close()


def test_production_service_run_sync_pass_sends_review_sweep_for_pending_backlog(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    fake_chat = _FakeBriefingChatService()
    service.household_chat_service = fake_chat
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
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
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_123",
            title="Young Minds invoice",
            summary="Needs confirmation.",
            state=CandidateState.PENDING_REVIEW,
            requires_confirmation=True,
            metadata={"confirmation_question": "Should I add this?"},
        )
    )

    result = service.run_sync_pass()

    assert result["review_sweeps"] == 1
    assert len(service.linq.sent) == 1
    assert service.linq.sent[0]["chat_id"] == "dm-thread-123"
    review_call = next(call for call in fake_chat.calls if call.get("kind") == "review_prompt")
    assert review_call["payload"]["trigger"] == "scheduled_review_sweep"
    assert review_call["payload"]["pending_review_count"] == 1
    store.close()


def test_production_service_run_sync_pass_sends_scheduled_sync_update_brief(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.linq = _FakeLinqClient()
    fake_chat = _FakeBriefingChatService()
    service.household_chat_service = fake_chat
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
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
    previous_connection = store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={
                "initial_sync_activation_brief_sent_at": "2026-03-20T08:00:00+00:00",
                "initial_sync_activation_brief_channel_id": "chan_dm_123",
                "last_sync_brief_channel_id": "chan_dm_123",
                "last_sync_brief_kind": "activation",
                "last_sync_brief_snapshot": {
                    "gmail_count": 3,
                    "calendar_count": 1,
                    "candidate_count": 1,
                    "candidate_titles": ["Science fair reminder"],
                    "candidate_ids": ["cand_123"],
                    "signature": "previous",
                },
                "last_gmail_item_count": 3,
                "last_calendar_item_count": 1,
                "last_candidate_count": 1,
                "primary_calendar_id": "primary",
                "primary_calendar_summary": "Family",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    updated_connection = store.upsert_google_connection(
        replace(
            previous_connection,
            metadata={
                **dict(previous_connection.metadata),
                "last_gmail_item_count": 2,
                "last_calendar_item_count": 1,
                "last_candidate_count": 1,
            },
        )
    )
    candidate = ImportedCandidate(
        id="cand_456",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
        source_identifier="google_calendar:event_456",
        title="Dentist appointment moved",
        summary="Pickup timing may need a tweak.",
        state=CandidateState.PENDING_REVIEW,
    )
    service.sync_worker = SimpleNamespace(
        sync_household=lambda **_kwargs: [
            SimpleNamespace(connection=updated_connection, sync_result=SimpleNamespace(candidates=[candidate]))
        ]
    )
    service._nudge_for_new_pending_candidates = lambda **_kwargs: False

    result = service.run_sync_pass()

    assert result["sync_update_briefs"] == 1
    assert len(service.linq.sent) == 1
    assert service.linq.sent[0]["chat_id"] == "dm-thread-123"
    assert "I finished another sync pass" in service.linq.sent[0]["message"]
    sync_update_call = next(call for call in fake_chat.calls if call["kind"] == "sync_update_brief")
    assert sync_update_call["payload"]["previous_sync"]["candidate_titles"] == ["Science fair reminder"]
    assert sync_update_call["payload"]["current_sync"]["candidate_count"] == 1
    events = store.list_pilot_events(household_id="hh_123", event_type="sync_update_brief_sent")
    assert len(events) == 1
    assert events[0].metadata["trigger"] == "scheduled_sync_pass"
    store.close()


def test_production_service_briefing_routine_planning_uses_hermes_for_operating_preferences(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    fake_chat = _FakeBriefingChatService()
    service.household_chat_service = fake_chat
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
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
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_briefing_123",
                household_id="hh_123",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Briefing cadence",
                metadata={
                    "category": "operating_rule",
                    "value": "Weekday morning brief at 6:45 and evening check-in on school nights at 8:30pm.",
                },
            )
        ],
    )

    routines = service.household_manager_service.ensure_briefing_routines(household_id="hh_123")

    assert len(routines) == 3
    assert any(call.get("kind") == "briefing_routine_plan" for call in fake_chat.calls)
    morning = next(routine for routine in routines if routine.metadata.get("brief_kind") == "morning")
    assert morning.metadata["planning_source"] == "hermes"
    store.close()
