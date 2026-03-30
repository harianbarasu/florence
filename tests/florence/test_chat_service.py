from florence.contracts import (
    Channel,
    ChannelType,
    ChildProfile,
    Household,
    HouseholdBriefingKind,
    HouseholdEvent,
    HouseholdMeal,
    HouseholdNudge,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdShoppingItem,
    Member,
    MemberRole,
    HouseholdWorkItem,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.state import FlorenceStateDB
from hermes_state import SessionDB


class _FakeAgent:
    created = []
    last_run = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _FakeAgent.created.append(kwargs)

    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None):
        _FakeAgent.last_run = {
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": conversation_history or [],
            "task_id": task_id,
        }
        return {"final_response": "Use the confirmed plan: Ava has soccer on Thursday."}


class _RotatingSessionAgent(_FakeAgent):
    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None):
        result = super().run_conversation(
            user_message,
            system_message,
            conversation_history=conversation_history,
            task_id=task_id,
        )
        self.session_id = "florence-channel-chan_dm_123-next"
        return result


def test_household_chat_service_uses_hermes_agent_with_confirmed_state(tmp_path):
    _FakeAgent.created.clear()
    _FakeAgent.last_run = None
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "manager_profile": {
                    "operating_preferences": "Weekday morning brief at 6:45, no texts after 9pm, always ask before spending money.",
                }
            },
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.replace_child_profiles(
        household_id="hh_123",
        children=[ChildProfile(id="child_ava", household_id="hh_123", full_name="Ava")],
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.ACTIVITY,
        items=[
            HouseholdProfileItem(
                id="activity_soccer",
                household_id="hh_123",
                kind=HouseholdProfileKind.ACTIVITY,
                label="Soccer",
                member_id="mem_123",
            )
        ],
    )
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_123",
            household_id="hh_123",
            title="Ava soccer practice",
            starts_at="2026-03-19T00:00:00+00:00",
            ends_at="2026-03-19T01:00:00+00:00",
            timezone="America/Los_Angeles",
            location="North field",
        )
    )
    store.upsert_household_work_item(
        HouseholdWorkItem(
            id="work_123",
            household_id="hh_123",
            title="Order school lunches",
            due_at="2026-03-20T17:00:00+00:00",
            metadata={"category": "school_admin"},
        )
    )
    store.upsert_household_routine(
        HouseholdRoutine(
            id="routine_123",
            household_id="hh_123",
            title="Friday lunch order check",
            cadence="weekly on Friday at 9am",
            next_due_at="2026-03-20T16:00:00+00:00",
            metadata={"category": "school_admin"},
        )
    )
    store.upsert_household_nudge(
        HouseholdNudge(
            id="nudge_123",
            household_id="hh_123",
            target_kind=HouseholdNudgeTargetKind.WORK_ITEM,
            target_id="work_123",
            message="Lunch order cutoff is today.",
            channel_id="chan_dm_123",
            scheduled_for="2026-03-20T15:00:00+00:00",
            metadata={"follow_up_policy": "until_acknowledged"},
        )
    )
    store.upsert_household_meal(
        HouseholdMeal(
            id="meal_123",
            household_id="hh_123",
            title="Taco night",
            meal_type="dinner",
            scheduled_for="2026-03-20T18:00:00+00:00",
            metadata={"serves": 4},
        )
    )
    store.upsert_household_shopping_item(
        HouseholdShoppingItem(
            id="shop_123",
            household_id="hh_123",
            title="tortillas",
            list_name="groceries",
            quantity="2",
            unit="packs",
            meal_id="meal_123",
            needed_by="2026-03-20T16:00:00+00:00",
            metadata={"store_section": "bakery"},
        )
    )

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="What is happening this week?",
    )

    assert reply is not None
    assert "soccer" in reply.text.lower()
    assert _FakeAgent.created[0]["provider"] == "anthropic"
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_chat"]
    assert _FakeAgent.created[0]["disabled_toolsets"] is None
    assert _FakeAgent.created[0]["session_id"] == "florence-channel-chan_dm_123"
    assert _FakeAgent.created[0]["session_db"] is not None
    assert "Confirmed household events" in _FakeAgent.last_run["system_message"]
    assert "Ava soccer practice" in _FakeAgent.last_run["system_message"]
    assert "Open household work items" in _FakeAgent.last_run["system_message"]
    assert "Order school lunches" in _FakeAgent.last_run["system_message"]
    assert "Active household routines" in _FakeAgent.last_run["system_message"]
    assert "Friday lunch order check" in _FakeAgent.last_run["system_message"]
    assert "Pending household nudges" in _FakeAgent.last_run["system_message"]
    assert "Lunch order cutoff is today." in _FakeAgent.last_run["system_message"]
    assert "Upcoming meal plan" in _FakeAgent.last_run["system_message"]
    assert "Taco night" in _FakeAgent.last_run["system_message"]
    assert "Open grocery list" in _FakeAgent.last_run["system_message"]
    assert "tortillas" in _FakeAgent.last_run["system_message"]
    assert "general household agent" in _FakeAgent.last_run["system_message"]
    assert "private parent DM" in _FakeAgent.last_run["system_message"]
    assert "Florence household-state tools" in _FakeAgent.last_run["system_message"]
    assert "household_search_google_inbox" in _FakeAgent.last_run["system_message"]
    assert "Do not ask the user to forward or paste an email" in _FakeAgent.last_run["system_message"]
    assert "Household operating policy:" in _FakeAgent.last_run["system_message"]
    assert _FakeAgent.last_run["task_id"].startswith("florence-household-")
    store.close()


def test_household_chat_service_prefers_session_db_transcript_for_channel(tmp_path):
    _FakeAgent.created.clear()
    _FakeAgent.last_run = None
    store = FlorenceStateDB(tmp_path / "florence.db")
    session_db = SessionDB(tmp_path / "hermes-state.db")
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
            metadata={"hermes_session_id": "florence-channel-chan_dm_123-next"},
        )
    )
    session_db.create_session(
        session_id="florence-channel-chan_dm_123-next",
        source="florence",
        model="anthropic/claude-opus-4.6",
    )
    session_db.append_message(
        session_id="florence-channel-chan_dm_123-next",
        role="user",
        content="Earlier question from Hermes transcript",
    )
    session_db.append_message(
        session_id="florence-channel-chan_dm_123-next",
        role="assistant",
        content="Earlier Hermes reply",
    )

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
        session_db=session_db,
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="What were we talking about?",
        conversation_history=[],
    )

    assert reply is not None
    assert _FakeAgent.created[0]["session_id"] == "florence-channel-chan_dm_123-next"
    assert _FakeAgent.last_run["conversation_history"] == [
        {"role": "user", "content": "Earlier question from Hermes transcript"},
        {"role": "assistant", "content": "Earlier Hermes reply"},
    ]
    session_db.close()
    store.close()


def test_household_chat_service_persists_rotated_session_id_to_channel(tmp_path):
    _FakeAgent.created.clear()
    _FakeAgent.last_run = None
    store = FlorenceStateDB(tmp_path / "florence.db")
    session_db = SessionDB(tmp_path / "hermes-state.db")
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_RotatingSessionAgent,
        session_db=session_db,
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="Keep going",
    )

    assert reply is not None
    updated = store.get_channel("chan_dm_123")
    assert updated is not None
    assert updated.metadata["hermes_session_id"] == "florence-channel-chan_dm_123-next"
    session_db.close()
    store.close()


def test_household_chat_service_compose_brief_uses_briefing_toolset(tmp_path):
    _FakeAgent.created.clear()
    _FakeAgent.last_run = None
    store = FlorenceStateDB(tmp_path / "florence.db")
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
    )

    brief = service.compose_brief(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        brief_kind=HouseholdBriefingKind.MORNING,
    )

    assert brief is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert _FakeAgent.created[0]["disabled_toolsets"] == []
    assert "automatic household briefing" in _FakeAgent.last_run["system_message"]
    assert "morning brief" in _FakeAgent.last_run["user_message"].lower()
    store.close()
