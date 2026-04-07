from types import SimpleNamespace

from florence.contracts import (
    Channel,
    ChannelType,
    ChildProfile,
    Household,
    HouseholdBriefingKind,
    HouseholdEvent,
    HouseholdEventStatus,
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
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_123",
                household_id="hh_123",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Kid spice preference",
                child_id="child_ava",
                metadata={"value": "Ava will not eat spicy food."},
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
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_124",
            household_id="hh_123",
            title="Possible camp carpool",
            starts_at="2026-03-21T15:00:00+00:00",
            ends_at="2026-03-21T16:00:00+00:00",
            timezone="America/Los_Angeles",
            status=HouseholdEventStatus.TENTATIVE,
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
    assert _FakeAgent.created[0]["skip_memory"] is False
    assert _FakeAgent.created[0]["skip_local_memory"] is True
    assert _FakeAgent.created[0]["honcho_session_key"] == "florence:member:hh_123:mem_123"
    assert _FakeAgent.created[0]["session_search_kwargs"] == {
        "source_filter": ["florence"],
        "allowed_session_ids": ["florence-channel-chan_dm_123"],
    }
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
    assert "inbox -> plan, capture -> handled, and briefs -> stay ahead" in _FakeAgent.last_run["system_message"]
    assert "school email, screenshots, flyers, photos, mental dumps, meals, groceries" in _FakeAgent.last_run["system_message"]
    assert "Your memory stack is: authoritative Florence household state, Florence session history, and Florence-scoped Honcho memory." in _FakeAgent.last_run["system_message"]
    assert "Household scope model:" in _FakeAgent.last_run["system_message"]
    assert "Shared household scope: facts, plans, reminders, meals, grocery items, routines, and events" in _FakeAgent.last_run["system_message"]
    assert "Private parent scope: DM-only context, mental-load triage, emotional processing" in _FakeAgent.last_run["system_message"]
    assert "Tentative scope: provisional plans or not-yet-confirmed details" in _FakeAgent.last_run["system_message"]
    assert "Unreviewed imports or unresolved source classifications are not shared household facts yet." in _FakeAgent.last_run["system_message"]
    assert "When a user tells Florence a stable preference, constraint, rule, or working style that should affect future behavior, save it with household_record_preference." in _FakeAgent.last_run["system_message"]
    assert "Use household_search_state when you need the latest tracked household picture" in _FakeAgent.last_run["system_message"]
    assert "household_search_state now returns scope context too" in _FakeAgent.last_run["system_message"]
    assert "ground the answer in household_search_state and session_search first" in _FakeAgent.last_run["system_message"]
    assert "Use session_search and Honcho memory to recover earlier commitments, preferences, and threads of work" in _FakeAgent.last_run["system_message"]
    assert "Remembered household preferences:" in _FakeAgent.last_run["system_message"]
    assert "Ava | Kid spice preference: Ava will not eat spicy food." in _FakeAgent.last_run["system_message"]
    assert "When the user asks what matters, what changed, or what they are forgetting" in _FakeAgent.last_run["system_message"]
    assert "household_upsert_meal and household_upsert_shopping_item" in _FakeAgent.last_run["system_message"]
    assert "The family group chat is the primary operating surface for shared household work." in _FakeAgent.last_run["system_message"]
    assert "private parent DM" in _FakeAgent.last_run["system_message"]
    assert "private side channel" in _FakeAgent.last_run["system_message"]
    assert "raw mental-load dumps, emotional support, and individually scoped reasoning stay private by default" in _FakeAgent.last_run["system_message"]
    assert "Memory policy: use private member-scoped memory and recall freely here" in _FakeAgent.last_run["system_message"]
    assert "Current scope: private parent DM. Florence may use shared household context plus this parent's private context here." in _FakeAgent.last_run["system_message"]
    assert "offer a concise group-safe summary instead of echoing the raw message" in _FakeAgent.last_run["system_message"]
    assert "Tentative tracked events:" in _FakeAgent.last_run["system_message"]
    assert "Possible camp carpool | starts 2026-03-21T15:00:00+00:00 | ends 2026-03-21T16:00:00+00:00 | status tentative" in _FakeAgent.last_run["system_message"]
    assert "Florence household-state tools" in _FakeAgent.last_run["system_message"]
    assert "household_search_google_inbox" in _FakeAgent.last_run["system_message"]
    assert "in a parent DM it defaults to that parent's inbox, while in the family group it only uses shared-household inbox scope" in _FakeAgent.last_run["system_message"]
    assert "Do not ask the user to forward or paste an email" in _FakeAgent.last_run["system_message"]
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


def test_household_chat_service_group_prompt_emphasizes_shared_coordination(tmp_path):
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
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group_thread_123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
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
        channel_id="chan_group_123",
        actor_member_id="mem_123",
        message_text="What matters this week?",
    )

    assert reply is not None
    assert "shared household group chat" in _FakeAgent.last_run["system_message"]
    assert "primary operating surface for the family" in _FakeAgent.last_run["system_message"]
    assert "optimize for shared visibility, coordination, ownership, schedule changes, reminders, meals, grocery planning" in _FakeAgent.last_run["system_message"]
    assert "Memory policy: treat this thread as shared household memory." in _FakeAgent.last_run["system_message"]
    assert "Current scope: shared household group. Florence may use shared household context here" in _FakeAgent.last_run["system_message"]
    assert "must not reveal private DM-only context unless it was explicitly promoted" in _FakeAgent.last_run["system_message"]
    assert _FakeAgent.created[0]["honcho_session_key"] == "florence:household:hh_123"
    store.close()


def test_household_chat_service_builds_household_session_search_scope(tmp_path):
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
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="group_thread_123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Family",
            metadata={"hermes_session_id": "florence-channel-chan_group_123-next"},
        )
    )
    session_db.create_session(
        session_id="florence-channel-chan_dm_123-root",
        source="florence",
        model="anthropic/claude-opus-4.6",
    )
    session_db.create_session(
        session_id="florence-channel-chan_dm_123-next",
        source="florence",
        model="anthropic/claude-opus-4.6",
        parent_session_id="florence-channel-chan_dm_123-root",
    )
    session_db.create_session(
        session_id="florence-channel-chan_group_123-root",
        source="florence",
        model="anthropic/claude-opus-4.6",
    )
    session_db.create_session(
        session_id="florence-channel-chan_group_123-next",
        source="florence",
        model="anthropic/claude-opus-4.6",
        parent_session_id="florence-channel-chan_group_123-root",
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
        message_text="What changed?",
    )

    assert reply is not None
    assert _FakeAgent.created[0]["skip_memory"] is False
    assert _FakeAgent.created[0]["skip_local_memory"] is True
    assert _FakeAgent.created[0]["honcho_session_key"] == "florence:member:hh_123:mem_123"
    assert _FakeAgent.created[0]["session_search_kwargs"] == {
        "source_filter": ["florence"],
        "allowed_session_ids": [
            "florence-channel-chan_dm_123-root",
            "florence-channel-chan_group_123-root",
        ],
    }
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
    assert "automatic household briefing" in _FakeAgent.last_run["system_message"]
    assert "surface what matters, what might slip, and the clearest next step" in _FakeAgent.last_run["system_message"]
    assert "use household_search_state to refresh the tracked household picture" in _FakeAgent.last_run["system_message"]
    assert "Use session_search and Honcho recall when recent commitments, context, or follow-through might matter for the brief." in _FakeAgent.last_run["system_message"]
    assert "use household_search_google_inbox" in _FakeAgent.last_run["system_message"]
    assert "Do not present uncertain Gmail or calendar imports as confirmed household facts." in _FakeAgent.last_run["system_message"]
    assert "morning brief" in _FakeAgent.last_run["user_message"].lower()
    store.close()


def test_household_chat_service_general_prompt_emphasizes_stateful_capture_and_recall(tmp_path):
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

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="Can you make dinner from what is in the fridge and build a grocery list for tomorrow?",
    )

    assert reply is not None
    assert "Before creating duplicate tasks, events, meals, grocery items, or reminders, check household state, recent Florence context, and connected inbox context when they help." in _FakeAgent.last_run["system_message"]
    assert "When the user is asking Florence to capture, track, plan, or manage something, prefer updating durable household state and reply with a concise handled summary of what Florence saved, planned, or still needs." in _FakeAgent.last_run["system_message"]
    assert "For meal and grocery requests, prefer creating or updating household meals and shopping items instead of leaving the plan only in chat." in _FakeAgent.last_run["system_message"]
    assert _FakeAgent.last_run["user_message"] == "Can you make dinner from what is in the fridge and build a grocery list for tomorrow?"
    store.close()


def test_household_chat_service_compose_operator_message_group_promotion_uses_group_safe_prompt(tmp_path):
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

    summary = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="group_promotion",
        payload={"source_text": "Parent: Science fair is Friday.\nFlorence: I can remind you and keep dinner simple."},
    )

    assert summary is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert "group-safe household update from a private parent DM" in _FakeAgent.last_run["system_message"]
    assert "Do not include raw feelings, therapy-like language, health-sensitive details" in _FakeAgent.last_run["system_message"]
    assert "Turn this recent private DM exchange into a short parent-group update if appropriate" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_operator_message_review_prompt_uses_agentic_review_prompt(tmp_path):
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

    prompt = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="review_prompt",
        payload={
            "candidate": {
                "title": "Science fair reminder",
                "summary": "Science fair is Friday at school.",
                "state": "pending_review",
                "confirmation_question": "Should I add science fair to the household plan?",
            },
            "source_prompt": "Reply share to treat future items from this source as household-shared.",
        },
    )

    assert prompt is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert "short Florence review prompt for one possible household item" in _FakeAgent.last_run["system_message"]
    assert "Do not say 'Imported item', 'candidate', 'queue'" in _FakeAgent.last_run["system_message"]
    assert "End exactly with: Reply yes if I should add it, no if it's wrong, or skip for later." in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"compose_review_prompt\"" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_operator_message_sync_waiting_uses_agentic_prompt(tmp_path):
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

    reply = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="sync_waiting",
        payload={"user_message": "What's the sync status?", "data_dependent": False},
    )

    assert reply is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert "first Gmail and Calendar sync is still running" in _FakeAgent.last_run["system_message"]
    assert "Do not replay the active onboarding question" in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"compose_sync_waiting_reply\"" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_operator_message_data_dependent_sync_waiting_uses_data_guidance(tmp_path):
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

    reply = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="sync_waiting",
        payload={"user_message": "Can you check tomorrow's calendar?", "data_dependent": True},
    )

    assert reply is not None
    assert "still syncing before it can answer confidently from that data" in _FakeAgent.last_run["system_message"]
    assert "\"data_dependent\": true" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_weekly_brief_uses_weekly_prompt(tmp_path):
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
        brief_kind=HouseholdBriefingKind.WEEKLY,
    )

    assert brief is not None
    assert "weekly household preview" in _FakeAgent.last_run["user_message"].lower()
    assert "meal planning" in _FakeAgent.last_run["user_message"].lower()
    store.close()


def test_household_chat_service_compose_operator_message_activation_brief_uses_agentic_prompt(tmp_path):
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

    brief = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="activation_brief",
        payload={
            "gmail_count": 500,
            "calendar_count": 14,
            "candidates": [],
        },
    )

    assert brief is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert "first activation brief after the initial Google sync finishes" in _FakeAgent.last_run["system_message"]
    assert "Do not say 'items need review', 'candidate queue', 'I scanned X emails'" in _FakeAgent.last_run["system_message"]
    assert "Collapse duplicate raw artifacts into one underlying household fact" in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"compose_initial_sync_activation_brief\"" in _FakeAgent.last_run["user_message"]
    store.close()
