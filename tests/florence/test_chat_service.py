from florence.contracts import ChildProfile, Household, HouseholdEvent, HouseholdProfileItem, HouseholdProfileKind, Member, MemberRole
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.state import FlorenceStateDB


class _FakeAgent:
    created = []
    last_run = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        _FakeAgent.created.append(kwargs)

    def run_conversation(self, user_message, system_message, conversation_history=None):
        _FakeAgent.last_run = {
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": conversation_history or [],
        }
        return {"final_response": "Use the confirmed plan: Ava has soccer on Thursday."}


def test_household_chat_service_uses_hermes_agent_with_confirmed_state(tmp_path):
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

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        agent_factory=_FakeAgent,
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="group_thread_123",
        actor_member_id="mem_123",
        message_text="What is happening this week?",
    )

    assert reply is not None
    assert "soccer" in reply.text.lower()
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_chat"]
    assert "Confirmed household events" in _FakeAgent.last_run["system_message"]
    assert "Ava soccer practice" in _FakeAgent.last_run["system_message"]
    store.close()
