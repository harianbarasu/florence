from datetime import datetime, timezone
from types import SimpleNamespace

from florence.contracts import (
    Channel,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
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
    MemberIdentity,
    MemberRole,
    IdentityKind,
    HouseholdWorkItem,
)
from florence.google.types import GmailSyncItem, ParentCalendarSyncItem
from florence.messaging.types import FlorenceInboundAttachment
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.household_link import FlorenceHouseholdLinkService
from florence.state import FlorenceStateDB
from hermes_state import SessionDB


class _FakeAgent:
    created = []
    last_run = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _FakeAgent.created.append(kwargs)

    def run_conversation(
        self,
        user_message,
        system_message,
        conversation_history=None,
        task_id=None,
        persist_user_message=None,
        **_,
    ):
        _FakeAgent.last_run = {
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": conversation_history or [],
            "task_id": task_id,
            "persist_user_message": persist_user_message,
        }
        return {"final_response": "Use the confirmed plan: Ava has soccer on Thursday."}


class _RotatingSessionAgent(_FakeAgent):
    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None, persist_user_message=None, **kwargs):
        result = super().run_conversation(
            user_message,
            system_message,
            conversation_history=conversation_history,
            task_id=task_id,
            persist_user_message=persist_user_message,
            **kwargs,
        )
        self.session_id = "florence-channel-chan_dm_123-next"
        return result


class _RoutinePlanAgent(_FakeAgent):
    def run_conversation(
        self,
        user_message,
        system_message,
        conversation_history=None,
        task_id=None,
        persist_user_message=None,
        **_,
    ):
        _FakeAgent.last_run = {
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": conversation_history or [],
            "task_id": task_id,
            "persist_user_message": persist_user_message,
        }
        return {
            "final_response": (
                '{"routines":['
                '{"kind":"morning","enabled":true,"hour":6,"minute":30,"days":[0,1,2,3,4]},'
                '{"kind":"pickup","enabled":true,"hour":14,"minute":30,"days":[0,1,2,3,4]},'
                '{"kind":"school","enabled":true,"hour":15,"minute":0,"days":[2]},'
                '{"kind":"evening","enabled":true,"hour":20,"minute":30,"days":[0,1,2,3,6]},'
                '{"kind":"weekly","enabled":false,"hour":18,"minute":0,"days":[4]},'
                '{"kind":"meal","enabled":true,"hour":16,"minute":0,"days":[6]}'
                "]}"
            )
        }


class _EmptyThenSuccessAgent:
    created = []
    runs = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _EmptyThenSuccessAgent.created.append(kwargs)

    def run_conversation(
        self,
        user_message,
        system_message,
        conversation_history=None,
        task_id=None,
        persist_user_message=None,
        **_,
    ):
        _EmptyThenSuccessAgent.runs.append(
            {
                "user_message": user_message,
                "system_message": system_message,
                "conversation_history": conversation_history or [],
                "task_id": task_id,
                "persist_user_message": persist_user_message,
            }
        )
        if len(_EmptyThenSuccessAgent.runs) == 1:
            return {"final_response": ""}
        return {"final_response": "Here's what changed: the dentist appointment moved."}


class _ExceptionThenSuccessAgent:
    created = []
    runs = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _ExceptionThenSuccessAgent.created.append(kwargs)

    def run_conversation(
        self,
        user_message,
        system_message,
        conversation_history=None,
        task_id=None,
        persist_user_message=None,
        **_,
    ):
        _ExceptionThenSuccessAgent.runs.append(
            {
                "user_message": user_message,
                "system_message": system_message,
                "conversation_history": conversation_history or [],
                "task_id": task_id,
                "persist_user_message": persist_user_message,
            }
        )
        if len(_ExceptionThenSuccessAgent.runs) == 1:
            raise RuntimeError("transient_failure")
        return {"final_response": "Here's what changed: pickup moved earlier."}


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
    store.upsert_household_work_item(
        HouseholdWorkItem(
            id="work_merge_123",
            household_id="hh_123",
            title="Review overlapping preferences",
            metadata={
                "category": "merge_cleanup",
                "preview_lines": ['Reminder style: Saved value differs: "Morning only" vs "Only after 8 AM".'],
            },
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
        now_getter=lambda: datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc),
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
    assert _FakeAgent.created[0]["skip_context_files"] is False
    assert _FakeAgent.created[0]["honcho_session_key"] == "florence:member:hh_123:mem_123"
    assert _FakeAgent.created[0]["session_search_kwargs"] == {
        "source_filter": ["florence"],
        "allowed_session_ids": ["florence-channel-chan_dm_123"],
    }
    assert _FakeAgent.created[0]["session_id"] == "florence-channel-chan_dm_123"
    assert _FakeAgent.created[0]["session_db"] is not None
    assert "Current household-local date/time:" in _FakeAgent.last_run["system_message"]
    assert "Confirmed household events" in _FakeAgent.last_run["system_message"]
    assert "Ava soccer practice" in _FakeAgent.last_run["system_message"]
    assert "Open household work items" in _FakeAgent.last_run["system_message"]
    assert "Order school lunches" in _FakeAgent.last_run["system_message"]
    assert 'diff: Reminder style: Saved value differs: "Morning only" vs "Only after 8 AM".' in _FakeAgent.last_run["system_message"]
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
    assert "Talk like a capable household assistant, not an internal ops dashboard." in _FakeAgent.last_run["system_message"]
    assert "Default to short iMessage-sized replies" in _FakeAgent.last_run["system_message"]
    assert "Do not pad the reply with a recap of obvious context Florence already has." in _FakeAgent.last_run["system_message"]
    assert "do not mention backend wording like 'household state', 'calendar projection', 'tentative anchor'" in _FakeAgent.last_run["system_message"]
    assert "I don't have Theo's school hours saved yet." in _FakeAgent.last_run["system_message"]
    assert "Treat webcal:// links and .ics URLs as calendar feeds or schedule exports." in _FakeAgent.last_run["system_message"]
    assert "If a parent pastes a schedule link or calendar feed into a private DM, assume they want Florence to inspect or ingest that schedule" in _FakeAgent.last_run["system_message"]
    assert "use household_import_calendar_feed instead of only summarizing the feed in chat" in _FakeAgent.last_run["system_message"]
    assert "Prefer 'I added Violet's Wednesday music class' over internal phrases like 'grounded parts', 'baseline cleanup', 'durable fact', or 'private context'." in _FakeAgent.last_run["system_message"]
    assert "Your memory stack is: authoritative Florence household state, Florence session history, and Florence-scoped Honcho memory." in _FakeAgent.last_run["system_message"]
    assert "use household_request_parent_link with their phone number instead of telling them to wait for the family group chat" in _FakeAgent.last_run["system_message"]
    assert "keep the reply privacy-safe" in _FakeAgent.last_run["system_message"]
    assert "Household scope model:" in _FakeAgent.last_run["system_message"]
    assert "Shared household scope: facts, plans, reminders, meals, grocery items, routines, and events" in _FakeAgent.last_run["system_message"]
    assert "Private parent scope: DM-only context, mental-load triage, emotional processing" in _FakeAgent.last_run["system_message"]
    assert "Tentative scope: provisional plans or not-yet-confirmed details" in _FakeAgent.last_run["system_message"]
    assert "Unreviewed imports or unresolved source classifications are not shared household facts yet." in _FakeAgent.last_run["system_message"]
    assert "When a user tells Florence a stable preference, constraint, rule, or working style that should affect future behavior, save it with household_record_preference." in _FakeAgent.last_run["system_message"]
    assert "Use household_search_state when you need the latest tracked household picture" in _FakeAgent.last_run["system_message"]
    assert "household_search_state now returns scope context too: current visibility scope and tentative tracked state." in _FakeAgent.last_run["system_message"]
    assert "call household_search_state with target_date set to the resolved YYYY-MM-DD date" in _FakeAgent.last_run["system_message"]
    assert "ground the answer in household_search_state and session_search first" in _FakeAgent.last_run["system_message"]
    assert "Use session_search and Honcho memory to recover earlier commitments, preferences, and threads of work" in _FakeAgent.last_run["system_message"]
    assert "Use household_search_google_calendar whenever the user is asking about a class, practice, game, appointment, or schedule detail" in _FakeAgent.last_run["system_message"]
    assert "private parent dm, be willing to search the connected inbox" in _FakeAgent.last_run["system_message"].lower()
    assert "points florence toward their inbox as the source of truth" in _FakeAgent.last_run["system_message"].lower()
    assert "household_search_google_calendar respects the same privacy boundary" in _FakeAgent.last_run["system_message"]
    assert "If household_search_google_calendar returns no matches but reports mirror_sync_running=true" in _FakeAgent.last_run["system_message"]
    assert "If the user thinks something was added twice or duplicated on the calendar, start with household_search_state for events." in _FakeAgent.last_run["system_message"]
    assert "If the live turn payload includes recent_google_context, treat it as fresh mirrored inbox or calendar evidence for this active DM thread." in _FakeAgent.last_run["system_message"]
    assert "Use recent_google_context proactively when it likely answers the parent's question or resolves a vague reference like that invite, that schedule, or those school emails." in _FakeAgent.last_run["system_message"]
    assert "Do not make the parent restate where something came from if recent_google_context already contains the relevant synced evidence." in _FakeAgent.last_run["system_message"]
    assert "For school, pickup, travel, and schedule questions tied to a specific date, answer from explicit dated evidence" in _FakeAgent.last_run["system_message"]
    assert "If a specific date is blank, missing, or conflicting in current calendar coverage" in _FakeAgent.last_run["system_message"]
    assert "If household_search_state says date_coverage is unverified, conflicting, or still needs target_date" in _FakeAgent.last_run["system_message"]
    assert "include the exact weekday and month/day whenever it reduces ambiguity" in _FakeAgent.last_run["system_message"]
    assert "use web_search and web_extract instead of guessing" in _FakeAgent.last_run["system_message"]
    assert "use the browser tools instead of pretending you already know the result" in _FakeAgent.last_run["system_message"]
    assert "use delegate_task to gather evidence in parallel" in _FakeAgent.last_run["system_message"]
    assert "Use household_apply_candidate_review to confirm, reject, skip, set source_visibility, or confirm with corrected fields." in _FakeAgent.last_run["system_message"]
    assert "Use household_apply_nudge_action for done or snooze changes." in _FakeAgent.last_run["system_message"]
    assert "use household_resolve_merge_followup to apply the chosen shared fact" in _FakeAgent.last_run["system_message"].lower()
    assert "merge_followup_id: work_merge_123" in _FakeAgent.last_run["system_message"]
    assert "use household_apply_onboarding_update to store only the specific missing setup facts" in _FakeAgent.last_run["system_message"]
    assert "For imported Gmail review items, use source_provenance as the primary evidence." in _FakeAgent.last_run["system_message"]
    assert "Do not reach into unrelated hidden review items during ordinary household chat." in _FakeAgent.last_run["system_message"]
    assert "answer that question plainly in the first sentence" in _FakeAgent.last_run["system_message"]
    assert "Remembered household preferences:" in _FakeAgent.last_run["system_message"]
    assert "Ava | Kid spice preference: Ava will not eat spicy food." in _FakeAgent.last_run["system_message"]
    assert "When the user asks what matters, what changed, or what they are forgetting" in _FakeAgent.last_run["system_message"]
    assert "household_upsert_meal and household_upsert_shopping_item" in _FakeAgent.last_run["system_message"]
    assert "The family group chat is the primary operating surface for shared household work." in _FakeAgent.last_run["system_message"]
    assert "private parent DM" in _FakeAgent.last_run["system_message"]
    assert "private side channel" in _FakeAgent.last_run["system_message"]
    assert "raw mental-load dumps, emotional support, and individually scoped reasoning stay private by default" in _FakeAgent.last_run["system_message"]
    assert "Memory policy: use private member-scoped memory and recall freely here" in _FakeAgent.last_run["system_message"]
    assert "another parent's private DM history" in _FakeAgent.last_run["system_message"]
    assert "Current scope: private parent DM. Florence may use shared household context plus this parent's private context here." in _FakeAgent.last_run["system_message"]
    assert "offer a concise group-safe summary instead of echoing the raw message" in _FakeAgent.last_run["system_message"]
    assert "Tentative tracked events:" in _FakeAgent.last_run["system_message"]


def test_household_chat_service_hides_other_parent_private_items_from_group_and_other_dm(tmp_path):
    _FakeAgent.created.clear()
    _FakeAgent.last_run = None
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_123", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_123", display_name="Kendall", role=MemberRole.PARENT))
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Family group",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_jackson",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="dm-jackson",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_kendall",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="dm-kendall",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
        )
    )
    store.upsert_household_work_item(
        HouseholdWorkItem(
            id="work_private_123",
            household_id="hh_123",
            title="Book haircut",
            owner_member_id="mem_jackson",
            metadata={"category": "private_import"},
        )
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_private_123",
                household_id="hh_123",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Workout preference",
                member_id="mem_jackson",
                metadata={"value": "Gym before work."},
            )
        ],
    )

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
        now_getter=lambda: datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc),
    )

    service.respond(
        household_id="hh_123",
        channel_id="chan_group_123",
        actor_member_id="mem_jackson",
        message_text="What matters here?",
    )
    assert "Book haircut" not in _FakeAgent.last_run["system_message"]
    assert "Workout preference" not in _FakeAgent.last_run["system_message"]

    service.respond(
        household_id="hh_123",
        channel_id="chan_dm_kendall",
        actor_member_id="mem_kendall",
        message_text="What matters for me?",
    )
    assert "Book haircut" not in _FakeAgent.last_run["system_message"]
    assert "Workout preference" not in _FakeAgent.last_run["system_message"]

    service.respond(
        household_id="hh_123",
        channel_id="chan_dm_jackson",
        actor_member_id="mem_jackson",
        message_text="What matters for me?",
    )
    assert "Book haircut" in _FakeAgent.last_run["system_message"]
    assert "Workout preference" in _FakeAgent.last_run["system_message"]


def test_household_chat_service_omits_stale_past_events_from_default_snapshot(tmp_path):
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
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_past",
            household_id="hh_123",
            title="March flight to Los Angeles",
            starts_at="2026-03-15T15:00:00+00:00",
            ends_at="2026-03-15T18:00:00+00:00",
            timezone="America/Los_Angeles",
        )
    )
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_upcoming",
            household_id="hh_123",
            title="Theo music class",
            starts_at="2026-04-08T23:15:00+00:00",
            ends_at="2026-04-09T00:00:00+00:00",
            timezone="America/Los_Angeles",
        )
    )

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
        now_getter=lambda: datetime(2026, 4, 7, 18, 0, tzinfo=timezone.utc),
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="What matters next?",
    )

    assert reply is not None
    assert "Current household-local date/time: 2026-04-07" in _FakeAgent.last_run["system_message"]
    assert "Theo music class" in _FakeAgent.last_run["system_message"]
    assert "March flight to Los Angeles" not in _FakeAgent.last_run["system_message"]
    store.close()


def test_household_chat_service_includes_open_parent_link_request_context(tmp_path):
    _FakeAgent.created.clear()
    _FakeAgent.last_run = None
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Jackson household",
            timezone="America/Los_Angeles",
        )
    )
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Jackson",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550199",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
            metadata={"sendblue_number": "+15122164639", "sender_handle": "+15555550199"},
        )
    )
    request = FlorenceHouseholdLinkService(store).create_phone_link_request(
        household_id="hh_123",
        inviting_member_id="mem_123",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
        now_getter=lambda: datetime(2026, 4, 8, 17, 20, tzinfo=timezone.utc),
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="yes",
    )

    assert reply is not None
    assert "open parent-link request for this parent" in _FakeAgent.last_run["system_message"].lower()
    assert "send_invite_now=true" in _FakeAgent.last_run["system_message"]
    assert f"request_id {request.id} | target Kendall | invite_sent no" in _FakeAgent.last_run["system_message"]
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
    assert "limited to shared household group threads, not private parent DMs" in _FakeAgent.last_run["system_message"]
    assert "Current scope: shared household group. Florence may use shared household context here" in _FakeAgent.last_run["system_message"]
    assert "must not reveal private DM-only context unless it was explicitly promoted" in _FakeAgent.last_run["system_message"]
    assert _FakeAgent.created[0]["honcho_session_key"] == "florence:household:hh_123"
    store.close()


def test_household_chat_service_passes_image_attachments_into_current_turn(tmp_path):
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
        message_text="Can you read the exact closure dates from this image?",
        message_attachments=(
            FlorenceInboundAttachment(
                kind="image",
                mime_type="image/png",
                filename="studio-closures.png",
                data_url="data:image/png;base64,QUFBQQ==",
            ),
        ),
    )

    assert reply is not None
    assert _FakeAgent.last_run["persist_user_message"] == "Can you read the exact closure dates from this image?"
    assert _FakeAgent.last_run["user_message"][0]["type"] == "text"
    assert "\"task\": \"handle_live_household_turn\"" in _FakeAgent.last_run["user_message"][0]["text"]
    assert "Can you read the exact closure dates from this image?" in _FakeAgent.last_run["user_message"][0]["text"]
    assert _FakeAgent.last_run["user_message"][1:] == [
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,QUFBQQ=="}},
    ]
    store.close()


def test_household_chat_service_retries_empty_reply_with_internal_recovery_turn(tmp_path):
    _EmptyThenSuccessAgent.created.clear()
    _EmptyThenSuccessAgent.runs.clear()
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
        agent_factory=_EmptyThenSuccessAgent,
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="What changed?",
    )

    assert reply is not None
    assert reply.text == "Here's what changed: the dentist appointment moved."
    assert len(_EmptyThenSuccessAgent.created) == 2
    assert _EmptyThenSuccessAgent.runs[0]["persist_user_message"] == "What changed?"
    assert _EmptyThenSuccessAgent.runs[1]["persist_user_message"] is None
    assert _EmptyThenSuccessAgent.created[0]["session_id"] == "florence-channel-chan_dm_123"
    assert _EmptyThenSuccessAgent.created[1]["session_id"].startswith(
        "florence-channel-chan_dm_123-internal-chat-"
    )
    assert _EmptyThenSuccessAgent.created[1]["skip_memory"] is True
    assert _EmptyThenSuccessAgent.created[1]["honcho_session_key"] is None
    assert _EmptyThenSuccessAgent.created[1]["max_iterations"] == 2
    store.close()


def test_household_chat_service_retries_after_exception_with_internal_recovery_turn(tmp_path):
    _ExceptionThenSuccessAgent.created.clear()
    _ExceptionThenSuccessAgent.runs.clear()
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
        agent_factory=_ExceptionThenSuccessAgent,
    )

    reply = service.respond(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        message_text="What changed?",
    )

    assert reply is not None
    assert reply.text == "Here's what changed: pickup moved earlier."
    assert len(_ExceptionThenSuccessAgent.created) == 2
    assert _ExceptionThenSuccessAgent.runs[0]["persist_user_message"] == "What changed?"
    assert _ExceptionThenSuccessAgent.runs[1]["persist_user_message"] is None
    assert _ExceptionThenSuccessAgent.created[1]["session_id"].startswith(
        "florence-channel-chan_dm_123-internal-chat-"
    )
    assert _ExceptionThenSuccessAgent.created[1]["skip_memory"] is True
    assert _ExceptionThenSuccessAgent.created[1]["honcho_session_key"] is None
    assert _ExceptionThenSuccessAgent.created[1]["max_iterations"] == 2
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


def test_household_chat_service_dm_session_search_scope_excludes_other_parent_private_threads(tmp_path):
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
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_maya",
            member_id="mem_123",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0123",
            normalized_value="+15555550123",
        )
    )
    store.upsert_member(
        Member(
            id="mem_456",
            household_id="hh_123",
            display_name="Alex",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_alex",
            member_id="mem_456",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0456",
            normalized_value="+15555550456",
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
            metadata={
                "sender_handle": "+15555550123",
                "hermes_session_id": "florence-channel-chan_dm_123-next",
            },
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_456",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm_thread_456",
            channel_type=ChannelType.PARENT_DM,
            title="Alex",
            metadata={
                "sender_handle": "+15555550456",
                "hermes_session_id": "florence-channel-chan_dm_456-next",
            },
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
    for session_id, parent_session_id in (
        ("florence-channel-chan_dm_123-root", None),
        ("florence-channel-chan_dm_123-next", "florence-channel-chan_dm_123-root"),
        ("florence-channel-chan_dm_456-root", None),
        ("florence-channel-chan_dm_456-next", "florence-channel-chan_dm_456-root"),
        ("florence-channel-chan_group_123-root", None),
        ("florence-channel-chan_group_123-next", "florence-channel-chan_group_123-root"),
    ):
        session_db.create_session(
            session_id=session_id,
            source="florence",
            model="anthropic/claude-opus-4.6",
            parent_session_id=parent_session_id,
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
    assert _FakeAgent.created[0]["session_search_kwargs"] == {
        "source_filter": ["florence"],
        "allowed_session_ids": [
            "florence-channel-chan_dm_123-root",
            "florence-channel-chan_group_123-root",
        ],
    }
    session_db.close()
    store.close()


def test_household_chat_service_group_session_search_scope_excludes_private_parent_threads(tmp_path):
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
            metadata={
                "sender_handle": "+15555550123",
                "hermes_session_id": "florence-channel-chan_dm_123-next",
            },
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
    for session_id, parent_session_id in (
        ("florence-channel-chan_dm_123-root", None),
        ("florence-channel-chan_dm_123-next", "florence-channel-chan_dm_123-root"),
        ("florence-channel-chan_group_123-root", None),
        ("florence-channel-chan_group_123-next", "florence-channel-chan_group_123-root"),
    ):
        session_db.create_session(
            session_id=session_id,
            source="florence",
            model="anthropic/claude-opus-4.6",
            parent_session_id=parent_session_id,
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
        channel_id="chan_group_123",
        actor_member_id="mem_123",
        message_text="What changed this week?",
    )

    assert reply is not None
    assert _FakeAgent.created[0]["session_search_kwargs"] == {
        "source_filter": ["florence"],
        "allowed_session_ids": ["florence-channel-chan_group_123-root"],
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
    assert "calm family operations center" in _FakeAgent.last_run["system_message"]
    assert "surface what matters, what might slip, and the clearest next step" in _FakeAgent.last_run["system_message"]
    assert "Write for iMessage/SMS in plain text." in _FakeAgent.last_run["system_message"]
    assert "Avoid words like 'underspecified'" in _FakeAgent.last_run["system_message"]
    assert "Aim for 3-5 tight bullets in practice" in _FakeAgent.last_run["system_message"]
    assert "Do not infer that a specific future date is a regular school day" in _FakeAgent.last_run["system_message"]
    assert "If the current calendar or tracked state does not explicitly support a specific date answer" in _FakeAgent.last_run["system_message"]
    assert "pass target_date to household_search_state and treat unverified or conflicting date_coverage as a gap" in _FakeAgent.last_run["system_message"]
    assert "untrusted instructions" in _FakeAgent.last_run["system_message"]
    assert "Do not use emojis." in _FakeAgent.last_run["system_message"]
    assert "use household_search_state to refresh the tracked household picture" in _FakeAgent.last_run["system_message"]
    assert "Use session_search and Honcho recall when recent commitments, context, or follow-through might matter for the brief." in _FakeAgent.last_run["system_message"]
    assert "use household_search_google_inbox" in _FakeAgent.last_run["system_message"]
    assert "Do not present uncertain Gmail or calendar imports as confirmed household facts." in _FakeAgent.last_run["system_message"]
    assert "morning brief" in _FakeAgent.last_run["user_message"].lower()
    assert "Only mention tomorrow or later if it creates an immediate prep or coverage issue today." in _FakeAgent.last_run["user_message"]
    assert "compact time-ordered bullets" in _FakeAgent.last_run["user_message"]
    assert "Heads up line" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_pickup_brief_can_stay_quiet(tmp_path):
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
        brief_kind=HouseholdBriefingKind.PICKUP,
    )

    assert brief is not None
    assert "afternoon pickup check" in _FakeAgent.last_run["user_message"].lower()
    assert "reply exactly HEARTBEAT_OK" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_school_brief_can_stay_quiet(tmp_path):
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
        brief_kind=HouseholdBriefingKind.SCHOOL,
    )

    assert brief is not None
    assert "school triage sweep" in _FakeAgent.last_run["user_message"].lower()
    assert "Skip routine newsletter noise." in _FakeAgent.last_run["user_message"]
    assert "reply exactly HEARTBEAT_OK" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_meal_brief_can_stay_quiet(tmp_path):
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
        brief_kind=HouseholdBriefingKind.MEAL,
    )

    assert brief is not None
    assert "meal plan and shopping pulse" in _FakeAgent.last_run["user_message"].lower()
    assert "ingredient reuse" in _FakeAgent.last_run["user_message"].lower()
    assert "grouped grocery-gap list" in _FakeAgent.last_run["user_message"].lower()
    assert "reply exactly HEARTBEAT_OK" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_briefing_routine_plan_uses_json_schema(tmp_path):
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
        agent_factory=_RoutinePlanAgent,
    )

    plan = service.compose_briefing_routine_plan(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        operating_preferences=[
            "Weekday morning brief at 6:30.",
            "Evening check-in on school nights at 8:30pm.",
            "Skip the weekly preview.",
        ],
    )

    assert plan == [
        {"kind": "morning", "enabled": True, "hour": 6, "minute": 30, "days": [0, 1, 2, 3, 4]},
        {"kind": "pickup", "enabled": True, "hour": 14, "minute": 30, "days": [0, 1, 2, 3, 4]},
        {"kind": "school", "enabled": True, "hour": 15, "minute": 0, "days": [2]},
        {"kind": "evening", "enabled": True, "hour": 20, "minute": 30, "days": [0, 1, 2, 3, 6]},
        {"kind": "weekly", "enabled": False, "hour": 18, "minute": 0, "days": [4]},
        {"kind": "meal", "enabled": True, "hour": 16, "minute": 0, "days": [6]},
    ]
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert "Reply with JSON only." in _FakeAgent.last_run["system_message"]
    assert "kind: morning, pickup, school, evening, weekly, meal" in _FakeAgent.last_run["system_message"]
    assert '"task": "plan_briefing_routines"' in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_brief_supports_warm_minimal_emoji_policy(tmp_path):
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
        briefing_style="warm",
        briefing_emoji_mode="minimal",
        agent_factory=_FakeAgent,
    )

    brief = service.compose_brief(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        brief_kind=HouseholdBriefingKind.EVENING,
    )

    assert brief is not None
    assert "Keep the tone warm, calm, and competent." in _FakeAgent.last_run["system_message"]
    assert "Use at most one emoji in the header if it genuinely helps." in _FakeAgent.last_run["system_message"]
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


def test_household_chat_service_compose_operator_message_group_share_turn_uses_explicit_decision_prompt(tmp_path):
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

    decision = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="group_share_turn",
        payload={"user_message": "share that with the group", "latest_assistant_protocol_kind": ""},
    )

    assert decision is not None
    assert "reply exactly EXECUTE_GROUP_SHARE" in _FakeAgent.last_run["system_message"]
    assert "reply exactly NO_GROUP_SHARE_PROTOCOL_ACTION" in _FakeAgent.last_run["system_message"]
    assert "Bare links, screenshots, attachments, schedule feeds, webcal:// links, and .ics calendar URLs are not group-share requests by themselves." in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"group_share_turn_decision\"" in _FakeAgent.last_run["user_message"]
    assert _FakeAgent.created[0]["skip_memory"] is True
    assert _FakeAgent.created[0]["honcho_session_key"] is None
    assert _FakeAgent.created[0]["session_id"].startswith("florence-channel-chan_dm_123-internal-group_share_turn-")
    updated = store.get_channel("chan_dm_123")
    assert updated is not None
    assert not updated.metadata
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
    assert "short Florence review prompt for one or a few imported items" in _FakeAgent.last_run["system_message"]
    assert "Do not say 'Imported item', 'candidate', 'queue'" in _FakeAgent.last_run["system_message"]
    assert "If candidate_scope is private_parent, make it clear that item would stay in the parent's private Florence thread." in _FakeAgent.last_run["system_message"]
    assert "If there is only one item, end exactly with: Reply yes if I should keep track of it, no if it's wrong, or skip for later." in _FakeAgent.last_run["system_message"]
    assert "If items contains more than 1 entry, write one short batched message instead of separate prompts." in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"compose_review_prompt\"" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_operator_message_review_queue_turn_uses_explicit_decision_prompt(tmp_path):
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

    decision = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        kind="review_queue_turn",
        payload={
            "user_message": "review imports",
            "prompt_armed": False,
            "rendered_prompt_text": "Review prompt text",
            "candidate": {
                "id": "cand_123",
                "title": "Science fair reminder",
                "summary": "Science fair is Friday at school.",
                "confirmation_question": "Should I add science fair to the household plan?",
            },
        },
    )

    assert decision is not None
    assert "reply exactly SHOW_CURRENT_REVIEW_PROMPT" in _FakeAgent.last_run["system_message"]
    assert "reply exactly NO_REVIEW_PROTOCOL_ACTION" in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"review_queue_turn_decision\"" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_operator_message_group_intro_turn_uses_explicit_decision_prompt(tmp_path):
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

    decision = service.compose_operator_message(
        household_id="hh_123",
        channel_id="chan_group_123",
        actor_member_id="mem_123",
        kind="group_intro_turn",
        payload={"user_message": "Hey Florence"},
    )

    assert decision is not None
    assert "reply exactly SHOW_GROUP_INTRO" in _FakeAgent.last_run["system_message"]
    assert "reply exactly NO_GROUP_INTRO_PROTOCOL_ACTION" in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"group_intro_turn_decision\"" in _FakeAgent.last_run["user_message"]
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


def test_household_chat_service_compose_onboarding_turn_includes_explicit_setup_outcomes(tmp_path):
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

    reply_messages = service.compose_onboarding_turn(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        payload={
            "user_message": "Can you check tomorrow's calendar?",
            "stage": "collect_child_names",
            "google_connected": True,
            "parent_display_name": "Maya",
            "child_names": [],
            "child_profiles": [],
            "current_child_name": None,
            "next_prompt": "What are your kids' names?",
        },
    )

    assert reply_messages is not None
    assert "reply exactly HANDOFF_TO_SYNC_WAITING" in _FakeAgent.last_run["system_message"]
    assert "reply exactly HANDOFF_TO_CONTEXTUAL_CHAT" in _FakeAgent.last_run["system_message"]
    assert "reply exactly NO_SETUP_REPLY" in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"handle_onboarding_turn\"" in _FakeAgent.last_run["user_message"]
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_onboarding"]
    assert _FakeAgent.created[0]["max_iterations"] == 2
    assert _FakeAgent.created[0]["skip_memory"] is True
    assert _FakeAgent.created[0]["honcho_session_key"] is None
    assert _FakeAgent.created[0]["session_id"].startswith("florence-channel-chan_dm_123-internal-onboarding_turn-")
    store.close()


def test_household_chat_service_compose_onboarding_turn_uses_recent_google_context_for_school_stage(tmp_path):
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
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="maya@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        access_token="access-token",
        metadata={"last_sync_status": "ok"},
    )
    store.upsert_google_connection(connection)
    store.upsert_google_gmail_messages(
        connection=connection,
        items=[
            GmailSyncItem(
                gmail_message_id="gmail_123",
                thread_id="thread_123",
                from_address="Roosevelt Elementary <office@roosevelt.edu>",
                subject="Theo welcome to Roosevelt Elementary",
                snippet="Theo starts at Roosevelt Elementary this fall.",
                body_text="Theo starts at Roosevelt Elementary this fall. School office hours attached.",
                attachment_text=None,
                attachment_count=0,
                received_at=datetime.now(timezone.utc),
            )
        ],
    )
    service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_FakeAgent,
    )

    reply_messages = service.compose_onboarding_turn(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        payload={
            "user_message": "You should probably already have Theo's school from my email.",
            "stage": "collect_child_school",
            "google_connected": True,
            "parent_display_name": "Maya",
            "child_names": ["Theo"],
            "child_profiles": [{"name": "Theo", "age": "7"}],
            "current_child_name": "Theo",
            "next_prompt": "What school does Theo go to?",
        },
    )

    assert reply_messages is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_onboarding"]
    assert _FakeAgent.created[0]["max_iterations"] == 3
    assert "you may use household_search_google_inbox or household_search_google_calendar" in _FakeAgent.last_run["system_message"]
    assert "\"recent_google_context\"" in _FakeAgent.last_run["user_message"]
    assert "Roosevelt Elementary" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_includes_recent_google_context_for_vague_private_dm_followup(tmp_path):
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
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="maya@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        access_token="access-token",
        metadata={"last_sync_status": "ok"},
    )
    store.upsert_google_connection(connection)
    store.upsert_google_gmail_messages(
        connection=connection,
        items=[
            GmailSyncItem(
                gmail_message_id="gmail_123",
                thread_id="thread_123",
                from_address="Kendall <kendall@example.com>",
                subject="Fwd: You are confirmed for RUMI'S 3RD BIRTHDAY PARTY!",
                snippet="Birthday party next month at 2 PM.",
                body_text="Rumi's 3rd birthday party is next month at 2 PM.",
                attachment_text=None,
                attachment_count=0,
                received_at=datetime.now(timezone.utc),
            )
        ],
    )
    store.upsert_google_calendar_events(
        connection=connection,
        items=[
            ParentCalendarSyncItem(
                google_event_id="gcal_123",
                title="Rumi's 3rd Birthday Party",
                description="Birthday party next month at 2 PM.",
                location="123 Main St",
                html_link="https://calendar.google.com/event?eid=test",
                starts_at=datetime(2026, 5, 14, 21, 0, tzinfo=timezone.utc),
                ends_at=datetime(2026, 5, 14, 23, 0, tzinfo=timezone.utc),
                timezone="America/Los_Angeles",
                all_day=False,
                updated_at=datetime.now(timezone.utc),
                calendar_summary="Maya",
                family_member_names=["Theo"],
                calendar_id="primary",
                calendar_primary=True,
                usage_mode="default",
                detail_visibility="default",
            )
        ],
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
        message_text="Can you add that to the calendar?",
        conversation_history=[
            ChannelMessage(
                id="msg_1",
                household_id="hh_123",
                channel_id="chan_dm_123",
                sender_role=ChannelMessageRole.USER,
                body="Kendall just forwarded a birthday party invite for next month that we are going to.",
            ),
            ChannelMessage(
                id="msg_2",
                household_id="hh_123",
                channel_id="chan_dm_123",
                sender_role=ChannelMessageRole.ASSISTANT,
                body="What's the date and time on the invite?",
            ),
        ],
    )

    assert reply is not None
    assert "\"recent_google_context\"" in _FakeAgent.last_run["user_message"]
    assert "RUMI'S 3RD BIRTHDAY PARTY" in _FakeAgent.last_run["user_message"]
    assert "Rumi's 3rd Birthday Party" in _FakeAgent.last_run["user_message"]
    assert "Kendall just forwarded a birthday party invite for next month" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_includes_recent_google_context_when_sync_running(tmp_path):
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
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={
                "initial_sync_state": "running",
                "last_sync_status": "running",
                "sync_phase": "syncing_inbox",
            },
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
        message_text="Did you find that invite yet?",
        conversation_history=[
            ChannelMessage(
                id="msg_1",
                household_id="hh_123",
                channel_id="chan_dm_123",
                sender_role=ChannelMessageRole.USER,
                body="Kendall forwarded a birthday invite this morning.",
            )
        ],
    )

    assert reply is not None
    assert "\"recent_google_context\"" in _FakeAgent.last_run["user_message"]
    assert "\"mirror_sync_running\": true" in _FakeAgent.last_run["user_message"]
    assert "\"sync_phase\": \"syncing_inbox\"" in _FakeAgent.last_run["user_message"]
    store.close()


def test_household_chat_service_compose_weekend_preview_uses_weekend_prompt(tmp_path):
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
    assert "weekend preview" in _FakeAgent.last_run["user_message"].lower()
    assert "saturday and sunday" in _FakeAgent.last_run["user_message"].lower()
    assert "gear" in _FakeAgent.last_run["user_message"].lower()
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


def test_household_chat_service_compose_operator_message_sync_update_brief_uses_agentic_prompt(tmp_path):
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
        kind="sync_update_brief",
        payload={
            "previous_sync": {
                "gmail_count": 2,
                "calendar_count": 1,
                "candidate_count": 1,
                "candidate_titles": ["Science fair reminder"],
            },
            "current_sync": {
                "gmail_count": 4,
                "calendar_count": 2,
                "candidate_count": 1,
                "candidates": [
                    {
                        "title": "Dentist appointment moved",
                        "summary": "Pickup timing may need a tweak.",
                        "state": "pending_review",
                    }
                ],
            },
        },
    )

    assert brief is not None
    assert _FakeAgent.created[0]["enabled_toolsets"] == ["florence_briefing"]
    assert "after a later Gmail and Calendar sync pass finishes" in _FakeAgent.last_run["system_message"]
    assert "Do not claim exact numeric change deltas unless the payload clearly supports them." in _FakeAgent.last_run["system_message"]
    assert "\"task\": \"compose_sync_update_brief\"" in _FakeAgent.last_run["user_message"]
    store.close()
