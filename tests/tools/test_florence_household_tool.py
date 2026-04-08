import json
from datetime import datetime, timezone

from florence.contracts import (
    CandidateState,
    Channel,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdSourceMatcherKind,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    ImportedCandidate,
    Member,
    MemberRole,
)
from florence.google.types import GmailSyncItem, ParentCalendarSyncItem
from florence.state import FlorenceStateDB
from model_tools import handle_function_call
from tools.florence_household_tool import (
    clear_household_tool_context,
    set_household_tool_context,
)


class _StubBriefingPlanChatService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def compose_briefing_routine_plan(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        operating_preferences: list[str] | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "operating_preferences": list(operating_preferences or []),
            }
        )
        return [
            {"kind": "morning", "enabled": True, "hour": 6, "minute": 30, "days": [0, 1, 2, 3, 4]},
            {"kind": "evening", "enabled": True, "hour": 20, "minute": 30, "days": [0, 1, 2, 3, 6]},
            {"kind": "weekly", "enabled": False, "hour": 17, "minute": 0, "days": [5]},
        ]


def test_household_tools_can_create_event_meal_shopping_item_and_nudge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="maya@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        access_token="access-token",
    )
    store.upsert_google_connection(connection)
    store.upsert_google_gmail_messages(
        connection=connection,
        items=[
            GmailSyncItem(
                gmail_message_id="gmail_123",
                thread_id="thread_123",
                from_address="Linda <linda@musicalbeginnings.com>",
                subject="Spring break and Family Day dates",
                snippet="No class April 1 and April 8.",
                body_text="For Violet's Musical Beginnings class: no class April 1 and April 8. Family Day May 6.",
                attachment_text=None,
                attachment_count=0,
                received_at=datetime.now(timezone.utc),
            )
        ],
    )
    task_id = "task-household-tools"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        event_result = json.loads(
            handle_function_call(
                "household_upsert_event",
                {
                    "title": "Connolly Ranch farm camp",
                    "starts_at": "2026-04-02T16:00:00+00:00",
                    "ends_at": "2026-04-02T23:00:00+00:00",
                    "status": "confirmed",
                    "location": "Napa",
                },
                task_id=task_id,
            )
        )
        event_id = event_result["result"]["id"]
        assert store.list_household_events(household_id="hh_123")[0].id == event_id

        meal_result = json.loads(
            handle_function_call(
                "household_upsert_meal",
                {
                    "title": "Taco night",
                    "meal_type": "dinner",
                    "scheduled_for": "2026-03-25T01:00:00+00:00",
                },
                task_id=task_id,
            )
        )
        meal_id = meal_result["result"]["id"]
        assert store.get_household_meal(meal_id) is not None

        shopping_result = json.loads(
            handle_function_call(
                "household_upsert_shopping_item",
                {
                    "title": "tortillas",
                    "list_name": "groceries",
                    "quantity": "2",
                    "unit": "packs",
                    "meal_title": "Taco night",
                },
                task_id=task_id,
            )
        )
        assert shopping_result["result"]["meal_id"] == meal_id

        nudge_result = json.loads(
            handle_function_call(
                "household_schedule_nudge",
                {
                    "message": "Taco night is tomorrow. Confirm groceries tonight.",
                    "scheduled_for": "2026-03-24T18:00:00+00:00",
                    "recipient_member_name": "me",
                },
                task_id=task_id,
            )
        )
        assert nudge_result["result"]["recipient_member_id"] == "mem_123"
        assert nudge_result["result"]["channel_id"] == "chan_dm_123"

        search_result = json.loads(
            handle_function_call(
                "household_search_state",
                {
                    "query": "",
                    "entity_types": ["events", "meals", "shopping_items", "nudges"],
                },
                task_id=task_id,
            )
        )
        assert search_result["results"]["events"][0]["title"] == "Connolly Ranch farm camp"
        assert search_result["results"]["meals"][0]["title"] == "Taco night"
        assert search_result["results"]["shopping_items"][0]["title"] == "tortillas"
        assert "Taco night is tomorrow" in search_result["results"]["nudges"][0]["message"]

        inbox_result = json.loads(
            handle_function_call(
                "household_search_google_inbox",
                {
                    "sender": "Linda",
                    "query": "spring break",
                },
                task_id=task_id,
            )
        )
        assert inbox_result["search_scope"] == "private_parent"
        assert inbox_result["scope_reason"] == "current_parent_dm"
        assert inbox_result["searched_connection_emails"] == ["maya@example.com"]
        assert inbox_result["results"][0]["from_address"] == "Linda <linda@musicalbeginnings.com>"
        assert "April 1 and April 8" in inbox_result["results"][0]["body_text"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_google_inbox_search_uses_shared_source_rules_across_connected_parents(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member(
        Member(
            id="mem_456",
            household_id="hh_123",
            display_name="Kendall",
            role=MemberRole.PARENT,
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
    maya_connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="maya@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        access_token="access-token-maya",
    )
    store.upsert_google_connection(maya_connection)
    kendall_connection = GoogleConnection(
        id="gconn_456",
        household_id="hh_123",
        member_id="mem_456",
        email="kendall@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        access_token="access-token-kendall",
    )
    store.upsert_google_connection(kendall_connection)
    store.upsert_google_gmail_messages(
        connection=kendall_connection,
        items=[
            GmailSyncItem(
                gmail_message_id="gmail_789",
                thread_id="thread_789",
                from_address="Linda <linda@musicalbeginnings.com>",
                subject="Spring break and Family Day dates",
                snippet="No class April 1 and April 8.",
                body_text="For Violet's Musical Beginnings class: no class April 1 and April 8. Family Day May 6.",
                attachment_text=None,
                attachment_count=0,
                received_at=datetime.now(timezone.utc),
            )
        ],
    )
    store.upsert_household_source_rule(
        HouseholdSourceRule(
            id="srcrule_linda_name",
            household_id="hh_123",
            source_kind=GoogleSourceKind.GMAIL,
            matcher_kind=HouseholdSourceMatcherKind.GMAIL_SENDER_NAME,
            matcher_value="linda",
            visibility=HouseholdSourceVisibility.SHARED,
            label="Linda",
            created_by_member_id="mem_123",
            metadata={},
        )
    )
    task_id = "task-shared-inbox-search"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        inbox_result = json.loads(
            handle_function_call(
                "household_search_google_inbox",
                {
                    "sender": "Linda",
                    "query": "spring break",
                },
                task_id=task_id,
            )
        )

        assert inbox_result["search_scope"] == "shared_household"
        assert inbox_result["scope_reason"] == "matched_shared_source_rule"
        assert set(inbox_result["searched_connection_emails"]) == {"maya@example.com", "kendall@example.com"}
        assert inbox_result["results"][0]["from_address"] == "Linda <linda@musicalbeginnings.com>"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_google_inbox_search_matches_human_query_terms_without_exact_phrase(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
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
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
        )
    )
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="jackson@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        access_token="access-token",
    )
    store.upsert_google_connection(connection)
    store.upsert_google_gmail_messages(
        connection=connection,
        items=[
            GmailSyncItem(
                gmail_message_id="gmail_invite_123",
                thread_id="thread_invite_123",
                from_address="Kendall <kendall@example.com>",
                subject="Theo birthday celebration at Pump It Up",
                snippet="Party is next month on Saturday at 2pm.",
                body_text="Kendall forwarded Theo's birthday celebration invite. The party is next month on Saturday at 2pm.",
                attachment_text=None,
                attachment_count=0,
                received_at=datetime.now(timezone.utc),
            )
        ],
    )
    task_id = "task-human-inbox-query"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        inbox_result = json.loads(
            handle_function_call(
                "household_search_google_inbox",
                {
                    "query": "birthday party invite next month",
                },
                task_id=task_id,
            )
        )

        assert inbox_result["results"]
        assert inbox_result["results"][0]["gmail_message_id"] == "gmail_invite_123"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_google_inbox_search_from_group_requires_shared_scope(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token-maya",
        )
    )
    task_id = "task-group-inbox-search"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_group_123",
    )
    try:
        inbox_result = json.loads(
            handle_function_call(
                "household_search_google_inbox",
                {
                    "sender": "Linda",
                    "query": "private note",
                },
                task_id=task_id,
            )
        )

        assert inbox_result["search_scope"] == "group_requires_shared_scope"
        assert inbox_result["scope_reason"] == "group_chat_disallows_private_inbox_search"
        assert inbox_result["searched_connection_emails"] == []
        assert inbox_result["results"] == []
        assert "family group only uses shared household scope" in inbox_result["error"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_google_calendar_search_matches_human_query_terms_without_exact_phrase(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Jackson's household", timezone="America/Los_Angeles"))
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
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
        )
    )
    connection = GoogleConnection(
        id="gconn_cal_123",
        household_id="hh_123",
        member_id="mem_123",
        email="jackson@example.com",
        connected_scopes=(GoogleSourceKind.GOOGLE_CALENDAR,),
        access_token="access-token",
        metadata={
            "calendar_last_synced_at": "2026-04-08T10:00:00+00:00",
            "last_calendar_item_count": 2,
            "last_sync_status": "ok",
        },
    )
    store.upsert_google_connection(connection)
    store.upsert_google_calendar_events(
        connection=connection,
        items=[
            ParentCalendarSyncItem(
                google_event_id="gcal_drall_123",
                title="Theo DRALL Baseball Practice",
                description="Practice at North Field",
                location="North Field",
                html_link="https://calendar.google.com/event?eid=drall123",
                starts_at=datetime(2026, 4, 12, 16, 0, tzinfo=timezone.utc),
                ends_at=datetime(2026, 4, 12, 17, 30, tzinfo=timezone.utc),
                timezone="America/Los_Angeles",
                all_day=False,
                updated_at=datetime(2026, 4, 8, 9, 0, tzinfo=timezone.utc),
                calendar_summary="GameChanger - DRALL Baseball",
                family_member_names=["Theo"],
                calendar_id="cal_gamechanger",
                calendar_primary=False,
            )
        ],
    )
    task_id = "task-calendar-query"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_google_calendar",
                {"query": "drall baseball Theo"},
                task_id=task_id,
            )
        )

        assert result["search_scope"] == "private_parent"
        assert result["scope_reason"] == "current_parent_dm"
        assert result["searched_connection_emails"] == ["jackson@example.com"]
        assert result["results"][0]["google_event_id"] == "gcal_drall_123"
        assert result["results"][0]["calendar_summary"] == "GameChanger - DRALL Baseball"
        assert result["results"][0]["family_member_names"] == ["Theo"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_google_calendar_search_from_group_requires_shared_scope(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GOOGLE_CALENDAR,),
            access_token="access-token-maya",
        )
    )
    task_id = "task-group-calendar-search"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_group_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_google_calendar",
                {"query": "Theo baseball"},
                task_id=task_id,
            )
        )

        assert result["search_scope"] == "group_requires_shared_scope"
        assert result["scope_reason"] == "group_chat_disallows_private_calendar_search"
        assert result["searched_connection_emails"] == []
        assert result["results"] == []
        assert "family group only uses shared household scope" in result["error"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_import_calendar_feed_ingests_webcal_schedule(monkeypatch, tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Jackson household", timezone="America/Los_Angeles"))
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
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
        )
    )
    store.replace_child_profiles(
        household_id="hh_123",
        children=[ChildProfile(id="child_theo", household_id="hh_123", full_name="Theo")],
    )

    class _Response:
        status_code = 200

        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("tools.florence_household_tool.is_safe_url", lambda _url: True)
    monkeypatch.setattr("tools.florence_household_tool.check_website_access", lambda _url: None)
    monkeypatch.setattr(
        "tools.florence_household_tool.httpx.get",
        lambda url, **_: _Response(
            "\r\n".join(
                [
                    "BEGIN:VCALENDAR",
                    "X-WR-CALNAME:GameChanger - DRALL Baseball",
                    "BEGIN:VEVENT",
                    "UID:evt-1",
                    "SUMMARY:Practice",
                    "DTSTART;TZID=America/Los_Angeles:20260415T153000",
                    "DTEND;TZID=America/Los_Angeles:20260415T170000",
                    "LOCATION:North Field",
                    "DESCRIPTION:Bring glove",
                    "END:VEVENT",
                    "END:VCALENDAR",
                ]
            )
        ),
    )

    task_id = "task-import-calendar-feed"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_import_calendar_feed",
                {
                    "url": "webcal://api.team-manager.gc.com/ics-calendar-documents/user/example.ics",
                    "child_name": "Theo",
                    "title_prefix": "DRALL Baseball",
                },
                task_id=task_id,
            )
        )

        assert result["feed_url"] == "https://api.team-manager.gc.com/ics-calendar-documents/user/example.ics"
        assert result["calendar_summary"] == "GameChanger - DRALL Baseball"
        assert result["imported_count"] == 1
        saved_event = store.list_household_events(household_id="hh_123")[0]
        assert saved_event.title == "Theo — DRALL Baseball — Practice"
        assert saved_event.metadata["imported_from_calendar_feed"] is True
        assert saved_event.metadata["calendar_feed_uid"] == "evt-1"
        assert saved_event.metadata["child_name"] == "Theo"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_request_parent_link_creates_privacy_safe_pending_request(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Jackson household", timezone="America/Los_Angeles"))
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
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
        )
    )
    task_id = "task-parent-link"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_request_parent_link",
                {
                    "phone_number": "+1 (555) 555-0124",
                    "display_name": "Kendall",
                },
                task_id=task_id,
            )
        )

        assert result["result"]["status"] == "pending"
        assert result["result"]["requires_merge_confirmation"] is False
        assert "link Kendall into this household once they confirm from their side" in result["result"]["reply_text"]
        saved_requests = store.list_household_link_requests(household_id="hh_123")
        assert len(saved_requests) == 1
        assert saved_requests[0].invited_identity_normalized_value == "+15555550124"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_search_state_includes_structured_preferences(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
        )
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_operating_rule_123",
                household_id="hh_123",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Briefing cadence",
                metadata={
                    "category": "operating_rule",
                    "value": "Weekday morning brief at 6:45 and no texts after 9pm.",
                },
            ),
            HouseholdProfileItem(
                id="pref_reminder_123",
                household_id="hh_123",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Reminder style",
                metadata={
                    "category": "reminder_style",
                    "value": "Morning-of is enough for practice reminders.",
                },
            ),
        ],
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
    task_id = "task-household-preferences"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_state",
                {
                    "query": "morning brief",
                    "entity_types": ["preferences"],
                },
                task_id=task_id,
            )
        )

        preferences = result["results"]["preferences"]
        assert any(item["kind"] == "preference" for item in preferences)
        assert any(item["metadata"]["category"] == "operating_rule" for item in preferences)
        assert any("morning brief" in str(item["metadata"]["value"]).lower() for item in preferences)
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_search_state_surfaces_scope_tentative_and_private_review_state(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_123",
            household_id="hh_123",
            title="Possible camp carpool",
            starts_at="2026-04-02T16:00:00+00:00",
            ends_at="2026-04-02T17:00:00+00:00",
            status=HouseholdEventStatus.TENTATIVE,
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:camp-1",
            title="Summer camp invoice",
            summary="Invoice from billing@camp-example.com.",
            state=CandidateState.PENDING_REVIEW,
            metadata={},
        )
    )
    task_id = "task-household-visibility"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_state",
                {
                    "query": "",
                    "entity_types": ["events"],
                },
                task_id=task_id,
            )
        )

        visibility = result["visibility"]
        assert visibility["current_scope"]["scope"] == "private_parent_dm"
        assert visibility["current_scope"]["private_review_available"] is True
        assert visibility["tentative_state"]["event_count"] == 1
        assert visibility["tentative_state"]["events"][0]["title"] == "Possible camp carpool"
        assert visibility["private_review_state"]["available_in_current_scope"] is True
        assert visibility["private_review_state"]["included_in_response"] is False
        assert visibility["private_review_state"]["pending_candidate_count"] == 0
        assert visibility["private_review_state"]["pending_candidates"] == []
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_search_state_surfaces_likely_duplicate_events(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.replace_child_profiles(
        household_id="hh_123",
        children=[ChildProfile(id="child_violet", household_id="hh_123", full_name="Violet")],
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
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_music_canonical",
            household_id="hh_123",
            title="Violet — Musical Beginnings Preschool Tunes",
            starts_at="2026-04-15T15:30:00-07:00",
            ends_at="2026-04-15T16:15:00-07:00",
            status=HouseholdEventStatus.CONFIRMED,
            metadata={"shared_google_calendar_event_id": "gcal_123"},
        )
    )
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_music_dup",
            household_id="hh_123",
            title="Violet music class",
            starts_at="2026-04-15T15:30:00-07:00",
            ends_at="2026-04-15T16:15:00-07:00",
            status=HouseholdEventStatus.CONFIRMED,
        )
    )
    task_id = "task-household-duplicate-events"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_state",
                {
                    "query": "violet music",
                    "entity_types": ["events"],
                },
                task_id=task_id,
            )
        )

        groups = result["event_insights"]["likely_duplicate_groups"]
        assert len(groups) == 1
        assert groups[0]["canonical_event_id"] == "evt_music_canonical"
        assert groups[0]["duplicate_event_ids"] == ["evt_music_dup"]
        assert {event["id"] for event in groups[0]["events"]} == {"evt_music_canonical", "evt_music_dup"}
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_search_state_includes_shared_calendar_projection_link(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "shared_google_calendar_projection": {
                    "calendar_id": "cal_shared_123",
                    "calendar_summary": "Florence - Maya's household",
                    "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
                    "host_email": "maya@example.com",
                    "shared_with_emails": ["kendall@example.com"],
                    "status": "active",
                    "last_synced_at": "2026-04-07T18:00:00+00:00",
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
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    task_id = "task-household-calendar-link"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_state",
                {
                    "query": "",
                    "entity_types": ["events"],
                },
                task_id=task_id,
            )
        )

        projection = result["visibility"]["shared_calendar_projection"]
        assert projection["available"] is True
        assert projection["calendar_id"] == "cal_shared_123"
        assert projection["calendar_web_url"] == "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123"
        assert projection["host_email"] == "maya@example.com"
        assert projection["shared_with_emails"] == ["kendall@example.com"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_search_state_can_explicitly_include_private_review_state(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_124",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:camp-2",
            title="Summer camp invoice",
            summary="Invoice from billing@camp-example.com.",
            state=CandidateState.PENDING_REVIEW,
            metadata={},
        )
    )
    task_id = "task-household-private-review"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_search_state",
                {
                    "query": "",
                    "entity_types": ["events"],
                    "include_private_review_state": True,
                },
                task_id=task_id,
            )
        )

        visibility = result["visibility"]
        assert visibility["private_review_state"]["available_in_current_scope"] is True
        assert visibility["private_review_state"]["included_in_response"] is True
        assert visibility["private_review_state"]["pending_candidate_count"] == 1
        assert visibility["private_review_state"]["pending_candidates"][0]["title"] == "Summer camp invoice"
        assert visibility["private_review_state"]["pending_candidates"][0]["source_visibility"] is None
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_apply_candidate_review_confirms_with_corrected_fields(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.append_channel_message(
        ChannelMessage(
            id="assistant_review_prompt_200",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Theo music class. Reply yes if I should add it, no if it's wrong, or skip for later.",
            metadata={
                "protocol_kind": "candidate_review_prompt",
                "pending_action_type": "candidate_review",
                "pending_action_target_kind": "imported_candidate",
                "pending_action_target_id": "cand_200",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_200",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:music-200",
            title="Theo music class",
            summary="Theo has a music class on June 10 at 4:15 PM.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "proposed_fields": {
                    "title": "Theo music class",
                    "starts_at": "2026-06-10T16:15:00-07:00",
                    "ends_at": "2026-06-10T17:00:00-07:00",
                    "timezone": "America/Los_Angeles",
                }
            },
        )
    )
    task_id = "task-apply-candidate-review"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_apply_candidate_review",
                {
                    "candidate_id": "cand_200",
                    "resolution": "confirm",
                    "starts_at": "2026-06-10T15:30:00-07:00",
                    "ends_at": "2026-06-10T16:15:00-07:00",
                },
                task_id=task_id,
            )
        )

        candidate = store.get_imported_candidate("cand_200")
        assert candidate is not None
        assert candidate.state == CandidateState.CONFIRMED
        assert result["result"]["event"]["starts_at"] == "2026-06-10T15:30:00-07:00"
        assert result["result"]["event"]["ends_at"] == "2026-06-10T16:15:00-07:00"
        assert result["result"]["reply_text"].startswith("Confirmed.")
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_upsert_event_can_cancel_existing_event_by_id_without_losing_fields(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_123",
            household_id="hh_123",
            title="Violet music class",
            starts_at="2026-04-15T15:30:00-07:00",
            ends_at="2026-04-15T16:15:00-07:00",
            timezone="America/Los_Angeles",
            location="Main Studio",
            description="Preschool Tunes",
            status=HouseholdEventStatus.CONFIRMED,
            metadata={"source": "manual"},
        )
    )
    task_id = "task-upsert-event-by-id"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_upsert_event",
                {
                    "id": "evt_123",
                    "status": "cancelled",
                },
                task_id=task_id,
            )
        )

        assert result["result"]["id"] == "evt_123"
        assert result["result"]["status"] == "cancelled"
        assert result["result"]["title"] == "Violet music class"
        assert result["result"]["starts_at"] == "2026-04-15T15:30:00-07:00"
        assert result["result"]["location"] == "Main Studio"
        assert result["result"]["metadata"]["source"] == "manual"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_apply_candidate_review_rejects_non_active_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.append_channel_message(
        ChannelMessage(
            id="assistant_review_prompt_201",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Theo music class. Reply yes if I should add it, no if it's wrong, or skip for later.",
            metadata={
                "protocol_kind": "candidate_review_prompt",
                "pending_action_type": "candidate_review",
                "pending_action_target_kind": "imported_candidate",
                "pending_action_target_id": "cand_201",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_201",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:music-201",
            title="Theo music class",
            summary="Theo has a music class on June 10 at 4:15 PM.",
            state=CandidateState.PENDING_REVIEW,
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_202",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:music-202",
            title="Coaching session",
            summary="Coaching session on April 15.",
            state=CandidateState.PENDING_REVIEW,
        )
    )
    task_id = "task-apply-candidate-review-mismatch"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_apply_candidate_review",
                {
                    "candidate_id": "cand_202",
                    "resolution": "confirm",
                },
                task_id=task_id,
            )
        )

        assert result["error"] == "Candidate review is only allowed for the one currently surfaced review item in this DM."
        assert result["active_candidate_id"] == "cand_201"
        assert store.get_imported_candidate("cand_202").state == CandidateState.PENDING_REVIEW
        assert store.list_household_events(household_id="hh_123") == []
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_apply_nudge_action_marks_active_nudge_done(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.upsert_household_nudge(
        HouseholdNudge(
            id="nudge_123",
            household_id="hh_123",
            target_kind=HouseholdNudgeTargetKind.GENERAL,
            message="Reminder: pack baseball gear.",
            status=HouseholdNudgeStatus.SENT,
            recipient_member_id="mem_123",
            channel_id="chan_dm_123",
            scheduled_for="2026-04-07T15:00:00+00:00",
            sent_at="2026-04-07T15:05:00+00:00",
        )
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_nudge_123",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Reminder: pack baseball gear.",
            metadata={
                "protocol_kind": "household_nudge_prompt",
                "pending_action_type": "household_nudge",
                "pending_action_target_kind": "household_nudge",
                "pending_action_target_id": "nudge_123",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
        )
    )
    task_id = "task-household-nudge"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_apply_nudge_action",
                {
                    "nudge_id": "nudge_123",
                    "action": "done",
                },
                task_id=task_id,
            )
        )

        assert "result" in result
        assert "done" in result["result"]["reply_text"].lower()
        assert store.get_household_nudge("nudge_123").status == HouseholdNudgeStatus.ACKNOWLEDGED
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_apply_onboarding_update_records_explicit_child_detail(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    task_id = "task-household-onboarding"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        handle_function_call(
            "household_apply_onboarding_update",
            {
                "parent_name": "Maya",
                "child_names": ["Ava"],
            },
            task_id=task_id,
        )
        result = json.loads(
            handle_function_call(
                "household_apply_onboarding_update",
                {
                    "age": "7",
                },
                task_id=task_id,
            )
        )

        session = store.get_onboarding_session(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="dm-thread-123",
        )
        assert session is not None
        assert session.current_child_name == "Ava"
        assert session.child_profiles[0]["age"] == "7"
        assert result["result"]["stage"] == "collect_child_school"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_apply_onboarding_update_records_batched_child_details(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    task_id = "task-household-onboarding-batch"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_apply_onboarding_update",
                {
                    "parent_name": "Maya",
                    "child_names": ["Theo", "Violet"],
                    "child_updates": [
                        {"name": "Theo", "age": "7"},
                        {"name": "Violet", "age": "4"},
                    ],
                },
                task_id=task_id,
            )
        )

        session = store.get_onboarding_session(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="dm-thread-123",
        )
        assert session is not None
        assert session.child_profiles[0]["age"] == "7"
        assert session.child_profiles[1]["age"] == "4"
        assert result["result"]["stage"] == "collect_child_school"
        assert result["result"]["child_names"] == ["Theo", "Violet"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_apply_onboarding_update_includes_google_connect_link_when_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-client-secret")
    monkeypatch.setenv("GOOGLE_OAUTH_STATE_SECRET", "google-state-secret")
    monkeypatch.setenv("FLORENCE_PUBLIC_BASE_URL", "https://florence.example.com")

    store = FlorenceStateDB(tmp_path / "florence.db")
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
            provider="sendblue",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    task_id = "task-household-onboarding-link"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_apply_onboarding_update",
                {
                    "parent_name": "Maya",
                },
                task_id=task_id,
            )
        )
        reply_messages = result["result"]["reply_messages"]
        assert any(
            message.startswith("https://florence.example.com/v1/florence/google/callback")
            or "accounts.google.com" in message
            for message in reply_messages
        )
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_record_preference_persists_preference_items(tmp_path):
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
        children=[
            ChildProfile(
                id="child_ava",
                household_id="hh_123",
                full_name="Ava",
                metadata={"aliases": ["Avs"]},
            )
        ],
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
    task_id = "task-household-record-preference"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        reminder_result = json.loads(
            handle_function_call(
                "household_record_preference",
                {
                    "label": "Reminder style",
                    "value": "Morning-of is enough for practice reminders.",
                    "category": "reminder_style",
                },
                task_id=task_id,
            )
        )
        child_result = json.loads(
            handle_function_call(
                "household_record_preference",
                {
                    "label": "Kid spice preference",
                    "value": "Ava will not eat spicy food.",
                    "category": "child_preference",
                    "child_name": "Avs",
                },
                task_id=task_id,
            )
        )

        assert reminder_result["result"]["label"] == "Reminder style"
        assert reminder_result["result"]["metadata"]["category"] == "reminder_style"
        assert reminder_result["result"]["metadata"]["value"] == "Morning-of is enough for practice reminders."
        assert child_result["result"]["child_id"] == "child_ava"
        assert child_result["result"]["metadata"]["value"] == "Ava will not eat spicy food."

        preferences = store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.PREFERENCE,
        )
        assert len(preferences) == 2
        assert any(item.label == "Reminder style" and item.metadata["value"] == "Morning-of is enough for practice reminders." for item in preferences)
        assert any(item.label == "Kid spice preference" and item.child_id == "child_ava" for item in preferences)
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_record_preference_refreshes_briefing_routines_for_operating_rules(tmp_path):
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
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    task_id = "task-household-record-operating-preference"
    chat_service = _StubBriefingPlanChatService()
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        household_chat_service=chat_service,
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_record_preference",
                {
                    "label": "Briefing cadence",
                    "value": "Weekday morning brief at 6:30, evening check-in on school nights at 8:30pm, and skip the weekly preview.",
                    "category": "operating_rule",
                },
                task_id=task_id,
            )
        )

        assert result["result"]["metadata"]["category"] == "operating_rule"
        assert result["briefing_routines_refreshed"] is True
        assert len(result["briefing_routines"]) == 3
        assert len(chat_service.calls) == 1
        morning = next(routine for routine in result["briefing_routines"] if routine["metadata"]["brief_kind"] == "morning")
        weekly = next(routine for routine in result["briefing_routines"] if routine["metadata"]["brief_kind"] == "weekly")
        assert morning["metadata"]["planning_source"] == "hermes"
        assert morning["metadata"]["local_time"] == "06:30"
        assert weekly["status"] == "paused"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_upsert_work_item_can_update_existing_item_by_id_without_title(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.upsert_household_work_item(
        HouseholdWorkItem(
            id="work_123",
            household_id="hh_123",
            title="Review overlapping preferences",
            description="Old description",
            status=HouseholdWorkItemStatus.OPEN,
            metadata={"category": "merge_cleanup"},
        )
    )
    task_id = "task-household-update-work-item"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_upsert_work_item",
                {
                    "id": "work_123",
                    "status": "done",
                    "metadata": {"resolved_by": "mem_123"},
                },
                task_id=task_id,
            )
        )

        updated = store.get_household_work_item("work_123")
        assert result["result"]["id"] == "work_123"
        assert result["result"]["title"] == "Review overlapping preferences"
        assert updated is not None
        assert updated.title == "Review overlapping preferences"
        assert updated.status == HouseholdWorkItemStatus.DONE
        assert updated.metadata["category"] == "merge_cleanup"
        assert updated.metadata["resolved_by"] == "mem_123"
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_resolve_merge_followup_updates_child_and_closes_followup(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
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
    store.replace_child_profiles(
        household_id="hh_123",
        children=[ChildProfile(id="child_theo", household_id="hh_123", full_name="Theo Williams")],
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.SCHOOL,
        items=[
            HouseholdProfileItem(
                id="school_1",
                household_id="hh_123",
                kind=HouseholdProfileKind.SCHOOL,
                label="Wish Elementary",
                child_id="child_theo",
                metadata={"domains": ["wish.example.org"]},
            ),
            HouseholdProfileItem(
                id="school_2",
                household_id="hh_123",
                kind=HouseholdProfileKind.SCHOOL,
                label="WISH Charter",
                child_id="child_theo",
                metadata={"contacts": ["Front desk"]},
            ),
        ],
    )
    store.upsert_household_work_item(
        HouseholdWorkItem(
            id="work_merge_123",
            household_id="hh_123",
            title="Review child details after linking",
            status=HouseholdWorkItemStatus.OPEN,
            metadata={
                "category": "merge_cleanup",
                "cleanup_kind": "child_profiles",
                "duplicate_groups": [
                    {
                        "child_id": "child_theo",
                        "items": [{"id": "child_theo", "label": "Theo Williams"}],
                        "birthdates": ["2018-03-01", "2019-03-01"],
                        "school_labels": ["Wish Elementary", "WISH Charter"],
                        "diff_lines": [
                            "Birthdate differs: 2018-03-01, 2019-03-01.",
                            "School differs: Wish Elementary, WISH Charter.",
                        ],
                    }
                ],
                "preview_lines": [
                    'Theo Williams: Birthdate differs: 2018-03-01, 2019-03-01.',
                ],
            },
        )
    )
    task_id = "task-household-resolve-merge-followup"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
        result = json.loads(
            handle_function_call(
                "household_resolve_merge_followup",
                {
                    "work_item_id": "work_merge_123",
                    "birthdate": "2018-03-01",
                    "school": "WISH Charter",
                    "resolution_note": "Jackson confirmed the current school name.",
                },
                task_id=task_id,
            )
        )

        updated_child = store.list_child_profiles(household_id="hh_123")[0]
        school_items = store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.SCHOOL,
        )
        updated_work_item = store.get_household_work_item("work_merge_123")

        assert result["resolved"] is True
        assert result["child"]["birthdate"] == "2018-03-01"
        assert result["remaining_conflicts"] == []
        assert updated_child.birthdate == "2018-03-01"
        assert updated_child.metadata["merge_resolution_note"] == "Jackson confirmed the current school name."
        assert len(school_items) == 1
        assert school_items[0].label == "WISH Charter"
        assert sorted(school_items[0].metadata["domains"]) == ["wish.example.org"]
        assert sorted(school_items[0].metadata["contacts"]) == ["Front desk"]
        assert updated_work_item is not None
        assert updated_work_item.status == HouseholdWorkItemStatus.DONE
    finally:
        clear_household_tool_context(task_id)
        store.close()
