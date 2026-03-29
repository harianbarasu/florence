import json
from datetime import datetime, timezone

from florence.contracts import (
    Channel,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdSourceMatcherKind,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    Member,
    MemberRole,
)
from florence.google.types import GmailSyncItem
from florence.state import FlorenceStateDB
from model_tools import handle_function_call
import tools.florence_household_tool as household_tool
from tools.florence_household_tool import (
    clear_household_tool_context,
    set_household_tool_context,
)


def test_household_tools_can_create_event_meal_shopping_item_and_nudge(tmp_path, monkeypatch):
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
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
        )
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
        monkeypatch.setattr(household_tool, "list_recent_gmail_sync_items", lambda **_: [
            GmailSyncItem(
                gmail_message_id="gmail_123",
                thread_id="thread_123",
                from_address="Linda <linda@musicalbeginnings.com>",
                subject="Spring break and Family Day dates",
                snippet="No class April 1 and April 8.",
                body_text="For Violet's Musical Beginnings class: no class April 1 and April 8. Family Day May 6.",
                attachment_text=None,
                attachment_count=0,
                received_at=None,
            )
        ])
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
        assert inbox_result["searched_connection_emails"] == ["maya@example.com"]
        assert inbox_result["results"][0]["from_address"] == "Linda <linda@musicalbeginnings.com>"
        assert "April 1 and April 8" in inbox_result["results"][0]["body_text"]
    finally:
        clear_household_tool_context(task_id)
        store.close()


def test_household_google_inbox_search_uses_shared_source_rules_across_connected_parents(tmp_path, monkeypatch):
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
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_456",
            household_id="hh_123",
            member_id="mem_456",
            email="kendall@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token-kendall",
        )
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
        def _fake_list_recent_gmail_sync_items(*, access_token, **_kwargs):
            if access_token == "access-token-maya":
                return []
            return [
                GmailSyncItem(
                    gmail_message_id="gmail_789",
                    thread_id="thread_789",
                    from_address="Linda <linda@musicalbeginnings.com>",
                    subject="Spring break and Family Day dates",
                    snippet="No class April 1 and April 8.",
                    body_text="For Violet's Musical Beginnings class: no class April 1 and April 8. Family Day May 6.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
                )
            ]

        monkeypatch.setattr(household_tool, "list_recent_gmail_sync_items", _fake_list_recent_gmail_sync_items)

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

        assert set(inbox_result["searched_connection_emails"]) == {"maya@example.com", "kendall@example.com"}
        assert inbox_result["results"][0]["from_address"] == "Linda <linda@musicalbeginnings.com>"
    finally:
        clear_household_tool_context(task_id)
        store.close()
