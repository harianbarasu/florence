import json

from florence.contracts import Channel, ChannelType, Household, Member, MemberRole
from florence.state import FlorenceStateDB
from model_tools import handle_function_call
from tools.florence_household_tool import (
    clear_household_tool_context,
    set_household_tool_context,
)


def test_household_tools_can_create_meal_shopping_item_and_nudge(tmp_path):
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
    task_id = "task-household-tools"
    set_household_tool_context(
        task_id,
        store=store,
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
    )
    try:
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
                    "query": "taco",
                    "entity_types": ["meals", "shopping_items", "nudges"],
                },
                task_id=task_id,
            )
        )
        assert search_result["results"]["meals"][0]["title"] == "Taco night"
        assert search_result["results"]["shopping_items"][0]["title"] == "tortillas"
        assert "Taco night is tomorrow" in search_result["results"]["nudges"][0]["message"]
    finally:
        clear_household_tool_context(task_id)
        store.close()
