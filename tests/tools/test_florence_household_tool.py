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
    ImportedCandidate,
    Member,
    MemberRole,
)
from florence.google.types import GmailSyncItem
from florence.state import FlorenceStateDB
from model_tools import handle_function_call
from tools.florence_household_tool import (
    clear_household_tool_context,
    set_household_tool_context,
)


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
