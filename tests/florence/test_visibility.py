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
from florence.runtime.visibility import (
    FlorenceConversationScope,
    build_scope_model_lines,
    resolve_conversation_scope,
    resolve_google_inbox_scope,
)
from florence.state import FlorenceStateDB


def test_resolve_conversation_scope_tracks_dm_and_group_visibility(tmp_path):
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
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Maya + Kendall",
        )
    )

    dm_scope = resolve_conversation_scope(
        store,
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
    )
    assert dm_scope.scope == "private_parent_dm"
    assert dm_scope.private_review_available is True

    group_scope = resolve_conversation_scope(
        store,
        channel_id="chan_group_123",
        actor_member_id="mem_123",
    )
    assert group_scope.scope == "shared_household_group"
    assert group_scope.private_review_available is False


def test_build_scope_model_lines_include_private_and_group_guards():
    private_lines = build_scope_model_lines(
        scope=FlorenceConversationScope(
            channel_type=ChannelType.PARENT_DM,
            scope="private_parent_dm",
            actor_member_id="mem_123",
        )
    )
    assert any("Current scope: private parent DM." in line for line in private_lines)
    assert any("group-safe summary" in line for line in private_lines)

    group_lines = build_scope_model_lines(
        scope=FlorenceConversationScope(
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            scope="shared_household_group",
            actor_member_id="mem_123",
        )
    )
    assert any("Current scope: shared household group." in line for line in group_lines)
    assert any("private source details stay out" in line for line in group_lines)


def test_resolve_google_inbox_scope_enforces_group_boundary_and_shared_rule(tmp_path):
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
            title="Maya + Kendall",
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="token-maya",
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_456",
            household_id="hh_123",
            member_id="mem_456",
            email="kendall@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="token-kendall",
        )
    )
    store.upsert_household_source_rule(
        HouseholdSourceRule(
            id="srcrule_roosevelt",
            household_id="hh_123",
            source_kind=GoogleSourceKind.GMAIL,
            matcher_kind=HouseholdSourceMatcherKind.GMAIL_SENDER_NAME,
            matcher_value="roosevelt",
            visibility=HouseholdSourceVisibility.SHARED,
            label="Roosevelt School",
            created_by_member_id="mem_123",
        )
    )

    blocked_scope = resolve_google_inbox_scope(
        store,
        household_id="hh_123",
        channel_id="chan_group_123",
        actor_member_id="mem_123",
        sender="Linda",
        query="private note",
        subject=None,
    )
    assert blocked_scope.search_scope == "group_requires_shared_scope"
    assert blocked_scope.scope_reason == "group_chat_disallows_private_inbox_search"
    assert blocked_scope.connections == []
    assert blocked_scope.error is not None

    shared_scope = resolve_google_inbox_scope(
        store,
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        sender="Roosevelt",
        query="school pickup update",
        subject=None,
    )
    assert shared_scope.search_scope == "shared_household"
    assert shared_scope.scope_reason == "matched_shared_source_rule"
    assert {connection.email for connection in shared_scope.connections} == {
        "maya@example.com",
        "kendall@example.com",
    }
