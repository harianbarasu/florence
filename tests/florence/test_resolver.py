from florence.contracts import ChannelType, IdentityKind
from florence.onboarding import OnboardingStage, OnboardingState
from florence.runtime import FlorenceIdentityResolver
from florence.state import FlorenceStateDB


def test_identity_resolver_direct_message_creates_household_member_identity_and_channel(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    resolver = FlorenceIdentityResolver(store)

    resolved = resolver.resolve_direct_message(
        sender_handle="+1 (555) 555-0123",
        thread_external_id="dm_thread_123",
    )

    assert resolved.household.id.startswith("hh_")
    assert resolved.member is not None
    assert resolved.member.display_name == "15555550123"
    assert resolved.channel.channel_type == ChannelType.PARENT_DM
    assert store.find_member_by_identity(kind=IdentityKind.PHONE, normalized_value="+15555550123") is not None
    store.close()


def test_identity_resolver_group_message_links_back_to_existing_household(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    resolver = FlorenceIdentityResolver(store)
    direct = resolver.resolve_direct_message(
        sender_handle="+15555550123",
        thread_external_id="dm_thread_123",
    )

    resolved_group = resolver.resolve_group_message(
        sender_handle="+15555550123",
        participant_handles=["+15555550123", "+15555550124"],
        thread_external_id="group_thread_123",
    )

    assert resolved_group is not None
    assert resolved_group.household.id == direct.household.id
    assert resolved_group.channel.channel_type == ChannelType.HOUSEHOLD_GROUP
    store.close()


def test_identity_resolver_existing_group_does_not_attribute_unknown_sender_to_anchor(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    resolver = FlorenceIdentityResolver(store)
    direct = resolver.resolve_direct_message(
        sender_handle="+15555550123",
        thread_external_id="dm_thread_123",
    )
    store.upsert_onboarding_session(
        OnboardingState(
            household_id=direct.household.id,
            member_id=direct.member.id,
            thread_id="dm_thread_123",
            stage=OnboardingStage.ACTIVATE_GROUP,
        )
    )

    first_group = resolver.resolve_group_message(
        sender_handle="+15555550124",
        participant_handles=["+15555550123", "+15555550124"],
        thread_external_id="group_thread_123",
    )
    assert first_group is not None
    assert first_group.member is not None

    repeated_group = resolver.resolve_group_message(
        sender_handle="+15555550999",
        participant_handles=["+15555550123", "+15555550124", "+15555550999"],
        thread_external_id="group_thread_123",
    )

    assert repeated_group is not None
    assert repeated_group.household.id == direct.household.id
    assert repeated_group.member is None
    store.close()
