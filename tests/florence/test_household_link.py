from datetime import datetime, timezone

from florence.contracts import (
    Channel,
    ChannelType,
    ChildProfile,
    Household,
    HouseholdEvent,
    HouseholdNudgeStatus,
    HouseholdLinkRequestStatus,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutine,
    Member,
    MemberIdentity,
    MemberRole,
    IdentityKind,
)
from florence.messaging.protocol_types import build_household_link_prompt_metadata
from florence.runtime.household_link import FlorenceHouseholdLinkService
from florence.runtime.resolver import normalize_identity_value
from florence.state import FlorenceStateDB


def test_household_link_service_creates_pending_phone_link_request_for_unknown_number(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_jackson",
            household_id="hh_jackson",
            display_name="Jackson",
            role=MemberRole.ADMIN,
        )
    )
    service = FlorenceHouseholdLinkService(
        store,
        now_getter=lambda: datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
    )

    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="(555) 555-0124",
        invited_display_name="Kendall",
    )

    assert request.status == HouseholdLinkRequestStatus.PENDING
    assert request.invited_identity_normalized_value == "+15555550124"
    assert request.requires_merge_confirmation is False
    assert request.invited_member_id is None
    assert request.source_household_id is None
    assert request.expires_at == "2026-04-15T20:00:00+00:00"
    assert service.find_active_phone_link_request(invited_phone="+1 555 555 0124") == request
    store.close()


def test_household_link_service_marks_lightweight_existing_parent_household_for_auto_merge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_jackson",
            household_id="hh_jackson",
            display_name="Jackson",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member(
        Member(
            id="mem_kendall",
            household_id="hh_kendall",
            display_name="Kendall",
            role=MemberRole.ADMIN,
        )
    )
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_kendall_dm",
            household_id="hh_kendall",
            provider="sendblue",
            provider_channel_id="+15122164639|+15555550124",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
        )
    )
    service = FlorenceHouseholdLinkService(store)

    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    assert request.status == HouseholdLinkRequestStatus.PENDING
    assert request.invited_member_id == "mem_kendall"
    assert request.source_household_id == "hh_kendall"
    assert request.requires_merge_confirmation is False
    assert request.metadata["source_household_maturity"] == "lightweight"
    store.close()


def test_household_link_service_marks_mature_existing_parent_household_for_guided_merge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_jackson",
            household_id="hh_jackson",
            display_name="Jackson",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member(
        Member(
            id="mem_kendall",
            household_id="hh_kendall",
            display_name="Kendall",
            role=MemberRole.ADMIN,
        )
    )
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo", household_id="hh_kendall", full_name="Theo Williams")],
    )
    service = FlorenceHouseholdLinkService(store)

    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    assert request.status == HouseholdLinkRequestStatus.PENDING
    assert request.invited_member_id == "mem_kendall"
    assert request.source_household_id == "hh_kendall"
    assert request.requires_merge_confirmation is True
    assert request.metadata["source_household_maturity"] == "mature"
    assert request.metadata["source_household_meaningful_counts"] == {"child_profiles": 1}
    store.close()


def test_household_link_service_accepts_invited_parent_and_auto_merges_lightweight_household(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_kendall_dm",
            household_id="hh_kendall",
            provider="sendblue",
            provider_channel_id="dm-thread-kendall",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
            metadata={"sender_handle": normalized},
        )
    )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    result = service.accept_from_invited(
        request_id=request.id,
        invited_member_id="mem_kendall",
    )

    assert result.request.status == HouseholdLinkRequestStatus.MERGED
    assert store.get_member("mem_kendall").household_id == "hh_jackson"
    assert store.get_channel("chan_kendall_dm").household_id == "hh_jackson"
    assert "linked into the same household now" in result.reply_text.lower()
    store.close()


def test_household_link_service_waits_for_both_parents_on_mature_merge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo", household_id="hh_kendall", full_name="Theo Williams")],
    )
    store.upsert_channel(
        Channel(
            id="chan_kendall_dm",
            household_id="hh_kendall",
            provider="sendblue",
            provider_channel_id="dm-thread-kendall",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
            metadata={"sender_handle": normalized},
        )
    )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    invited_result = service.accept_from_invited(
        request_id=request.id,
        invited_member_id="mem_kendall",
    )
    assert invited_result.request.status == HouseholdLinkRequestStatus.ACCEPTED
    assert invited_result.request.metadata["awaiting_inviting_confirmation"] is True
    assert store.get_member("mem_kendall").household_id == "hh_kendall"

    inviting_result = service.accept_from_inviting_member(
        request_id=request.id,
        inviting_member_id="mem_jackson",
    )
    assert inviting_result.request.status == HouseholdLinkRequestStatus.MERGED
    assert store.get_member("mem_kendall").household_id == "hh_jackson"
    assert store.get_household("hh_kendall") is None
    store.close()


def test_household_link_service_sends_outbound_invite_text_once(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    service = FlorenceHouseholdLinkService(
        store,
        now_getter=lambda: datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
    )
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )
    sent: list[dict[str, object]] = []

    def _send_invite(active_request, invite_text):
        sent.append(
            {
                "request_id": active_request.id,
                "invite_text": invite_text,
            }
        )
        return {"provider": "sendblue", "thread_id": "+15122164639|+15555550124"}

    first = service.send_invited_parent_invite(
        request_id=request.id,
        inviting_member_id="mem_jackson",
        send_invite=_send_invite,
    )
    second = service.send_invited_parent_invite(
        request_id=request.id,
        inviting_member_id="mem_jackson",
        send_invite=_send_invite,
    )

    assert len(sent) == 1
    assert "Jackson invited you to join your household in Florence." in sent[0]["invite_text"]
    assert first.request.metadata["invited_message_sent_at"] == "2026-04-08T20:00:00+00:00"
    assert first.request.metadata["invite_delivery"]["provider"] == "sendblue"
    assert "Done. I texted Kendall." in first.reply_text
    assert "already texted Kendall" in second.reply_text
    store.close()


def test_household_link_service_schedules_inviting_confirmation_prompt_for_mature_merge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo", household_id="hh_kendall", full_name="Theo Williams")],
    )
    store.upsert_channel(
        Channel(
            id="chan_jackson_dm",
            household_id="hh_jackson",
            provider="sendblue",
            provider_channel_id="dm-thread-jackson",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
            metadata={"sender_handle": "+15555550199"},
        )
    )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    accepted = service.accept_from_invited(
        request_id=request.id,
        invited_member_id="mem_kendall",
    )

    scheduled = [
        item
        for item in store.list_household_nudges(household_id="hh_jackson")
        if item.target_id == request.id
    ]
    assert accepted.request.status == HouseholdLinkRequestStatus.ACCEPTED
    assert len(scheduled) == 1
    assert scheduled[0].status == HouseholdNudgeStatus.SCHEDULED
    assert scheduled[0].recipient_member_id == "mem_jackson"
    assert scheduled[0].metadata["delivery_message_metadata"] == build_household_link_prompt_metadata(request.id, role="inviting")
    store.close()


def test_household_link_service_creates_merge_cleanup_work_items_for_obvious_duplicates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.replace_child_profiles(
        household_id="hh_jackson",
        children=[ChildProfile(id="child_theo_1", household_id="hh_jackson", full_name="Theo Williams", birthdate="2018-03-01")],
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo_2", household_id="hh_kendall", full_name="Theo Williams", birthdate="2019-03-01")],
    )
    store.replace_household_profile_items(
        household_id="hh_jackson",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_1",
                household_id="hh_jackson",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Reminder style",
                metadata={"category": "reminder_style", "value": "Morning only"},
            )
        ],
    )
    store.replace_household_profile_items(
        household_id="hh_kendall",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_2",
                household_id="hh_kendall",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Reminder style",
                metadata={"category": "reminder_style", "value": "Only after 8 AM"},
            )
        ],
    )
    store.upsert_household_routine(
        HouseholdRoutine(
            id="routine_1",
            household_id="hh_jackson",
            title="School prep",
            cadence="weekday",
        )
    )
    store.upsert_household_routine(
        HouseholdRoutine(
            id="routine_2",
            household_id="hh_kendall",
            title="School prep",
            cadence="weekday",
            next_due_at="2026-04-09T14:00:00+00:00",
        )
    )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )
    service.accept_from_invited(request_id=request.id, invited_member_id="mem_kendall")
    result = service.accept_from_inviting_member(
        request_id=request.id,
        inviting_member_id="mem_jackson",
    )

    work_items = store.list_household_work_items(household_id="hh_jackson")
    titles = {item.title for item in work_items}
    child_profiles = store.list_child_profiles(household_id="hh_jackson")
    assert result.request.status == HouseholdLinkRequestStatus.MERGED
    assert "Review child details after linking" in titles
    assert "Review overlapping preferences" in titles
    assert "Review overlapping routines" in titles
    assert len(child_profiles) == 1
    assert child_profiles[0].id == "child_theo_1"
    assert "follow-up item" in result.reply_text.lower()
    assert "Birthdate differs" in result.reply_text
    store.close()


def test_household_link_service_auto_dedupes_exact_preferences_and_routines(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.replace_child_profiles(
        household_id="hh_jackson",
        children=[ChildProfile(id="child_theo_1", household_id="hh_jackson", full_name="Theo Williams")],
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo_2", household_id="hh_kendall", full_name="Theo Williams")],
    )
    for household_id, pref_id, routine_id in (
        ("hh_jackson", "pref_1", "routine_1"),
        ("hh_kendall", "pref_2", "routine_2"),
    ):
        store.replace_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
            items=[
                HouseholdProfileItem(
                    id=pref_id,
                    household_id=household_id,
                    kind=HouseholdProfileKind.PREFERENCE,
                    label="Reminder style",
                    metadata={"category": "reminder_style", "value": "Morning only"},
                )
            ],
        )
        store.upsert_household_routine(
            HouseholdRoutine(
                id=routine_id,
                household_id=household_id,
                title="School prep",
                cadence="weekday",
            )
        )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )
    service.accept_from_invited(request_id=request.id, invited_member_id="mem_kendall")
    result = service.accept_from_inviting_member(
        request_id=request.id,
        inviting_member_id="mem_jackson",
    )

    work_items = store.list_household_work_items(household_id="hh_jackson")
    titles = {item.title for item in work_items}
    preferences = store.list_household_profile_items(
        household_id="hh_jackson",
        kind=HouseholdProfileKind.PREFERENCE,
    )
    routines = [item for item in store.list_household_routines(household_id="hh_jackson") if item.status != "archived"]
    child_profiles = store.list_child_profiles(household_id="hh_jackson")

    assert result.request.status == HouseholdLinkRequestStatus.MERGED
    assert titles == set()
    assert len(preferences) == 1
    assert len(routines) == 1
    assert len(child_profiles) == 1
    assert child_profiles[0].id == "child_theo_1"
    store.close()


def test_household_link_service_repoints_child_linked_records_to_canonical_child(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_jackson", name="Jackson's household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall's household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    normalized = normalize_identity_value(IdentityKind.PHONE, "+1 (555) 555-0124")
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value=normalized,
        )
    )
    store.replace_child_profiles(
        household_id="hh_jackson",
        children=[ChildProfile(id="child_theo_target", household_id="hh_jackson", full_name="Theo Williams")],
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo_source", household_id="hh_kendall", full_name="Theo Williams")],
    )
    store.replace_household_profile_items(
        household_id="hh_kendall",
        kind=HouseholdProfileKind.SCHOOL,
        items=[
            HouseholdProfileItem(
                id="school_theo",
                household_id="hh_kendall",
                kind=HouseholdProfileKind.SCHOOL,
                label="WISH Elementary",
                child_id="child_theo_source",
            )
        ],
    )
    store.upsert_household_routine(
        HouseholdRoutine(
            id="routine_theo",
            household_id="hh_kendall",
            title="Theo school prep",
            cadence="weekday",
            child_id="child_theo_source",
        )
    )
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_theo",
            household_id="hh_kendall",
            title="Theo baseball",
            starts_at="2026-04-11T17:00:00+00:00",
            ends_at="2026-04-11T18:00:00+00:00",
            metadata={"child_id": "child_theo_source", "child_name": "Theo Williams"},
        )
    )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )

    service.accept_from_invited(request_id=request.id, invited_member_id="mem_kendall")
    result = service.accept_from_inviting_member(
        request_id=request.id,
        inviting_member_id="mem_jackson",
    )

    child_profiles = store.list_child_profiles(household_id="hh_jackson")
    school_items = store.list_household_profile_items(
        household_id="hh_jackson",
        kind=HouseholdProfileKind.SCHOOL,
    )
    routine = store.get_household_routine("routine_theo")
    event = store.get_household_event("evt_theo")

    assert result.request.status == HouseholdLinkRequestStatus.MERGED
    assert len(child_profiles) == 1
    assert child_profiles[0].id == "child_theo_target"
    assert school_items[0].child_id == "child_theo_target"
    assert routine is not None
    assert routine.child_id == "child_theo_target"
    assert event is not None
    assert event.metadata["child_id"] == "child_theo_target"
    assert event.metadata["child_name"] == "Theo Williams"
    store.close()
