from datetime import datetime, timezone

from florence.contracts import (
    CandidateState,
    Channel,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdContext,
    HouseholdNudgeTargetKind,
    HouseholdProfileKind,
    HouseholdRoutineStatus,
    Member,
    MemberRole,
)
from florence.google import FlorenceGoogleSyncBatch, GmailSyncItem, ParentCalendarSyncItem
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceGoogleSyncPersistenceService,
    FlorenceHouseholdManagerService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


def test_google_sync_persistence_service_stores_connection_and_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    google_service = FlorenceGoogleSyncPersistenceService(store)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    google_service.save_google_connection(connection)

    result = google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="chan_dm_123",
                visible_child_names=["Ava"],
                school_labels=[],
                activity_labels=[],
            ),
            gmail_items=[
                GmailSyncItem(
                    gmail_message_id="gmail_123",
                    thread_id="thread_123",
                    from_address="Ms. Kim <teacher@roosevelt.k12.ca.us>",
                    subject="Roosevelt Elementary soccer practice update",
                    snippet="ParentSquare reminder",
                    body_text="Ava soccer practice is on September 18 from 4pm to 5pm.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
            calendar_items=[
                ParentCalendarSyncItem(
                    google_event_id="event_123",
                    title="Soccer practice",
                    description="Weekly team practice on the north field",
                    location="North Field",
                    html_link=None,
                    starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
                    ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
                    timezone="America/Los_Angeles",
                    all_day=False,
                    updated_at=None,
                    calendar_summary="Family calendar",
                    family_member_names=["Ava"],
                )
            ],
        )
    )

    assert store.get_google_connection("gconn_123") == connection
    assert len(result.candidates) == 2
    assert result.candidates[0].state == CandidateState.QUARANTINED
    assert len(store.list_imported_candidates(household_id="hh_123", member_id="mem_123")) == 2
    household = store.get_household("hh_123")
    assert household is not None
    grounding_hints = household.settings["grounding_hints"]
    assert grounding_hints["schools"][0]["label"] == "Roosevelt Elementary"
    assert grounding_hints["schools"][0]["domains"] == ["roosevelt.k12.ca.us"]
    assert grounding_hints["schools"][0]["platforms"] == ["ParentSquare"]
    assert grounding_hints["activities"][0]["label"] == "Soccer"
    assert grounding_hints["activities"][0]["locations"] == ["North Field"]

    store.close()


def test_onboarding_service_releases_quarantined_candidates_once_grounded(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    google_service = FlorenceGoogleSyncPersistenceService(store)
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    google_service.save_google_connection(connection)
    google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="chan_dm_123",
                visible_child_names=["Ava"],
                school_labels=[],
                activity_labels=[],
            ),
            gmail_items=[
                GmailSyncItem(
                    gmail_message_id="gmail_123",
                    thread_id="thread_123",
                    from_address="teacher@school.edu",
                    subject="Soccer practice update",
                    snippet="Practice moves to Thursday 4pm to 5pm",
                    body_text="Ava soccer practice is on September 18 from 4pm to 5pm.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
        )
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        child_names=["Ava"],
    )
    candidates_before = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.QUARANTINED,
    )
    assert len(candidates_before) == 1

    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        school_labels=["Roosevelt Elementary"],
    )
    onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        activity_labels=["Soccer"],
    )

    pending = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.PENDING_REVIEW,
    )
    assert len(pending) == 1
    assert pending[0].source_identifier == "gmail:gmail_123"
    assert [child.full_name for child in store.list_child_profiles(household_id="hh_123")] == ["Ava"]
    assert [
        item.label
        for item in store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.SCHOOL,
        )
    ] == ["Roosevelt Elementary"]
    assert [
        item.label
        for item in store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.ACTIVITY,
        )
    ] == ["Soccer"]

    store.close()


def test_second_parent_early_onboarding_does_not_clear_existing_household_grounding(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(store)

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        child_names=["Ava"],
    )
    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        school_labels=["Roosevelt Elementary"],
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_second",
        thread_id="dm_second",
        display_name="Chris",
    )

    assert [child.full_name for child in store.list_child_profiles(household_id="hh_123")] == ["Ava"]
    assert [
        item.label
        for item in store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.SCHOOL,
        )
    ] == ["Roosevelt Elementary"]
    store.close()


def test_onboarding_prompt_surfaces_google_grounding_suggestions(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "grounding_hints": {
                    "schools": [
                        {
                            "label": "Roosevelt Elementary",
                            "domains": ["roosevelt.k12.ca.us"],
                            "platforms": ["ParentSquare"],
                            "contacts": ["Ms. Kim"],
                        }
                    ],
                    "activities": [
                        {
                            "label": "Soccer",
                            "locations": ["North Field"],
                            "contacts": ["Coach Ben"],
                        }
                    ],
                    "contacts": ["Ms. Kim", "Coach Ben"],
                    "locations": ["North Field"],
                }
            },
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        child_names=["Ava"],
    )
    school_prompt = onboarding_service.get_prompt(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    assert school_prompt is not None
    assert "Google already surfaced a few likely school signals:" in school_prompt.text
    assert "Roosevelt Elementary" in school_prompt.text
    assert "ParentSquare" in school_prompt.text

    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        school_labels=["Roosevelt Elementary"],
    )
    activity_prompt = onboarding_service.get_prompt(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    assert activity_prompt is not None
    assert "Google also found likely activity signals:" in activity_prompt.text
    assert "Soccer" in activity_prompt.text
    assert "North Field" in activity_prompt.text
    store.close()


def test_onboarding_sync_merges_grounding_hints_into_profile_metadata(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "grounding_hints": {
                    "schools": [
                        {
                            "label": "Roosevelt Elementary",
                            "domains": ["roosevelt.k12.ca.us"],
                            "platforms": ["ParentSquare"],
                            "contacts": ["Ms. Kim"],
                        }
                    ],
                    "activities": [
                        {
                            "label": "Soccer",
                            "locations": ["North Field"],
                            "contacts": ["Coach Ben"],
                        }
                    ],
                }
            },
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        child_names=["Ava Johnson"],
    )
    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        school_labels=["Roosevelt Elementary"],
    )
    onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        activity_labels=["Soccer"],
    )

    child = store.list_child_profiles(household_id="hh_123")[0]
    school = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.SCHOOL,
    )[0]
    activity = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.ACTIVITY,
    )[0]

    assert child.metadata["aliases"] == ["Ava"]
    assert school.metadata["domains"] == ["roosevelt.k12.ca.us"]
    assert school.metadata["platforms"] == ["ParentSquare"]
    assert school.metadata["contacts"] == ["Ms. Kim"]
    assert activity.metadata["locations"] == ["North Field"]
    assert activity.metadata["contacts"] == ["Coach Ben"]
    store.close()


def test_onboarding_sync_persists_manager_profile_preferences(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    onboarding_service = FlorenceOnboardingSessionService(store)

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        child_names=["Ava"],
    )
    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        school_labels=["Roosevelt Elementary"],
    )
    onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        activity_labels=["Soccer"],
    )
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        household_operations=["school forms", "pickup planning"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    onboarding_service.record_nudge_preferences(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        nudge_preferences="Day before and morning of for school things.",
    )
    onboarding_service.record_operating_preferences(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        operating_preferences="Weekday morning brief at 6:45 and no messages after 9pm.",
    )

    household = store.get_household("hh_123")
    assert household is not None
    manager_profile = household.settings["manager_profile"]
    assert manager_profile["household_operations"] == ["school forms", "pickup planning"]
    assert manager_profile["nudge_preferences"] == "Day before and morning of for school things."
    assert manager_profile["operating_preferences"] == "Weekday morning brief at 6:45 and no messages after 9pm."
    store.close()


def test_household_manager_service_schedules_due_nudge_with_default_dm_context(tmp_path):
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
    onboarding_service = FlorenceOnboardingSessionService(store)
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )

    manager = FlorenceHouseholdManagerService(store)
    nudge = manager.schedule_nudge(
        household_id="hh_123",
        message="Remember to order groceries for taco night.",
        scheduled_for="2026-03-24T12:00:00+00:00",
        target_kind=HouseholdNudgeTargetKind.GENERAL,
    )

    due = manager.list_due_nudges(
        household_id="hh_123",
        now=datetime(2026, 3, 24, 12, 30, tzinfo=timezone.utc),
    )
    assert due == [nudge]
    assert nudge.recipient_member_id == "mem_123"
    assert nudge.channel_id == "chan_dm_123"

    sent = manager.mark_nudge_sent(
        nudge_id=nudge.id,
        sent_at=datetime(2026, 3, 24, 12, 31, tzinfo=timezone.utc),
    )
    assert sent is not None
    assert sent.sent_at == "2026-03-24T12:31:00+00:00"
    store.close()


def test_household_manager_service_briefing_routines_due_and_advance(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "manager_profile": {
                    "operating_preferences": "Weekday morning brief at 6:45 and evening check-in on school nights at 8:30pm.",
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
    onboarding_service = FlorenceOnboardingSessionService(store)
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )

    manager = FlorenceHouseholdManagerService(store)
    routines = manager.ensure_briefing_routines(
        household_id="hh_123",
        now=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )
    assert len(routines) == 2
    assert all(routine.status == HouseholdRoutineStatus.ACTIVE for routine in routines)

    morning = next(routine for routine in routines if routine.metadata.get("brief_kind") == "morning")
    due = manager.list_due_briefing_routines(
        household_id="hh_123",
        now=datetime(2026, 3, 24, 14, 0, tzinfo=timezone.utc),
    )
    assert morning in due

    updated = manager.mark_briefing_routine_sent(
        routine_id=morning.id,
        sent_at=datetime(2026, 3, 24, 14, 1, tzinfo=timezone.utc),
    )
    assert updated is not None
    assert updated.last_completed_at == "2026-03-24T14:01:00+00:00"
    assert updated.next_due_at is not None
    assert updated.next_due_at > "2026-03-24T14:01:00+00:00"
    store.close()


def test_household_manager_service_records_reminder_feedback_and_event(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    manager = FlorenceHouseholdManagerService(store)
    profile = manager.record_reminder_feedback(
        household_id="hh_123",
        feedback_text="Too many reminders too early. Morning-of is enough for practice.",
        member_id="mem_123",
        channel_id="chan_dm_123",
        now=datetime(2026, 3, 24, 18, 30, tzinfo=timezone.utc),
    )

    assert profile["nudge_preferences_override"] == "Too many reminders too early. Morning-of is enough for practice."
    assert profile["reminder_feedback"][-1]["member_id"] == "mem_123"
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_feedback_received")
    assert len(events) == 1
    assert events[0].metadata["text"] == "Too many reminders too early. Morning-of is enough for practice."
    store.close()
