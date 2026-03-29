from florence.contracts import (
    CandidateState,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdMeal,
    HouseholdMealStatus,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    HouseholdShoppingItem,
    HouseholdShoppingItemStatus,
    HouseholdSourceMatcherKind,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    ImportedCandidate,
    PilotEvent,
)
from florence.onboarding import OnboardingStage, OnboardingState, OnboardingVariant
from florence.state import FlorenceStateDB


def test_state_db_round_trips_onboarding_google_and_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")

    onboarding = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        stage=OnboardingStage.COLLECT_ACTIVITY_BASICS,
        parent_display_name="Maya",
        google_connected=True,
        child_names=["Ava"],
        school_labels=["Roosevelt Elementary"],
        activity_labels=["Soccer"],
        school_basics_collected=True,
        activity_basics_collected=True,
        metadata={
            "variant": OnboardingVariant.HYBRID.value,
            "household_operations": ["school forms", "returns"],
            "nudge_preferences": "Day before and morning of",
        },
    )
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        access_token="access-token",
        refresh_token="refresh-token",
        access_token_expires_at="2026-09-10T12:00:00Z",
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
        title="Soccer practice update",
        summary="teacher@school.edu - Practice moved to Thursday.",
        state=CandidateState.QUARANTINED,
        confidence_bps=7800,
        metadata={"confirmation_question": "Add this to Florence?"},
    )

    store.upsert_onboarding_session(onboarding)
    store.upsert_google_connection(connection)
    store.upsert_imported_candidate(candidate)

    loaded_onboarding = store.get_onboarding_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    loaded_connection = store.get_google_connection("gconn_123")
    loaded_candidate = store.get_imported_candidate_by_source(
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
    )

    assert loaded_onboarding == onboarding
    assert loaded_connection == connection
    assert loaded_candidate == candidate

    store.close()


def test_state_db_round_trips_household_profiles(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    children = [
        ChildProfile(id="child_ava", household_id="hh_123", full_name="Ava"),
        ChildProfile(id="child_noah", household_id="hh_123", full_name="Noah"),
    ]
    schools = [
        HouseholdProfileItem(
            id="school_roosevelt",
            household_id="hh_123",
            kind=HouseholdProfileKind.SCHOOL,
            label="Roosevelt Elementary",
            member_id="mem_123",
        )
    ]
    activities = [
        HouseholdProfileItem(
            id="activity_soccer",
            household_id="hh_123",
            kind=HouseholdProfileKind.ACTIVITY,
            label="Soccer",
            member_id="mem_123",
        )
    ]

    store.replace_child_profiles(household_id="hh_123", children=children)
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.SCHOOL,
        items=schools,
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.ACTIVITY,
        items=activities,
    )

    assert store.list_child_profiles(household_id="hh_123") == children
    assert store.list_household_profile_items(household_id="hh_123", kind=HouseholdProfileKind.SCHOOL) == schools
    assert store.list_household_profile_items(household_id="hh_123", kind=HouseholdProfileKind.ACTIVITY) == activities

    store.close()


def test_state_db_updates_candidate_state(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
        title="Soccer practice update",
        summary="teacher@school.edu - Practice moved to Thursday.",
    )
    store.upsert_imported_candidate(candidate)

    store.set_imported_candidate_state("cand_123", CandidateState.PENDING_REVIEW)
    updated = store.get_imported_candidate_by_source(
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
    )

    assert updated is not None
    assert updated.state == CandidateState.PENDING_REVIEW

    store.close()


def test_state_db_round_trips_household_source_rules(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    rule = HouseholdSourceRule(
        id="srcrule_123",
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        matcher_kind=HouseholdSourceMatcherKind.GMAIL_FROM_DOMAIN,
        matcher_value="musicalbeginnings.com",
        visibility=HouseholdSourceVisibility.SHARED,
        label="Musical Beginnings",
        created_by_member_id="mem_123",
        metadata={"source_label": "Linda / musicalbeginnings.com"},
    )

    store.upsert_household_source_rule(rule)

    loaded = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.SHARED,
    )

    assert loaded == [rule]
    store.close()


def test_state_db_round_trips_house_manager_objects(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    work_item = HouseholdWorkItem(
        id="work_123",
        household_id="hh_123",
        title="Order school lunches for next week",
        description="Submit lunches before Friday cutoff.",
        status=HouseholdWorkItemStatus.IN_PROGRESS,
        owner_member_id="mem_123",
        due_at="2026-03-27T18:00:00+00:00",
        metadata={"category": "school_admin"},
    )
    routine = HouseholdRoutine(
        id="routine_123",
        household_id="hh_123",
        title="Friday lunch-order check",
        cadence="weekly on Friday at 9am",
        status=HouseholdRoutineStatus.ACTIVE,
        owner_member_id="mem_123",
        next_due_at="2026-03-27T16:00:00+00:00",
        metadata={"category": "school_admin"},
    )
    nudge = HouseholdNudge(
        id="nudge_123",
        household_id="hh_123",
        target_kind=HouseholdNudgeTargetKind.WORK_ITEM,
        target_id="work_123",
        message="Lunch order cutoff is today at 10am.",
        status=HouseholdNudgeStatus.SCHEDULED,
        recipient_member_id="mem_123",
        channel_id="chan_dm_123",
        scheduled_for="2026-03-27T15:00:00+00:00",
        metadata={"follow_up_policy": "until_acknowledged"},
    )
    meal = HouseholdMeal(
        id="meal_123",
        household_id="hh_123",
        title="Taco night",
        meal_type="dinner",
        scheduled_for="2026-03-27T18:00:00+00:00",
        description="Easy Friday dinner after soccer.",
        status=HouseholdMealStatus.PLANNED,
        metadata={"serves": 4},
    )
    shopping_item = HouseholdShoppingItem(
        id="shop_123",
        household_id="hh_123",
        title="ground turkey",
        list_name="groceries",
        status=HouseholdShoppingItemStatus.NEEDED,
        quantity="2",
        unit="lb",
        meal_id="meal_123",
        needed_by="2026-03-27T16:00:00+00:00",
        metadata={"store_section": "meat"},
    )

    store.upsert_household_work_item(work_item)
    store.upsert_household_routine(routine)
    store.upsert_household_nudge(nudge)
    store.upsert_household_meal(meal)
    store.upsert_household_shopping_item(shopping_item)

    assert store.get_household_work_item("work_123") == work_item
    assert store.get_household_routine("routine_123") == routine
    assert store.get_household_nudge("nudge_123") == nudge
    assert store.get_household_meal("meal_123") == meal
    assert store.get_household_shopping_item("shop_123") == shopping_item
    assert store.list_household_work_items(household_id="hh_123") == [work_item]
    assert store.list_household_routines(household_id="hh_123") == [routine]
    assert store.list_household_nudges(household_id="hh_123") == [nudge]
    assert store.list_household_meals(household_id="hh_123") == [meal]
    assert store.list_household_shopping_items(household_id="hh_123", list_name="groceries") == [shopping_item]

    store.close()


def test_state_db_round_trips_pilot_events(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    event = PilotEvent(
        id="pilot_123",
        household_id="hh_123",
        event_type="briefing_sent",
        member_id="mem_123",
        channel_id="chan_dm_123",
        metadata={"brief_kind": "morning"},
        created_at=1711300000.0,
    )
    store.upsert_pilot_event(event)

    assert store.list_pilot_events(household_id="hh_123") == [event]
    assert store.list_pilot_events(household_id="hh_123", event_type="briefing_sent") == [event]

    store.close()
