from florence.contracts import (
    CandidateState,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    ImportedCandidate,
)
from florence.onboarding import OnboardingStage, OnboardingState
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
