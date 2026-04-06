from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

from florence.contracts import (
    Channel,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdProfileItem,
    HouseholdProfileKind,
    ImportedCandidate,
    CandidateState,
    Member,
    MemberIdentity,
    MemberRole,
    IdentityKind,
)
from florence.onboarding import OnboardingStage, OnboardingState
from florence.state import FlorenceStateDB


_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "reset_florence_state.py"
_SPEC = importlib.util.spec_from_file_location("reset_florence_state", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
reset_script = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = reset_script
_SPEC.loader.exec_module(reset_script)


def _populate_household(store: FlorenceStateDB, *, household_id: str = "hh_123") -> None:
    suffix = household_id.split("_")[-1]
    phone_value = "(555) 555-0123" if household_id == "hh_123" else f"(555) 555-0{suffix}"
    normalized_phone = "+15555550123" if household_id == "hh_123" else f"+15555550{suffix}"
    store.upsert_household(Household(id=household_id, name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id=f"mem_{suffix}",
            household_id=household_id,
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member_identity(
        MemberIdentity(
            id=f"ident_{suffix}",
            member_id=f"mem_{suffix}",
            kind=IdentityKind.PHONE,
            value=phone_value,
            normalized_value=normalized_phone,
        )
    )
    store.upsert_channel(
        Channel(
            id=f"chan_dm_{suffix}",
            household_id=household_id,
            provider="sendblue",
            provider_channel_id=f"dm-thread-{suffix}",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.replace_child_profiles(
        household_id=household_id,
        children=[ChildProfile(id=f"child_ava_{suffix}", household_id=household_id, full_name="Ava")],
    )
    store.replace_household_profile_items(
        household_id=household_id,
        kind=HouseholdProfileKind.SCHOOL,
        items=[
            HouseholdProfileItem(
                id=f"school_{suffix}",
                household_id=household_id,
                kind=HouseholdProfileKind.SCHOOL,
                label="Roosevelt Elementary",
                member_id=f"mem_{suffix}",
            )
        ],
    )
    store.upsert_onboarding_session(
        OnboardingState(
            household_id=household_id,
            member_id=f"mem_{suffix}",
            thread_id=f"dm-thread-{suffix}",
            stage=OnboardingStage.COLLECT_CHILD_ACTIVITIES,
            parent_display_name="Maya",
            child_names=["Ava"],
            metadata={"child_profiles": [{"name": "Ava", "age": "7", "school": "Roosevelt Elementary"}]},
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id=f"gconn_{suffix}",
            household_id=household_id,
            member_id=f"mem_{suffix}",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id=f"cand_{suffix}",
            household_id=household_id,
            member_id=f"mem_{suffix}",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier=f"gmail:cand_{suffix}",
            title="Science fair",
            summary="Reminder",
            state=CandidateState.PENDING_REVIEW,
        )
    )
    store.upsert_household_event(
        HouseholdEvent(
            id=f"event_{suffix}",
            household_id=household_id,
            title="Science fair",
            status=HouseholdEventStatus.CONFIRMED,
        )
    )


def test_delete_household_by_phone_removes_associated_rows(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    _populate_household(store)

    summary = reset_script.delete_household_by_phone(store, "(555) 555-0123")

    assert summary.household_id == "hh_123"
    assert summary.normalized_phone == "+15555550123"
    assert summary.deleted_counts["households"] == 1
    assert summary.deleted_counts["members"] == 1
    assert summary.remaining_counts["households"] == 0
    assert store.get_household("hh_123") is None
    assert store.find_member_by_identity(kind=IdentityKind.PHONE, normalized_value="+15555550123") is None
    store.close()


def test_wipe_all_data_clears_all_florence_tables(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    _populate_household(store, household_id="hh_123")
    _populate_household(store, household_id="hh_456")

    summary = reset_script.wipe_all_data(store)

    assert summary.mode == "all"
    assert summary.deleted_counts["households"] == 2
    assert all(count == 0 for count in summary.remaining_counts.values())
    store.close()
