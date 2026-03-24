from florence.contracts import (
    CandidateState,
    GoogleSourceKind,
    HouseholdContext,
    ImportedCandidate,
)
from florence.onboarding import OnboardingStage, OnboardingState


def test_household_context_requires_child_and_school_or_activity_grounding():
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Ava"],
        school_labels=[],
        activity_labels=[],
    )
    assert context.is_grounded_for_google_matching is False

    context.school_labels.append("Roosevelt Elementary")
    assert context.is_grounded_for_google_matching is True


def test_imported_candidate_defaults_to_quarantined_confirmation():
    candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:abc123",
        title="Soccer practice update",
        summary="Practice moved to Thursday at 4 PM.",
    )

    assert candidate.state == CandidateState.QUARANTINED
    assert candidate.requires_confirmation is True


def test_onboarding_state_is_not_grounded_without_school_or_activity_basics():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_123",
        stage=OnboardingStage.COLLECT_SCHOOL_BASICS,
        google_connected=True,
        child_names=["Noah"],
    )
    assert state.is_grounded_for_google_matching is False

    state.activity_labels.append("Soccer")
    assert state.is_grounded_for_google_matching is True
