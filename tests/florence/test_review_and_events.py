from florence.contracts import CandidateState, GoogleSourceKind, ImportedCandidate
from florence.runtime import FlorenceCandidateReviewService
from florence.state import FlorenceStateDB


def test_review_confirmation_promotes_candidate_into_household_event(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_123",
        title="Soccer practice update",
        summary="teacher@school.edu - Practice moved to Thursday.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "confirmation_question": "Add this?",
            "proposed_fields": {
                "title": "Ava soccer practice",
                "starts_at": "2026-09-18T23:00:00+00:00",
                "ends_at": "2026-09-19T00:00:00+00:00",
                "timezone": "America/Los_Angeles",
            },
        },
    )
    store.upsert_imported_candidate(candidate)

    result = review_service.confirm_candidate(candidate_id="cand_123")

    assert result.event is not None
    assert result.event.title == "Ava soccer practice"
    assert result.candidate.state == CandidateState.CONFIRMED
    assert len(store.list_household_events(household_id="hh_123")) == 1
    store.close()
