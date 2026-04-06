from florence.contracts import CandidateState, GoogleSourceKind, HouseholdSourceVisibility, ImportedCandidate
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


def test_review_response_can_classify_source_and_confirm_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_456",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:linda-1",
        title="Violet music class update",
        summary="Linda <linda@musicalbeginnings.com> - no class April 8.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "from_address": "Linda <linda@musicalbeginnings.com>",
            "confirmation_question": "Should I add Violet music class update to your household plan?",
        },
    )
    store.upsert_imported_candidate(candidate)

    reply = review_service.apply_review_response(
        candidate_id="cand_456",
        member_id="mem_123",
        source_visibility=HouseholdSourceVisibility.SHARED,
        resolution="confirm",
    )

    rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.SHARED,
    )
    assert "shared household context" in reply.reply_text
    assert "Confirmed." in reply.reply_text
    assert any(rule.matcher_value == "musicalbeginnings.com" for rule in rules)
    assert store.get_imported_candidate("cand_456").state == CandidateState.CONFIRMED
    store.close()


def test_review_response_can_classify_source_without_confirming_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_789",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:billing-1",
        title="Summer camp invoice",
        summary="Invoice from billing@camp-example.com.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "from_address": "billing@camp-example.com",
        },
    )
    store.upsert_imported_candidate(candidate)

    reply = review_service.apply_review_response(
        candidate_id="cand_789",
        member_id="mem_123",
        source_visibility=HouseholdSourceVisibility.PRIVATE,
    )

    persisted = store.get_imported_candidate("cand_789")
    rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.PRIVATE,
    )
    assert "private to your review queue" in reply.reply_text
    assert "Reply yes if you want me to add this item too." in reply.reply_text
    assert persisted is not None
    assert persisted.state == CandidateState.PENDING_REVIEW
    assert any(rule.matcher_value == "billing@camp-example.com" for rule in rules)
    store.close()
