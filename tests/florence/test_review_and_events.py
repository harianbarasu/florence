from florence.contracts import CandidateState, GoogleSourceKind, Household, HouseholdSourceVisibility, ImportedCandidate
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


def test_review_confirmation_can_apply_corrected_fields(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_124",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_124",
        title="Theo music class",
        summary="Theo has music class June 10 at 4:15 PM.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "proposed_fields": {
                "title": "Theo music class",
                "starts_at": "2026-06-10T16:15:00-07:00",
                "ends_at": "2026-06-10T17:00:00-07:00",
                "timezone": "America/Los_Angeles",
            },
        },
    )
    store.upsert_imported_candidate(candidate)

    result = review_service.confirm_candidate(
        candidate_id="cand_124",
        overrides={
            "starts_at": "2026-06-10T15:30:00-07:00",
            "ends_at": "2026-06-10T16:15:00-07:00",
        },
    )

    assert result.event is not None
    assert result.event.starts_at == "2026-06-10T15:30:00-07:00"
    assert result.event.ends_at == "2026-06-10T16:15:00-07:00"
    assert result.candidate.state == CandidateState.CONFIRMED
    store.close()


def test_review_confirmation_can_keep_private_candidate_as_member_work_item(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_private_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_private_123",
        title="Dentist appointment reminder",
        summary="Reminder from dentist@example.com for next Tuesday.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "candidate_scope": "private_parent",
            "confirmation_question": "Want me to keep track of this just for you?",
            "proposed_fields": {
                "title": "Dentist appointment reminder",
                "description": "Reminder from dentist@example.com for next Tuesday.",
                "starts_at": "2026-06-10T15:30:00-07:00",
            },
        },
    )
    store.upsert_imported_candidate(candidate)

    result = review_service.confirm_candidate(candidate_id="cand_private_123")

    assert result.event is None
    assert result.work_item is not None
    assert result.work_item.owner_member_id == "mem_123"
    assert result.work_item.starts_at == "2026-06-10T15:30:00-07:00"
    assert result.candidate.state == CandidateState.CONFIRMED
    assert store.list_household_events(household_id="hh_123") == []
    assert len(store.list_household_work_items(household_id="hh_123", owner_member_id="mem_123")) == 1
    store.close()


def test_review_confirmation_syncs_household_calendar_projection(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_projection_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:gmail_projection_123",
        title="Ava soccer practice",
        summary="Soccer practice moved to Thursday.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "proposed_fields": {
                "title": "Ava soccer practice",
                "starts_at": "2026-09-18T23:00:00+00:00",
                "ends_at": "2026-09-19T00:00:00+00:00",
                "timezone": "America/Los_Angeles",
            },
        },
    )
    store.upsert_imported_candidate(candidate)
    synced_households: list[str] = []
    monkeypatch.setattr(
        "florence.runtime.candidate_review.FlorenceHouseholdCalendarProjectionService.sync_household",
        lambda self, *, household_id, preferred_connection_id=None: synced_households.append(household_id) or None,
    )

    review_service.confirm_candidate(candidate_id="cand_projection_123")

    assert synced_households == ["hh_123"]
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


def test_review_prompt_groups_recurring_calendar_candidates_and_confirms_them_together(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    for candidate_id, starts_at, ends_at in (
        ("cand_gym_june", "2026-06-04T17:30:00-07:00", "2026-06-04T18:30:00-07:00"),
        ("cand_gym_aug", "2026-08-06T17:30:00-07:00", "2026-08-06T18:30:00-07:00"),
    ):
        store.upsert_imported_candidate(
            ImportedCandidate(
                id=candidate_id,
                household_id="hh_123",
                member_id="mem_123",
                source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
                source_identifier=f"google_calendar:{candidate_id}",
                title="Violet Gymnastics",
                summary="Family calendar · 2026-06-04T17:30:00-07:00",
                state=CandidateState.PENDING_REVIEW,
                metadata={
                    "confirmation_question": "Should I add this to the shared household plan?",
                    "calendar_id": "family-calendar",
                    "proposed_fields": {
                        "title": "Violet Gymnastics",
                        "starts_at": starts_at,
                        "ends_at": ends_at,
                        "timezone": "America/Los_Angeles",
                        "location": "The Little Gym",
                    },
                    "source_provenance": {
                        "location": "The Little Gym",
                    },
                },
            )
        )

    prompt = review_service.build_review_batch_prompt(
        household_id="hh_123",
        member_id="mem_123",
        limit=3,
    )

    assert prompt is not None
    assert len(prompt.candidates) == 1
    assert "Violet Gymnastics" in prompt.text
    assert "Thursdays at 5:30 PM" in prompt.text
    assert "Jun 4 and Aug 6" in prompt.text

    group_candidate_ids = review_service.resolve_review_group_candidate_ids(
        household_id="hh_123",
        member_id="mem_123",
        candidate_id=prompt.candidate.id,
        candidate_ids=["cand_gym_june", "cand_gym_aug"],
    )
    reply = review_service.apply_review_response(
        candidate_id=prompt.candidate.id,
        candidate_ids=group_candidate_ids,
        member_id="mem_123",
        resolution="confirm",
    )

    assert "2 dates for Violet Gymnastics" in reply.reply_text
    assert store.get_imported_candidate("cand_gym_june").state == CandidateState.CONFIRMED
    assert store.get_imported_candidate("cand_gym_aug").state == CandidateState.CONFIRMED
    assert len(store.list_household_events(household_id="hh_123")) == 2
    store.close()


def test_review_prompt_shows_household_and_source_timezone_for_calendar_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_zimmi",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
            source_identifier="google_calendar:zimmi",
            title="Reservation at Zimmi's",
            summary="Family calendar · 2026-04-17T19:00:00-04:00",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add this to the shared household plan?",
                "calendar_id": "family-calendar",
                "proposed_fields": {
                    "title": "Reservation at Zimmi's",
                    "starts_at": "2026-04-17T19:00:00-04:00",
                    "ends_at": "2026-04-17T21:00:00-04:00",
                    "timezone": "America/New_York",
                },
            },
        )
    )

    prompt = review_service.build_next_review_prompt(
        household_id="hh_123",
        member_id="mem_123",
    )

    assert prompt is not None
    assert "Reservation at Zimmi's" in prompt.text
    assert "Fri, Apr 17 at 4:00 PM PDT (7:00 PM EDT)" in prompt.text
    store.close()
