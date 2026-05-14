from datetime import datetime, timezone

from florence.contracts import (
    CandidateState,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdContext,
    HouseholdProfileKind,
    HouseholdSourceVisibility,
    HouseholdSourceMatcherKind,
    ImportedCandidate,
)
from florence.source_rules import build_account_source_rule
from florence.google import FlorenceGoogleSyncBatch
from florence.google.types import GmailSyncItem
from florence.runtime import FlorenceCandidateReviewService
from florence.runtime.google_services import FlorenceGoogleSyncPersistenceService
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
            "source_provenance": {
                "from_address": "coach@example.org",
                "subject": "Soccer practice update",
            },
            "raw_metadata": {
                "temporal_evidence": {
                    "date_match": {"date": "2026-09-18"},
                },
            },
        },
    )
    store.upsert_imported_candidate(candidate)

    result = review_service.confirm_candidate(candidate_id="cand_123")

    assert result.event is not None
    assert result.event.title == "Ava soccer practice"
    assert result.event.source_candidate_id == "cand_123"
    assert result.event.metadata["source_candidate_id"] == "cand_123"
    assert result.event.metadata["source_identifier"] == "gmail:gmail_123"
    assert result.event.metadata["source_provenance"]["from_address"] == "coach@example.org"
    assert result.event.metadata["candidate_raw_metadata"]["temporal_evidence"]["date_match"]["date"] == "2026-09-18"
    assert result.event.metadata["proposed_fields"]["starts_at"] == "2026-09-18T23:00:00+00:00"
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


def test_review_feedback_ignore_sender_creates_ignored_source_rule_and_suppresses_future_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    candidate = ImportedCandidate(
        id="cand_ignore_123",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:spam-1",
        title="Open gym booking",
        summary="Booking for earlier today.",
        state=CandidateState.PENDING_REVIEW,
        metadata={"from_address": "Bookings <booking@example-gym.com>"},
    )
    store.upsert_imported_candidate(candidate)

    reply = review_service.apply_feedback_response(
        candidate_id="cand_ignore_123",
        member_id="mem_123",
        feedback_kind="ignore_source",
        source_visibility=HouseholdSourceVisibility.IGNORED,
        user_text="ignore this sender",
    )

    persisted = store.get_imported_candidate("cand_ignore_123")
    rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.IGNORED,
    )
    assert "ignore future items" in reply.reply_text
    assert persisted is not None
    assert persisted.state == CandidateState.REJECTED
    assert persisted.metadata["review_feedback_kind"] == "ignore_source"
    assert persisted.metadata["suppressed_reason"] == "source_ignored_by_parent"
    assert any(rule.matcher_value == "booking@example-gym.com" for rule in rules)

    future = ImportedCandidate(
        id="cand_ignore_456",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:spam-2",
        title="Another open gym booking",
        summary="Another booking.",
        state=CandidateState.PENDING_REVIEW,
        metadata={"from_address": "Bookings <booking@example-gym.com>"},
    )
    suppressed = review_service.source_rule_service.apply_candidate_policy(future)
    assert suppressed.state == CandidateState.REJECTED
    assert suppressed.metadata["source_visibility"] == "ignored"
    assert suppressed.metadata["suppressed_reason"] == "source_rule_ignored"
    store.close()


def test_review_feedback_already_handled_closes_candidate_without_creating_household_state(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_handled_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:handled-1",
            title="School form reminder",
            summary="A form reminder that the parent already handled.",
            state=CandidateState.PENDING_REVIEW,
        )
    )

    reply = review_service.apply_feedback_response(
        candidate_id="cand_handled_123",
        member_id="mem_123",
        feedback_kind="already_handled",
        user_text="already handled",
    )

    persisted = store.get_imported_candidate("cand_handled_123")
    assert reply.reply_text == "Got it. I marked that as already handled and left it out."
    assert persisted is not None
    assert persisted.state == CandidateState.REJECTED
    assert persisted.metadata["review_feedback"]["kind"] == "already_handled"
    assert persisted.metadata["suppressed_reason"] == "already_handled_by_parent"
    assert store.list_household_events(household_id="hh_123") == []
    store.close()


def test_review_feedback_too_late_closes_past_time_bound_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_stale_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:stale-1",
            title="Open gym booking",
            summary="Open gym booking for earlier today.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "from_address": "Bookings <booking@example-gym.com>",
                "proposed_fields": {
                    "title": "Open gym booking",
                    "starts_at": "2026-05-08T09:00:00-07:00",
                    "timezone": "America/Los_Angeles",
                },
            },
        )
    )

    reply = review_service.apply_feedback_response(
        candidate_id="cand_stale_123",
        member_id="mem_123",
        feedback_kind="stale",
        user_text="too late",
    )

    persisted = store.get_imported_candidate("cand_stale_123")
    assert reply.reply_text == "Got it. I marked that as too late and left it out."
    assert persisted is not None
    assert persisted.state == CandidateState.REJECTED
    assert persisted.metadata["review_feedback"]["kind"] == "stale"
    assert persisted.metadata["suppressed_reason"] == "stale_by_parent_feedback"
    assert store.list_household_events(household_id="hh_123") == []
    store.close()


def test_review_feedback_already_responded_keeps_same_gmail_candidate_suppressed_on_resync(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    google_service = FlorenceGoogleSyncPersistenceService(store)
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    store.upsert_google_connection(connection)
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_dm_123",
        visible_child_names=["Ava"],
        school_labels=["Roosevelt Elementary"],
        activity_labels=["Soccer"],
    )
    gmail_item = GmailSyncItem(
        gmail_message_id="gmail_handled_123",
        thread_id="thread_handled_123",
        from_address="Ms. Kim <teacher@roosevelt.k12.ca.us>",
        subject="Roosevelt Elementary soccer form",
        snippet="Please sign the soccer form.",
        body_text="Ava needs her soccer form signed by Friday.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
        label_ids=("INBOX", "UNREAD", "CATEGORY_UPDATES"),
    )
    first = google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(connection=connection, context=context, gmail_items=[gmail_item])
    )
    assert len(first.candidates) == 1
    candidate_id = first.candidates[0].id

    review_service.apply_feedback_response(
        candidate_id=candidate_id,
        member_id="mem_123",
        feedback_kind="already_handled",
        user_text="read and responded",
    )
    second = google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(connection=connection, context=context, gmail_items=[gmail_item])
    )

    persisted = store.get_imported_candidate(candidate_id)
    assert second.candidates
    assert persisted is not None
    assert persisted.state == CandidateState.REJECTED
    assert persisted.metadata["review_feedback"]["user_text"] == "read and responded"
    assert persisted.metadata["suppressed_reason"] == "already_handled_by_parent"
    store.close()


def test_review_feedback_private_only_and_always_share_create_source_rules_without_closing_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_private_source",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:private-source",
            title="Medical bill",
            summary="A private medical bill.",
            state=CandidateState.PENDING_REVIEW,
            metadata={"from_address": "Billing <billing@clinic.example>"},
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_shared_source",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:shared-source",
            title="Music class update",
            summary="A music class update.",
            state=CandidateState.PENDING_REVIEW,
            metadata={"from_address": "Linda <linda@musicalbeginnings.com>"},
        )
    )

    private_reply = review_service.apply_feedback_response(
        candidate_id="cand_private_source",
        member_id="mem_123",
        feedback_kind="private_only",
        source_visibility=HouseholdSourceVisibility.PRIVATE,
        user_text="private only",
    )
    shared_reply = review_service.apply_feedback_response(
        candidate_id="cand_shared_source",
        member_id="mem_123",
        feedback_kind="always_share",
        source_visibility=HouseholdSourceVisibility.SHARED,
        user_text="always share this source",
    )

    private_rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.PRIVATE,
    )
    shared_rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.SHARED,
    )
    assert "private to your review queue" in private_reply.reply_text
    assert "shared household context" in shared_reply.reply_text
    assert store.get_imported_candidate("cand_private_source").state == CandidateState.PENDING_REVIEW
    assert store.get_imported_candidate("cand_shared_source").state == CandidateState.PENDING_REVIEW
    assert any(rule.matcher_value == "billing@clinic.example" for rule in private_rules)
    assert any(rule.matcher_value == "musicalbeginnings.com" for rule in shared_rules)
    store.close()


def test_account_level_source_rule_keeps_future_work_email_candidates_private(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    rule = build_account_source_rule(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        email="jackson@creatorsinc.com",
        visibility=HouseholdSourceVisibility.PRIVATE,
        created_by_member_id="mem_123",
        label="jackson@creatorsinc.com",
    )
    assert rule is not None
    store.upsert_household_source_rule(rule)
    candidate = ImportedCandidate(
        id="cand_work",
        household_id="hh_123",
        member_id="mem_123",
        source_kind=GoogleSourceKind.GMAIL,
        source_identifier="gmail:work",
        title="Company Team Meeting",
        summary="Work account email that mentions a schedule.",
        state=CandidateState.PENDING_REVIEW,
        metadata={
            "connected_email": "jackson@creatorsinc.com",
            "from_address": "Calendar Bot <calendar-bot@example.com>",
            "candidate_scope": "shared_household",
        },
    )

    updated = review_service.source_rule_service.apply_candidate_policy(candidate)

    assert updated.metadata["source_visibility"] == "private"
    assert updated.metadata["source_rule_id"] == rule.id
    assert updated.metadata["source_rule_label"] == "jackson@creatorsinc.com"
    assert rule.matcher_kind == HouseholdSourceMatcherKind.GMAIL_CONNECTED_ACCOUNT
    store.close()


def test_old_private_gmail_candidates_do_not_surface_as_fresh_review_items(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(
        store,
        now_getter=lambda: datetime(2026, 5, 13, 16, 0, tzinfo=timezone.utc),
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_old_private_work",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:old-private-work",
            title="Company Team Meeting",
            summary="Old work email from mid-April.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=7_500,
            metadata={
                "candidate_scope": "private_parent",
                "received_at": "2026-04-14T15:00:00+00:00",
                "raw_metadata": {"reason_tags": ["private_parent", "schedule_signal"]},
            },
        )
    )

    prompt = review_service.build_next_review_prompt(household_id="hh_123", member_id="mem_123")

    assert prompt is None
    assert store.get_imported_candidate("cand_old_private_work").state == CandidateState.PENDING_REVIEW
    store.close()


def test_review_feedback_ignore_item_type_records_rule_and_suppresses_future_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_gym_old",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gym-old",
            title="4 Star open gym booking",
            summary="Booking reminder for open gym.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=8_300,
            metadata={
                "raw_metadata": {
                    "reason_tags": ["activity_signal", "schedule_signal"],
                    "temporal_evidence": {"date_match": {"date": "2026-10-10"}},
                }
            },
        )
    )

    reply = review_service.apply_feedback_response(
        candidate_id="cand_gym_old",
        member_id="mem_123",
        feedback_kind="ignore_item_type",
        user_text="ignore this type",
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_gym_new",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gym-new",
            title="4 Star open gym booking",
            summary="Booking reminder for another open gym.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=8_300,
            metadata={
                "raw_metadata": {
                    "reason_tags": ["activity_signal", "schedule_signal"],
                    "temporal_evidence": {"date_match": {"date": "2026-10-17"}},
                }
            },
        )
    )

    prompt = review_service.build_next_review_prompt(household_id="hh_123", member_id="mem_123")
    preferences = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
    )
    persisted = store.get_imported_candidate("cand_gym_new")
    assert "suppress future activity items" in reply.reply_text.lower()
    assert prompt is None
    assert any(item.metadata.get("rule_kind") == "ignore_item_type" for item in preferences)
    assert persisted is not None
    assert persisted.state == CandidateState.REJECTED
    assert persisted.metadata["suppressed_reason"] == "relevance_rule_ignored_item_type"
    store.close()


def test_review_feedback_too_noisy_suppresses_low_confidence_future_but_preserves_important_items(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_school_noise",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:school-noise",
            title="WISH weekly update",
            summary="Routine weekly school update.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=7_800,
            metadata={"raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]}},
        )
    )

    review_service.apply_feedback_response(
        candidate_id="cand_school_noise",
        member_id="mem_123",
        feedback_kind="too_noisy",
        user_text="not worth a text unless important",
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_school_future_noise",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:school-future-noise",
            title="WISH weekly update",
            summary="Routine weekly school update.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=7_600,
            metadata={"raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]}},
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_school_urgent",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:school-urgent",
            title="Urgent school form due today",
            summary="Please return Violet's form today.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=9_300,
            metadata={"raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]}},
        )
    )

    prompt = review_service.build_review_batch_prompt(household_id="hh_123", member_id="mem_123", limit=3)
    low_value = store.get_imported_candidate("cand_school_future_noise")

    assert prompt is not None
    assert [candidate.id for candidate in prompt.candidates] == ["cand_school_urgent"]
    assert low_value is not None
    assert low_value.state == CandidateState.REJECTED
    assert low_value.metadata["suppressed_reason"] == "relevance_rule_too_noisy"
    store.close()


def test_review_feedback_always_surface_overrides_noisy_rule_for_same_item_type(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    for candidate_id, title in (
        ("cand_school_noise", "WISH weekly update"),
        ("cand_school_matters", "Young Minds classroom reminder"),
    ):
        store.upsert_imported_candidate(
            ImportedCandidate(
                id=candidate_id,
                household_id="hh_123",
                member_id="mem_123",
                source_kind=GoogleSourceKind.GMAIL,
                source_identifier=f"gmail:{candidate_id}",
                title=title,
                summary="Routine school update.",
                state=CandidateState.PENDING_REVIEW,
                confidence_bps=7_500,
                metadata={"raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]}},
            )
        )

    review_service.apply_feedback_response(
        candidate_id="cand_school_noise",
        member_id="mem_123",
        feedback_kind="too_noisy",
        user_text="too noisy",
    )
    review_service.apply_feedback_response(
        candidate_id="cand_school_matters",
        member_id="mem_123",
        feedback_kind="always_surface",
        user_text="this matters",
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_school_future",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:school-future",
            title="Young Minds classroom reminder",
            summary="Routine school update.",
            state=CandidateState.PENDING_REVIEW,
            confidence_bps=7_300,
            metadata={"raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]}},
        )
    )

    prompt = review_service.build_review_batch_prompt(household_id="hh_123", member_id="mem_123", limit=3)

    assert prompt is not None
    assert len(prompt.candidates) == 1
    assert set(prompt.candidate.metadata["review_group_candidate_ids"]) == {"cand_school_future", "cand_school_matters"}
    assert store.get_imported_candidate("cand_school_future").state == CandidateState.PENDING_REVIEW
    store.close()


def test_review_feedback_duplicate_and_wrong_timing_close_candidate_with_reasons(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_duplicate",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:duplicate",
            title="Family meeting",
            summary="Duplicate family meeting candidate.",
            state=CandidateState.PENDING_REVIEW,
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_wrong_timing",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:wrong-timing",
            title="Bloomz reminder",
            summary="Please pack library books.",
            state=CandidateState.PENDING_REVIEW,
            metadata={"raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]}},
        )
    )

    duplicate_reply = review_service.apply_feedback_response(
        candidate_id="cand_duplicate",
        member_id="mem_123",
        feedback_kind="duplicate",
        user_text="duplicate",
    )
    timing_reply = review_service.apply_feedback_response(
        candidate_id="cand_wrong_timing",
        member_id="mem_123",
        feedback_kind="wrong_timing",
        user_text="tell me sooner",
    )

    duplicate = store.get_imported_candidate("cand_duplicate")
    wrong_timing = store.get_imported_candidate("cand_wrong_timing")
    preferences = store.list_household_profile_items(household_id="hh_123", kind=HouseholdProfileKind.PREFERENCE)
    assert "duplicate" in duplicate_reply.reply_text
    assert "adjust the timing" in timing_reply.reply_text
    assert duplicate is not None
    assert wrong_timing is not None
    assert duplicate.metadata["suppressed_reason"] == "duplicate_by_parent"
    assert wrong_timing.metadata["suppressed_reason"] == "wrong_timing_by_parent"
    assert any(item.metadata.get("rule_kind") == "wrong_timing" for item in preferences)
    store.close()


def test_review_prompt_collapses_duplicate_school_admin_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    for candidate_id, source_identifier in (
        ("cand_wish_a", "gmail:wish-a"),
        ("cand_wish_b", "gmail:wish-b"),
    ):
        store.upsert_imported_candidate(
            ImportedCandidate(
                id=candidate_id,
                household_id="hh_123",
                member_id="mem_123",
                source_kind=GoogleSourceKind.GMAIL,
                source_identifier=source_identifier,
                title="WISH weekly update",
                summary="Routine WISH school update.",
                state=CandidateState.PENDING_REVIEW,
                confidence_bps=8_400,
                metadata={
                    "source_provenance": {"subject": "WISH weekly update"},
                    "raw_metadata": {"sender_looks_school": True, "reason_tags": ["school_source"]},
                },
            )
        )

    prompt = review_service.build_review_batch_prompt(household_id="hh_123", member_id="mem_123", limit=3)

    assert prompt is not None
    assert len(prompt.candidates) == 1
    assert prompt.candidate.metadata["review_group_size"] == 2
    assert set(prompt.candidate.metadata["review_group_candidate_ids"]) == {"cand_wish_a", "cand_wish_b"}
    assert "2 matching school/admin items" in prompt.text
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
            summary="Family calendar · 2027-04-17T19:00:00-04:00",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add this to the shared household plan?",
                "calendar_id": "family-calendar",
                "proposed_fields": {
                    "title": "Reservation at Zimmi's",
                    "starts_at": "2027-04-17T19:00:00-04:00",
                    "ends_at": "2027-04-17T21:00:00-04:00",
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
    assert "Sat, Apr 17 at 4:00 PM PDT (7:00 PM EDT)" in prompt.text
    store.close()
