import json
import sys
import types
from datetime import datetime, timezone

from florence.contracts import HouseholdContext
from florence.google import GmailSyncItem, ParentCalendarSyncItem
from florence.relevance import (
    CandidateDecisionKind,
    build_gmail_candidate_decision,
    build_parent_calendar_candidate_decision,
)


def test_gmail_candidate_detects_school_logistics_event():
    item = GmailSyncItem(
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

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.requires_confirmation is False
    assert decision.proposed_fields is not None
    assert decision.proposed_fields["title"] == "Soccer practice update"


def test_gmail_candidate_skips_unrelated_email():
    item = GmailSyncItem(
        gmail_message_id="gmail_124",
        thread_id="thread_124",
        from_address="news@example.com",
        subject="Weekend sale now on",
        snippet="Save 20 percent on patio furniture",
        body_text="Shop patio furniture and decor this weekend only.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    decision = build_gmail_candidate_decision(item, "America/Los_Angeles")

    assert decision.kind == CandidateDecisionKind.SKIP
    assert decision.reason == "promotional_noise"


def test_gmail_candidate_uses_two_pass_llm_when_configured(monkeypatch):
    calls: list[dict[str, object]] = []

    class _FakeResponses:
        def create(self, **kwargs):
            calls.append(kwargs)
            model = kwargs["model"]
            payload = {
                "kind": "candidate",
                "reason": "known_contact_schedule_email",
                "title": "Spring break and Family Day dates",
                "summary": "Linda sent the spring break and Family Day dates for Violet's class.",
                "confidence_bps": 9100,
                "requires_confirmation": False,
                "confirmation_question": None,
                "signals": ["known_contact", "known_activity", "no_class", "family_day"],
                "proposed_fields": {
                    "title": "Spring break and Family Day dates",
                    "description": "No class April 1 and April 8. Family Day May 6.",
                },
            }
            if model == "gpt-5.4-mini":
                payload["confidence_bps"] = 7300
            return types.SimpleNamespace(
                output_text=json.dumps(payload)
            )

    class _FakeOpenAI:
        def __init__(self, *, api_key, base_url):
            calls.append({"api_key": api_key, "base_url": base_url})
            self.responses = _FakeResponses()

    monkeypatch.delenv("FLORENCE_GMAIL_RELEVANCE_DISABLE", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))

    item = GmailSyncItem(
        gmail_message_id="gmail_125",
        thread_id="thread_125",
        from_address="Linda <linda@musicalbeginnings.com>",
        subject="Spring break and Family Day dates",
        snippet="No class April 1 and April 8.",
        body_text="For Violet's class: no class April 1 and April 8. Family Day May 6.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Violet"],
        activity_labels=["Musical Beginnings"],
        contact_names=["Linda"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.title == "Spring break and Family Day dates"
    assert decision.raw_metadata["classifier"] == "gmail_llm_deep_v1"
    assert decision.raw_metadata["triage_model"] == "gpt-5.4-mini"
    assert decision.raw_metadata["signals"] == ["known_contact", "known_activity", "no_class", "family_day"]
    assert calls[0]["api_key"] == "sk-test"
    assert [entry["model"] for entry in calls if "model" in entry] == ["gpt-5.4-mini", "gpt-5.4"]


def test_gmail_candidate_escalates_low_confidence_skip_to_deep_pass(monkeypatch):
    calls: list[str] = []

    class _FakeResponses:
        def create(self, **kwargs):
            calls.append(kwargs["model"])
            if kwargs["model"] == "gpt-5.4-mini":
                return types.SimpleNamespace(
                    output_text=json.dumps(
                        {
                            "kind": "skip",
                            "reason": "uncertain_schoolish_message",
                            "confidence_bps": 6200,
                            "signals": ["known_contact", "schedule_change"],
                        }
                    )
                )
            return types.SimpleNamespace(
                output_text=json.dumps(
                    {
                        "kind": "candidate",
                        "reason": "known_contact_schedule_email",
                        "title": "Camp reminder",
                        "summary": "Camp schedule update from a known contact.",
                        "confidence_bps": 8900,
                        "requires_confirmation": False,
                        "signals": ["known_contact", "schedule_change"],
                        "proposed_fields": {"title": "Camp reminder"},
                    }
                )
            )

    class _FakeOpenAI:
        def __init__(self, *, api_key, base_url):  # noqa: ARG002
            self.responses = _FakeResponses()

    monkeypatch.delenv("FLORENCE_GMAIL_RELEVANCE_DISABLE", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))

    item = GmailSyncItem(
        gmail_message_id="gmail_125b",
        thread_id="thread_125b",
        from_address="director@farmcamp.com",
        subject="Camp reminder",
        snippet="Schedule update for Theo",
        body_text="Theo's pickup moved to 3pm this Friday.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo"],
        contact_names=["Camp Director"],
    )

    decision = build_gmail_candidate_decision(item, "America/Los_Angeles", context=context)

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["classifier"] == "gmail_llm_deep_v1"
    assert calls == ["gpt-5.4-mini", "gpt-5.4"]


def test_gmail_candidate_falls_back_to_heuristics_when_llm_output_is_invalid(monkeypatch):
    class _FakeResponses:
        def create(self, **kwargs):  # noqa: ARG002
            return types.SimpleNamespace(output_text="not-json")

    class _FakeOpenAI:
        def __init__(self, *, api_key, base_url):  # noqa: ARG002
            self.responses = _FakeResponses()

    monkeypatch.delenv("FLORENCE_GMAIL_RELEVANCE_DISABLE", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=_FakeOpenAI))

    item = GmailSyncItem(
        gmail_message_id="gmail_126-fallback",
        thread_id="thread_126-fallback",
        from_address="updates@parentsquare.com",
        subject="Practice reminder",
        snippet="Aves soccer is Thursday from 4pm to 5pm",
        body_text="Please arrive early for soccer practice on September 18.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Ava"],
        child_aliases=["Aves"],
        school_platforms=["ParentSquare"],
        activity_labels=["Soccer"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["classifier"] == "gmail_heuristics_v1"


def test_parent_calendar_candidate_detects_child_activity():
    item = ParentCalendarSyncItem(
        google_event_id="event_123",
        title="Ava soccer practice",
        description="Weekly team practice on the north field",
        location="North field",
        html_link=None,
        starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Family calendar",
        family_member_names=["Ava"],
    )

    decision = build_parent_calendar_candidate_decision(item)

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.proposed_fields is not None
    assert decision.proposed_fields["title"] == "Ava soccer practice"


def test_parent_calendar_candidate_skips_personal_meeting():
    item = ParentCalendarSyncItem(
        google_event_id="event_124",
        title="Client meeting",
        description="Quarterly planning Zoom",
        location="Zoom",
        html_link=None,
        starts_at=datetime(2026, 9, 18, 16, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 18, 17, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Personal calendar",
        family_member_names=["Ava"],
    )

    decision = build_parent_calendar_candidate_decision(item)

    assert decision.kind == CandidateDecisionKind.SKIP
    assert decision.reason == "not_child_or_family_logistics"


def test_gmail_candidate_uses_household_platform_and_child_alias_context():
    item = GmailSyncItem(
        gmail_message_id="gmail_126",
        thread_id="thread_126",
        from_address="updates@parentsquare.com",
        subject="Practice reminder",
        snippet="Aves soccer is Thursday from 4pm to 5pm",
        body_text="Please arrive early for soccer practice on September 18.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Ava"],
        child_aliases=["Aves"],
        school_platforms=["ParentSquare"],
        activity_labels=["Soccer"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["platform_hits"] == 1
    assert decision.raw_metadata["known_child_hits"] == 1
    assert decision.raw_metadata["known_activity_hits"] == 1


def test_gmail_candidate_ignores_invalid_slash_dates_instead_of_crashing():
    item = GmailSyncItem(
        gmail_message_id="gmail_127",
        thread_id="thread_127",
        from_address="teacher@school.edu",
        subject="Soccer practice update",
        snippet="Schedule change",
        body_text="Ava soccer practice is 13/24 at 4pm.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.requires_confirmation is True
    assert decision.confirmation_question is not None


def test_parent_calendar_candidate_uses_known_location_context():
    item = ParentCalendarSyncItem(
        google_event_id="event_126",
        title="Scrimmage",
        description="",
        location="North Field",
        html_link=None,
        starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        timezone="America/Los_Angeles",
        all_day=False,
        updated_at=None,
        calendar_summary="Family calendar",
        family_member_names=[],
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Ava"],
        activity_labels=["Soccer"],
        location_labels=["North Field"],
    )

    decision = build_parent_calendar_candidate_decision(item, context=context)

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["known_location_hits"] == 1


def test_gmail_candidate_skips_newsletter_with_schedule_words_when_no_household_anchor():
    item = GmailSyncItem(
        gmail_message_id="gmail_128",
        thread_id="thread_128",
        from_address="pod@mail.scalablepod.com",
        subject="Scalable: Creators Want Their Red Carpet Moment Too",
        snippet="The schedule looks conditional. Which date or time applies?",
        body_text="Kaya & Jasmine plus little news. Episode drops Friday at 4pm.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo", "Violet"],
        school_labels=["Wish Community School", "Young Minds Preschool"],
        activity_labels=["Baseball", "Dance", "Music"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.SKIP
    assert decision.reason == "promotional_noise"


def test_gmail_candidate_accepts_non_school_sender_when_child_and_activity_match_context():
    item = GmailSyncItem(
        gmail_message_id="gmail_129",
        thread_id="thread_129",
        from_address="coach.jen@gmail.com",
        subject="Theo baseball practice moved",
        snippet="Practice is Thursday 5pm instead of Wednesday.",
        body_text="Theo baseball practice is on September 18 at 5pm.",
        attachment_text=None,
        attachment_count=0,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )
    context = HouseholdContext(
        household_id="hh_123",
        actor_member_id="mem_123",
        channel_id="chan_123",
        visible_child_names=["Theo", "Violet"],
        activity_labels=["Baseball", "Dance"],
    )

    decision = build_gmail_candidate_decision(
        item,
        "America/Los_Angeles",
        context=context,
        now=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert decision.kind == CandidateDecisionKind.CANDIDATE
    assert decision.raw_metadata["known_child_hits"] >= 1
    assert decision.raw_metadata["known_activity_hits"] >= 1
