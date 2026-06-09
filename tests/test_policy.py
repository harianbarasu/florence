from datetime import datetime, timedelta, timezone

from florence.models import SourceDecision, SourceItem, SourcePreference, SourcePreferenceKind
from florence.policy import NeedToKnowPolicy


def _item(**overrides):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    data = {
        "id": "source-1",
        "household_id": "household-1",
        "source_type": "email",
        "title": "Weekly newsletter",
        "body": "Here is the weekly update.",
        "observed_at_utc": now,
        "event_at_utc": None,
    }
    data.update(overrides)
    return SourceItem(**data)


def test_past_source_is_suppressed():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="School concert",
            body="Concert was yesterday.",
            event_at_utc=now - timedelta(days=1),
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.SUPPRESS
    assert triage.reason == "event_is_in_the_past"


def test_low_signal_newsletter_is_stored_only():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="Weekly school newsletter",
            body="A general recap with spirit wear sale details.",
            event_at_utc=now + timedelta(hours=12),
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.STORE_ONLY


def test_routine_security_notice_stays_quiet():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="New sign-in alert",
            body="A new device signed in to your account.",
            sender="security@example.com",
            event_at_utc=None,
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.STORE_ONLY
    assert triage.reason == "automated_background_notice"


def test_no_reply_actionable_school_item_can_still_surface():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="Permission slip due",
            body="Please sign and bring the permission slip tomorrow.",
            sender="no-reply@school.example",
            event_at_utc=now + timedelta(hours=8),
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.SURFACE
    assert triage.reason == "urgent_actionable_source"


def test_no_school_without_extracted_time_surfaces():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="No school tomorrow",
            body="Campus will be closed for a teacher work day.",
            event_at_utc=None,
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.SURFACE
    assert triage.reason == "high_signal_without_known_due_time"
    assert triage.suggested_due_at_utc is None


def test_early_dismissal_without_extracted_time_surfaces_even_in_newsletter():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="Weekly school newsletter",
            body="Reminder: early dismissal Friday. Spirit wear sale details are below.",
            event_at_utc=None,
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.SURFACE
    assert triage.reason == "high_signal_without_known_due_time"


def test_old_no_time_source_stays_quiet_even_when_high_signal():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="No school tomorrow",
            body="Campus will be closed for a teacher work day.",
            observed_at_utc=now - timedelta(days=3),
            event_at_utc=None,
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.STORE_ONLY
    assert triage.reason == "source_observed_too_old_without_due_time"


def test_single_non_schedule_action_without_time_stays_quiet():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="Class newsletter",
            body="Students can bring canned goods for the fundraiser.",
            event_at_utc=None,
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.STORE_ONLY


def test_imminent_actionable_school_item_surfaces():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()

    triage = policy.classify(
        _item(
            title="Permission slip due",
            body="Please sign and bring the permission slip for tomorrow's field trip.",
            event_at_utc=now + timedelta(hours=8),
        ),
        now_utc=now,
    )

    assert triage.decision == SourceDecision.SURFACE
    assert triage.priority >= 90


def test_always_surface_preference_does_not_downgrade_actionable_item():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()
    preference = _preference("permission slips", SourcePreferenceKind.ALWAYS_SURFACE, now)

    triage = policy.classify(
        _item(
            title="Permission slip due",
            body="Please sign and bring the permission slip for tomorrow's field trip.",
            event_at_utc=now + timedelta(hours=8),
        ),
        now_utc=now,
        preferences=[preference],
    )

    assert triage.decision == SourceDecision.SURFACE
    assert triage.reason == "urgent_actionable_source"
    assert triage.suggested_due_at_utc == now + timedelta(hours=8)


def test_source_preference_matches_natural_singular_plural_variants():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()
    preference = _preference("permission slip", SourcePreferenceKind.ALWAYS_SURFACE, now)

    triage = policy.classify(
        _item(
            title="Permission slips for field trip",
            body="Please return permission slips by next week.",
            event_at_utc=now + timedelta(days=5),
        ),
        now_utc=now,
        preferences=[preference],
    )

    assert triage.decision == SourceDecision.SURFACE
    assert triage.reason == "household_requested_source"


def test_broad_requested_source_does_not_surface_promotional_newsletter():
    now = datetime(2026, 6, 9, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()
    preference = _preference("camp", SourcePreferenceKind.ALWAYS_SURFACE, now)

    triage = policy.classify(
        _item(
            title="Unique Belgian Camps + NYC Theater + Father's Day Gift Ideas",
            body=(
                "Hey there! Stay tuned for family-friendly travel content coming your way. "
                "4 Unique Belgian Summer Camps."
            ),
            sender="Maddy Novich <maddy@cargobikemomma.com>",
            observed_at_utc=now,
            event_at_utc=None,
        ),
        now_utc=now,
        preferences=[preference],
    )

    assert triage.decision == SourceDecision.STORE_ONLY
    assert triage.reason == "requested_source_low_signal"


def test_broad_requested_source_does_not_surface_promotional_event():
    now = datetime(2026, 6, 9, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()
    preference = _preference("camp", SourcePreferenceKind.ALWAYS_SURFACE, now)

    triage = policy.classify(
        _item(
            title="Barong but Make it Dodgers - 6/15",
            body="View Online. Instagram Facebook YouTube Dodgers Youth Camps All-Access Events.",
            sender="Los Angeles Dodgers <dodgers@marketing.mlbemail.com>",
            observed_at_utc=now,
            event_at_utc=now + timedelta(days=6),
        ),
        now_utc=now,
        preferences=[preference],
    )

    assert triage.decision == SourceDecision.STORE_ONLY
    assert triage.reason == "requested_source_low_signal"


def test_mute_preference_matches_plural_of_parent_phrase():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    policy = NeedToKnowPolicy()
    preference = _preference("newsletter", SourcePreferenceKind.MUTE, now)

    triage = policy.classify(
        _item(
            title="Weekly newsletters",
            body="A digest of classroom news.",
            event_at_utc=now + timedelta(hours=12),
        ),
        now_utc=now,
        preferences=[preference],
    )

    assert triage.decision == SourceDecision.STORE_ONLY
    assert triage.reason == "household_muted_source"


def _preference(
    phrase: str,
    kind: SourcePreferenceKind,
    now: datetime,
) -> SourcePreference:
    return SourcePreference(
        id=f"preference-{phrase}",
        household_id="household-1",
        phrase=phrase,
        preference=kind,
        created_at_utc=now,
        updated_at_utc=now,
    )
