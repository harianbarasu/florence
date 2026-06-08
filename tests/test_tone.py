from datetime import datetime, timezone

from florence import tone
from florence.models import MemorySnapshot


def test_common_tone_paths_are_warm_and_actionable():
    samples = [
        tone.reminder_needs_time(),
        tone.reminder_needs_ampm("at 8"),
        tone.google_connection_not_configured(),
        tone.partner_invite_not_configured(),
        tone.approval_not_found(),
        tone.source_preferences([]),
        tone.source_feedback_without_recent_item(),
        tone.memory_snapshot(MemorySnapshot(household_id="household-1", memories=[])),
        tone.due_reminder("pack lunch"),
        tone.help_text("privacy"),
        tone.fallback_reply(),
        tone.reminder_created(
            "pack lunch",
            datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc),
            "America/Los_Angeles",
        ),
    ]

    for sample in samples:
        lowered = sample.lower()
        assert "you should have" not in lowered
        assert "why didn't" not in lowered
        assert "cannot" not in lowered
        assert "not your fault" not in lowered
        assert sample.strip()

    assert "what day and time" in tone.reminder_needs_time().lower()
    assert "am or pm" in tone.reminder_needs_ampm("at 8").lower()
    assert tone.due_reminder("pack lunch") == "Quick reminder: pack lunch"
    assert "still need to be configured" in tone.google_connection_not_configured().lower()
    assert "text 'remember that ...'" in tone.memory_snapshot(
        MemorySnapshot(household_id="household-1", memories=[]),
    ).lower()
    assert "did not make any household changes" in tone.fallback_reply()
    assert "support" in tone.fallback_reply().lower()
    assert "keeping this in household context" not in tone.fallback_reply().lower()


def test_help_text_has_topic_specific_routes():
    assert "For more detail" in tone.help_text()
    assert "Setup help:" in tone.help_text("setup")
    assert "Source help:" in tone.help_text("sources")
    assert "Calendar help:" in tone.help_text("calendar")
    assert "Memory help:" in tone.help_text("memory")
    assert "Privacy help:" in tone.help_text("privacy")
    assert "Reminder help:" in tone.help_text("reminders")
    assert "Approval help:" in tone.help_text("approvals")
    assert "Support help:" in tone.help_text("support")
