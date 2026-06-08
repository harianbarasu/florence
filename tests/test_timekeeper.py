from datetime import datetime, timezone

from florence.timekeeper import parse_due_time


def test_tomorrow_at_time_uses_household_timezone():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind us tomorrow at 8am to pack the permission slip",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is False
    assert parsed.due_at_utc == datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)


def test_today_in_the_past_requires_clarification():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind me today at 8am to call school",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is True
    assert parsed.reason == "due_time_is_in_the_past"
    assert parsed.due_at_utc is None


def test_next_weekday_rolls_forward():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind us next Friday at 5pm to bring snacks",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.due_at_utc == datetime(2026, 6, 13, 0, 0, tzinfo=timezone.utc)


def test_tomorrow_morning_uses_explicit_daypart_time():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind us tomorrow morning to pack lunch",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is False
    assert parsed.due_at_utc == datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)


def test_bare_hour_without_meridiem_requires_clarification():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind us tomorrow at 8 to pack lunch",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is True
    assert parsed.reason == "ambiguous_clock_time"
    assert parsed.due_at_utc is None


def test_compact_meridiem_time_is_not_treated_as_ambiguous():
    now = datetime(2026, 6, 5, 13, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind us today at 7:20am to leave for school",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is False
    assert parsed.due_at_utc == datetime(2026, 6, 5, 14, 20, tzinfo=timezone.utc)


def test_bare_24_hour_time_parses_without_meridiem():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind us tomorrow at 17 to pack lunch",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is False
    assert parsed.due_at_utc == datetime(2026, 6, 7, 0, 0, tzinfo=timezone.utc)


def test_this_afternoon_uses_local_afternoon_when_future():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind me this afternoon to call school",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is False
    assert parsed.due_at_utc == datetime(2026, 6, 5, 22, 0, tzinfo=timezone.utc)


def test_this_afternoon_after_afternoon_requires_clarification():
    now = datetime(2026, 6, 5, 23, 30, tzinfo=timezone.utc)

    parsed = parse_due_time(
        "remind me this afternoon to call school",
        "America/Los_Angeles",
        now_utc=now,
    )

    assert parsed.needs_clarification is True
    assert parsed.reason == "due_time_is_in_the_past"
