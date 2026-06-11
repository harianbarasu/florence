from datetime import datetime, timezone

import pytest

from florence.timeutil import (
    format_local,
    next_local_time,
    next_occurrence,
    parse_local,
)


def test_parse_local_converts_to_utc():
    # 9:00 AM PDT == 16:00 UTC
    result = parse_local("2026-06-12 09:00", "America/Los_Angeles")
    assert result == datetime(2026, 6, 12, 16, 0, tzinfo=timezone.utc)


def test_parse_local_accepts_t_separator_and_seconds():
    assert parse_local("2026-06-12T09:00", "UTC") == datetime(2026, 6, 12, 9, 0, tzinfo=timezone.utc)
    assert parse_local("2026-06-12 09:00:30", "UTC") == datetime(
        2026, 6, 12, 9, 0, 30, tzinfo=timezone.utc
    )


def test_parse_local_rejects_garbage():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        parse_local("next friday", "UTC")
    with pytest.raises(ValueError, match="timezone"):
        parse_local("2026-06-12 09:00", "America/Nowhere")


def test_daily_recurrence_keeps_wall_clock_across_dst():
    # US DST starts 2026-03-08 in America/Los_Angeles.
    before = parse_local("2026-03-07 07:15", "America/Los_Angeles")
    after = next_occurrence(before, "daily", "America/Los_Angeles")
    assert format_local(after, "America/Los_Angeles").endswith("7:15 AM")
    # UTC offset shifts by an hour, local time doesn't.
    assert (after - before).total_seconds() == 23 * 3600


def test_weekdays_recurrence_skips_weekend():
    friday = parse_local("2026-06-12 07:15", "UTC")  # a Friday
    nxt = next_occurrence(friday, "weekdays", "UTC")
    assert nxt == parse_local("2026-06-15 07:15", "UTC")  # Monday


def test_monthly_recurrence_clamps_short_months():
    jan31 = parse_local("2026-01-31 09:00", "UTC")
    feb = next_occurrence(jan31, "monthly", "UTC")
    assert feb == parse_local("2026-02-28 09:00", "UTC")


def test_yearly_and_unknown():
    base = parse_local("2026-06-12 09:00", "UTC")
    assert next_occurrence(base, "yearly", "UTC") == parse_local("2027-06-12 09:00", "UTC")
    with pytest.raises(ValueError, match="recurrence"):
        next_occurrence(base, "fortnightly", "UTC")


def test_next_local_time_rolls_to_tomorrow():
    after = datetime(2026, 6, 12, 16, 0, tzinfo=timezone.utc)  # 9:00 AM PDT
    nxt = next_local_time(7, 15, "America/Los_Angeles", after=after)
    assert format_local(nxt, "America/Los_Angeles") == "Sat Jun 13, 2026 7:15 AM"
