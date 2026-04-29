"""Tests for gap-fill time utility helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms


def test_last_closed_open_ms_uses_previous_interval() -> None:
    interval_ms = 60_000
    now = datetime(2026, 4, 27, 10, 5, 30, tzinfo=UTC)

    result = _last_closed_open_ms(interval_ms=interval_ms, now_utc=now)

    expected = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert result == expected


def test_missing_ranges_ms_returns_internal_and_tail_gaps() -> None:
    interval_ms = 60_000
    open_times = [
        datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 2, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 3, tzinfo=UTC),
    ]
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)

    ranges = _missing_ranges_ms(
        existing_open_times=open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )

    gap_one_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    gap_tail_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert ranges == [(gap_one_ms, gap_one_ms), (gap_tail_start_ms, end_open_ms)]


def test_missing_ranges_ms_returns_empty_without_existing_points() -> None:
    ranges = _missing_ranges_ms(
        existing_open_times=[],
        interval_ms=60_000,
        end_open_ms=int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000),
    )

    assert ranges == []


def test_missing_ranges_ms_ignores_naive_and_future_points() -> None:
    interval_ms = 60_000
    end_open_ms = int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000)
    open_times = [
        datetime(2026, 4, 27, 10, 0),
        datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 3, tzinfo=UTC),
    ]

    ranges = _missing_ranges_ms(
        existing_open_times=open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )

    gap_ms = int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000)
    assert ranges == [(gap_ms, gap_ms)]
