"""Tests for Deribit-only open-interest ingestion interface."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion import open_interest as oi
from ingestion.exchanges import deribit_open_interest


def test_normalize_open_interest_timeframe_deribit() -> None:
    assert oi.normalize_open_interest_timeframe("deribit", "M1") == "1m"


def test_fetch_open_interest_all_history_returns_empty_for_spot() -> None:
    rows = oi.fetch_open_interest_all_history(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        market="spot",
    )
    assert rows == []


def test_fetch_open_interest_range_deribit_historical(monkeypatch: pytest.MonkeyPatch) -> None:
    point_ms = int(datetime(2026, 4, 28, 9, 2, tzinfo=UTC).timestamp() * 1000)
    monkeypatch.setattr(
        deribit_open_interest,
        "fetch_open_interest_range",
        lambda **kwargs: [{"timestamp": point_ms, "open_interest": 1000.0}],
    )
    monkeypatch.setattr(
        deribit_open_interest,
        "parse_open_interest_row",
        lambda symbol, period, row: {
            "open_time": datetime(2026, 4, 28, 9, 2, tzinfo=UTC),
            "close_time": datetime(2026, 4, 28, 9, 2, 59, 999000, tzinfo=UTC),
            "open_interest": float(row["open_interest"]),
            "open_interest_value": 0.0,
        },
    )

    start = int(datetime(2026, 4, 28, 9, 0, tzinfo=UTC).timestamp() * 1000)
    end = int(datetime(2026, 4, 28, 9, 5, tzinfo=UTC).timestamp() * 1000)

    rows = oi.fetch_open_interest_range(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        start_open_ms=start,
        end_open_ms=end,
        market="perp",
    )
    assert len(rows) == 1
    assert rows[0].exchange == "deribit"
    assert rows[0].open_interest == 1000.0
