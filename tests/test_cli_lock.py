"""Tests for single-instance CLI locking."""

from __future__ import annotations

import fcntl
from datetime import datetime, timezone
from pathlib import Path

import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from ingestion.spot import SpotCandle



def test_single_instance_lock_creates_lock_file(tmp_path: Path) -> None:
    lock_file = tmp_path / "test.lock"

    with SingleInstanceLock(str(lock_file)):
        assert lock_file.exists()
        content = lock_file.read_text().strip()
        assert content.isdigit()



def test_single_instance_lock_raises_on_contention(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lock_file = tmp_path / "test.lock"

    def fake_flock(fd: int, operation: int) -> None:
        del fd, operation
        raise BlockingIOError("locked")

    monkeypatch.setattr(fcntl, "flock", fake_flock)

    with pytest.raises(SingleInstanceError):
        with SingleInstanceLock(str(lock_file)):
            pass


def test_gap_fill_bootstraps_when_no_lake_data(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    sample = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=timezone.utc),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "max_candles_per_request", lambda exchange: 1000)

    def fake_fetch_candles(**kwargs: object) -> list[SpotCandle]:
        called.update(kwargs)
        return [sample]

    monkeypatch.setattr(cli, "fetch_candles", fake_fetch_candles)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        limit=None,
        all_history=False,
        mode="gap-fill",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert called["limit"] == 1000


def test_gap_fill_fetches_internal_and_tail_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    interval_ms = 60_000
    open_times = [
        datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 27, 10, 2, tzinfo=timezone.utc),
        datetime(2026, 4, 27, 10, 3, tzinfo=timezone.utc),
    ]
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=timezone.utc).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: open_times)
    monkeypatch.setattr(cli, "normalize_storage_symbol", lambda **kwargs: "BTCUSDT")
    monkeypatch.setattr(cli, "interval_to_milliseconds", lambda **kwargs: interval_ms)
    monkeypatch.setattr(cli, "_last_closed_open_ms", lambda **kwargs: end_open_ms)

    def fake_fetch_candles_range(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(kwargs["start_open_ms"]), int(kwargs["end_open_ms"])))
        return []

    monkeypatch.setattr(cli, "fetch_candles_range", fake_fetch_candles_range)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        limit=None,
        all_history=False,
        mode="gap-fill",
        lake_root="lake/bronze",
    )

    gap_one_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=timezone.utc).timestamp() * 1000)
    gap_tail_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=timezone.utc).timestamp() * 1000)
    assert candles == []
    assert calls == [(gap_one_ms, gap_one_ms), (gap_tail_start_ms, end_open_ms)]


def test_all_history_mode_uses_all_history_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    sample = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=timezone.utc),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    calls: list[dict[str, object]] = []

    def fake_fetch_candles_all_history(**kwargs: object) -> list[SpotCandle]:
        calls.append(kwargs)
        return [sample]

    monkeypatch.setattr(cli, "fetch_candles_all_history", fake_fetch_candles_all_history)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        limit=None,
        all_history=True,
        mode="gap-fill",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert len(calls) == 1
    assert calls[0]["exchange"] == "binance"
    assert calls[0]["symbol"] == "BTCUSDT"
