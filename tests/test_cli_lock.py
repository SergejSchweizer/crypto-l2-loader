"""Tests for single-instance CLI locking."""

from __future__ import annotations

import fcntl
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from ingestion.open_interest import OpenInterestPoint
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


def test_auto_mode_fetches_all_history_when_no_lake_data(monkeypatch: pytest.MonkeyPatch) -> None:
    sample = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
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
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert len(calls) == 1
    assert calls[0]["exchange"] == "binance"
    assert calls[0]["symbol"] == "BTCUSDT"


def test_gap_fill_fetches_internal_and_tail_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    interval_ms = 60_000
    open_times = [
        datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 2, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 3, tzinfo=UTC),
    ]
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: open_times)
    monkeypatch.setattr(cli, "normalize_storage_symbol", lambda **kwargs: "BTCUSDT")
    monkeypatch.setattr(cli, "interval_to_milliseconds", lambda **kwargs: interval_ms)
    monkeypatch.setattr(cli, "_last_closed_open_ms", lambda **kwargs: end_open_ms)

    def fake_fetch_candles_range(**kwargs: object) -> list[SpotCandle]:
        start_open_ms = cast(int, kwargs["start_open_ms"])
        end_open_ms = cast(int, kwargs["end_open_ms"])
        calls.append((start_open_ms, end_open_ms))
        return []

    monkeypatch.setattr(cli, "fetch_candles_range", fake_fetch_candles_range)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    gap_one_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    gap_tail_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert candles == []
    assert calls == [(gap_one_ms, gap_one_ms), (gap_tail_start_ms, end_open_ms)]


def test_main_loader_command_still_uses_single_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Locked:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise SingleInstanceError("loader already running")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", Locked)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "loader",
            "--exchange",
            "binance",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--no-json-output",
        ],
    )

    with pytest.raises(SystemExit, match="loader already running"):
        cli.main()


def test_main_loader_saves_timescaledb_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    sample = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "fetch_candles_all_history", lambda **kwargs: [sample])
    monkeypatch.setattr(cli, "_write_loader_samples", lambda **kwargs: None)
    monkeypatch.setattr(
        cli,
        "save_market_data_to_timescaledb",
        lambda **kwargs: captured.update(kwargs) or {"schema": "market_data", "ohlcv_rows": 1, "open_interest_rows": 0},
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "loader",
            "--exchange",
            "binance",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--save-timescaledb",
            "--timescaledb-schema",
            "crypto_market",
            "--timescaledb-no-bootstrap",
            "--no-json-output",
        ],
    )

    cli.main()
    assert captured["schema"] == "crypto_market"
    assert captured["create_schema"] is False


def test_write_loader_samples_writes_grouped_csv_and_matching_plot_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    candle = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="5m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 4, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    oi_point = OpenInterestPoint(
        exchange="binance",
        symbol="BTCUSDT",
        interval="5m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 4, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )

    cli._write_loader_samples(
        candles_for_storage={"spot": {"binance": {"BTCUSDT": [candle]}}},
        open_interest_for_storage={"perp": {"binance": {"BTCUSDT": [oi_point]}}},
        logger=logging.getLogger("test_loader_samples"),
    )

    sample_files = {path.name for path in (tmp_path / "samples").glob("*")}
    assert "spot_binance_BTCUSDT_5m_sample_10_rows.csv" in sample_files
    assert "spot_binance_BTCUSDT_5m_sample_10_rows.png" in sample_files
    assert "oi_perp_binance_BTCUSDT_5m_sample_10_rows.csv" in sample_files
    assert "oi_perp_binance_BTCUSDT_5m_sample_10_rows.png" in sample_files
    assert not any(path.suffix == ".parquet" for path in (tmp_path / "samples").glob("*"))
