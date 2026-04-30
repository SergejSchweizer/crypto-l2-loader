"""Tests for single-instance CLI locking."""

from __future__ import annotations

import fcntl
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from ingestion.l2 import L2MinuteBar
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep CLI tests from writing to the configured runtime log directory."""

    monkeypatch.setenv("L2_SYNC_LOG_DIR", str(tmp_path / "logs"))


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
        exchange="deribit",
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
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert len(calls) == 1
    assert calls[0]["exchange"] == "deribit"
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
        exchange="deribit",
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
            "deribit",
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


def test_l2_parser_defaults_can_come_from_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("L2_INGEST_SYMBOLS", "BTC,ETH")
    monkeypatch.setenv("L2_INGEST_LEVELS", "25")
    monkeypatch.setenv("L2_INGEST_SNAPSHOT_COUNT", "5")
    monkeypatch.setenv("L2_INGEST_POLL_INTERVAL_S", "10")
    monkeypatch.setenv("L2_INGEST_MAX_RUNTIME_S", "55")
    monkeypatch.setenv("L2_INGEST_SAVE_PARQUET_LAKE", "true")
    monkeypatch.setenv("L2_INGEST_LAKE_ROOT", "custom/bronze")
    monkeypatch.setenv("L2_INGEST_NO_JSON_OUTPUT", "true")

    args = cli.build_parser().parse_args(["loader-l2-m1"])

    assert args.symbols == ["BTC", "ETH"]
    assert args.levels == 25
    assert args.snapshot_count == 5
    assert args.poll_interval_s == 10.0
    assert args.max_runtime_s == 55.0
    assert args.save_parquet_lake is True
    assert args.lake_root == "custom/bronze"
    assert args.no_json_output is True


def test_main_loader_saves_timescaledb_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    sample = SpotCandle(
        exchange="deribit",
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
            "deribit",
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
        exchange="deribit",
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
        exchange="deribit",
        symbol="BTCUSDT",
        interval="5m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 4, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )

    cli._write_loader_samples(
        candles_for_storage={"spot": {"deribit": {"BTCUSDT": [candle]}}},
        open_interest_for_storage={"perp": {"deribit": {"BTCUSDT": [oi_point]}}},
        logger=logging.getLogger("test_loader_samples"),
    )

    sample_files = {path.name for path in (tmp_path / "samples").glob("*")}
    assert "spot_deribit_BTCUSDT_5m_sample_10_rows.csv" in sample_files
    assert "spot_deribit_BTCUSDT_5m_sample_10_rows.png" in sample_files
    assert "oi_perp_deribit_BTCUSDT_5m_sample_10_rows.csv" in sample_files
    assert "oi_perp_deribit_BTCUSDT_5m_sample_10_rows.png" in sample_files
    assert not any(path.suffix == ".parquet" for path in (tmp_path / "samples").glob("*"))


def test_export_descriptive_stats_writes_csv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    rows = [
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0},
        {"open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5, "volume": 20.0},
    ]
    monkeypatch.setattr(cli, "load_combined_dataframe_from_lake", lambda **kwargs: pd.DataFrame(rows))
    output_csv = tmp_path / "docs" / "tables" / "descriptive_stats_baseline.csv"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "export-descriptive-stats",
            "--lake-root",
            str(tmp_path / "lake"),
            "--output-csv",
            str(output_csv),
            "--start-time",
            "2026-01-01T00:00:00+00:00",
            "--end-time",
            "2026-01-31T23:59:59+00:00",
            "--no-json-output",
        ],
    )

    cli.main()
    assert output_csv.exists()
    written = pd.read_csv(output_csv)
    assert list(written.columns) == ["Variable", "Mean", "Std", "Min", "Max"]
    assert set(written["Variable"]) == {"open", "high", "low", "close", "volume"}


def test_main_loader_l2_m1_command_saves_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    captured: dict[str, object] = {}
    row = L2MinuteBar(
        minute_ts=datetime(2026, 4, 29, 10, 0, tzinfo=UTC),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        snapshot_count=10,
        mid_open=100.0,
        mid_high=101.0,
        mid_low=99.0,
        mid_close=100.5,
        mark_close=100.4,
        index_close=100.3,
        spread_bps_mean=10.0,
        spread_bps_max=12.0,
        spread_bps_last=11.0,
        bid_depth_1_mean=10.0,
        ask_depth_1_mean=11.0,
        bid_depth_10_mean=100.0,
        ask_depth_10_mean=110.0,
        bid_depth_50_mean=500.0,
        ask_depth_50_mean=510.0,
        imbalance_1_mean=0.1,
        imbalance_10_mean=0.2,
        imbalance_50_mean=0.3,
        imbalance_10_last=0.25,
        imbalance_50_last=0.35,
        microprice_close=100.45,
        microprice_minus_mid_mean=0.01,
        bid_vwap_10_mean=99.5,
        ask_vwap_10_mean=101.5,
        open_interest_last=1000.0,
        funding_8h_last=0.0001,
        current_funding_last=0.00001,
        fetch_duration_s_mean=0.11,
        fetch_duration_s_max=0.22,
        fetch_duration_s_last=0.12,
    )

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli, "fetch_l2_snapshots_for_symbols", lambda **kwargs: {"BTC": [object(), object()]})
    monkeypatch.setattr(cli, "aggregate_snapshots_to_m1", lambda snapshots: [row])
    monkeypatch.setattr(
        cli,
        "save_l2_m1_parquet_lake",
        lambda **kwargs: captured.update(kwargs) or ["/tmp/l2.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "loader-l2-m1",
            "--exchange",
            "deribit",
            "--symbols",
            "BTC",
            "--snapshot-count",
            "2",
            "--save-parquet-lake",
            "--no-json-output",
        ],
    )

    cli.main()
    assert captured["lake_root"] == "lake/bronze"


def test_l2_minute_bar_stats_log_is_expressive(caplog: pytest.LogCaptureFixture) -> None:
    row = L2MinuteBar(
        minute_ts=datetime(2026, 4, 29, 10, 0, tzinfo=UTC),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        snapshot_count=5,
        mid_open=100.0,
        mid_high=101.0,
        mid_low=99.0,
        mid_close=100.5,
        mark_close=100.4,
        index_close=100.3,
        spread_bps_mean=10.0,
        spread_bps_max=12.0,
        spread_bps_last=11.0,
        bid_depth_1_mean=10.0,
        ask_depth_1_mean=11.0,
        bid_depth_10_mean=100.0,
        ask_depth_10_mean=110.0,
        bid_depth_50_mean=500.0,
        ask_depth_50_mean=510.0,
        imbalance_1_mean=0.1,
        imbalance_10_mean=0.2,
        imbalance_50_mean=0.3,
        imbalance_10_last=0.25,
        imbalance_50_last=0.35,
        microprice_close=100.45,
        microprice_minus_mid_mean=0.01,
        bid_vwap_10_mean=99.5,
        ask_vwap_10_mean=101.5,
        open_interest_last=1000.0,
        funding_8h_last=0.0001,
        current_funding_last=0.00001,
        fetch_duration_s_mean=0.11,
        fetch_duration_s_max=0.22,
        fetch_duration_s_last=0.12,
    )
    logger = logging.getLogger("test_l2_stats_log")

    with caplog.at_level(logging.INFO, logger="test_l2_stats_log"):
        cli._log_l2_minute_bar_stats(
            logger=logger,
            row=row,
            collected_snapshots=5,
            requested_snapshots=5,
        )

    message = caplog.messages[0]
    assert "L2 minute stats" in message
    assert "symbol=BTC-PERPETUAL" in message
    assert "snapshots_collected=5" in message
    assert "mid_close=100.50000000" in message
    assert "spread_bps_mean=10.00000000" in message
    assert "open_interest_last=1000.00000000" in message
    assert "fetch_duration_s_mean=0.110000" in message
    assert "fetch_duration_s_max=0.220000" in message
    assert "fetch_duration_s_last=0.120000" in message
