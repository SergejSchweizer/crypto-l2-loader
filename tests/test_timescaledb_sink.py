"""Integration-style tests for TimescaleDB sink flows."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest

from api import cli
from infra.timescaledb import sink
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


class _FakeCursor:
    """Cursor stub that records executed SQL and payloads."""

    def __init__(self, state: dict[str, object]) -> None:
        self._state = state

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb

    def execute(self, sql: str) -> None:
        statements = self._state.setdefault("execute_sql", [])
        assert isinstance(statements, list)
        statements.append(sql)

    def executemany(self, sql: str, payload: list[dict[str, object]]) -> None:
        statements = self._state.setdefault("executemany_sql", [])
        assert isinstance(statements, list)
        statements.append(sql)
        payloads = self._state.setdefault("payloads", [])
        assert isinstance(payloads, list)
        payloads.append(payload)


class _FakeConnection:
    """Connection stub providing transaction/cursor context managers."""

    def __init__(self, state: dict[str, object]) -> None:
        self._state = state

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._state)

    def transaction(self) -> _FakeConnection:
        return self


def _install_fake_psycopg(monkeypatch: pytest.MonkeyPatch, state: dict[str, object]) -> None:
    """Inject fake psycopg modules so sink functions run without a real DB."""

    psycopg_mod = ModuleType("psycopg")
    psycopg_mod.connect = lambda **kwargs: _FakeConnection(state)  # type: ignore[attr-defined]
    json_mod = ModuleType("psycopg.types.json")
    json_mod.Json = lambda value: value  # type: ignore[attr-defined]
    types_mod = ModuleType("psycopg.types")
    types_mod.json = json_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "psycopg", psycopg_mod)
    monkeypatch.setitem(sys.modules, "psycopg.types", types_mod)
    monkeypatch.setitem(sys.modules, "psycopg.types.json", json_mod)


def _sample_candle() -> SpotCandle:
    return SpotCandle(
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


def _sample_oi() -> OpenInterestPoint:
    return OpenInterestPoint(
        exchange="binance",
        symbol="BTCUSDT",
        interval="5m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 4, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )


def test_save_market_data_to_timescaledb_rejects_invalid_schema_name() -> None:
    with pytest.raises(ValueError, match="Invalid SQL schema"):
        sink.save_market_data_to_timescaledb(
            candles_for_storage={},
            open_interest_for_storage={},
            schema='market_data;DROP SCHEMA public;--',
            create_schema=False,
        )


def test_save_market_data_to_timescaledb_uses_timezone_aware_ingested_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, object] = {}
    _install_fake_psycopg(monkeypatch, state)

    result = sink.save_market_data_to_timescaledb(
        candles_for_storage={"spot": {"binance": {"BTCUSDT": [_sample_candle()]}}},
        open_interest_for_storage={"perp": {"binance": {"BTCUSDT": [_sample_oi()]}}},
        schema="market_data",
        create_schema=True,
    )

    assert result["schema"] == "market_data"
    payloads = state.get("payloads")
    assert isinstance(payloads, list)
    flattened = [row for batch in payloads for row in batch]
    assert flattened
    assert all(isinstance(row.get("ingested_at"), datetime) for row in flattened)
    assert all(cast(datetime, row["ingested_at"]).tzinfo == UTC for row in flattened)


def test_save_market_data_to_timescaledb_serializes_datetime_in_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, object] = {}
    _install_fake_psycopg(monkeypatch, state)

    sink.save_market_data_to_timescaledb(
        candles_for_storage={"spot": {"binance": {"BTCUSDT": [_sample_candle()]}}},
        open_interest_for_storage={},
        schema="market_data",
        create_schema=False,
    )

    payloads = state.get("payloads")
    assert isinstance(payloads, list)
    ohlcv_payload = cast(list[dict[str, object]], payloads[0])[0]
    extra = cast(dict[str, object], ohlcv_payload["extra"])
    assert isinstance(extra.get("open_time"), str)
    assert isinstance(extra.get("close_time"), str)


def test_loader_parquet_then_ingest_timescaledb_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "open_times_in_lake_by_dataset", lambda **kwargs: [])
    monkeypatch.setattr(cli, "fetch_candles_all_history", lambda **kwargs: [_sample_candle()])
    monkeypatch.setattr(cli, "fetch_open_interest_all_history", lambda **kwargs: [_sample_oi()])
    monkeypatch.setattr(cli, "_write_loader_samples", lambda **kwargs: None)

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "loader",
            "--exchange",
            "binance",
            "--market",
            "spot",
            "oi",
            "--symbols",
            "BTCUSDT",
                "--timeframe",
                "5m",
            "--save-parquet-lake",
            "--lake-root",
            str(tmp_path),
            "--no-json-output",
        ],
    )
    cli.main()

    state: dict[str, object] = {}
    _install_fake_psycopg(monkeypatch, state)
    summary = sink.save_parquet_lake_to_timescaledb(
        lake_root=str(tmp_path),
        schema="market_data",
        create_schema=True,
    )

    assert summary["ohlcv_rows"] == 1
    assert summary["open_interest_rows"] == 1
    executed = state.get("executemany_sql")
    assert isinstance(executed, list)
    assert any("market_data.ohlcv" in sql for sql in executed)
    assert any("market_data.open_interest" in sql for sql in executed)
