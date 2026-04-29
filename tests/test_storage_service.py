"""Tests for loader storage orchestration service."""

from __future__ import annotations

from datetime import UTC, datetime

from application.services.storage_service import persist_loader_outputs
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Market, SpotCandle


def _sample_candle() -> SpotCandle:
    return SpotCandle(
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


def _sample_oi() -> OpenInterestPoint:
    return OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="5m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 4, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )


def test_persist_loader_outputs_writes_parquet_and_timescaledb() -> None:
    candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {
        "spot": {"deribit": {"BTCUSDT": [_sample_candle()]}}
    }
    oi: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {
        "perp": {"deribit": {"BTCUSDT": [_sample_oi()]}}
    }
    calls: dict[str, int] = {"spot": 0, "oi": 0, "tsdb": 0}

    def fake_save_spot_lake_fn(**kwargs: object) -> list[str]:
        del kwargs
        calls["spot"] += 1
        return ["spot.parquet"]

    def fake_save_oi_lake_fn(**kwargs: object) -> list[str]:
        del kwargs
        calls["oi"] += 1
        return ["oi.parquet"]

    def fake_save_tsdb_fn(**kwargs: object) -> dict[str, int | str]:
        del kwargs
        calls["tsdb"] += 1
        return {"schema": "market_data", "ohlcv_rows": 1, "open_interest_rows": 1}

    result = persist_loader_outputs(
        candles_for_storage=candles,
        open_interest_for_storage=oi,
        save_parquet_lake=True,
        save_timescaledb=True,
        lake_root="lake/bronze",
        timescaledb_schema="market_data",
        create_schema=True,
        oi_requested=True,
        save_spot_lake_fn=fake_save_spot_lake_fn,
        save_oi_lake_fn=fake_save_oi_lake_fn,
        save_tsdb_fn=fake_save_tsdb_fn,
    )

    assert result["_parquet_files"] == ["spot.parquet", "oi.parquet"]
    assert result["_timescaledb"] == {"schema": "market_data", "ohlcv_rows": 1, "open_interest_rows": 1}
    assert calls == {"spot": 1, "oi": 1, "tsdb": 1}
