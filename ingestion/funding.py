"""Funding-rate ingestion interface (Deribit-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from ingestion.exchanges import deribit_funding
from ingestion.spot import Exchange, Market, interval_to_milliseconds, normalize_storage_symbol, normalize_timeframe


@dataclass(frozen=True)
class FundingPoint:
    """Funding-rate datapoint for one instrument interval.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.funding import FundingPoint

        point = FundingPoint(
            exchange="deribit",
            symbol="ETH-PERPETUAL",
            interval="1h",
            open_time=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            close_time=datetime(2026, 1, 1, 0, 59, 59, 999000, tzinfo=UTC),
            funding_rate=0.0001,
            index_price=3200.0,
            mark_price=3205.0,
        )
        ```
    """

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    funding_rate: float
    index_price: float
    mark_price: float


def normalize_funding_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize funding timeframe by exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return normalize_timeframe(exchange=exchange, value=value)


def funding_interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert funding interval to milliseconds."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return interval_to_milliseconds(exchange=exchange, interval=interval)


def fetch_funding_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str,
    market: Market,
) -> list[FundingPoint]:
    """Fetch all available funding-rate history."""

    if market != "perp":
        return []
    if exchange != "deribit":
        return []
    normalized_interval = normalize_funding_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    rows = deribit_funding.fetch_funding_all(symbol=normalized_symbol, period=normalized_interval)
    parsed = [deribit_funding.parse_funding_row(normalized_symbol, normalized_interval, row) for row in rows]
    return [
        FundingPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            funding_rate=float(cast(Any, item["funding_rate"])),
            index_price=float(cast(Any, item["index_price"])),
            mark_price=float(cast(Any, item["mark_price"])),
        )
        for item in parsed
    ]


def fetch_funding_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market,
) -> list[FundingPoint]:
    """Fetch funding-rate rows by inclusive open-time range."""

    if market != "perp":
        return []
    if exchange != "deribit":
        return []
    normalized_interval = normalize_funding_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    rows = deribit_funding.fetch_funding_range(
        symbol=normalized_symbol,
        period=normalized_interval,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )
    parsed = [deribit_funding.parse_funding_row(normalized_symbol, normalized_interval, row) for row in rows]
    return [
        FundingPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            funding_rate=float(cast(Any, item["funding_rate"])),
            index_price=float(cast(Any, item["index_price"])),
            mark_price=float(cast(Any, item["mark_price"])),
        )
        for item in parsed
    ]
