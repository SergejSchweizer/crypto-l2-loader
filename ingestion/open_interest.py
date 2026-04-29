"""Open-interest ingestion interface (Deribit-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from ingestion.exchanges import deribit_open_interest
from ingestion.spot import Exchange, Market, interval_to_milliseconds, normalize_storage_symbol, normalize_timeframe


@dataclass(frozen=True)
class OpenInterestPoint:
    """Open-interest datapoint for one instrument interval."""

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_interest: float
    open_interest_value: float


def normalize_open_interest_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize open-interest timeframe by exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    # Deribit adapter currently supports snapshot collection bucketed by requested timeframe.
    return normalize_timeframe(exchange=exchange, value=value)


def open_interest_interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert open-interest interval to milliseconds."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return interval_to_milliseconds(exchange=exchange, interval=interval)


def fetch_open_interest_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str,
    market: Market,
) -> list[OpenInterestPoint]:
    """Fetch all available open-interest history."""

    if market != "perp":
        return []
    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    parsed: list[dict[str, object]] = []
    if exchange != "deribit":
        return []
    rows = deribit_open_interest.fetch_open_interest_all(
        symbol=normalized_symbol,
        period=normalized_interval,
    )
    parsed = [
        deribit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
        for row in rows
    ]
    return [
        OpenInterestPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            open_interest=float(cast(Any, item["open_interest"])),
            open_interest_value=float(cast(Any, item["open_interest_value"])),
        )
        for item in parsed
    ]


def fetch_open_interest_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market,
) -> list[OpenInterestPoint]:
    """Fetch open-interest by inclusive open-time range."""

    if market != "perp":
        return []
    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    parsed: list[dict[str, object]] = []
    if exchange != "deribit":
        return []
    rows = deribit_open_interest.fetch_open_interest_range(
        symbol=normalized_symbol,
        period=normalized_interval,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )
    parsed = [
        deribit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
        for row in rows
    ]
    return [
        OpenInterestPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            open_interest=float(cast(Any, item["open_interest"])),
            open_interest_value=float(cast(Any, item["open_interest_value"])),
        )
        for item in parsed
    ]
