"""Spot/perpetual candle ingestion interface (Deribit-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from ingestion.exchanges import deribit

Exchange = Literal["deribit"]
Market = Literal["spot", "perp"]


@dataclass(frozen=True)
class SpotCandle:
    """OHLCV candle for an instrument.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.spot import SpotCandle

        candle = SpotCandle(
            exchange="deribit",
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            close_time=datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=UTC),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            volume=12.0,
            quote_volume=1200.0,
            trade_count=34,
        )
        ```
    """

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    trade_count: int


def _ms_to_utc(ts_ms: int) -> datetime:
    """Convert epoch milliseconds to timezone-aware UTC datetime."""

    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC)


def parse_kline(exchange: Exchange, symbol: str, interval: str, row: list[Any]) -> SpotCandle:
    """Parse a common kline row into a typed candle object."""

    return SpotCandle(
        exchange=exchange,
        symbol=symbol,
        interval=interval,
        open_time=_ms_to_utc(int(row[0])),
        close_time=_ms_to_utc(int(row[6])),
        open_price=float(row[1]),
        high_price=float(row[2]),
        low_price=float(row[3]),
        close_price=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
    )


def list_supported_intervals(exchange: Exchange) -> tuple[str, ...]:
    """List supported intervals for the requested exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.list_supported_intervals()


def normalize_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize timeframe aliases per exchange format."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.normalize_timeframe(value)


def max_candles_per_request(exchange: Exchange) -> int:
    """Return max single-request candle count for exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.max_limit()


def interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert normalized interval into milliseconds for exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.interval_to_milliseconds(interval)


def normalize_storage_symbol(exchange: Exchange, symbol: str, market: Market) -> str:
    """Normalize symbol to storage form for selected exchange/market."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.normalize_symbol(symbol=symbol, market=market)


def fetch_candles(
    exchange: Exchange,
    symbol: str,
    interval: str = "1h",
    limit: int = 100,
    market: Market = "spot",
) -> list[SpotCandle]:
    """Fetch latest candles from supported exchanges."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    rows = deribit.fetch_klines(
        symbol=symbol,
        market=market,
        interval=normalized_interval,
        limit=limit,
    )

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row) for row in rows
    ]


def fetch_candles_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str = "1h",
    market: Market = "spot",
) -> list[SpotCandle]:
    """Fetch all available candles from exchange history."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    rows = deribit.fetch_klines_all(
        symbol=symbol,
        market=market,
        interval=normalized_interval,
    )

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row) for row in rows
    ]


def fetch_candles_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market = "spot",
) -> list[SpotCandle]:
    """Fetch candles by open-time range inclusive."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    rows = deribit.fetch_klines_range(
        symbol=symbol,
        market=market,
        interval=normalized_interval,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row) for row in rows
    ]
