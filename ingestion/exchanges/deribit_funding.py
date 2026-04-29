"""Deribit perpetual funding-rate adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_FUNDING_URL = "https://www.deribit.com/api/v2/public/get_funding_rate_history"
DERIBIT_FUNDING_MAX_POINTS_PER_REQUEST = 500


def fetch_funding_range(
    symbol: str,
    period: str,
    start_open_ms: int,
    end_open_ms: int,
) -> list[dict[str, object]]:
    """Fetch Deribit funding-rate records by inclusive range."""

    if end_open_ms < start_open_ms:
        return []

    period_ms = _period_to_milliseconds(period)
    cursor = start_open_ms
    rows: list[dict[str, object]] = []

    while cursor <= end_open_ms:
        window_end_ms = min(
            end_open_ms,
            cursor + (DERIBIT_FUNDING_MAX_POINTS_PER_REQUEST * period_ms) - 1,
        )
        page = _fetch_funding_page(
            symbol=symbol,
            start_time_ms=cursor,
            end_time_ms=window_end_ms,
        )
        if page:
            rows.extend(
                [item for item in page if start_open_ms <= int(cast(Any, item["timestamp"])) <= end_open_ms]
            )
            last_ts = max(int(cast(Any, item["timestamp"])) for item in page)
            cursor = max(cursor + period_ms, last_ts + period_ms)
        else:
            cursor = window_end_ms + 1

    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_funding_all(symbol: str, period: str) -> list[dict[str, object]]:
    """Fetch funding-rate history over broad bounded horizon."""

    end_ms = int(datetime.now(UTC).timestamp() * 1000)
    start_ms = int(datetime(2019, 1, 1, tzinfo=UTC).timestamp() * 1000)
    return fetch_funding_range(symbol=symbol, period=period, start_open_ms=start_ms, end_open_ms=end_ms)


def parse_funding_row(symbol: str, period: str, row: dict[str, object]) -> dict[str, object]:
    """Convert Deribit funding payload to normalized record fields."""

    open_time_ms = int(cast(Any, row["timestamp"]))
    period_ms = _period_to_milliseconds(period)
    bucket_open_ms = (open_time_ms // period_ms) * period_ms
    open_time = datetime.fromtimestamp(bucket_open_ms / 1000, tz=UTC)
    close_time = datetime.fromtimestamp((bucket_open_ms + period_ms - 1) / 1000, tz=UTC)

    funding_rate = row.get("interest_8h", row.get("interest_1h", 0.0))

    return {
        "symbol": symbol,
        "timeframe": period,
        "open_time": open_time,
        "close_time": close_time,
        "funding_rate": float(cast(Any, funding_rate)),
        "index_price": float(cast(Any, row.get("index_price", 0.0))),
        "mark_price": float(cast(Any, row.get("prev_index_price", 0.0))),
    }


def _period_to_milliseconds(period: str) -> int:
    if period.endswith("m"):
        return int(period[:-1]) * 60_000
    if period.endswith("h"):
        return int(period[:-1]) * 3_600_000
    if period.endswith("d"):
        return int(period[:-1]) * 86_400_000
    raise ValueError(f"Unsupported period '{period}'")


def _fetch_funding_page(symbol: str, start_time_ms: int, end_time_ms: int) -> list[dict[str, object]]:
    """Fetch one funding page from Deribit."""

    params: dict[str, Any] = {
        "instrument_name": symbol,
        "start_timestamp": start_time_ms,
        "end_timestamp": end_time_ms,
    }
    payload = get_json(DERIBIT_FUNDING_URL, params=params)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit funding response format")
    result = payload.get("result")
    if not isinstance(result, list):
        return []

    rows: list[dict[str, object]] = []
    for item in result:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        if ts is None:
            continue
        rows.append(item)
    rows.sort(key=lambda x: int(cast(Any, x["timestamp"])))
    return rows

