"""Deribit perpetual open-interest adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_LAST_SETTLEMENTS_URL = "https://www.deribit.com/api/v2/public/get_last_settlements_by_instrument"
DERIBIT_MAX_POINTS_PER_REQUEST = 1000


def fetch_open_interest_all(symbol: str, period: str) -> list[dict[str, object]]:
    """Fetch all available Deribit historical open-interest points."""

    del period
    continuation: str | None = None
    pages: list[list[dict[str, object]]] = []

    while True:
        page, next_continuation = _fetch_open_interest_page(symbol=symbol, continuation=continuation)
        if not page:
            break
        pages.append(page)
        if next_continuation is None:
            break
        continuation = next_continuation

    rows = [row for page in reversed(pages) for row in page]
    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_open_interest_range(
    symbol: str,
    period: str,
    start_open_ms: int,
    end_open_ms: int,
) -> list[dict[str, object]]:
    """Fetch Deribit historical open-interest records by inclusive range."""

    if end_open_ms < start_open_ms:
        return []

    del period
    continuation: str | None = None
    rows: list[dict[str, object]] = []
    while True:
        page, next_continuation = _fetch_open_interest_page(symbol=symbol, continuation=continuation)
        if not page:
            break
        rows.extend(
            [
                item
                for item in page
                if start_open_ms <= int(cast(Any, item["timestamp"])) <= end_open_ms
            ]
        )
        min_ts = min(int(cast(Any, item["timestamp"])) for item in page)
        if min_ts < start_open_ms or next_continuation is None:
            break
        continuation = next_continuation

    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def parse_open_interest_row(symbol: str, period: str, row: dict[str, object]) -> dict[str, object]:
    """Convert Deribit settlement payload to normalized record fields."""

    open_time_ms = int(cast(Any, row["timestamp"]))
    period_ms = _period_to_milliseconds(period)
    bucket_open_ms = (open_time_ms // period_ms) * period_ms
    open_time = datetime.fromtimestamp(bucket_open_ms / 1000, tz=UTC)
    close_time = datetime.fromtimestamp((bucket_open_ms + period_ms - 1) / 1000, tz=UTC)

    return {
        "symbol": symbol,
        "timeframe": period,
        "open_time": open_time,
        "close_time": close_time,
        "open_interest": float(cast(Any, row["open_interest"])),
        "open_interest_value": 0.0,
    }


def _period_to_milliseconds(period: str) -> int:
    """Convert normalized timeframe to milliseconds."""

    if period.endswith("m"):
        return int(period[:-1]) * 60_000
    if period.endswith("h"):
        return int(period[:-1]) * 3_600_000
    if period.endswith("d"):
        return int(period[:-1]) * 86_400_000
    raise ValueError(f"Unsupported period '{period}'")


def _fetch_open_interest_page(symbol: str, continuation: str | None) -> tuple[list[dict[str, object]], str | None]:
    """Fetch one historical-open-interest page from Deribit settlements."""

    params: dict[str, Any] = {
        "instrument_name": symbol,
        "count": DERIBIT_MAX_POINTS_PER_REQUEST,
    }
    if continuation:
        params["continuation"] = continuation

    payload = get_json(DERIBIT_LAST_SETTLEMENTS_URL, params=params)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit open-interest response format")

    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit open-interest result payload")
    settlements = result.get("settlements")
    if not isinstance(settlements, list):
        return [], None
    next_continuation = result.get("continuation")
    continuation_token = str(next_continuation) if isinstance(next_continuation, str) and next_continuation else None

    rows: list[dict[str, object]] = []
    for item in settlements:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        position = item.get("position")
        if ts is None or position is None:
            continue
        rows.append({"timestamp": int(cast(Any, ts)), "open_interest": float(cast(Any, position))})

    rows.sort(key=lambda x: int(cast(Any, x["timestamp"])))
    return rows, continuation_token
