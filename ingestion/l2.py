"""L2 snapshot ingestion and M1 aggregation utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import sleep
from typing import Any, TypedDict, cast

from ingestion.exchanges.deribit_l2 import fetch_order_book_snapshot


@dataclass(frozen=True)
class L2Snapshot:
    """One normalized L2 snapshot.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.l2 import L2Snapshot

        snapshot = L2Snapshot(
            exchange="deribit",
            symbol="BTC-PERPETUAL",
            timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            bids=[(100.0, 10.0)],
            asks=[(100.1, 9.0)],
            mark_price=100.05,
            index_price=100.0,
            open_interest=1000.0,
            funding_8h=0.0001,
            current_funding=0.00001,
        )
        ```
    """

    exchange: str
    symbol: str
    timestamp: datetime
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    mark_price: float | None
    index_price: float | None
    open_interest: float | None
    funding_8h: float | None
    current_funding: float | None


@dataclass(frozen=True)
class L2MinuteBar:
    """One aggregated M1 feature row."""

    minute_ts: datetime
    exchange: str
    symbol: str
    snapshot_count: int
    mid_open: float
    mid_high: float
    mid_low: float
    mid_close: float
    mark_close: float | None
    index_close: float | None
    spread_bps_mean: float
    spread_bps_max: float
    spread_bps_last: float
    bid_depth_1_mean: float
    ask_depth_1_mean: float
    bid_depth_10_mean: float
    ask_depth_10_mean: float
    bid_depth_50_mean: float
    ask_depth_50_mean: float
    imbalance_1_mean: float | None
    imbalance_10_mean: float | None
    imbalance_50_mean: float | None
    imbalance_10_last: float | None
    imbalance_50_last: float | None
    microprice_close: float | None
    microprice_minus_mid_mean: float | None
    bid_vwap_10_mean: float | None
    ask_vwap_10_mean: float | None
    open_interest_last: float | None
    funding_8h_last: float | None
    current_funding_last: float | None


class SnapshotFeatures(TypedDict):
    """Typed snapshot-level feature container."""

    mid: float
    spread_bps: float
    bid_depth_1: float
    ask_depth_1: float
    bid_depth_10: float
    ask_depth_10: float
    bid_depth_50: float
    ask_depth_50: float
    imbalance_1: float | None
    imbalance_10: float | None
    imbalance_50: float | None
    microprice: float | None
    microprice_minus_mid: float | None
    bid_vwap_10: float | None
    ask_vwap_10: float | None


def fetch_l2_snapshots(
    exchange: str,
    symbol: str,
    depth: int,
    snapshot_count: int,
    poll_interval_s: float,
) -> list[L2Snapshot]:
    """Collect a finite sequence of L2 snapshots for one symbol."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    if snapshot_count <= 0:
        raise ValueError("snapshot_count must be positive")
    if poll_interval_s < 0:
        raise ValueError("poll_interval_s must be >= 0")

    snapshots: list[L2Snapshot] = []
    for index in range(snapshot_count):
        raw = fetch_order_book_snapshot(symbol=symbol, depth=depth)
        snapshots.append(_snapshot_from_raw(raw))
        if index < snapshot_count - 1 and poll_interval_s > 0:
            sleep(poll_interval_s)
    return snapshots


def aggregate_snapshots_to_m1(snapshots: list[L2Snapshot]) -> list[L2MinuteBar]:
    """Aggregate snapshots into M1 rows with unweighted means."""

    grouped: dict[tuple[str, str, datetime], list[L2Snapshot]] = {}
    for snapshot in snapshots:
        if not snapshot.bids or not snapshot.asks:
            continue
        best_bid = snapshot.bids[0][0]
        best_ask = snapshot.asks[0][0]
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            continue
        minute_ts = snapshot.timestamp.replace(second=0, microsecond=0)
        key = (snapshot.exchange, snapshot.symbol, minute_ts)
        grouped.setdefault(key, []).append(snapshot)

    rows: list[L2MinuteBar] = []
    for (exchange, symbol, minute_ts), minute_snaps in sorted(grouped.items(), key=lambda item: item[0][2]):
        minute_snaps.sort(key=lambda item: item.timestamp)
        stats: list[SnapshotFeatures] = [_snapshot_features(item) for item in minute_snaps]

        mids = [item["mid"] for item in stats]
        spreads = [item["spread_bps"] for item in stats]
        bid_depth_1 = [item["bid_depth_1"] for item in stats]
        ask_depth_1 = [item["ask_depth_1"] for item in stats]
        bid_depth_10 = [item["bid_depth_10"] for item in stats]
        ask_depth_10 = [item["ask_depth_10"] for item in stats]
        bid_depth_50 = [item["bid_depth_50"] for item in stats]
        ask_depth_50 = [item["ask_depth_50"] for item in stats]

        imbalance_1 = [item["imbalance_1"] for item in stats if item["imbalance_1"] is not None]
        imbalance_10 = [item["imbalance_10"] for item in stats if item["imbalance_10"] is not None]
        imbalance_50 = [item["imbalance_50"] for item in stats if item["imbalance_50"] is not None]
        micro_minus_mid = [item["microprice_minus_mid"] for item in stats if item["microprice_minus_mid"] is not None]
        bid_vwap_10 = [item["bid_vwap_10"] for item in stats if item["bid_vwap_10"] is not None]
        ask_vwap_10 = [item["ask_vwap_10"] for item in stats if item["ask_vwap_10"] is not None]

        rows.append(
            L2MinuteBar(
                minute_ts=minute_ts,
                exchange=exchange,
                symbol=symbol,
                snapshot_count=len(stats),
                mid_open=mids[0],
                mid_high=max(mids),
                mid_low=min(mids),
                mid_close=mids[-1],
                mark_close=minute_snaps[-1].mark_price,
                index_close=minute_snaps[-1].index_price,
                spread_bps_mean=sum(spreads) / len(spreads),
                spread_bps_max=max(spreads),
                spread_bps_last=spreads[-1],
                bid_depth_1_mean=sum(bid_depth_1) / len(bid_depth_1),
                ask_depth_1_mean=sum(ask_depth_1) / len(ask_depth_1),
                bid_depth_10_mean=sum(bid_depth_10) / len(bid_depth_10),
                ask_depth_10_mean=sum(ask_depth_10) / len(ask_depth_10),
                bid_depth_50_mean=sum(bid_depth_50) / len(bid_depth_50),
                ask_depth_50_mean=sum(ask_depth_50) / len(ask_depth_50),
                imbalance_1_mean=_mean_or_none(imbalance_1),
                imbalance_10_mean=_mean_or_none(imbalance_10),
                imbalance_50_mean=_mean_or_none(imbalance_50),
                imbalance_10_last=stats[-1]["imbalance_10"],
                imbalance_50_last=stats[-1]["imbalance_50"],
                microprice_close=stats[-1]["microprice"],
                microprice_minus_mid_mean=_mean_or_none(micro_minus_mid),
                bid_vwap_10_mean=_mean_or_none(bid_vwap_10),
                ask_vwap_10_mean=_mean_or_none(ask_vwap_10),
                open_interest_last=minute_snaps[-1].open_interest,
                funding_8h_last=minute_snaps[-1].funding_8h,
                current_funding_last=minute_snaps[-1].current_funding,
            )
        )

    return rows


def _snapshot_from_raw(raw: dict[str, object]) -> L2Snapshot:
    """Convert normalized adapter payload into ``L2Snapshot``."""

    timestamp_ms = int(cast(int | float, raw["timestamp_ms"]))
    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    bids = [(float(price), float(amount)) for price, amount in cast(list[tuple[float, float]], raw["bids"])]
    asks = [(float(price), float(amount)) for price, amount in cast(list[tuple[float, float]], raw["asks"])]

    return L2Snapshot(
        exchange=str(raw["exchange"]),
        symbol=str(raw["symbol"]),
        timestamp=timestamp,
        bids=bids,
        asks=asks,
        mark_price=_optional_float(raw.get("mark_price")),
        index_price=_optional_float(raw.get("index_price")),
        open_interest=_optional_float(raw.get("open_interest")),
        funding_8h=_optional_float(raw.get("funding_8h")),
        current_funding=_optional_float(raw.get("current_funding")),
    )


def _snapshot_features(snapshot: L2Snapshot) -> SnapshotFeatures:
    """Compute snapshot-level microstructure features."""

    best_bid, bid_amount_1 = snapshot.bids[0]
    best_ask, ask_amount_1 = snapshot.asks[0]
    mid = (best_bid + best_ask) / 2.0
    spread_bps = ((best_ask - best_bid) / mid) * 10_000.0

    bid_depth_1 = _depth(snapshot.bids, 1)
    ask_depth_1 = _depth(snapshot.asks, 1)
    bid_depth_10 = _depth(snapshot.bids, 10)
    ask_depth_10 = _depth(snapshot.asks, 10)
    bid_depth_50 = _depth(snapshot.bids, 50)
    ask_depth_50 = _depth(snapshot.asks, 50)

    microprice = _microprice(best_bid, bid_amount_1, best_ask, ask_amount_1)

    return {
        "mid": mid,
        "spread_bps": spread_bps,
        "bid_depth_1": bid_depth_1,
        "ask_depth_1": ask_depth_1,
        "bid_depth_10": bid_depth_10,
        "ask_depth_10": ask_depth_10,
        "bid_depth_50": bid_depth_50,
        "ask_depth_50": ask_depth_50,
        "imbalance_1": _imbalance(bid_depth_1, ask_depth_1),
        "imbalance_10": _imbalance(bid_depth_10, ask_depth_10),
        "imbalance_50": _imbalance(bid_depth_50, ask_depth_50),
        "microprice": microprice,
        "microprice_minus_mid": None if microprice is None else microprice - mid,
        "bid_vwap_10": _vwap(snapshot.bids, 10),
        "ask_vwap_10": _vwap(snapshot.asks, 10),
    }


def _depth(levels: list[tuple[float, float]], limit: int) -> float:
    """Return cumulative amount up to ``limit`` levels."""

    return sum(amount for _, amount in levels[:limit])


def _imbalance(bid_depth: float, ask_depth: float) -> float | None:
    """Return side imbalance or ``None`` when denominator is zero."""

    denominator = bid_depth + ask_depth
    if denominator == 0:
        return None
    return (bid_depth - ask_depth) / denominator


def _microprice(best_bid: float, bid_amount_1: float, best_ask: float, ask_amount_1: float) -> float | None:
    """Return L1 microprice."""

    denominator = bid_amount_1 + ask_amount_1
    if denominator == 0:
        return None
    return ((best_ask * bid_amount_1) + (best_bid * ask_amount_1)) / denominator


def _vwap(levels: list[tuple[float, float]], limit: int) -> float | None:
    """Return side VWAP up to ``limit`` levels."""

    subset = levels[:limit]
    denominator = sum(amount for _, amount in subset)
    if denominator == 0:
        return None
    numerator = sum(price * amount for price, amount in subset)
    return numerator / denominator


def _mean_or_none(values: list[float]) -> float | None:
    """Return mean of values or ``None`` for empty lists."""

    if not values:
        return None
    return sum(values) / len(values)


def l2_m1_record(row: L2MinuteBar, run_id: str, ingested_at: datetime) -> dict[str, object]:
    """Convert ``L2MinuteBar`` to parquet row format."""

    return {
        "schema_version": "v1",
        "dataset_type": "l2_m1",
        "exchange": row.exchange,
        "symbol": row.symbol,
        "instrument_type": "perp",
        "event_time": row.minute_ts,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": "public_l2_orderbook",
        "open_time": row.minute_ts,
        "close_time": row.minute_ts.replace(second=59, microsecond=999000),
        "timeframe": "1m",
        "snapshot_count": row.snapshot_count,
        "mid_open": row.mid_open,
        "mid_high": row.mid_high,
        "mid_low": row.mid_low,
        "mid_close": row.mid_close,
        "mark_close": row.mark_close,
        "index_close": row.index_close,
        "spread_bps_mean": row.spread_bps_mean,
        "spread_bps_max": row.spread_bps_max,
        "spread_bps_last": row.spread_bps_last,
        "bid_depth_1_mean": row.bid_depth_1_mean,
        "ask_depth_1_mean": row.ask_depth_1_mean,
        "bid_depth_10_mean": row.bid_depth_10_mean,
        "ask_depth_10_mean": row.ask_depth_10_mean,
        "bid_depth_50_mean": row.bid_depth_50_mean,
        "ask_depth_50_mean": row.ask_depth_50_mean,
        "imbalance_1_mean": row.imbalance_1_mean,
        "imbalance_10_mean": row.imbalance_10_mean,
        "imbalance_50_mean": row.imbalance_50_mean,
        "imbalance_10_last": row.imbalance_10_last,
        "imbalance_50_last": row.imbalance_50_last,
        "microprice_close": row.microprice_close,
        "microprice_minus_mid_mean": row.microprice_minus_mid_mean,
        "bid_vwap_10_mean": row.bid_vwap_10_mean,
        "ask_vwap_10_mean": row.ask_vwap_10_mean,
        "open_interest_last": row.open_interest_last,
        "funding_8h_last": row.funding_8h_last,
        "current_funding_last": row.current_funding_last,
    }


def l2_m1_partition_key(row: L2MinuteBar) -> tuple[str, str, str, str, str]:
    """Build partition key for L2 M1 rows."""

    return (
        row.exchange,
        "perp",
        row.symbol,
        "1m",
        row.minute_ts.strftime("%Y-%m"),
    )


def load_l2_m1_from_rows(rows: list[dict[str, Any]]) -> list[L2MinuteBar]:
    """Rebuild ``L2MinuteBar`` rows from generic dictionaries."""

    output: list[L2MinuteBar] = []
    for item in rows:
        minute_ts = item.get("open_time")
        if not isinstance(minute_ts, datetime):
            continue
        output.append(
            L2MinuteBar(
                minute_ts=minute_ts,
                exchange=str(item.get("exchange", "")),
                symbol=str(item.get("symbol", "")),
                snapshot_count=int(item.get("snapshot_count", 0)),
                mid_open=float(item.get("mid_open", 0.0)),
                mid_high=float(item.get("mid_high", 0.0)),
                mid_low=float(item.get("mid_low", 0.0)),
                mid_close=float(item.get("mid_close", 0.0)),
                mark_close=_optional_float(item.get("mark_close")),
                index_close=_optional_float(item.get("index_close")),
                spread_bps_mean=float(item.get("spread_bps_mean", 0.0)),
                spread_bps_max=float(item.get("spread_bps_max", 0.0)),
                spread_bps_last=float(item.get("spread_bps_last", 0.0)),
                bid_depth_1_mean=float(item.get("bid_depth_1_mean", 0.0)),
                ask_depth_1_mean=float(item.get("ask_depth_1_mean", 0.0)),
                bid_depth_10_mean=float(item.get("bid_depth_10_mean", 0.0)),
                ask_depth_10_mean=float(item.get("ask_depth_10_mean", 0.0)),
                bid_depth_50_mean=float(item.get("bid_depth_50_mean", 0.0)),
                ask_depth_50_mean=float(item.get("ask_depth_50_mean", 0.0)),
                imbalance_1_mean=_optional_float(item.get("imbalance_1_mean")),
                imbalance_10_mean=_optional_float(item.get("imbalance_10_mean")),
                imbalance_50_mean=_optional_float(item.get("imbalance_50_mean")),
                imbalance_10_last=_optional_float(item.get("imbalance_10_last")),
                imbalance_50_last=_optional_float(item.get("imbalance_50_last")),
                microprice_close=_optional_float(item.get("microprice_close")),
                microprice_minus_mid_mean=_optional_float(item.get("microprice_minus_mid_mean")),
                bid_vwap_10_mean=_optional_float(item.get("bid_vwap_10_mean")),
                ask_vwap_10_mean=_optional_float(item.get("ask_vwap_10_mean")),
                open_interest_last=_optional_float(item.get("open_interest_last")),
                funding_8h_last=_optional_float(item.get("funding_8h_last")),
                current_funding_last=_optional_float(item.get("current_funding_last")),
            )
        )
    return sorted(output, key=lambda item: (item.minute_ts, item.exchange, item.symbol))


def _optional_float(value: object) -> float | None:
    """Convert optional numeric values to floats."""

    if value is None:
        return None
    return float(cast(int | float, value))
