"""Tests for L2 snapshot aggregation to M1 features."""

from __future__ import annotations

from datetime import UTC, datetime
from threading import Event, Lock

import pytest

from ingestion import l2
from ingestion.l2 import L2Snapshot, aggregate_snapshots_to_m1, fetch_l2_snapshots_for_symbols


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _depth(levels: list[tuple[float, float]], limit: int) -> float:
    return sum(amount for _, amount in levels[:limit])


def _imbalance(bid_depth: float, ask_depth: float) -> float:
    return (bid_depth - ask_depth) / (bid_depth + ask_depth)


def _microprice(best_bid: float, bid_amount: float, best_ask: float, ask_amount: float) -> float:
    return ((best_ask * bid_amount) + (best_bid * ask_amount)) / (bid_amount + ask_amount)


def _vwap(levels: list[tuple[float, float]], limit: int) -> float:
    subset = levels[:limit]
    return sum(price * amount for price, amount in subset) / sum(amount for _, amount in subset)


def _snapshot_feature_expectations(snapshot: L2Snapshot) -> dict[str, float]:
    best_bid, bid_amount = snapshot.bids[0]
    best_ask, ask_amount = snapshot.asks[0]
    mid = (best_bid + best_ask) / 2.0
    spread_bps = ((best_ask - best_bid) / mid) * 10_000
    bid_depth_1 = _depth(snapshot.bids, 1)
    ask_depth_1 = _depth(snapshot.asks, 1)
    bid_depth_10 = _depth(snapshot.bids, 10)
    ask_depth_10 = _depth(snapshot.asks, 10)
    bid_depth_50 = _depth(snapshot.bids, 50)
    ask_depth_50 = _depth(snapshot.asks, 50)
    microprice = _microprice(best_bid, bid_amount, best_ask, ask_amount)
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
        "microprice_minus_mid": microprice - mid,
        "bid_vwap_10": _vwap(snapshot.bids, 10),
        "ask_vwap_10": _vwap(snapshot.asks, 10),
    }


def _snapshot(ts_second: int, best_bid: float, best_ask: float, bid_1: float, ask_1: float) -> L2Snapshot:
    base = datetime(2026, 4, 29, 10, 0, ts_second, tzinfo=UTC)
    bids = [(best_bid, bid_1)] + [(best_bid - i * 0.1, 2.0 + i) for i in range(1, 50)]
    asks = [(best_ask, ask_1)] + [(best_ask + i * 0.1, 1.5 + i) for i in range(1, 50)]
    return L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=base,
        fetch_duration_s=ts_second / 100.0,
        bids=bids,
        asks=asks,
        mark_price=best_ask,
        index_price=best_bid,
        open_interest=1000.0 + ts_second,
        funding_8h=0.0001,
        current_funding=0.00001,
    )


def test_aggregate_snapshots_to_m1_builds_expected_feature_row() -> None:
    snapshots = [
        _snapshot(ts_second=1, best_bid=100.0, best_ask=101.0, bid_1=10.0, ask_1=20.0),
        _snapshot(ts_second=20, best_bid=101.0, best_ask=102.0, bid_1=20.0, ask_1=10.0),
    ]

    rows = aggregate_snapshots_to_m1(snapshots)

    assert len(rows) == 1
    row = rows[0]
    assert row.minute_ts == datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    assert row.snapshot_count == 2

    expected = [_snapshot_feature_expectations(snapshot) for snapshot in snapshots]
    mids = [item["mid"] for item in expected]
    spreads = [item["spread_bps"] for item in expected]

    assert row.mid_open == pytest.approx(mids[0])
    assert row.mid_close == pytest.approx(mids[-1])
    assert row.mid_low == pytest.approx(min(mids))
    assert row.mid_high == pytest.approx(max(mids))
    assert row.mark_close == pytest.approx(snapshots[-1].mark_price)
    assert row.index_close == pytest.approx(snapshots[-1].index_price)

    assert row.spread_bps_mean == pytest.approx(_mean(spreads))
    assert row.spread_bps_last == pytest.approx(spreads[-1])
    assert row.spread_bps_max == pytest.approx(max(spreads))

    assert row.bid_depth_1_mean == pytest.approx(_mean([item["bid_depth_1"] for item in expected]))
    assert row.ask_depth_1_mean == pytest.approx(_mean([item["ask_depth_1"] for item in expected]))
    assert row.bid_depth_10_mean == pytest.approx(_mean([item["bid_depth_10"] for item in expected]))
    assert row.ask_depth_10_mean == pytest.approx(_mean([item["ask_depth_10"] for item in expected]))
    assert row.bid_depth_50_mean == pytest.approx(_mean([item["bid_depth_50"] for item in expected]))
    assert row.ask_depth_50_mean == pytest.approx(_mean([item["ask_depth_50"] for item in expected]))

    assert row.imbalance_1_mean == pytest.approx(_mean([item["imbalance_1"] for item in expected]))
    assert row.imbalance_10_mean == pytest.approx(_mean([item["imbalance_10"] for item in expected]))
    assert row.imbalance_50_mean == pytest.approx(_mean([item["imbalance_50"] for item in expected]))
    assert row.imbalance_10_last == pytest.approx(expected[-1]["imbalance_10"])
    assert row.imbalance_50_last == pytest.approx(expected[-1]["imbalance_50"])

    assert row.microprice_close == pytest.approx(expected[-1]["microprice"])
    assert row.microprice_minus_mid_mean == pytest.approx(_mean([item["microprice_minus_mid"] for item in expected]))
    assert row.bid_vwap_10_mean == pytest.approx(_mean([item["bid_vwap_10"] for item in expected]))
    assert row.ask_vwap_10_mean == pytest.approx(_mean([item["ask_vwap_10"] for item in expected]))

    assert row.open_interest_last == pytest.approx(1020.0)
    assert row.funding_8h_last == pytest.approx(0.0001)
    assert row.current_funding_last == pytest.approx(0.00001)
    assert row.fetch_duration_s_mean == pytest.approx(_mean([item.fetch_duration_s for item in snapshots]))
    assert row.fetch_duration_s_max == pytest.approx(max(item.fetch_duration_s for item in snapshots))
    assert row.fetch_duration_s_last == pytest.approx(snapshots[-1].fetch_duration_s)


def test_aggregate_snapshots_to_m1_drops_crossed_books() -> None:
    valid = _snapshot(ts_second=1, best_bid=100.0, best_ask=101.0, bid_1=10.0, ask_1=20.0)
    crossed = _snapshot(ts_second=2, best_bid=101.0, best_ask=100.0, bid_1=10.0, ask_1=20.0)

    rows = aggregate_snapshots_to_m1([valid, crossed])
    assert len(rows) == 1
    assert rows[0].snapshot_count == 1


def test_fetch_l2_snapshots_for_symbols_polls_all_symbols_each_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []
    lock = Lock()
    both_started = Event()

    def fake_fetch_order_book_snapshot(symbol: str, depth: int) -> dict[str, object]:
        del depth
        with lock:
            events.append(("start", symbol))
            if len([event for event in events if event[0] == "start"]) == 2:
                both_started.set()
        both_started.wait(timeout=0.5)
        with lock:
            events.append(("finish", symbol))
            event_count = len(events)
        timestamp_ms = 1_700_000_000_000 + event_count
        return {
            "exchange": "deribit",
            "symbol": f"{symbol}-PERPETUAL",
            "timestamp_ms": timestamp_ms,
            "bids": [(100.0, 1.0)],
            "asks": [(101.0, 1.0)],
            "mark_price": 100.5,
            "index_price": 100.0,
            "open_interest": 1000.0,
            "funding_8h": 0.0001,
            "current_funding": 0.00001,
        }

    monkeypatch.setattr(l2, "fetch_order_book_snapshot", fake_fetch_order_book_snapshot)

    snapshots = fetch_l2_snapshots_for_symbols(
        exchange="deribit",
        symbols=["BTC", "ETH"],
        depth=50,
        snapshot_count=1,
        poll_interval_s=0,
        concurrency=2,
    )

    assert events[0][0] == "start"
    assert events[1][0] == "start"
    assert len(snapshots["BTC"]) == 1
    assert len(snapshots["ETH"]) == 1
