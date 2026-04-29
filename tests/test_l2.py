"""Tests for L2 snapshot aggregation to M1 features."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.l2 import L2Snapshot, aggregate_snapshots_to_m1


def _snapshot(ts_second: int, best_bid: float, best_ask: float, bid_1: float, ask_1: float) -> L2Snapshot:
    base = datetime(2026, 4, 29, 10, 0, ts_second, tzinfo=UTC)
    bids = [(best_bid, bid_1)] + [(best_bid - i * 0.1, 2.0 + i) for i in range(1, 50)]
    asks = [(best_ask, ask_1)] + [(best_ask + i * 0.1, 1.5 + i) for i in range(1, 50)]
    return L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=base,
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

    mid_1 = (100.0 + 101.0) / 2.0
    mid_2 = (101.0 + 102.0) / 2.0
    assert row.mid_open == pytest.approx(mid_1)
    assert row.mid_close == pytest.approx(mid_2)
    assert row.mid_low == pytest.approx(min(mid_1, mid_2))
    assert row.mid_high == pytest.approx(max(mid_1, mid_2))

    spread_1 = ((101.0 - 100.0) / mid_1) * 10_000
    spread_2 = ((102.0 - 101.0) / mid_2) * 10_000
    assert row.spread_bps_mean == pytest.approx((spread_1 + spread_2) / 2.0)
    assert row.spread_bps_last == pytest.approx(spread_2)
    assert row.spread_bps_max == pytest.approx(max(spread_1, spread_2))

    assert row.imbalance_10_mean is not None
    assert row.imbalance_10_last is not None
    assert row.microprice_close is not None
    assert row.bid_vwap_10_mean is not None
    assert row.ask_vwap_10_mean is not None
    assert row.open_interest_last == pytest.approx(1020.0)


def test_aggregate_snapshots_to_m1_drops_crossed_books() -> None:
    valid = _snapshot(ts_second=1, best_bid=100.0, best_ask=101.0, bid_1=10.0, ask_1=20.0)
    crossed = _snapshot(ts_second=2, best_bid=101.0, best_ask=100.0, bid_1=10.0, ask_1=20.0)

    rows = aggregate_snapshots_to_m1([valid, crossed])
    assert len(rows) == 1
    assert rows[0].snapshot_count == 1
