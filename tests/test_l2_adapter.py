"""Tests for Deribit L2 adapter normalization."""

from __future__ import annotations

import pytest

from ingestion.exchanges import deribit_l2


def test_fetch_order_book_snapshot_normalizes_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s
        assert params is not None
        assert params["depth"] == 10
        return {
            "result": {
                "instrument_name": "BTC-PERPETUAL",
                "timestamp": 1_700_000_000_000,
                "bids": [[100.0, 2.0], [99.9, 1.0]],
                "asks": [[100.1, 3.0], [100.2, 1.5]],
                "mark_price": 100.05,
                "index_price": 100.0,
                "open_interest": 1234,
                "funding_8h": 0.0001,
                "current_funding": 0.00001,
            }
        }

    monkeypatch.setattr(deribit_l2, "get_json", fake_get_json)

    snapshot = deribit_l2.fetch_order_book_snapshot(symbol="BTC", depth=10)
    assert snapshot["symbol"] == "BTC-PERPETUAL"
    assert snapshot["timestamp_ms"] == 1_700_000_000_000
    assert snapshot["bids"] == [(100.0, 2.0), (99.9, 1.0)]
    assert snapshot["asks"] == [(100.1, 3.0), (100.2, 1.5)]
    assert snapshot["open_interest"] == 1234.0
