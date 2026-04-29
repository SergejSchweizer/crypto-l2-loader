"""Tests for canonical dataset contract mapping."""

from __future__ import annotations

from application.schema import dataset_contract


def test_dataset_contract_maps_spot_perp_oi() -> None:
    spot = dataset_contract("spot")
    perp = dataset_contract("perp")
    oi = dataset_contract("oi")

    assert spot.dataset_type == "ohlcv"
    assert spot.instrument_type == "spot"

    assert perp.dataset_type == "ohlcv"
    assert perp.instrument_type == "perp"

    assert oi.dataset_type == "open_interest"
    assert oi.instrument_type == "perp"
