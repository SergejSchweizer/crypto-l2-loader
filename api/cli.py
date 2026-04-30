"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Literal, cast

import pandas as pd

from application.dto import (
    ArtifactOptionsDTO,
    CandleFetchTaskDTO,
    FundingFetchTaskDTO,
    LoaderStorageDTO,
    OpenInterestFetchTaskDTO,
    PersistOptionsDTO,
)
from application.services.artifact_service import write_loader_samples_dto
from application.services.fetch_service import (
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_symbol_candles,
    fetch_symbol_funding,
    fetch_symbol_open_interest,
)
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from application.services.runtime_service import (
    SingleInstanceError,
    SingleInstanceLock,
    configure_logging,
    env_bool,
    env_float,
    env_int,
    env_list,
    env_str,
    fetch_concurrency,
    load_env_file,
)
from application.services.storage_service import persist_loader_outputs_dto
from infra.timescaledb import save_market_data_to_timescaledb, save_parquet_lake_to_timescaledb
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.l2 import L2MinuteBar, L2Snapshot, aggregate_snapshots_to_m1, fetch_l2_snapshots_for_symbols
from ingestion.lake import (
    load_combined_dataframe_from_lake,
    load_funding_from_lake,
    load_open_interest_from_lake,
    load_spot_candles_from_lake,
    open_times_in_lake,
    open_times_in_lake_by_dataset,
    save_l2_m1_parquet_lake,
)
from ingestion.open_interest import (
    OpenInterestPoint,
    fetch_open_interest_all_history,
    fetch_open_interest_range,
    normalize_open_interest_timeframe,
    open_interest_interval_to_milliseconds,
)
from ingestion.spot import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    list_supported_intervals,
    normalize_storage_symbol,
    normalize_timeframe,
)

DataType = Literal["spot", "perp", "oi", "funding"]
__all__ = ["SingleInstanceError", "SingleInstanceLock", "build_parser", "main"]


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Convert a ``SpotCandle`` into JSON-safe dictionary."""

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[SpotCandle]:
    """Fetch candles for one symbol.

    Behavior:
    - If no data exists in parquet lake for the symbol/timeframe, fetch full exchange history.
    - If data exists, perform gap-fill up to latest fully closed candle.
    """

    return fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_candles_all_history,
        range_fetcher=fetch_candles_range,
    )


def _fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[OpenInterestPoint]:
    """Fetch open-interest data for one symbol (perp only)."""

    return fetch_symbol_open_interest(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_open_interest_timeframe,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=open_interest_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_open_interest_all_history,
        range_fetcher=fetch_open_interest_range,
    )


def _fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[FundingPoint]:
    """Fetch funding data for one symbol (perp only)."""

    return fetch_symbol_funding(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_funding_timeframe,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=funding_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_funding_all_history,
        range_fetcher=fetch_funding_range,
    )


async def _fetch_candle_tasks_parallel(
    tasks: list[tuple[Exchange, Market, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    dict[tuple[Exchange, Market, str, str], str],
]:
    """Fetch OHLCV tasks concurrently with bounded parallelism."""

    service_tasks = [
        CandleFetchTaskDTO(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe)
        for exchange, market, symbol, timeframe in tasks
    ]
    result = await fetch_candle_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_candles,
    )
    return result.rows, result.errors


async def _fetch_open_interest_tasks_parallel(
    oi_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
) -> tuple[
    dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    dict[tuple[Exchange, str, str], str],
]:
    """Fetch open-interest tasks concurrently with bounded parallelism."""

    service_tasks = [
        OpenInterestFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in oi_tasks
    ]
    result = await fetch_open_interest_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_open_interest,
    )
    return result.rows, result.errors


async def _fetch_funding_tasks_parallel(
    funding_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
) -> tuple[
    dict[tuple[Exchange, str, str], list[FundingPoint]],
    dict[tuple[Exchange, str, str], str],
]:
    """Fetch funding tasks concurrently with bounded parallelism."""

    service_tasks = [
        FundingFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in funding_tasks
    ]
    result = await fetch_funding_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_funding,
    )
    return result.rows, result.errors


def _write_loader_samples(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    logger: logging.Logger,
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
    generate_plots: bool = True,
) -> None:
    """Write per exchange/symbol/timeframe samples and matching full-data plots."""

    write_loader_samples_dto(
        storage=LoaderStorageDTO(
            candles=candles_for_storage,
            open_interest=open_interest_for_storage,
            funding=funding_for_storage or {},
        ),
        logger=logger,
        options=ArtifactOptionsDTO(generate_plots=generate_plots),
    )


def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="crypto-l2-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    spot_parser = subparsers.add_parser("loader", help="Fetch candles from supported exchanges")
    spot_parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    spot_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["deribit"],
        help="Optional list of exchanges to fetch in one run",
    )
    spot_parser.add_argument(
        "--market",
        nargs="+",
        choices=["spot", "perp", "oi", "funding"],
        default=["spot"],
        help="One or more data types to fetch, e.g. --market spot perp oi funding",
    )
    spot_parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols or instrument aliases (exchange specific)",
    )
    spot_parser.add_argument(
        "--timeframe",
        "--interval",
        dest="timeframe",
        default="1h",
        help="Candle timeframe, e.g. M1, M5, H1, D1, 1m, 1h, 1d",
    )
    spot_parser.add_argument(
        "--timeframes",
        nargs="+",
        help="Optional list of timeframes. When set, fetch runs for each timeframe sequentially.",
    )
    spot_parser.add_argument("--plot", action="store_true", help="Create and save price/volume plots")
    spot_parser.add_argument(
        "--plot-dir",
        default="samples",
        help="Deprecated: plots are always written under samples/; this option is kept for compatibility.",
    )
    spot_parser.add_argument(
        "--plot-price",
        choices=["spot", "close", "open", "high", "low"],
        default="close",
        help="Price field to plot (spot maps to close)",
    )
    spot_parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    spot_parser.add_argument(
        "--save-timescaledb",
        action="store_true",
        help="Save fetched candles/open-interest rows to TimescaleDB",
    )
    spot_parser.add_argument(
        "--timescaledb-schema",
        default="market_data",
        help="Target TimescaleDB schema for market tables",
    )
    spot_parser.add_argument(
        "--timescaledb-no-bootstrap",
        action="store_true",
        help="Skip TimescaleDB schema/table bootstrap and write into existing tables",
    )
    spot_parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    spot_parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from loader command",
    )

    tf_parser = subparsers.add_parser("list-spot-timeframes", help="List exchange-supported candle timeframes")
    tf_parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    tf_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["deribit"],
        help="Optional list of exchanges to list in one run",
    )

    ingest_parser = subparsers.add_parser(
        "ingest-timescaledb",
        help="Read existing parquet lake files and ingest them into TimescaleDB (no exchange fetch)",
    )
    ingest_parser.add_argument("--lake-root", default="lake/bronze", help="Root directory for parquet lake files")
    ingest_parser.add_argument("--timescaledb-schema", default="market_data", help="Target TimescaleDB schema")
    ingest_parser.add_argument(
        "--timescaledb-no-bootstrap",
        action="store_true",
        help="Skip TimescaleDB schema/table bootstrap and write into existing tables",
    )
    ingest_parser.add_argument("--exchanges", nargs="+", choices=["deribit"])
    ingest_parser.add_argument("--symbols", nargs="+", help="Optional symbol filter")
    ingest_parser.add_argument("--timeframes", nargs="+", help="Optional timeframe filter")
    ingest_parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        help="Optional instrument filter",
    )
    ingest_parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")

    stats_parser = subparsers.add_parser(
        "export-descriptive-stats",
        help="Export reproducible OHLCV descriptive statistics from parquet lake",
    )
    stats_parser.add_argument("--lake-root", default="lake/bronze", help="Root directory for parquet lake files")
    stats_parser.add_argument("--output-csv", default="docs/tables/descriptive_stats_baseline.csv")
    stats_parser.add_argument("--start-time", default="2026-01-01T00:00:00+00:00")
    stats_parser.add_argument("--end-time", default="2026-01-31T23:59:59+00:00")
    stats_parser.add_argument("--exchanges", nargs="+", choices=["deribit"])
    stats_parser.add_argument("--symbols", nargs="+", help="Optional symbol filter")
    stats_parser.add_argument("--timeframes", nargs="+", help="Optional timeframe filter")
    stats_parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        default=["spot", "perp"],
    )
    stats_parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")

    l2_parser = subparsers.add_parser(
        "loader-l2-m1",
        help="Fetch Deribit L2 snapshots and aggregate features to minute bars",
    )
    l2_parser.add_argument("--exchange", choices=["deribit"], default=env_str("L2_INGEST_EXCHANGE", "deribit"))
    l2_parser.add_argument("--symbols", nargs="+", default=env_list("L2_INGEST_SYMBOLS", ["BTC", "ETH"]))
    l2_parser.add_argument(
        "--levels",
        type=int,
        default=env_int("L2_INGEST_LEVELS", 50),
        help="Number of book levels per side to request",
    )
    l2_parser.add_argument(
        "--snapshot-count",
        type=int,
        default=env_int("L2_INGEST_SNAPSHOT_COUNT", 60),
        help="Snapshots per symbol to collect",
    )
    l2_parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=env_float("L2_INGEST_POLL_INTERVAL_S", 1.0),
        help="Sleep interval between snapshots",
    )
    l2_parser.add_argument(
        "--lake-root",
        default=env_str("L2_INGEST_LAKE_ROOT", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    l2_parser.add_argument(
        "--max-runtime-s",
        type=float,
        default=env_float("L2_INGEST_MAX_RUNTIME_S", 0.0),
        help="Maximum L2 collection runtime in seconds; 0 disables the budget",
    )
    l2_parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        default=env_bool("L2_INGEST_SAVE_PARQUET_LAKE", False),
        help="Save aggregated L2 M1 rows to parquet lake partitions",
    )
    l2_parser.add_argument(
        "--no-json-output",
        action="store_true",
        default=env_bool("L2_INGEST_NO_JSON_OUTPUT", False),
        help="Suppress JSON output",
    )

    return parser


def _run_loader(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run loader command."""

    if bool(args.plot) and cast(str, args.plot_dir) != "samples":
        logger.info("Ignoring --plot-dir=%s; plots are always written under samples/", args.plot_dir)

    try:
        with SingleInstanceLock(".run/crypto-l2-loader.lock"):
            exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
            data_types = cast(list[DataType], args.market)
            ohlcv_markets = [item for item in data_types if item in {"spot", "perp"}]
            oi_requested = "oi" in data_types
            funding_requested = "funding" in data_types
            multi_market = len(data_types) > 1
            requested_timeframes = cast(list[str], args.timeframes if args.timeframes else [args.timeframe])
            multi_timeframe = len(requested_timeframes) > 1
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            oi_tasks: list[tuple[Exchange, str, str]] = []
            funding_tasks: list[tuple[Exchange, str, str]] = []

            for exchange in exchanges:
                exchange_output: dict[str, object] = {}
                output[exchange] = exchange_output
                normalized_timeframes: list[str] = []
                for timeframe_value in requested_timeframes:
                    try:
                        normalized_timeframes.append(normalize_timeframe(exchange=exchange, value=timeframe_value))
                    except Exception as exc:  # noqa: BLE001
                        exchange_output[f"_timeframe_error_{timeframe_value}"] = str(exc)
                        logger.exception(
                            "Failed to normalize timeframe exchange=%s timeframe=%s",
                            exchange,
                            timeframe_value,
                        )
                if not normalized_timeframes:
                    continue
                for market in cast(list[Market], ohlcv_markets):
                    for timeframe in normalized_timeframes:
                        for symbol in args.symbols:
                            tasks.append((exchange, market, symbol, timeframe))
                if oi_requested:
                    for timeframe in normalized_timeframes:
                        for symbol in args.symbols:
                            oi_tasks.append((exchange, symbol, timeframe))
                if funding_requested:
                    for timeframe in normalized_timeframes:
                        for symbol in args.symbols:
                            funding_tasks.append((exchange, symbol, timeframe))

            current_fetch_concurrency = fetch_concurrency()
            logger.info(
                "Parallel fetch enabled with asyncio concurrency=%s",
                current_fetch_concurrency,
            )
            task_results, task_errors = asyncio.run(
                _fetch_candle_tasks_parallel(
                    tasks=tasks,
                    lake_root=args.lake_root,
                    concurrency=current_fetch_concurrency,
                    logger=logger,
                )
            )
            oi_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
            oi_errors: dict[tuple[Exchange, str, str], str] = {}
            if oi_tasks:
                oi_results, oi_errors = asyncio.run(
                    _fetch_open_interest_tasks_parallel(
                        oi_tasks=oi_tasks,
                        lake_root=args.lake_root,
                        concurrency=current_fetch_concurrency,
                        logger=logger,
                    )
                )
            funding_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
            funding_errors: dict[tuple[Exchange, str, str], str] = {}
            if funding_tasks:
                funding_results, funding_errors = asyncio.run(
                    _fetch_funding_tasks_parallel(
                        funding_tasks=funding_tasks,
                        lake_root=args.lake_root,
                        concurrency=current_fetch_concurrency,
                        logger=logger,
                    )
                )

            for exchange, market, symbol, timeframe in tasks:
                exchange_output = cast(dict[str, object], output[exchange])
                symbol_key = symbol.upper()
                result_key = (exchange, market, symbol, timeframe)
                if multi_market:
                    market_bucket = cast(dict[str, object], exchange_output.setdefault(market, {}))
                else:
                    market_bucket = exchange_output
                if multi_timeframe:
                    timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                else:
                    timeframe_bucket = market_bucket
                if result_key in task_errors:
                    timeframe_bucket[symbol_key] = {"error": task_errors[result_key]}
                    continue
                candles = task_results.get(result_key, [])
                timeframe_bucket[symbol_key] = [_serialize_candle(item) for item in candles]
                by_market = candles_for_storage.setdefault(market, {})
                exchange_candles = by_market.setdefault(exchange, {})
                if multi_market and multi_timeframe:
                    plot_key = f"{market}_{symbol_key}__{timeframe}"
                elif multi_market:
                    plot_key = f"{market}_{symbol_key}"
                elif multi_timeframe:
                    plot_key = f"{symbol_key}__{timeframe}"
                else:
                    plot_key = symbol_key
                exchange_candles[plot_key] = candles

            if oi_requested:
                for exchange, symbol, timeframe in oi_tasks:
                    symbol_key = symbol.upper()
                    oi_key = (exchange, symbol, timeframe)
                    exchange_output = cast(dict[str, object], output[exchange])
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault("oi", {}))
                    else:
                        market_bucket = exchange_output
                    if multi_timeframe:
                        timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                    else:
                        timeframe_bucket = market_bucket
                    if oi_key in oi_errors:
                        timeframe_bucket[symbol_key] = {"error": oi_errors[oi_key]}
                        continue
                    oi_rows = oi_results.get(oi_key, [])
                    timeframe_bucket[symbol_key] = [
                        {
                            "exchange": item.exchange,
                            "symbol": item.symbol,
                            "interval": item.interval,
                            "open_time": item.open_time.isoformat(),
                            "close_time": item.close_time.isoformat(),
                            "open_interest": item.open_interest,
                            "open_interest_value": item.open_interest_value,
                        }
                        for item in oi_rows
                    ]
                    oi_by_market = open_interest_for_storage.setdefault("perp", {})
                    oi_exchange_rows = oi_by_market.setdefault(exchange, {})
                    if multi_timeframe:
                        oi_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        oi_plot_key = symbol_key
                    oi_exchange_rows[oi_plot_key] = oi_rows

            if funding_requested:
                for exchange, symbol, timeframe in funding_tasks:
                    symbol_key = symbol.upper()
                    funding_key = (exchange, symbol, timeframe)
                    exchange_output = cast(dict[str, object], output[exchange])
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault("funding", {}))
                    else:
                        market_bucket = exchange_output
                    if multi_timeframe:
                        timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                    else:
                        timeframe_bucket = market_bucket
                    if funding_key in funding_errors:
                        timeframe_bucket[symbol_key] = {"error": funding_errors[funding_key]}
                        continue
                    funding_rows = funding_results.get(funding_key, [])
                    timeframe_bucket[symbol_key] = [
                        {
                            "exchange": item.exchange,
                            "symbol": item.symbol,
                            "interval": item.interval,
                            "open_time": item.open_time.isoformat(),
                            "close_time": item.close_time.isoformat(),
                            "funding_rate": item.funding_rate,
                            "index_price": item.index_price,
                            "mark_price": item.mark_price,
                        }
                        for item in funding_rows
                    ]
                    funding_by_market = funding_for_storage.setdefault("perp", {})
                    funding_exchange_rows = funding_by_market.setdefault(exchange, {})
                    if multi_timeframe:
                        funding_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        funding_plot_key = symbol_key
                    funding_exchange_rows[funding_plot_key] = funding_rows

            if args.save_parquet_lake:
                try:
                    storage_result = persist_loader_outputs_dto(
                        storage=LoaderStorageDTO(
                            candles=candles_for_storage,
                            open_interest=open_interest_for_storage,
                            funding=funding_for_storage,
                        ),
                        options=PersistOptionsDTO(
                            save_parquet_lake=True,
                            save_timescaledb=False,
                            lake_root=cast(str, args.lake_root),
                            timescaledb_schema=cast(str, args.timescaledb_schema),
                            create_schema=not bool(args.timescaledb_no_bootstrap),
                            oi_requested=oi_requested,
                            funding_requested=funding_requested,
                        ),
                        save_tsdb_fn=save_market_data_to_timescaledb,
                    )
                    output.update(storage_result.to_output_dict())
                except Exception as exc:  # noqa: BLE001
                    output["_parquet_error"] = str(exc)
                    logger.exception("Parquet lake write failed")

            if args.save_timescaledb:
                try:
                    storage_result = persist_loader_outputs_dto(
                        storage=LoaderStorageDTO(
                            candles=candles_for_storage,
                            open_interest=open_interest_for_storage,
                            funding=funding_for_storage,
                        ),
                        options=PersistOptionsDTO(
                            save_parquet_lake=False,
                            save_timescaledb=True,
                            lake_root=cast(str, args.lake_root),
                            timescaledb_schema=cast(str, args.timescaledb_schema),
                            create_schema=not bool(args.timescaledb_no_bootstrap),
                            oi_requested=oi_requested,
                            funding_requested=funding_requested,
                        ),
                        save_tsdb_fn=save_market_data_to_timescaledb,
                    )
                    output.update(storage_result.to_output_dict())
                except Exception as exc:  # noqa: BLE001
                    output["_timescaledb_error"] = str(exc)
                    logger.exception("TimescaleDB write failed")

            artifact_candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            artifact_oi: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            artifact_funding: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            for exchange, market, symbol, timeframe in tasks:
                symbol_key = symbol.upper()
                if multi_market and multi_timeframe:
                    plot_key = f"{market}_{symbol_key}__{timeframe}"
                elif multi_market:
                    plot_key = f"{market}_{symbol_key}"
                elif multi_timeframe:
                    plot_key = f"{symbol_key}__{timeframe}"
                else:
                    plot_key = symbol_key

                result_key = (exchange, market, symbol, timeframe)
                fetched = task_results.get(result_key, [])
                merged_by_open_time = {item.open_time: item for item in fetched}
                try:
                    storage_symbol = normalize_storage_symbol(
                        exchange=exchange,
                        symbol=symbol,
                        market=market,
                    )
                    stored_times = open_times_in_lake(
                        lake_root=args.lake_root,
                        market=market,
                        exchange=exchange,
                        symbol=storage_symbol,
                        timeframe=timeframe,
                    )
                    if stored_times:
                        lake_candles = load_spot_candles_from_lake(
                            lake_root=args.lake_root,
                            market=market,
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=timeframe,
                        )
                        for candle_row in lake_candles:
                            merged_by_open_time[candle_row.open_time] = candle_row
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Failed to load full-history OHLCV source exchange=%s symbol=%s timeframe=%s",
                        exchange,
                        symbol,
                        timeframe,
                    )
                merged = [merged_by_open_time[key] for key in sorted(merged_by_open_time)]
                artifact_candles.setdefault(market, {}).setdefault(exchange, {})[plot_key] = merged

            if oi_requested:
                for exchange, symbol, timeframe in oi_tasks:
                    symbol_key = symbol.upper()
                    if multi_timeframe:
                        oi_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        oi_plot_key = symbol_key
                    oi_key = (exchange, symbol, timeframe)
                    fetched_oi = oi_results.get(oi_key, [])
                    merged_oi_by_open_time: dict[datetime, OpenInterestPoint] = {
                        oi_row.open_time: oi_row for oi_row in fetched_oi
                    }
                    try:
                        storage_symbol = normalize_storage_symbol(
                            exchange=exchange,
                            symbol=symbol,
                            market="perp",
                        )
                        normalized_oi_timeframe = normalize_open_interest_timeframe(exchange=exchange, value=timeframe)
                        stored_oi_times = open_times_in_lake_by_dataset(
                            lake_root=args.lake_root,
                            dataset_type="open_interest",
                            market="perp",
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=normalized_oi_timeframe,
                        )
                        if stored_oi_times:
                            lake_oi = load_open_interest_from_lake(
                                lake_root=args.lake_root,
                                market="perp",
                                exchange=exchange,
                                symbol=storage_symbol,
                                timeframe=normalized_oi_timeframe,
                            )
                            for oi_row in lake_oi:
                                merged_oi_by_open_time[oi_row.open_time] = oi_row
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Failed to load full-history OI source exchange=%s symbol=%s timeframe=%s",
                            exchange,
                            symbol,
                            timeframe,
                        )
                    merged_oi = [merged_oi_by_open_time[key] for key in sorted(merged_oi_by_open_time)]
                    artifact_oi.setdefault("perp", {}).setdefault(exchange, {})[oi_plot_key] = merged_oi

            if funding_requested:
                for exchange, symbol, timeframe in funding_tasks:
                    symbol_key = symbol.upper()
                    funding_key = (exchange, symbol, timeframe)
                    if multi_timeframe:
                        funding_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        funding_plot_key = symbol_key
                    fetched_funding = funding_results.get(funding_key, [])
                    merged_funding_by_open_time: dict[datetime, FundingPoint] = {
                        item.open_time: item for item in fetched_funding
                    }
                    try:
                        storage_symbol = normalize_storage_symbol(
                            exchange=exchange,
                            symbol=symbol,
                            market="perp",
                        )
                        normalized_funding_timeframe = normalize_funding_timeframe(exchange=exchange, value=timeframe)
                        stored_funding_times = open_times_in_lake_by_dataset(
                            lake_root=args.lake_root,
                            dataset_type="funding",
                            market="perp",
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=normalized_funding_timeframe,
                        )
                        if stored_funding_times:
                            lake_funding = load_funding_from_lake(
                                lake_root=args.lake_root,
                                market="perp",
                                exchange=exchange,
                                symbol=storage_symbol,
                                timeframe=normalized_funding_timeframe,
                            )
                            for item in lake_funding:
                                merged_funding_by_open_time[item.open_time] = item
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Failed to load full-history funding source exchange=%s symbol=%s timeframe=%s",
                            exchange,
                            symbol,
                            timeframe,
                        )
                    merged_funding = [merged_funding_by_open_time[key] for key in sorted(merged_funding_by_open_time)]
                    artifact_funding.setdefault("perp", {}).setdefault(exchange, {})[funding_plot_key] = merged_funding

            try:
                _write_loader_samples(
                    candles_for_storage=artifact_candles,
                    open_interest_for_storage=artifact_oi,
                    funding_for_storage=artifact_funding,
                    logger=logger,
                    generate_plots=bool(args.plot),
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to generate loader samples")

            if not args.no_json_output:
                print(json.dumps(output, indent=2))
            logger.info("Command complete: loader")
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active")
        raise SystemExit(str(exc)) from exc


def _run_list_spot_timeframes(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run list-spot-timeframes command."""

    exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
    output = {exchange: list(list_supported_intervals(exchange=exchange)) for exchange in exchanges}
    print(json.dumps(output, indent=2))
    logger.info("Command complete: list-spot-timeframes")


def _serialize_l2_row(item: L2MinuteBar) -> dict[str, object]:
    """Convert L2 M1 row into JSON-safe output dictionary."""

    return {
        "minute_ts": item.minute_ts.isoformat(),
        "exchange": item.exchange,
        "symbol": item.symbol,
        "snapshot_count": item.snapshot_count,
        "mid_open": item.mid_open,
        "mid_high": item.mid_high,
        "mid_low": item.mid_low,
        "mid_close": item.mid_close,
        "mark_close": item.mark_close,
        "index_close": item.index_close,
        "spread_bps_mean": item.spread_bps_mean,
        "spread_bps_max": item.spread_bps_max,
        "spread_bps_last": item.spread_bps_last,
        "bid_depth_1_mean": item.bid_depth_1_mean,
        "ask_depth_1_mean": item.ask_depth_1_mean,
        "bid_depth_10_mean": item.bid_depth_10_mean,
        "ask_depth_10_mean": item.ask_depth_10_mean,
        "bid_depth_50_mean": item.bid_depth_50_mean,
        "ask_depth_50_mean": item.ask_depth_50_mean,
        "imbalance_1_mean": item.imbalance_1_mean,
        "imbalance_10_mean": item.imbalance_10_mean,
        "imbalance_50_mean": item.imbalance_50_mean,
        "imbalance_10_last": item.imbalance_10_last,
        "imbalance_50_last": item.imbalance_50_last,
        "microprice_close": item.microprice_close,
        "microprice_minus_mid_mean": item.microprice_minus_mid_mean,
        "bid_vwap_10_mean": item.bid_vwap_10_mean,
        "ask_vwap_10_mean": item.ask_vwap_10_mean,
        "open_interest_last": item.open_interest_last,
        "funding_8h_last": item.funding_8h_last,
        "current_funding_last": item.current_funding_last,
    }


def _log_l2_minute_bar_stats(
    logger: logging.Logger,
    row: L2MinuteBar,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Write an expressive stats line for one L2 M1 feature bar."""

    status = "partial" if collected_snapshots < requested_snapshots else "complete"
    logger.info(
        "L2 minute stats exchange=%s symbol=%s minute=%s status=%s snapshots_collected=%s "
        "snapshots_requested=%s bar_snapshot_count=%s mid_open=%.8f mid_high=%.8f mid_low=%.8f "
        "mid_close=%.8f spread_bps_mean=%.8f spread_bps_max=%.8f spread_bps_last=%.8f "
        "bid_depth_10_mean=%.8f ask_depth_10_mean=%.8f imbalance_10_mean=%s imbalance_10_last=%s "
        "microprice_close=%s open_interest_last=%s funding_8h_last=%s current_funding_last=%s "
        "fetch_duration_s_mean=%.6f fetch_duration_s_max=%.6f fetch_duration_s_last=%.6f",
        row.exchange,
        row.symbol,
        row.minute_ts.isoformat(),
        status,
        collected_snapshots,
        requested_snapshots,
        row.snapshot_count,
        row.mid_open,
        row.mid_high,
        row.mid_low,
        row.mid_close,
        row.spread_bps_mean,
        row.spread_bps_max,
        row.spread_bps_last,
        row.bid_depth_10_mean,
        row.ask_depth_10_mean,
        _format_optional_float(row.imbalance_10_mean),
        _format_optional_float(row.imbalance_10_last),
        _format_optional_float(row.microprice_close),
        _format_optional_float(row.open_interest_last),
        _format_optional_float(row.funding_8h_last),
        _format_optional_float(row.current_funding_last),
        row.fetch_duration_s_mean,
        row.fetch_duration_s_max,
        row.fetch_duration_s_last,
    )


def _log_l2_empty_symbol_stats(
    logger: logging.Logger,
    exchange: str,
    symbol: str,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Write a stats line when no M1 feature bars are produced for a symbol."""

    status = "partial" if collected_snapshots < requested_snapshots else "no_valid_bars"
    logger.info(
        "L2 minute stats exchange=%s symbol=%s status=%s snapshots_collected=%s "
        "snapshots_requested=%s bars=0",
        exchange,
        symbol,
        status,
        collected_snapshots,
        requested_snapshots,
    )


def _format_optional_float(value: float | None) -> str:
    """Format optional float values for stable log output."""

    if value is None:
        return "null"
    return f"{value:.8f}"


def _log_l2_run_summary(
    logger: logging.Logger,
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    rows_by_exchange: dict[str, dict[str, list[L2MinuteBar]]],
    requested_snapshots: int,
    parquet_files: list[str],
    elapsed_s: float,
    parquet_error: str | None = None,
) -> None:
    """Write a compact run-level L2 ingestion summary."""

    collected_total = sum(len(snapshots_by_symbol.get(symbol.upper(), [])) for symbol in symbols)
    requested_total = requested_snapshots * len(symbols)
    bars_total = sum(len(rows_by_exchange.get(exchange, {}).get(symbol.upper(), [])) for symbol in symbols)
    status = "partial" if collected_total < requested_total else "complete"
    if parquet_error is not None:
        status = "parquet_error"
    logger.info(
        "L2 run summary exchange=%s symbols=%s status=%s elapsed_s=%.3f snapshots_collected=%s "
        "snapshots_requested=%s bars=%s parquet_files=%s parquet_error=%s",
        exchange,
        ",".join(symbol.upper() for symbol in symbols),
        status,
        elapsed_s,
        collected_total,
        requested_total,
        bars_total,
        len(parquet_files),
        parquet_error or "none",
    )


def _run_loader_l2_m1(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run L2 snapshot collection and M1 aggregation command."""

    try:
        with SingleInstanceLock(".run/crypto-l2-loader-l2.lock"):
            started_at = perf_counter()
            exchange = cast(Exchange, args.exchange)
            output: dict[str, object] = {exchange: {}}
            rows_by_exchange: dict[str, dict[str, list[L2MinuteBar]]] = {exchange: {}}
            symbols = cast(list[str], args.symbols)
            requested_snapshots = int(args.snapshot_count)
            max_runtime_s = float(args.max_runtime_s)
            snapshots_by_symbol = fetch_l2_snapshots_for_symbols(
                exchange=exchange,
                symbols=symbols,
                depth=int(args.levels),
                snapshot_count=requested_snapshots,
                poll_interval_s=float(args.poll_interval_s),
                max_runtime_s=max_runtime_s if max_runtime_s > 0 else None,
                concurrency=fetch_concurrency(),
            )

            for symbol in symbols:
                symbol_key = symbol.upper()
                snapshots = snapshots_by_symbol.get(symbol_key, [])
                if len(snapshots) < requested_snapshots:
                    logger.warning(
                        "L2 run collected partial snapshots symbol=%s collected=%s requested=%s",
                        symbol_key,
                        len(snapshots),
                        requested_snapshots,
                    )
                rows = aggregate_snapshots_to_m1(snapshots)
                cast(dict[str, object], output[exchange])[symbol_key] = [_serialize_l2_row(item) for item in rows]
                rows_by_exchange[exchange][symbol_key] = rows
                if rows:
                    for row in rows:
                        _log_l2_minute_bar_stats(
                            logger=logger,
                            row=row,
                            collected_snapshots=len(snapshots),
                            requested_snapshots=requested_snapshots,
                        )
                else:
                    _log_l2_empty_symbol_stats(
                        logger=logger,
                        exchange=exchange,
                        symbol=symbol_key,
                        collected_snapshots=len(snapshots),
                        requested_snapshots=requested_snapshots,
                    )

            parquet_files: list[str] = []
            parquet_error: str | None = None
            if bool(args.save_parquet_lake):
                try:
                    parquet_files = save_l2_m1_parquet_lake(
                        rows_by_exchange=rows_by_exchange,
                        lake_root=cast(str, args.lake_root),
                    )
                    output["_parquet_files"] = parquet_files
                except Exception as exc:  # noqa: BLE001
                    parquet_error = str(exc)
                    output["_parquet_error"] = parquet_error
                    logger.exception("L2 M1 parquet write failed")

            if not bool(args.no_json_output):
                print(json.dumps(output, indent=2))
            _log_l2_run_summary(
                logger=logger,
                exchange=exchange,
                symbols=symbols,
                snapshots_by_symbol=snapshots_by_symbol,
                rows_by_exchange=rows_by_exchange,
                requested_snapshots=requested_snapshots,
                parquet_files=parquet_files,
                elapsed_s=perf_counter() - started_at,
                parquet_error=parquet_error,
            )
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active for L2 loader")
        raise SystemExit(str(exc)) from exc


def _run_export_descriptive_stats(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Export deterministic descriptive statistics table from parquet-lake OHLCV rows."""

    start_time = datetime.fromisoformat(cast(str, args.start_time))
    end_time = datetime.fromisoformat(cast(str, args.end_time))
    if start_time.tzinfo is None or end_time.tzinfo is None:
        raise ValueError("start-time and end-time must include timezone offset (for example +00:00)")
    if start_time > end_time:
        raise ValueError("start-time must be <= end-time")

    dataframe = load_combined_dataframe_from_lake(
        lake_root=cast(str, args.lake_root),
        exchanges=cast(list[str] | None, args.exchanges),
        symbols=cast(list[str] | None, args.symbols),
        timeframes=cast(list[str] | None, args.timeframes),
        instrument_types=cast(list[str] | None, args.instrument_types),
        start_time=start_time,
        end_time=end_time,
        include_open_interest=False,
    )

    stats_variables = ["open", "high", "low", "close", "volume"]
    records: list[dict[str, object]] = []
    for variable in stats_variables:
        if variable not in dataframe.columns:
            records.append({"Variable": variable, "Mean": None, "Std": None, "Min": None, "Max": None})
            continue
        series = dataframe[variable].astype("float64")
        records.append(
            {
                "Variable": variable,
                "Mean": float(series.mean()) if len(series) else None,
                "Std": float(series.std()) if len(series) else None,
                "Min": float(series.min()) if len(series) else None,
                "Max": float(series.max()) if len(series) else None,
            }
        )

    stats_df = pd.DataFrame(records, columns=["Variable", "Mean", "Std", "Min", "Max"])
    output_path = Path(cast(str, args.output_csv))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats_df.to_csv(output_path, index=False)

    result = {
        "output_csv": str(output_path.resolve()),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "row_count": int(len(dataframe)),
        "variables": stats_variables,
    }
    if not bool(args.no_json_output):
        print(json.dumps(result, indent=2))
    logger.info(
        "Command complete: export-descriptive-stats rows=%s output=%s",
        len(dataframe),
        output_path,
    )


def main() -> None:
    """CLI entrypoint."""

    load_env_file()
    parser = build_parser()
    args = parser.parse_args()
    logger = configure_logging(module_name=str(args.command))
    if args.command != "loader-l2-m1":
        logger.info("Command start: %s", args.command)

    if args.command == "loader":
        _run_loader(args=args, logger=logger)
    elif args.command == "list-spot-timeframes":
        _run_list_spot_timeframes(args=args, logger=logger)
    elif args.command == "ingest-timescaledb":
        summary = save_parquet_lake_to_timescaledb(
            lake_root=cast(str, args.lake_root),
            schema=cast(str, args.timescaledb_schema),
            create_schema=not bool(args.timescaledb_no_bootstrap),
            exchanges=cast(list[str] | None, args.exchanges),
            symbols=cast(list[str] | None, args.symbols),
            timeframes=cast(list[str] | None, args.timeframes),
            instrument_types=cast(list[str] | None, args.instrument_types),
        )
        if not bool(args.no_json_output):
            print(json.dumps(summary, indent=2))
        logger.info(
            "Command complete: ingest-timescaledb ohlcv_rows=%s open_interest_rows=%s",
            summary["ohlcv_rows"],
            summary["open_interest_rows"],
        )
    elif args.command == "export-descriptive-stats":
        _run_export_descriptive_stats(args=args, logger=logger)
    elif args.command == "loader-l2-m1":
        _run_loader_l2_m1(args=args, logger=logger)


if __name__ == "__main__":
    main()
