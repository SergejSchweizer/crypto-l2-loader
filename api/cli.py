"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import logging
import os
import re
from dataclasses import asdict
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import cast

import pandas as pd

from infra.timescaledb import save_market_data_to_timescaledb, save_parquet_lake_to_timescaledb
from ingestion.lake import (
    candle_record,
    load_spot_candles_from_lake,
    open_interest_record,
    open_times_in_lake,
    open_times_in_lake_by_dataset,
    save_open_interest_parquet_lake,
    save_spot_candles_parquet_lake,
)
from ingestion.open_interest import (
    OpenInterestPoint,
    fetch_open_interest_all_history,
    fetch_open_interest_range,
    normalize_open_interest_timeframe,
    open_interest_interval_to_milliseconds,
)
from ingestion.plotting import PriceField, save_candle_plots, save_open_interest_plot
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

LOGGER_NAME = "crypto_l2_loader"
DEFAULT_LOG_DIR = "/volume1/Temp/logs"
DEFAULT_FETCH_CONCURRENCY = 8
DatasetType = str
CliMarket = str


class SingleInstanceError(RuntimeError):
    """Raised when another CLI instance is already running."""


class SingleInstanceLock:
    """Non-blocking process lock backed by a lock file."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = Path(lock_path)
        self._fd: int | None = None

    def __enter__(self) -> SingleInstanceLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise SingleInstanceError("Another crypto-l2-loader instance is already running. Exiting.") from exc
        os.ftruncate(self._fd, 0)
        os.write(self._fd, str(os.getpid()).encode("utf-8"))
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None


def _configure_logging() -> logging.Logger:
    """Configure file logging with weekly rotation."""

    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    log_dir = Path(os.getenv("L2_SYNC_LOG_DIR", DEFAULT_LOG_DIR))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=log_dir / "crypto-l2-loader.log",
            when="D",
            interval=7,
            backupCount=0,
            encoding="utf-8",
            utc=True,
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("Falling back to stderr logging; cannot create log directory '%s'", log_dir)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Convert a ``SpotCandle`` into JSON-safe dictionary."""

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def _last_closed_open_ms(interval_ms: int, now_utc: datetime | None = None) -> int:
    """Return open timestamp (ms) of latest fully closed candle."""

    now = now_utc or datetime.now(UTC)
    now_ms = int(now.timestamp() * 1000)
    return ((now_ms // interval_ms) - 1) * interval_ms


def _missing_ranges_ms(
    existing_open_times: list[datetime],
    interval_ms: int,
    end_open_ms: int,
) -> list[tuple[int, int]]:
    """Build contiguous missing open-time ranges from known candles to end-open timestamp."""

    existing_ms = sorted(
        {
            int(item.timestamp() * 1000)
            for item in existing_open_times
            if item.tzinfo is not None and int(item.timestamp() * 1000) <= end_open_ms
        }
    )
    if not existing_ms:
        return []

    ranges: list[tuple[int, int]] = []
    for previous, current in zip(existing_ms, existing_ms[1:], strict=False):
        gap_start_ms = previous + interval_ms
        gap_end_ms = current - interval_ms
        if gap_start_ms <= gap_end_ms:
            ranges.append((gap_start_ms, gap_end_ms))

    last_existing_ms = existing_ms[-1]
    if last_existing_ms + interval_ms <= end_open_ms:
        ranges.append((last_existing_ms + interval_ms, end_open_ms))

    return ranges


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

    storage_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    stored_open_times = open_times_in_lake(
        lake_root=lake_root,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=timeframe,
    )

    interval_ms = interval_to_milliseconds(exchange=exchange, interval=timeframe)
    end_open_ms = _last_closed_open_ms(interval_ms=interval_ms)
    if not stored_open_times:
        return fetch_candles_all_history(
            exchange=exchange,
            symbol=symbol,
            market=market,
            interval=timeframe,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _missing_ranges_ms(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[SpotCandle] = []
    for start_open_ms, gap_end_ms in missing_ranges:
        fetched.extend(
            fetch_candles_range(
                exchange=exchange,
                symbol=symbol,
                interval=timeframe,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def _fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[OpenInterestPoint]:
    """Fetch open-interest data for one symbol (perp only)."""

    if market != "perp":
        return []

    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=timeframe)
    storage_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    stored_open_times = open_times_in_lake_by_dataset(
        lake_root=lake_root,
        dataset_type="open_interest",
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    interval_ms = open_interest_interval_to_milliseconds(exchange=exchange, interval=normalized_interval)
    end_open_ms = _last_closed_open_ms(interval_ms=interval_ms)
    if not stored_open_times:
        return fetch_open_interest_all_history(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            market=market,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _missing_ranges_ms(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[OpenInterestPoint] = []
    for start_open_ms, gap_end_ms in missing_ranges:
        fetched.extend(
            fetch_open_interest_range(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def _fetch_concurrency() -> int:
    """Return bounded fetch concurrency from environment."""

    raw = os.getenv("L2_FETCH_CONCURRENCY", str(DEFAULT_FETCH_CONCURRENCY))
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_FETCH_CONCURRENCY
    return max(1, value)


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

    semaphore = asyncio.Semaphore(max(1, concurrency))
    total_tasks = len(tasks)

    async def _worker(
        idx: int,
        task: tuple[Exchange, Market, str, str],
    ) -> tuple[tuple[Exchange, Market, str, str], list[SpotCandle], str | None]:
        exchange, market, symbol, timeframe = task
        logger.info(
            "Fetch start [%s/%s] exchange=%s market=%s symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            exchange,
            market,
            symbol,
            timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        async with semaphore:
            try:
                candles = await asyncio.to_thread(
                    _fetch_symbol_candles,
                    exchange,
                    market,
                    symbol,
                    timeframe,
                    lake_root,
                )
                logger.info(
                    "Fetch complete [%s/%s] exchange=%s market=%s symbol=%s timeframe=%s candles=%s",
                    idx,
                    total_tasks,
                    exchange,
                    market,
                    symbol,
                    timeframe,
                    len(candles),
                )
                return task, candles, None
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Fetch failed exchange=%s market=%s symbol=%s timeframe=%s",
                    exchange,
                    market,
                    symbol,
                    timeframe,
                )
                return task, [], str(exc)

    results = await asyncio.gather(*[_worker(idx, task) for idx, task in enumerate(tasks, start=1)])
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    for key, candles, error in results:
        if error is None:
            task_results[key] = candles
        else:
            task_errors[key] = error
    return task_results, task_errors


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

    semaphore = asyncio.Semaphore(max(1, concurrency))
    total_tasks = len(oi_tasks)

    async def _worker(
        idx: int,
        task: tuple[Exchange, str, str],
    ) -> tuple[tuple[Exchange, str, str], list[OpenInterestPoint], str | None]:
        exchange, symbol, timeframe = task
        logger.info(
            "OI fetch start [%s/%s] exchange=%s market=oi symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            exchange,
            symbol,
            timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        async with semaphore:
            try:
                rows = await asyncio.to_thread(
                    _fetch_symbol_open_interest,
                    exchange,
                    "perp",
                    symbol,
                    timeframe,
                    lake_root,
                )
                logger.info(
                    "OI fetch complete [%s/%s] exchange=%s symbol=%s timeframe=%s rows=%s",
                    idx,
                    total_tasks,
                    exchange,
                    symbol,
                    timeframe,
                    len(rows),
                )
                return task, rows, None
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "OI fetch failed exchange=%s symbol=%s timeframe=%s",
                    exchange,
                    symbol,
                    timeframe,
                )
                return task, [], str(exc)

    results = await asyncio.gather(*[_worker(idx, task) for idx, task in enumerate(oi_tasks, start=1)])
    oi_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    oi_errors: dict[tuple[Exchange, str, str], str] = {}
    for key, rows, error in results:
        if error is None:
            oi_results[key] = rows
        else:
            oi_errors[key] = error
    return oi_results, oi_errors


def _write_loader_samples(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    logger: logging.Logger,
) -> None:
    """Write per exchange/symbol/timeframe samples and matching full-data plots."""

    sample_dir = Path("samples")
    sample_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"sample_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    ingested_at = datetime.now(UTC)

    def _safe_name(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)

    for market, candle_by_exchange in candles_for_storage.items():
        for exchange, candle_by_symbol in candle_by_exchange.items():
            for symbol_key, candles in candle_by_symbol.items():
                if not candles:
                    continue
                timeframe = candles[0].interval
                base_name = (
                    f"{market}_{_safe_name(exchange)}_{_safe_name(symbol_key)}_"
                    f"{_safe_name(timeframe)}_sample_10_rows"
                )
                rows = [
                    candle_record(
                        candle=item,
                        market=market,
                        run_id=run_id,
                        ingested_at=ingested_at,
                    )
                    for item in candles
                ]
                frame = pd.DataFrame(rows)
                sample = frame.sample(n=10, replace=len(frame) < 10, random_state=42)
                csv_path = sample_dir / f"{base_name}.csv"
                sample.to_csv(csv_path, index=False)
                logger.info("Sample CSV written rows=%s path=%s", len(sample), csv_path)

                generated = save_candle_plots(
                    candles_by_exchange={exchange: {symbol_key: candles}},
                    output_dir=str(sample_dir),
                    price_field="spot",
                )
                if generated:
                    source = Path(generated[0])
                    target = sample_dir / f"{base_name}.png"
                    if source.resolve() != target.resolve():
                        source.replace(target)
                    logger.info("Sample plot written path=%s", target)

    for market, oi_by_exchange in open_interest_for_storage.items():
        for exchange, oi_by_symbol in oi_by_exchange.items():
            for symbol_key, items in oi_by_symbol.items():
                if not items:
                    continue
                timeframe = items[0].interval
                base_name = (
                    f"oi_{market}_{_safe_name(exchange)}_{_safe_name(symbol_key)}_{_safe_name(timeframe)}_sample_10_rows"
                )
                rows = [
                    open_interest_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                    for item in items
                ]
                frame = pd.DataFrame(rows)
                sample = frame.sample(n=10, replace=len(frame) < 10, random_state=42)
                csv_path = sample_dir / f"{base_name}.csv"
                sample.to_csv(csv_path, index=False)
                logger.info("Sample CSV written rows=%s path=%s", len(sample), csv_path)

                sorted_items = sorted(items, key=lambda value: value.open_time)
                plot_path = sample_dir / f"{base_name}.png"
                save_open_interest_plot(
                    exchange=exchange,
                    symbol=symbol_key,
                    interval=timeframe,
                    times=[item.open_time for item in sorted_items],
                    open_interest_values=[item.open_interest for item in sorted_items],
                    output_path=str(plot_path),
                )
                logger.info("Sample plot written path=%s", plot_path)


def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="crypto-l2-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    spot_parser = subparsers.add_parser("loader", help="Fetch candles from supported exchanges")
    spot_parser.add_argument("--exchange", choices=["binance", "deribit", "bybit"], default="binance")
    spot_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit", "bybit"],
        help="Optional list of exchanges to fetch in one run",
    )
    spot_parser.add_argument(
        "--market",
        nargs="+",
        choices=["spot", "perp", "oi"],
        default=["spot"],
        help="One or more market modes to fetch, e.g. --market spot perp oi",
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
    spot_parser.add_argument("--plot-dir", default="plots", help="Output directory for generated plots")
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
    tf_parser.add_argument("--exchange", choices=["binance", "deribit", "bybit"], default="binance")
    tf_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit", "bybit"],
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
    ingest_parser.add_argument("--exchanges", nargs="+", choices=["binance", "deribit", "bybit"])
    ingest_parser.add_argument("--symbols", nargs="+", help="Optional symbol filter")
    ingest_parser.add_argument("--timeframes", nargs="+", help="Optional timeframe filter")
    ingest_parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        help="Optional instrument filter",
    )
    ingest_parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")

    return parser


def _run_loader(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run loader command."""

    try:
        with SingleInstanceLock(".run/crypto-l2-loader.lock"):
            exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
            cli_markets = cast(list[CliMarket], args.market)
            ohlcv_markets = [item for item in cli_markets if item in {"spot", "perp"}]
            oi_requested = "oi" in cli_markets
            multi_market = len(cli_markets) > 1
            requested_timeframes = cast(list[str], args.timeframes if args.timeframes else [args.timeframe])
            multi_timeframe = len(requested_timeframes) > 1
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            oi_tasks: list[tuple[Exchange, str, str]] = []

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

            fetch_concurrency = _fetch_concurrency()
            logger.info(
                "Parallel fetch enabled with asyncio concurrency=%s",
                fetch_concurrency,
            )
            task_results, task_errors = asyncio.run(
                _fetch_candle_tasks_parallel(
                    tasks=tasks,
                    lake_root=args.lake_root,
                    concurrency=fetch_concurrency,
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
                        concurrency=fetch_concurrency,
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

            if args.save_parquet_lake:
                try:
                    parquet_files: list[str] = []
                    for market_key, candles_by_exchange in candles_for_storage.items():
                        parquet_files.extend(
                            save_spot_candles_parquet_lake(
                                candles_by_exchange=candles_by_exchange,
                                market=market_key,
                                lake_root=args.lake_root,
                            )
                        )
                    if oi_requested:
                        for market_key, oi_by_exchange in open_interest_for_storage.items():
                            parquet_files.extend(
                                save_open_interest_parquet_lake(
                                    open_interest_by_exchange=oi_by_exchange,
                                    market=market_key,
                                    lake_root=args.lake_root,
                                )
                            )
                    output["_parquet_files"] = parquet_files
                except Exception as exc:  # noqa: BLE001
                    output["_parquet_error"] = str(exc)
                    logger.exception("Parquet lake write failed")

            if args.save_timescaledb:
                try:
                    tsdb_summary = save_market_data_to_timescaledb(
                        candles_for_storage=candles_for_storage,
                        open_interest_for_storage=open_interest_for_storage,
                        schema=cast(str, args.timescaledb_schema),
                        create_schema=not bool(args.timescaledb_no_bootstrap),
                    )
                    output["_timescaledb"] = tsdb_summary
                except Exception as exc:  # noqa: BLE001
                    output["_timescaledb_error"] = str(exc)
                    logger.exception("TimescaleDB write failed")

            try:
                _write_loader_samples(
                    candles_for_storage=candles_for_storage,
                    open_interest_for_storage=open_interest_for_storage,
                    logger=logger,
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to generate loader samples")

            if args.plot:
                if not ohlcv_markets:
                    output["_plot_info"] = "No OHLCV dataset selected; skipping plot generation."
                    if not args.no_json_output:
                        print(json.dumps(output, indent=2))
                    logger.info("Command complete: loader")
                    return
                plot_source: dict[str, dict[str, list[SpotCandle]]] = {}
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
                        lake_candles = load_spot_candles_from_lake(
                            lake_root=args.lake_root,
                            market=market,
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=timeframe,
                        )
                        for item in lake_candles:
                            merged_by_open_time[item.open_time] = item
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Failed to load full-history plot source exchange=%s symbol=%s timeframe=%s",
                            exchange,
                            symbol,
                            timeframe,
                        )

                    exchange_plots = plot_source.setdefault(exchange, {})
                    exchange_plots[plot_key] = [merged_by_open_time[key] for key in sorted(merged_by_open_time)]

                try:
                    saved_paths = save_candle_plots(
                        candles_by_exchange=plot_source,
                        output_dir=args.plot_dir,
                        price_field=cast(PriceField, args.plot_price),
                    )
                    output["_plots"] = saved_paths
                except Exception as exc:  # noqa: BLE001
                    output["_plot_error"] = str(exc)
                    logger.exception("Plot generation failed")

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


def main() -> None:
    """CLI entrypoint."""

    logger = _configure_logging()
    parser = build_parser()
    args = parser.parse_args()
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


if __name__ == "__main__":
    main()
