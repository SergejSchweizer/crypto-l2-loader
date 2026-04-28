"""TimescaleDB sink for OHLCV and open-interest data."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.lake import candle_record, open_interest_record
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Market, SpotCandle

OhlcvTableName = "ohlcv"
OpenInterestTableName = "open_interest"
_SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_PARQUET_BATCH_SIZE = 10_000
DEFAULT_DB_WRITE_BATCH_SIZE = 5_000
PartitionFilter = Callable[[dict[str, str]], bool]


def _validate_sql_identifier(value: str, kind: str = "identifier") -> str:
    """Validate SQL identifier against strict safe pattern."""

    if not _SQL_IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid SQL {kind}: '{value}'")
    return value


def _db_settings() -> dict[str, Any]:
    """Read TimescaleDB connection settings from environment."""

    return {
        "host": os.getenv("TIMESCALEDB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TIMESCALEDB_PORT", "54321")),
        "user": os.getenv("TIMESCALEDB_USER", "crypto"),
        "password": os.getenv("TIMESCALEDB_PASSWORD", "784542"),
        "dbname": os.getenv("TIMESCALEDB_DB", "crypto"),
        "sslmode": os.getenv("PGSSLMODE", "disable"),
    }


def _positive_env_int(name: str, default: int) -> int:
    """Read positive integer env var with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _json_safe(value: object) -> object:
    """Convert nested objects into JSON-serializable primitives."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _partition_from_path(path: Path) -> dict[str, str]:
    """Extract hive-style partition key/value pairs from path segments."""

    partition: dict[str, str] = {}
    for segment in path.parts:
        if "=" in segment:
            key, value = segment.split("=", 1)
            partition[key] = value
    return partition


def _iter_parquet_rows(
    lake_root: str,
    glob_pattern: str,
    allow_partition: PartitionFilter,
    batch_size: int,
) -> Iterator[dict[str, object]]:
    """Yield parquet rows for matching files while honoring partition filters."""

    import pyarrow.parquet as pq

    for data_file in sorted(Path(lake_root).glob(glob_pattern)):
        if not allow_partition(_partition_from_path(data_file)):
            continue
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=batch_size):  # type: ignore[no-untyped-call]
            yield from batch.to_pylist()


def _build_ohlcv_db_row(row: dict[str, object]) -> dict[str, object]:
    """Map one parquet OHLCV row into DB upsert payload format."""

    return {
        "exchange": row.get("exchange"),
        "symbol": row.get("symbol"),
        "instrument_type": row.get("instrument_type"),
        "timeframe": row.get("timeframe"),
        "open_time": row.get("open_time"),
        "close_time": row.get("close_time"),
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "volume": row.get("volume"),
        "quote_volume": row.get("quote_volume"),
        "trade_count": row.get("trade_count"),
        "schema_version": row.get("schema_version", "v1"),
        "dataset_type": row.get("dataset_type", "ohlcv"),
        "event_time": row.get("event_time", row.get("open_time")),
        "ingested_at": row.get("ingested_at"),
        "run_id": row.get("run_id", "unknown"),
        "source_endpoint": row.get("source_endpoint", "unknown"),
        "extra": row.get("extra"),
    }


def _build_open_interest_db_row(row: dict[str, object]) -> dict[str, object]:
    """Map one parquet open-interest row into DB upsert payload format."""

    return {
        "exchange": row.get("exchange"),
        "symbol": row.get("symbol"),
        "instrument_type": row.get("instrument_type"),
        "timeframe": row.get("timeframe"),
        "open_time": row.get("open_time"),
        "close_time": row.get("close_time"),
        "open_interest": row.get("open_interest"),
        "open_interest_value": row.get("open_interest_value", 0.0),
        "schema_version": row.get("schema_version", "v1"),
        "dataset_type": row.get("dataset_type", "open_interest"),
        "event_time": row.get("event_time", row.get("open_time")),
        "ingested_at": row.get("ingested_at"),
        "run_id": row.get("run_id", "unknown"),
        "source_endpoint": row.get("source_endpoint", "unknown"),
    }


def _upsert_rows_in_batches(
    row_iter: Iterator[dict[str, object]],
    write_batch_size: int,
    upsert_fn: Callable[[Any, str, list[dict[str, object]]], int],
    conn: Any,
    schema: str,
) -> int:
    """Accumulate rows in bounded batches and flush them via provided upsert function."""

    total_count = 0
    buffer: list[dict[str, object]] = []
    for row in row_iter:
        buffer.append(row)
        if len(buffer) >= write_batch_size:
            total_count += upsert_fn(conn, schema, buffer)
            buffer = []
    if buffer:
        total_count += upsert_fn(conn, schema, buffer)
    return total_count


def _create_schema_and_tables(conn: Any, schema: str) -> None:
    """Create schema/tables/hypertables/indexes when missing."""

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {safe_schema};")
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {safe_schema}.{OhlcvTableName} (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                quote_volume DOUBLE PRECISION NOT NULL,
                trade_count BIGINT NOT NULL,
                schema_version TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                extra JSONB,
                PRIMARY KEY (exchange, instrument_type, symbol, timeframe, open_time)
            );
            """
        )
        cur.execute(
            f"""
            SELECT create_hypertable(
                '{safe_schema}.{OhlcvTableName}',
                'open_time',
                if_not_exists => TRUE
            );
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{OhlcvTableName}_symbol_time
            ON {safe_schema}.{OhlcvTableName} (exchange, symbol, timeframe, open_time DESC);
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {safe_schema}.{OpenInterestTableName} (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                open_interest DOUBLE PRECISION NOT NULL,
                open_interest_value DOUBLE PRECISION NOT NULL,
                schema_version TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                PRIMARY KEY (exchange, instrument_type, symbol, timeframe, open_time)
            );
            """
        )
        cur.execute(
            f"""
            SELECT create_hypertable(
                '{safe_schema}.{OpenInterestTableName}',
                'open_time',
                if_not_exists => TRUE
            );
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{OpenInterestTableName}_symbol_time
            ON {safe_schema}.{OpenInterestTableName} (exchange, symbol, timeframe, open_time DESC);
            """
        )


def _upsert_ohlcv(conn: Any, schema: str, rows: list[dict[str, object]]) -> int:
    """Upsert OHLCV rows into TimescaleDB."""

    if not rows:
        return 0

    try:
        from psycopg.types.json import Json
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    sql = f"""
        INSERT INTO {safe_schema}.{OhlcvTableName} (
            exchange, symbol, instrument_type, timeframe, open_time, close_time,
            open, high, low, close, volume, quote_volume, trade_count,
            schema_version, dataset_type, event_time, ingested_at, run_id,
            source_endpoint, extra
        ) VALUES (
            %(exchange)s, %(symbol)s, %(instrument_type)s, %(timeframe)s, %(open_time)s, %(close_time)s,
            %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(quote_volume)s, %(trade_count)s,
            %(schema_version)s, %(dataset_type)s, %(event_time)s, %(ingested_at)s, %(run_id)s,
            %(source_endpoint)s, %(extra)s
        )
        ON CONFLICT (exchange, instrument_type, symbol, timeframe, open_time)
        DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            trade_count = EXCLUDED.trade_count,
            schema_version = EXCLUDED.schema_version,
            dataset_type = EXCLUDED.dataset_type,
            event_time = EXCLUDED.event_time,
            ingested_at = EXCLUDED.ingested_at,
            run_id = EXCLUDED.run_id,
            source_endpoint = EXCLUDED.source_endpoint,
            extra = EXCLUDED.extra;
    """
    payload = [{**row, "extra": Json(_json_safe(row.get("extra")))} for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, payload)
    return len(payload)


def _upsert_open_interest(conn: Any, schema: str, rows: list[dict[str, object]]) -> int:
    """Upsert open-interest rows into TimescaleDB."""

    if not rows:
        return 0

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    sql = f"""
        INSERT INTO {safe_schema}.{OpenInterestTableName} (
            exchange, symbol, instrument_type, timeframe, open_time, close_time,
            open_interest, open_interest_value,
            schema_version, dataset_type, event_time, ingested_at, run_id, source_endpoint
        ) VALUES (
            %(exchange)s, %(symbol)s, %(instrument_type)s, %(timeframe)s, %(open_time)s, %(close_time)s,
            %(open_interest)s, %(open_interest_value)s,
            %(schema_version)s, %(dataset_type)s, %(event_time)s, %(ingested_at)s, %(run_id)s, %(source_endpoint)s
        )
        ON CONFLICT (exchange, instrument_type, symbol, timeframe, open_time)
        DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open_interest = EXCLUDED.open_interest,
            open_interest_value = EXCLUDED.open_interest_value,
            schema_version = EXCLUDED.schema_version,
            dataset_type = EXCLUDED.dataset_type,
            event_time = EXCLUDED.event_time,
            ingested_at = EXCLUDED.ingested_at,
            run_id = EXCLUDED.run_id,
            source_endpoint = EXCLUDED.source_endpoint;
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def save_market_data_to_timescaledb(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    schema: str = "market_data",
    create_schema: bool = True,
) -> dict[str, int | str]:
    """Persist fetched market data into TimescaleDB."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    ingested_at = datetime.now(UTC)

    ohlcv_rows: list[dict[str, object]] = []
    for market, candle_by_exchange in candles_for_storage.items():
        for candle_by_symbol in candle_by_exchange.values():
            for candles in candle_by_symbol.values():
                ohlcv_rows.extend(
                    [
                        candle_record(candle=item, market=market, run_id=run_id, ingested_at=ingested_at)
                        for item in candles
                    ]
                )

    oi_rows: list[dict[str, object]] = []
    for market, oi_by_exchange in open_interest_for_storage.items():
        for oi_by_symbol in oi_by_exchange.values():
            for items in oi_by_symbol.values():
                oi_rows.extend(
                    [
                        open_interest_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                        for item in items
                    ]
                )

    settings = _db_settings()
    with psycopg.connect(**settings) as conn:
        with conn.transaction():
            if create_schema:
                _create_schema_and_tables(conn=conn, schema=safe_schema)
            ohlcv_count = _upsert_ohlcv(conn=conn, schema=safe_schema, rows=ohlcv_rows)
            oi_count = _upsert_open_interest(conn=conn, schema=safe_schema, rows=oi_rows)

    return {
        "schema": safe_schema,
        "ohlcv_rows": ohlcv_count,
        "open_interest_rows": oi_count,
    }


def save_parquet_lake_to_timescaledb(
    lake_root: str,
    schema: str = "market_data",
    create_schema: bool = True,
    exchanges: list[str] | None = None,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    instrument_types: list[str] | None = None,
) -> dict[str, int | str]:
    """Ingest existing parquet lake rows into TimescaleDB without fetching from exchanges."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet ingestion. Install project dependencies.") from exc

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    parquet_batch_size = _positive_env_int("L2_TSDB_PARQUET_BATCH_SIZE", DEFAULT_PARQUET_BATCH_SIZE)
    write_batch_size = _positive_env_int("L2_TSDB_WRITE_BATCH_SIZE", DEFAULT_DB_WRITE_BATCH_SIZE)
    exchange_filter = {item.lower() for item in exchanges} if exchanges else None
    symbol_filter = {item.upper() for item in symbols} if symbols else None
    timeframe_filter = {item.lower() for item in timeframes} if timeframes else None
    instrument_filter = {item.lower() for item in instrument_types} if instrument_types else None

    def _allow(partition: dict[str, str]) -> bool:
        exchange_value = partition.get("exchange", "")
        instrument_value = partition.get("instrument_type", "")
        symbol_value = partition.get("symbol", "")
        timeframe_value = partition.get("timeframe", "")
        if exchange_filter is not None and exchange_value.lower() not in exchange_filter:
            return False
        if instrument_filter is not None and instrument_value.lower() not in instrument_filter:
            return False
        if symbol_filter is not None and symbol_value.upper() not in symbol_filter:
            return False
        if timeframe_filter is not None and timeframe_value.lower() not in timeframe_filter:
            return False
        return True

    ohlcv_rows = (
        _build_ohlcv_db_row(row)
        for row in _iter_parquet_rows(
            lake_root=lake_root,
            glob_pattern="dataset_type=ohlcv/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet",
            allow_partition=_allow,
            batch_size=parquet_batch_size,
        )
    )
    oi_rows = (
        _build_open_interest_db_row(row)
        for row in _iter_parquet_rows(
            lake_root=lake_root,
            glob_pattern=(
                "dataset_type=open_interest/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet"
            ),
            allow_partition=_allow,
            batch_size=parquet_batch_size,
        )
    )

    settings = _db_settings()
    with psycopg.connect(**settings) as conn:
        with conn.transaction():
            if create_schema:
                _create_schema_and_tables(conn=conn, schema=safe_schema)
            ohlcv_count = _upsert_rows_in_batches(
                row_iter=ohlcv_rows,
                write_batch_size=write_batch_size,
                upsert_fn=_upsert_ohlcv,
                conn=conn,
                schema=safe_schema,
            )
            oi_count = _upsert_rows_in_batches(
                row_iter=oi_rows,
                write_batch_size=write_batch_size,
                upsert_fn=_upsert_open_interest,
                conn=conn,
                schema=safe_schema,
            )

    return {
        "schema": safe_schema,
        "ohlcv_rows": ohlcv_count,
        "open_interest_rows": oi_count,
    }
