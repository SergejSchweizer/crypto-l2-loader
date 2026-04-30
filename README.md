# crypto-l2-loader

## 1. Project Overview

This repository provides a modular framework for ingesting crypto market data with emphasis on reproducibility and production quality.

Current implemented scope (Step 1):
- Pull BTC/ETH data from Deribit public APIs for `spot`, `perp`, `oi`, and `funding`.
- Expose a CLI command for repeatable loader runs.
- Ingest Deribit perp L2 snapshots and aggregate to 1-minute (`M1`) microstructure feature bars.

Scope note:
- The repository name reflects the long-term direction (`crypto-l2-loader`). The current production implementation supports Deribit OHLCV, open interest, funding, and perpetual L2 order book snapshots aggregated into M1 microstructure feature bars.

## 2. Architecture Diagram

```text
CLI -> Application Services (fetch/gapfill/storage/artifacts)
    -> Ingestion Adapters -> HTTP Client -> Exchange REST API
    -> Parquet Lake/TimescaleDB

CLI (loader-l2-m1) -> L2 Fetch Config -> Async Polling Ticks
    -> Deribit L2 Adapter -> L2Snapshot -> M1 Feature Aggregation
    -> Parquet Lake
```

## 3. Installation Guide

### 3.1 Prerequisites

- Python 3.11+

### 3.2 Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

If the repository folder is renamed or moved, recreate the virtualenv (`rm -rf .venv && make setup`) so script shebangs remain valid.

## 4. Dependency Setup

Core dependencies are managed through `pyproject.toml` and include:
- `matplotlib` for optional plot generation
- `pyarrow` for parquet lake output

## 5. Module Explanations

### 5.1 Application Layer
- `application/dto.py`: shared DTO definitions for fetch/storage/artifact service boundaries.
- `application/schema.py`: canonical contract mapping for CLI data types to storage `dataset_type` + `instrument_type`.
- `application/services/gapfill_service.py`: pure time/gap range helpers used by loader synchronization logic.
- `application/services/fetch_service.py`: fetch orchestration service (task DTOs + parallel execution + symbol-level bootstrap/gap-fill fetch).
- `application/services/storage_service.py`: orchestration for parquet-lake and TimescaleDB persistence side effects.
- `application/services/artifact_service.py`: sample CSV/plot artifact generation service.

### 5.2 Ingestion Layer
- `ingestion/http_client.py`: lightweight JSON HTTP utilities.
- `ingestion/spot.py`: Deribit candle load/normalization interface for `spot` and `perp`.
- `ingestion/open_interest.py`: open-interest load/normalization interface for perpetual instruments.
- `ingestion/funding.py`: funding-rate load/normalization interface for perpetual instruments.
- `ingestion/exchanges/deribit.py`: Deribit adapter with symbol and timeframe mapping.
- `ingestion/exchanges/deribit_open_interest.py`: Deribit open-interest adapter.
- `ingestion/exchanges/deribit_funding.py`: Deribit funding-rate adapter.
- `ingestion/lake.py`: parquet lake read/write and partition utility functions.
- `ingestion/plotting.py`: chart rendering for loaded price/volume/open-interest/funding data.
- `ingestion/l2.py`: Deribit perp L2 fetch configuration, bounded async multi-symbol polling, snapshot normalization, and M1 feature aggregation.
- `ingestion/exchanges/deribit_l2.py`: Deribit L2 orderbook adapter.

### 5.3 API Layer
- `api/cli.py`: CLI command registration, argument parsing, orchestration entrypoint, and JSON output formatting.

### 5.4 Infrastructure Layer
- `infra/timescaledb/sink.py`: TimescaleDB schema bootstrap and idempotent upsert sink for OHLCV/OI/funding.

### 5.0 Canonical Data Type Naming

Use these names consistently in code, CLI usage, and documentation:

| Data Type | Canonical Name | Meaning |
|---|---|---|
| Spot candles | `spot` | Cash/spot OHLCV candles. |
| Perpetual candles | `perp` | Perpetual futures/swap OHLCV candles. |
| Open interest | `oi` | Open-interest time series for perpetual instruments. |
| Funding rate | `funding` | Perpetual funding-rate time series for perpetual instruments. |

Naming rules:
- Use `spot`, `perp`, `oi`, and `funding` as the user-facing data-type names.
- `dataset_type=open_interest` is the storage-layer parquet label for `oi`.
- `dataset_type=funding` is the storage-layer parquet label for `funding`.

### 5.0.1 Data Type Purpose And Meaning

#### `spot`
- Why this data exists:
  Spot candles are the base market state for unlevered price discovery and benchmark return construction.
- Meaning:
  `spot` represents executed cash-market OHLCV bars for assets such as BTC and ETH.
- Typical usage:
  Baseline volatility, return, and liquidity feature generation.

#### `perp`
- Why this data exists:
  Perpetual futures dominate crypto derivatives flow and often lead or amplify directional moves.
- Meaning:
  `perp` represents OHLCV bars from perpetual swap markets, normalized to the same schema as `spot`.
- Typical usage:
  Derivatives-aware price/volume signals and cross-market spread analysis against `spot`.

#### `oi`
- Why this data exists:
  Open interest captures positioning intensity and participation in derivatives markets.
- Meaning:
  `oi` represents open-interest time series aligned to perpetual instruments and intervals.
- Typical usage:
  Position build-up/unwind detection, confirmation/divergence checks with `perp` price action.

#### `funding`
- Why this data exists:
  Funding captures the periodic transfer mechanism that anchors perpetual prices to spot.
- Meaning:
  `funding` represents time-bucketed perpetual funding-rate observations.
- Typical usage:
  Carry analysis, perp crowding diagnostics, and regime filters for derivatives positioning.

### 5.1 Data Dictionary

Loaded candle variables (`SpotCandle`):

| Variable | Type | Description |
|---|---|---|
| `exchange` | `str` | Exchange identifier used by the adapter (`deribit`). |
| `symbol` | `str` | Normalized instrument symbol in storage form for the selected exchange/market. |
| `interval` | `str` | Candle granularity (for example `1m`, `5m`, `1h`, `1d`). |
| `open_time` | `datetime (UTC)` | Timestamp when the candle interval starts (inclusive). |
| `close_time` | `datetime (UTC)` | Timestamp when the candle interval ends (inclusive in exchange payload mapping). |
| `open_price` | `float` | First traded price observed in the candle interval. |
| `high_price` | `float` | Maximum traded price observed in the candle interval. |
| `low_price` | `float` | Minimum traded price observed in the candle interval. |
| `close_price` | `float` | Last traded price observed in the candle interval. |
| `volume` | `float` | Base-asset traded volume during the interval. |
| `quote_volume` | `float` | Quote-asset traded volume during the interval (or exchange-equivalent field). |
| `trade_count` | `int` | Number of trades aggregated into the candle (exchange dependent). |

### 5.1.1 Dataset Semantics And Variable Computation

#### `spot` dataset (`dataset_type=ohlcv`, `instrument_type=spot`)
- Meaning:
  Executed cash-market candles used as the baseline non-derivative price/volume process.
- Canonical normalized variables:
  `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`.
- Computation in pipeline:
  - `open_time = datetime_utc(row[0] / 1000)`.
  - `close_time = datetime_utc(row[6] / 1000)` for endpoints that return explicit close timestamp.
  - `open, high, low, close = float(row[1]), float(row[2]), float(row[3]), float(row[4])`.
  - `volume = float(row[5])`.
  - `quote_volume = float(row[7])` when provided by endpoint.
  - `trade_count = int(row[8])` when provided by endpoint, else `0` on adapters without trade-count payloads.
- Exchange-specific details:
  - Deribit spot: built from `ticks/open/high/low/close/volume`; `close_time = open_time + timeframe_ms - 1`, `quote_volume = volume` fallback, `trade_count = 0`.

#### `perp` dataset (`dataset_type=ohlcv`, `instrument_type=perp`)
- Meaning:
  Executed perpetual-futures candles (derivatives market) normalized to the same schema as `spot`.
- Canonical normalized variables:
  Same OHLCV schema and formulas as `spot`; only instrument source/endpoint differs.
- Computation in pipeline:
  Uses the same `SpotCandle` parser and same storage row builder (`candle_record`) as `spot`, preserving identical field semantics.
- Exchange-specific details:
  - Deribit perp: TradingView chart endpoint with computed `close_time` and fallback fields identical to Deribit spot behavior.

#### `oi` dataset (`dataset_type=open_interest`, `instrument_type=perp`)
- Meaning:
  Time-bucketed open interest for perpetual instruments; reflects outstanding open positions rather than executed candle flow.
- Availability rule:
  Collected only when market context is `perp`; requesting `oi` for `spot` returns no rows by design.
- Canonical normalized variables:
  `open_time`, `close_time`, `open_interest`, `open_interest_value`.
- Computation in pipeline:
  - `open_time` is parsed from exchange timestamp and converted to UTC.
  - `close_time = open_time + timeframe_ms - 1` (inclusive interval boundary).
  - `open_interest` comes from exchange OI quantity field.
  - `open_interest_value` is used when exchange provides notional/value OI, otherwise set to `0.0`.
- Exchange-specific OI mapping:
  - Deribit (`/api/v2/public/get_last_settlements_by_instrument`):
    - Raw settlement `timestamp` is bucketed to requested timeframe:
      `bucket_open_ms = floor(timestamp / timeframe_ms) * timeframe_ms`.
    - `open_interest = float(position)`.
    - `open_interest_value = 0.0` (not provided by adapter source payload).

#### `funding` dataset (`dataset_type=funding`, `instrument_type=perp`)
- Meaning:
  Time-bucketed funding-rate observations for perpetual instruments.
- Availability rule:
  Collected only when market context is `perp`; requesting `funding` for `spot` returns no rows by design.
- Canonical normalized variables:
  `open_time`, `close_time`, `funding_rate`, `index_price`, `mark_price`.
- Computation in pipeline:
  - `open_time` is parsed from exchange timestamp and converted to UTC.
  - `close_time = open_time + timeframe_ms - 1` (inclusive interval boundary).
  - `funding_rate` is taken from exchange funding-rate field for the interval.
  - `index_price` and `mark_price` are propagated from exchange payload when available.
- Exchange-specific funding mapping:
  - Deribit (`/api/v2/public/get_funding_rate_history`): records are parsed and bucketed to the requested timeframe, then stored as normalized funding rows.

Parquet row metadata fields:

| Variable | Type | Description |
|---|---|---|
| `schema_version` | `str` | Version marker for row schema evolution (`v1` currently). |
| `dataset_type` | `str` | Dataset family label (`ohlcv`, `open_interest`, or `funding`). |
| `instrument_type` | `str` | Market class used for loading (`spot` or `perp`). |
| `event_time` | `datetime (UTC)` | Canonical event timestamp for the row (currently aligned to `open_time`). |
| `ingested_at` | `datetime (UTC)` | Wall-clock timestamp when the row was written by the pipeline. |
| `run_id` | `str` | Unique ingestion execution identifier for traceability. |
| `source_endpoint` | `str` | Exchange endpoint group used to produce the row (`public_market_data`, `public_open_interest`, or `public_funding`). |
| `timeframe` | `str` | Storage timeframe field (same semantic meaning as `interval`). |
| `open`, `high`, `low`, `close` | `float` | Parquet OHLC aliases mapped from candle prices. |
| `extra` | `json/object` | Full normalized candle payload snapshot for reproducibility/debugging. |

### 5.2 Market Types

Supported `--market` values (data types):

| Market Type | Storage `instrument_type` | Meaning |
|---|---|---|
| `spot` | `spot` | Cash/spot market candles. |
| `perp` | `perp` | Perpetual futures/swap candles. |
| `oi` | `perp` | Open-interest time series for perpetual instruments. |
| `funding` | `perp` | Funding-rate time series for perpetual instruments. |

Exchange coverage:

| Exchange | Spot | Perp | OI | Funding | Notes |
|---|---|---|---|---|---|
| Deribit | Yes | Yes | Yes | Yes | Spot maps to instruments like `BTC_USDC`; perp maps to `BTC-PERPETUAL`. |

Current production note:
- The ingestion runtime supports Deribit only. Any non-Deribit sample artifacts are historical comparison outputs and not active adapter coverage.

### 5.3 Perpetual Field Mapping (Deribit Origin -> Storage)

Perpetual rows are normalized into the same storage schema as spot:
`open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`, plus metadata.

Deribit perp (`/api/v2/public/get_tradingview_chart_data`) mapping:

| Origin Field | Storage Field |
|---|---|
| `result.ticks[i]` | `open_time` |
| `result.ticks[i] + candle_width_ms - 1` | `close_time` |
| `result.open[i]` | `open` |
| `result.high[i]` | `high` |
| `result.low[i]` | `low` |
| `result.close[i]` | `close` |
| `result.volume[i]` | `volume` |
| `result.volume[i]` (fallback proxy) | `quote_volume` |
| not provided by endpoint | `trade_count = 0` |

### 5.4 Perpetual Symbol Naming by Exchange

Perpetual symbol normalization (Deribit):

| Exchange | Canonical Perp Symbols | Accepted Input Aliases | Normalized Output |
|---|---|---|---|
| Deribit | `BTC-PERPETUAL`, `ETH-PERPETUAL` | `BTC`, `BTCUSDT`, `BTCUSD`, `BTC-PERPETUAL`; `ETH`, `ETHUSDT`, `ETHUSD`, `ETH-PERPETUAL` | `BTC-PERPETUAL`, `ETH-PERPETUAL` |

CLI recommendation:
- Use `BTC` / `ETH`; Deribit adapters normalize to canonical Deribit symbols.

## 6. Execution Workflow

Load BTC/ETH spot candles:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe H1
```

Load spot and perp in one run:

```bash
python3 main.py loader --exchange deribit --market spot perp --symbols BTC ETH --timeframe M1
```

Load in one run:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe M1
```

Load multiple timeframes in one run:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframes M1 M5 H1 --no-json-output
```

Load and generate plots (price + volume) under `samples/`:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe M5 --plot --plot-price close
```

Save loaded data to parquet lake format:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe H1 --save-parquet-lake --lake-root lake/bronze
```

Save loaded data to TimescaleDB:

```bash
python3 main.py loader --exchange deribit --market spot perp oi funding --symbols BTC ETH --timeframe 1m --save-timescaledb --timescaledb-schema market_data
```

TimescaleDB bootstrap options:
- Default behavior creates schema/tables/hypertables if missing.
- Use `--timescaledb-no-bootstrap` to write into pre-existing schema objects only.

Fetch OHLCV (spot+perp), open interest, and funding in one run:

```bash
python3 main.py loader --exchange deribit --market spot perp oi funding --symbols BTC ETH --timeframe 5m --save-parquet-lake --lake-root lake/bronze
```

Fetch L2 snapshots and aggregate/store M1 feature bars:

```bash
python3 main.py loader-l2-m1
```

The `loader-l2-m1` command loads defaults from local `.env` config when present. CLI flags still override config values for one-off runs.

```bash
L2_INGEST_EXCHANGE=deribit
L2_INGEST_SYMBOLS=BTC ETH
L2_INGEST_LEVELS=50
L2_INGEST_SNAPSHOT_COUNT=5
L2_INGEST_POLL_INTERVAL_S=10
L2_INGEST_MAX_RUNTIME_S=55
L2_INGEST_SAVE_PARQUET_LAKE=true
L2_INGEST_LAKE_ROOT=lake/bronze
L2_INGEST_NO_JSON_OUTPUT=true
```

Runtime behavior:
- `snapshot-count` controls the number of polling ticks per run.
- `poll-interval-s` controls the wait between ticks.
- Each tick fetches all configured symbols concurrently before sleeping, so BTC/ETH perp snapshots are collected on the same polling cadence.
- `L2_FETCH_CONCURRENCY` bounds concurrent per-tick symbol fetches.
- `max-runtime-s` caps collection time; if Deribit/API latency pushes the run near the budget, the command writes whatever snapshots were collected and logs a warning.

Parquet lake write mode uses a stable file per partition (`data.parquet`) with staged merge+rewrite on each run to keep file counts bounded. Partition schema:

```text
dataset_type=ohlcv/
  exchange=<exchange>/
  instrument_type=<spot|perp>/
  symbol=<symbol>/
  timeframe=<interval>/
  date=<YYYY-MM>/
    data.parquet

dataset_type=open_interest/
  exchange=<exchange>/
  instrument_type=<perp>/
  symbol=<symbol>/
  timeframe=<interval>/
  date=<YYYY-MM>/
    data.parquet

dataset_type=funding/
  exchange=<exchange>/
  instrument_type=<perp>/
  symbol=<symbol>/
  timeframe=<interval>/
  date=<YYYY-MM>/
    data.parquet
```

Loader mode is automatic:
- Only two modes exist:
  1. `fetch all history` when no parquet data exists for the symbol/timeframe.
  2. `fill gaps` when parquet data exists (internal gaps + tail to latest closed candle).
- If no parquet data exists for a symbol/timeframe, it fetches full available exchange history.
- If parquet data exists, it performs gap-fill (internal gaps + tail to latest closed candle).

Example full-history bootstrap (first run can be long-running):

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe M1 --save-parquet-lake --lake-root lake/bronze --no-json-output
```

Note:
- Loader network fetch tasks run in parallel via `asyncio` with bounded concurrency.
- Parallel fetch orchestration is implemented in `application/services/fetch_service.py`; `api/cli.py` delegates to this service.
- Parquet partition writes are parallelized.
- Concurrency is controlled by `L2_FETCH_CONCURRENCY` (default: `8`).

Run silently without JSON output:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC --timeframe M1 --no-json-output
```

Ingest existing parquet lake files into TimescaleDB (no internet fetch):

```bash
python3 main.py ingest-timescaledb --lake-root lake/bronze --timescaledb-schema market_data
```

Optional filters for offline parquet->Timescale ingestion:

```bash
python3 main.py ingest-timescaledb --lake-root lake/bronze --exchanges deribit --instrument-types perp --timeframes 1m
```

Export deterministic descriptive statistics for reporting:

```bash
python3 main.py export-descriptive-stats --lake-root lake/bronze --output-csv docs/tables/descriptive_stats_baseline.csv --start-time 2026-01-01T00:00:00+00:00 --end-time 2026-01-31T23:59:59+00:00
```

Load Deribit perpetual candles (portable perp inputs):

```bash
python3 main.py loader --exchange deribit --market perp --symbols BTC ETH --timeframe M5
```

List all currently supported spot timeframes:

```bash
python3 main.py list-spot-timeframes
python3 main.py list-spot-timeframes --exchange deribit
```

## 7. Datatype Plots

### 7.1 OHLCV (Spot)
Description:
OHLCV spot rows capture exchange-traded candle bars for cash markets.  
The loader writes grouped sample artifacts per run under `samples/`.

Plot:
`samples/spot_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history price + volume plot for that group).

Example:
`samples/spot_deribit_BTCUSDT_1m_sample_10_rows.png`

### 7.2 OHLCV (Perp)
Description:
Perpetual OHLCV rows follow the same candle schema and are stored independently per market group.

Plot:
`samples/perp_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history price + volume plot for that group).

Example:
`samples/perp_deribit_BTCUSDT_1m_sample_10_rows.png`

### 7.3 Open Interest (OI)
Description:
Open-interest rows are stored under `dataset_type=open_interest` and sampled per run when `--market oi` is used.

Plot:
`samples/oi_<market>_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history OI time-series line chart for that group).

Example:
`samples/oi_perp_deribit_BTCUSDT_5m_sample_10_rows.png`

### 7.4 Funding
Description:
Funding rows are stored under `dataset_type=funding` and sampled per run when `--market funding` is used.

Plot:
`samples/funding_<market>_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history funding-rate time-series line chart for that group).

Example:
`samples/funding_perp_deribit_BTCUSDT_5m_sample_10_rows.png`

## 8. Testing Instructions

```bash
make check
```

Quality gate purpose:
- `ruff check .`: fast static linting and import/style/error checks (for example unused imports, bad patterns, formatting-related lint rules).
- `mypy .`: static type checking to catch type mismatches before runtime (`mupy` in notes/messages usually means `mypy`).
- `pytest`: runtime test suite for behavioral correctness of ingestion, storage, CLI, and service flows.

Equivalent direct commands:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
```

Pre-commit hooks:

```bash
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
```

Current coverage includes:
- Exchange adapter normalization/routing and pagination behavior.
- Gap-fill utility logic (`application/services/gapfill_service.py`).
- Fetch orchestration success/error split for parallel task runners (`application/services/fetch_service.py`).
- Storage orchestration behavior for parquet+Timescale side effects (`application/services/storage_service.py`).
- Canonical dataset contract mapping (`application/schema.py`).
- CLI locking, parquet lake persistence, plotting, TimescaleDB sink behavior, and async L2 multi-symbol polling.

## 9. Deployment Instructions

- For now this is a local CLI tool.
- Next stage will add scheduled runs and orchestrated pipelines.
- The main loader enforces a single running instance using `.run/crypto-l2-loader.lock`.
- The L2 M1 loader enforces a separate single running instance using `.run/crypto-l2-loader-l2.lock`.
- Runtime logs are written to a command-specific file under `/volume1/Temp/logs/` by default.
- Log filenames match the CLI command/module, for example `loader.log`, `loader-l2-m1.log`, and `ingest-timescaledb.log`.
- Logs rotate every 7 days and rotated files are date-suffixed (for example `loader-l2-m1.log.2026-04-27`) and retained in the same directory.
- `loader-l2-m1.log` includes one `L2 minute stats` line per generated M1 bar with snapshot counts, fetch durations, mid-price OHLC, spread, depth, imbalance, microprice, open interest, and funding fields, plus one `L2 run summary` line per cron run.
- Local `.env` is loaded automatically when present and is excluded from git tracking.
- Optional override: set `L2_SYNC_LOG_DIR` to change the log directory.
- Optional override: set `L2_FETCH_CONCURRENCY` to control loader fetch parallelism (minimum `1`, default `8`).
- TimescaleDB sink env vars: `TIMESCALEDB_HOST`, `TIMESCALEDB_PORT`, `TIMESCALEDB_USER`, `TIMESCALEDB_PASSWORD`, `TIMESCALEDB_DB`, `PGSSLMODE`.
- `TIMESCALEDB_PASSWORD` is required at runtime (no insecure default fallback).
- Run quality gates via module form (`python -m pytest`, `python -m mypy`, `python -m ruff`) to avoid local venv entrypoint shebang drift when directories move.

## 10. Known Limitations

- L2 ingestion currently uses finite REST order book snapshots, not a continuous websocket depth stream.
- No exchange failover yet.

## 11. Future Improvements

- Add continuous Deribit websocket order book ingestion for higher-frequency depth reconstruction.
- Add scheduled lake compaction and retention policies.
