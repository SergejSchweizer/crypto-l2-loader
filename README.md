# L2 Synchronizer

## 1. Project Overview

This repository provides a modular framework for ingesting crypto market data with emphasis on reproducibility and production quality.

Current implemented scope (Step 1):
- Pull BTC/ETH candles from Binance (spot) and Deribit (spot/perp) public APIs.
- Expose a CLI command for repeatable fetch runs.

## 2. Architecture Diagram

```text
CLI -> Ingestion Adapter -> HTTP Client -> Exchange REST API -> (next step) Database
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

## 4. Dependency Setup

Core dependencies are managed through `pyproject.toml` and include:
- `matplotlib` for optional plot generation
- `pyarrow` for parquet lake output

## 5. Module Explanations

- `ingestion/http_client.py`: lightweight JSON HTTP utilities.
- `ingestion/spot.py`: exchange-agnostic candle fetch/normalization interface.
- `ingestion/exchanges/binance.py`: Binance adapter with pagination support.
- `ingestion/exchanges/deribit.py`: Deribit adapter with symbol and timeframe mapping.
- `ingestion/plotting.py`: chart rendering for fetched price and volume data.
- `ingestion/lake.py`: parquet lake writer for partitioned candle datasets.
- `api/cli.py`: CLI command registration and output formatting.
- `infra/`: shared domain and time window utilities for upcoming steps.

## 6. Execution Workflow

Fetch latest BTC/ETH spot candles:

```bash
python3 main.py fetch-spot --exchange binance --market spot --symbols BTCUSDT ETHUSDT --timeframe H1 --limit 5
```

Fetch multiple exchanges in one run:

```bash
python3 main.py fetch-spot --exchanges binance deribit --market spot --symbols BTCUSDT ETHUSDT --timeframe M1 --limit 10
```

Fetch and generate plots (price + volume) under `plots/`:

```bash
python3 main.py fetch-spot --exchanges binance deribit --market spot --symbols BTCUSDT ETHUSDT --timeframe M5 --limit 200 --plot --plot-dir plots --plot-price close
```

Save fetched data to parquet lake format:

```bash
python3 main.py fetch-spot --exchanges binance deribit --market spot --symbols BTCUSDT ETHUSDT --timeframe H1 --limit 1200 --save-parquet-lake --lake-root lake/bronze
```

Parquet lake write mode uses a stable file per partition (`data.parquet`) with staged merge+rewrite on each run to keep file counts bounded.

Fetch all available history from exchanges (can be long-running):

```bash
python3 main.py fetch-spot --exchanges binance deribit --market spot --symbols BTCUSDT ETHUSDT --timeframe M1 --all-history --save-parquet-lake --lake-root lake/bronze --no-json-output
```

Run gap-fill mode (default when `--limit` is omitted): detects and fills all missing candles within stored history and also backfills from latest stored candle to current closed candle.

```bash
python3 main.py fetch-spot --exchange binance --market spot --symbols BTCUSDT ETHUSDT --timeframe H1 --save-parquet-lake --lake-root lake/bronze
```

If no parquet data exists and `--limit` is omitted, the script bootstraps with the maximum single-request amount supported by the exchange/timeframe.

Use explicit latest mode without a fixed count:

```bash
python3 main.py fetch-spot --exchange deribit --market perp --symbols BTC ETH --timeframe M5 --mode latest
```

Run silently without JSON output:

```bash
python3 main.py fetch-spot --exchange binance --market spot --symbols BTCUSDT --timeframe M1 --limit 100 --no-json-output
```

Fetch more than 1000 candles (automatic pagination):

```bash
python3 main.py fetch-spot --exchange binance --market spot --symbols BTCUSDT --timeframe M1 --limit 1200
```

Fetch Deribit perpetual candles:

```bash
python3 main.py fetch-spot --exchange deribit --market perp --symbols BTC ETH --timeframe M5 --limit 50
```

List all currently supported spot timeframes:

```bash
python3 main.py list-spot-timeframes
python3 main.py list-spot-timeframes --exchange deribit
python3 main.py list-spot-timeframes --exchanges binance deribit
```

## 7. Example Plots

The following plots are versioned under `docs/figures/plot_outputs/` and generated from CLI output candles.

### Figure 1. Binance BTCUSDT (M1 close)

![Figure 1 - Binance BTCUSDT M1 close](docs/figures/plot_outputs/binance_BTCUSDT_1m_close.png)

### Figure 2. Binance ETHUSDT (M1 close)

![Figure 2 - Binance ETHUSDT M1 close](docs/figures/plot_outputs/binance_ETHUSDT_1m_close.png)

### Figure 3. Deribit BTCUSDT alias -> BTC_USDC (M1 close)

![Figure 3 - Deribit BTCUSDT M1 close](docs/figures/plot_outputs/deribit_BTCUSDT_1m_close.png)

### Figure 4. Deribit ETHUSDT alias -> ETH_USDC (M1 close)

![Figure 4 - Deribit ETHUSDT M1 close](docs/figures/plot_outputs/deribit_ETHUSDT_1m_close.png)

## 8. Testing Instructions

```bash
pytest
ruff check .
mypy .
```

## 9. Deployment Instructions

- For now this is a local CLI tool.
- Next stage will add scheduled runs and database persistence.
- The CLI enforces a single running instance using `.run/l2-synchronizer.lock`.

### 9.1 TimescaleDB via Docker Compose

1. Copy environment template:

```bash
cp docker/.env.example docker/.env
```

2. Start TimescaleDB:

```bash
docker compose -f docker/docker-compose.timescaledb.yml --env-file docker/.env up -d
```

3. Check service health:

```bash
docker compose -f docker/docker-compose.timescaledb.yml ps
```

4. Stop service:

```bash
docker compose -f docker/docker-compose.timescaledb.yml down
```

Notes:
- Data persists in named volume `timescaledb_data`.
- `infra/db/init/001_enable_timescaledb.sql` auto-runs on first initialization.

## 10. Known Limitations

- Step 1 currently supports candles only (no funding or L2 yet).
- No database persistence yet.
- No exchange failover yet.

## 11. Future Improvements

- Add perpetual and funding endpoints.
- Add Deribit adapter for L2 order book snapshots.
- Add full-update vs last-N-days database ingestion mode.
