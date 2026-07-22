# dbt Infrastructure

Runtime image for `transformation/` — dbt-core + `dbt-duckdb`, built from `python:3.11-slim`.
No cluster, no persistent DuckDB file: each run is a short-lived container that opens an
in-memory DuckDB process, reads bronze Parquet/Delta straight off Azure Blob, and writes
silver/gold back out as Parquet — same ephemeral-container pattern as `dlt-pipeline` and
`spark-pipeline`.

## What's bundled in the image

| Component | Purpose |
|-----------|---------|
| `dbt-core` | dbt CLI + Jinja/SQL compilation |
| `dbt-duckdb` | DuckDB adapter — `httpfs`, `azure`, `delta` extensions loaded via `profiles.yml` |

## Build

Build context must be the **project root**:

```bash
# From project root
docker build -f infrastructure/dbt/Dockerfile -t dbt-pipeline:latest .
```

## Usage

```bash
docker run --rm \
  -e AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dbt-pipeline:latest dbt build --profiles-dir /app/profiles

# Single model / domain
docker run --rm -e AZURE_STORAGE_ACCOUNT_NAME=<account> -e AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dbt-pipeline:latest dbt build --select ecommerce --profiles-dir /app/profiles

# Backfill a date range (staging models default to "latest partition only" —
# see transformation/macros/filter_ingestion_date.sql)
docker run --rm -e AZURE_STORAGE_ACCOUNT_NAME=<account> -e AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dbt-pipeline:latest dbt build --profiles-dir /app/profiles \
  --vars '{"start_date": "2026-07-01", "end_date": "2026-07-20"}'
```

See [transformation/README.md](../../transformation/README.md) for the model layout and
why this doesn't run as a distributed DuckDB cluster.
