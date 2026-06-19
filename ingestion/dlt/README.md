# dlt — Python-native Incremental EL

Python-native pipelines for sources that need **incremental state tracking**: PostgreSQL CDC and paginated REST APIs.

---

## Why dlt here?

| Decision factor | Choice |
|---|---|
| Schema inference | Auto-detected, evolved across runs |
| Incremental state | Persisted per-resource cursor in `.dlt/pipelines/` |
| Write disposition | `merge` (CDC dedup) or `append` (immutable events) |
| Destination | MinIO via S3-compatible filesystem destination |
| Credentials | `.dlt/secrets.toml` or env vars (`POSTGRES_CONNECTION_STRING`, etc.) |

---

## Pipelines

### `postgres_pipeline.py` — PostgreSQL incremental CDC

Reads `orders`, `customers`, `products` via `updated_at` cursor.  
State persists the last seen value — re-runs only fetch new/changed rows.

```
bronze/dlt/postgres/orders/_dlt_load_id=<uuid>/*.parquet
bronze/dlt/postgres/customers/_dlt_load_id=<uuid>/*.parquet
bronze/dlt/postgres/products/_dlt_load_id=<uuid>/*.parquet
```

| Table | PK | Cursor | Disposition |
|---|---|---|---|
| orders | id | updated_at | merge |
| customers | id | updated_at | merge |
| products | id | updated_at | merge |

### `api_pipeline.py` — REST APIs

| Stream | Source | Cursor | Disposition |
|---|---|---|---|
| `github_events` | GitHub Events API | `created_at` | append |
| `openmeteo_hourly` | Open-Meteo (free, no key) | `date` | append |
| `exchange_rates` | Frankfurter / ExchangeRates API | `date` | append |

```
bronze/dlt/api/github_events/_dlt_load_id=<uuid>/*.parquet
bronze/dlt/api/openmeteo_hourly/_dlt_load_id=<uuid>/*.parquet
bronze/dlt/api/exchange_rates/_dlt_load_id=<uuid>/*.parquet
```

---

## Setup

```bash
cd dlt
pip install -r requirements.txt

# Copy and fill secrets
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Edit `.dlt/secrets.toml`:
```toml
[sources.postgres]
connection_string = "postgresql://admin:admin123@localhost:5432/platform"

[destination.filesystem.credentials]
aws_access_key_id     = "minioadmin"
aws_secret_access_key = "minioadmin123"
endpoint_url          = "http://localhost:9000"
region_name           = "us-east-1"
```

Or set equivalent env vars:
```bash
export POSTGRES_CONNECTION_STRING="postgresql://admin:admin123@localhost:5432/platform"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="minioadmin"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="minioadmin123"
export DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL="http://localhost:9000"
```

---

## Run

```bash
# PostgreSQL CDC (incremental from last cursor)
python pipelines/postgres_pipeline.py

# REST APIs (incremental from last date)
python pipelines/api_pipeline.py
```

Re-runs are incremental by default — state is persisted in `.dlt/pipelines/`.  
First run loads all historical data from `initial_value` in the cursor definition.

---

## Config reference

| File | Purpose |
|---|---|
| `.dlt/config.toml` | Non-sensitive config: `bucket_url`, `log_level` |
| `.dlt/secrets.toml` | Credentials — gitignored, never commit |
| `config/sources.yml` | Human-readable source reference (not loaded by dlt) |
