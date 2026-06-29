# dlt — Python-native Incremental EL

Python-native pipelines for sources that need **incremental state tracking**: PostgreSQL CDC and paginated REST APIs.

---

## Why dlt here?

| Decision factor | Choice |
|---|---|
| Schema inference | Auto-detected, evolved across runs |
| Incremental state | Persisted per-resource cursor in `.dlt/pipelines/` |
| Write disposition | `replace` (full load) or `append` (immutable events) |
| Destination | Azure Blob Storage via filesystem destination (adlfs) |
| Credentials | `.dlt/secrets.toml` or env vars (`POSTGRES_CONNECTION_STRING`, etc.) |

---

## Pipelines

### `postgres_pipeline.py` — CRM full load

Reads `orders`, `customers`, `products` from the `platform` DB (CRM) in full on every run (`replace` disposition).

```
bronze/dlt/crm/orders/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
bronze/dlt/crm/customers/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
bronze/dlt/crm/products/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
```

| Table | Disposition |
|---|---|
| orders | replace |
| customers | replace |
| products | replace |

### `api_pipeline.py` — Events (incremental REST APIs)

| Stream | Source | Cursor | Disposition |
|---|---|---|---|
| `earthquake_events` | USGS Earthquake API (free, no key) | `time` | append |
| `openmeteo_hourly` | Open-Meteo (free, no key) | `date` | append |
| `exchange_rates` | Frankfurter / ExchangeRates API | `date` | append |

```
bronze/dlt/events/earthquake_events/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
bronze/dlt/events/openmeteo_hourly/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
bronze/dlt/events/exchange_rates/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
```

---

## Demo (Docker — local run)

The demo compose starts **source infrastructure only** (PostgreSQL with seed data).  
Pipelines are run as one-shot `docker run` commands against the pre-built image.

### 1. Build the dlt image

```bash
# From repo root
docker build -f infrastructure/dlt/Dockerfile -t dlt-pipeline:latest .
```

### 2. Start source infrastructure

```bash
cd ingestion/dlt/demo
docker compose up -d
```

This starts `dlt_postgres` (PostgreSQL 15, seeded with orders / customers / products).

### 3. Run pipelines

```bash
# CRM full load (PostgreSQL → Azure Blob)
docker run --rm \
  --network ingestion_dlt_demo_dlt_network \
  -e POSTGRES_CONNECTION_STRING=postgresql://admin:admin123@postgres:5432/platform \
  -e DESTINATION__FILESYSTEM__BUCKET_URL=az://bronze/dlt \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dlt-pipeline:latest python pipelines/postgres_pipeline.py

# Events incremental load (REST APIs → Azure Blob)
docker run --rm \
  -e DESTINATION__FILESYSTEM__BUCKET_URL=az://bronze/dlt \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dlt-pipeline:latest python pipelines/api_pipeline.py
```

> The `--network` flag connects the dlt container to the same bridge network as `dlt_postgres`
> so it can resolve the hostname `postgres`. The events pipeline hits public APIs — no network flag needed.

### 4. Browse output

Connect Azure Storage Explorer to your storage account (or Azurite) and navigate to the `bronze` container:

```
bronze/
└── dlt/
    ├── crm/
    │   ├── orders/ingestion_date=YYYY-MM-DD/
    │   ├── customers/ingestion_date=YYYY-MM-DD/
    │   └── products/ingestion_date=YYYY-MM-DD/
    └── events/
        ├── earthquake_events/ingestion_date=YYYY-MM-DD/
        ├── openmeteo_hourly/ingestion_date=YYYY-MM-DD/
        └── exchange_rates/ingestion_date=YYYY-MM-DD/
```

---

## Setup (without Docker)

```bash
cd ingestion/dlt
pip install -r requirements.txt

cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Edit `.dlt/secrets.toml`:
```toml
[sources.postgres]
connection_string = "postgresql://admin:admin123@localhost:5432/platform"

[destination.filesystem.credentials]
azure_storage_account_name = "<your-storage-account>"
azure_storage_account_key  = "<your-account-key>"
```

---

## Run (without Docker)

```bash
# PostgreSQL full load
python pipelines/postgres_pipeline.py

# REST APIs (incremental from last cursor)
python pipelines/api_pipeline.py
```

---

## Config reference

| File | Purpose |
|---|---|
| `.dlt/config.toml` | Non-sensitive config: `bucket_url`, `log_level` |
| `.dlt/secrets.toml` | Credentials — gitignored, never commit |
| `config/sources.yml` | Human-readable source reference (not loaded by dlt) |
| `demo/docker-compose.yml` | Source infrastructure only: PostgreSQL with seed data |
