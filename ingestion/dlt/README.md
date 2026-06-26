# dlt — Python-native Incremental EL

Python-native pipelines for sources that need **incremental state tracking**: PostgreSQL CDC and paginated REST APIs.

---

## Why dlt here?

| Decision factor | Choice |
|---|---|
| Schema inference | Auto-detected, evolved across runs |
| Incremental state | Persisted per-resource cursor in `.dlt/pipelines/` |
| Write disposition | `replace` (full load) or `append` (immutable events) |
| Destination | MinIO via S3-compatible filesystem destination |
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
| `github_events` | GitHub Events API | `created_at` | append |
| `openmeteo_hourly` | Open-Meteo (free, no key) | `date` | append |
| `exchange_rates` | Frankfurter / ExchangeRates API | `date` | append |

```
bronze/dlt/events/github_events/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
bronze/dlt/events/openmeteo_hourly/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
bronze/dlt/events/exchange_rates/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
```

---

## Demo (Docker — local run)

Self-contained stack — no external dependencies needed.  
Source: PostgreSQL `platform` DB with seed data.  
Destination: MinIO (S3-compatible) on ports 9000/9001.

### 1. Start the stack

```bash
cd ingestion/dlt/demo
docker compose up --build
```

| Container | Role |
|---|---|
| `dlt_minio` | MinIO — S3-compatible `bronze/` bucket (console: localhost:9001) |
| `dlt_minio_init` | One-shot: creates `bronze` bucket in MinIO |
| `dlt_postgres` | PostgreSQL 15 — `platform` DB with seed data (orders, customers, products) |
| `dlt_postgres_pipeline` | Runs `postgres_pipeline.py` → exits on completion |
| `dlt_api_pipeline` | Runs `api_pipeline.py` → exits on completion |

Both pipeline containers exit with code 0 on success.

### 2. Check pipeline output

```bash
# View logs for each pipeline
docker compose logs dlt-postgres
docker compose logs dlt-api
```

### 3. Browse output in Azurite

Connect Azure Storage Explorer to Azurite:
- **Connection string:** `DefaultEndpointsProtocol=http;AccountName=dataplatformstrorage;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMw==;BlobEndpoint=http://localhost:10000/dataplatformstrorage;`

Data lands in the `bronze` container (Hive-partitioned by `ingestion_date`):
```
bronze/
└── dlt/
    ├── platform/
    │   ├── orders/ingestion_date=YYYY-MM-DD/<load_id>.<file_id>.parquet
    │   ├── customers/ingestion_date=YYYY-MM-DD/
    │   └── products/ingestion_date=YYYY-MM-DD/
    └── api/
        ├── github_events/ingestion_date=YYYY-MM-DD/
        ├── openmeteo_hourly/ingestion_date=YYYY-MM-DD/
        └── exchange_rates/ingestion_date=YYYY-MM-DD/
```

### 4. Optional: GitHub token (higher API rate limit)

```bash
GITHUB_TOKEN=ghp_... docker compose up --build
```

### Re-running pipelines

```bash
cd ingestion/dlt/demo

# Re-run after initial `docker compose up`
docker compose run --rm dlt-postgres
docker compose run --rm dlt-api
```

Incremental state is persisted in the `dlt_state` Docker volume — re-runs fetch only new data from the last cursor position.

---

## Setup (without Docker)

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

# Production: real Azure Blob Storage
[destination.filesystem.credentials]
azure_storage_account_name = "<your-storage-account>"
azure_storage_account_key  = "<your-account-key>"
```

For local Azurite, set this env var instead (adlfs picks it up automatically):
```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;\
  AccountName=dataplatformstrorage;\
  AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMw==;\
  BlobEndpoint=http://localhost:10000/dataplatformstrorage;"
export POSTGRES_CONNECTION_STRING="postgresql://admin:admin123@localhost:5432/platform"
```

---

## Run (without Docker)

```bash
# PostgreSQL full load
python pipelines/postgres_pipeline.py

# REST APIs (incremental from last date)
python pipelines/api_pipeline.py
```

---

## Config reference

| File | Purpose |
|---|---|
| `.dlt/config.toml` | Non-sensitive config: `bucket_url`, `log_level` |
| `.dlt/secrets.toml` | Credentials — gitignored, never commit |
| `config/sources.yml` | Human-readable source reference (not loaded by dlt) |
| `demo/docker-compose.yml` | Full demo stack: MinIO + PostgreSQL + dlt runners |
