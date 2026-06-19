# Architecture — data-platform-ingestion

## Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEMS                             │
│                                                                     │
│  PostgreSQL 15 (CDC)   MySQL 8 (JDBC)    GitHub API   Open-Meteo  │
│  orders/customers       sales/orders      Events        Weather    │
│  products               customers         (SaaS)        FX Rates   │
│  manage DB (JDBC)                                                   │
│  users/projects/tasks                                               │
└──────┬──────────────────────┬─────────────────┬──────────┬─────────┘
       │                      │                 │          │
       ▼                      ▼                 ▼          ▼
┌──────────────┐  ┌───────────────────┐  ┌───────────────────────────┐
│     dlt      │  │    Spark SDP      │  │       Airbyte CE          │
│              │  │                   │  │                           │
│ Incremental  │  │ PySpark 4.1       │  │ Server VM + Worker VMs    │
│ CDC + REST   │  │ Declarative       │  │ Connector-based, no code  │
│ State-aware  │  │ Pipelines         │  │ 300+ connectors           │
│ merge/append │  │ @dp.materialized  │  │ Temporal-dispatched       │
│              │  │ _view             │  │                           │
└──────┬───────┘  └─────────┬─────────┘  └──────────────┬────────────┘
       │                    │                            │
       └────────────────────┴────────────────────────────┘
                                     │
                        ┌────────────▼────────────┐
                        │      MinIO (S3-compat)   │
                        │    bucket: bronze/       │
                        │                          │
                        │  dlt/postgres/           │
                        │  dlt/api/                │
                        │  sdp/mysql/        │
                        │  sdp/postgresql/   │
                        │  airbyte/github/         │
                        │  airbyte/openmeteo/      │
                        │  airbyte/exchangerates/  │
                        └──────────────────────────┘
```

---

## Component breakdown

### data-platform-infra (separate repo)
Shared Docker Compose — all ingestion layers depend on this:
- **PostgreSQL 15** — `wal_level=logical`, publication `data_platform_pub` ready for CDC; seeds `platform` DB (ecommerce) + `manage` DB (project mgmt)
- **MySQL 8.0** — binlog ROW format + GTID enabled; seeds `sales` DB
- **MinIO** — S3-compatible object storage, auto-init `bronze` / `silver` / `gold` buckets
- **Spark image** — `apache/spark:4.1.0-python3` + `pyspark[pipelines]` + Delta/S3A/JDBC JARs

### dlt/
Python-native EL for sources needing **incremental state tracking**:
- `postgres_pipeline.py` — reads `orders`, `customers`, `products` with `updated_at` cursor; `merge` disposition deduplicates on re-run
- `api_pipeline.py` — GitHub Events (`append`), Open-Meteo (`append`), Exchange Rates (`append`)
- State persisted in `.dlt/pipelines/` — only new/changed rows loaded on each run
- Filesystem destination → MinIO `s3://bronze/dlt/`

### sdp/
**PySpark 4.1 Declarative Pipelines** (`pyspark[pipelines]`) for bulk batch ingestion:
- `spark-pipelines` CLI reads a YAML spec and discovers Python modules via glob
- Python modules declare views with `@dp.materialized_view` — no custom runner code
- Results are Delta Lake tables stored at the `storage:` path in the spec
- Base image defined in **data-platform-infra** `spark/Dockerfile`
- **Production trigger**: Airflow in `data-platform-orchestration` via `spark-pipelines run`

**Pipelines:**
- `mysql/ecommerce` — MySQL `sales` DB → `bronze_customers`, `bronze_orders`
- `postgresql/manage` — PostgreSQL `manage` DB → `bronze_users`, `bronze_projects`, `bronze_tasks`, `bronze_issues`

### airbyte/
**Airbyte Community Edition** — connector templates only; infrastructure in **data-platform-infra**:
- `connections/` — JSON templates for GitHub Events, Open-Meteo hourly, Exchange Rates daily
- `destination/` — shared MinIO S3-compatible destination config
- Server + worker docker-compose templates live in `data-platform-infra/airbyte/`
- **Production trigger**: Airflow via `AirbyteTriggerSyncOperator`

---

## Data flow per run

```
# Prerequisites (data-platform-infra)
make up               → infra healthy: PostgreSQL, MySQL, MinIO buckets ready
make build-spark      → platform-spark:4.1 image built

# dlt (data-platform-ingestion/dlt/)
python pipelines/postgres_pipeline.py  → bronze/dlt/postgres/<table>/
python pipelines/api_pipeline.py       → bronze/dlt/api/<stream>/

# Spark SDP (data-platform-ingestion/sdp/)
docker exec sdp_demo spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml
  → bronze/sdp/mysql/sales/bronze_customers/
  → bronze/sdp/mysql/sales/bronze_orders/

docker exec sdp_demo spark-pipelines run --spec /opt/spark/pipeline/manage/pipeline.yml
  → bronze/sdp/postgresql/manage/bronze_users/
  → bronze/sdp/postgresql/manage/bronze_projects/
  → bronze/sdp/postgresql/manage/bronze_tasks/
  → bronze/sdp/postgresql/manage/bronze_issues/

# Airbyte → deploy server + workers (data-platform-infra)
#   connections configured via Airbyte UI (templates in airbyte/)
#   Airflow triggers sync → bronze/airbyte/<source>/<stream>/YYYY/MM/DD/
```

---

## Tech stack

| Layer | Technology | Version |
|---|---|---|
| Local orchestration | Makefile | — |
| Object storage | MinIO | RELEASE.2024-01-31 |
| Incremental EL | dlt (data load tool) | 0.5.x |
| Batch ingestion | Apache Spark | 4.1.0 |
| Spark pipelines CLI | pyspark[pipelines] | 4.1.x |
| Table format | Delta Lake | 4.0.0 |
| Connector EL | Airbyte Community Edition | 0.63.0 |
| Workflow engine | Temporal | 1.23.0 |
| Databases | PostgreSQL 15, MySQL 8.0 | — |
| Runtime | Python | 3.11 |
| Container | Docker + Compose | — |

---

## Repo relationships

```
data-platform-infra            (databases, MinIO, Spark image, Airbyte server)
        ↓  provides runtime
data-platform-ingestion  (this)
        ↓  bronze/ Delta + Parquet
data-platform-transformation    (dbt: bronze → silver → gold)
        ↓  gold/ aggregated tables
data-platform-orchestration     (Airflow: schedules all three layers)
        ↓  curated data
data-platform-visualization     (Metabase / Superset: dashboards)
```
