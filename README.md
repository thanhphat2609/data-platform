# data-platform-ingestion

EL ingestion layer — dlt (PostgreSQL CDC + REST), Spark SDP (MySQL + PostgreSQL batch), Airbyte CE (GitHub / Open-Meteo / FX) → MinIO bronze.

Part of the data-platform series. Infrastructure (databases, object storage, Spark image) lives in [data-platform-infra](https://github.com/thanhphat2609/data-platform-infra).

[![data-engineering](https://img.shields.io/badge/data--engineering-blue)]()
[![apache-spark](https://img.shields.io/badge/apache--spark-4.1-orange)]()
[![airbyte](https://img.shields.io/badge/airbyte-CE-purple)]()
[![dlt](https://img.shields.io/badge/dlt-0.5-green)]()
[![delta-lake](https://img.shields.io/badge/delta--lake-4.0-teal)]()
[![minio](https://img.shields.io/badge/minio-bronze-red)]()
[![medallion](https://img.shields.io/badge/medallion--architecture-grey)]()

---

## Architecture

```
PostgreSQL (CDC)    MySQL (JDBC)    PostgreSQL (JDBC)   GitHub / Open-Meteo / FX
      │                  │                 │                       │
      ▼                  ▼                 ▼                       ▼
   [ dlt ]         [ Spark SDP ]    [ Spark SDP ]          [ Airbyte CE ]
 Incremental       mysql/ecommerce  postgresql/manage      Connector-based
 CDC + REST        batch reads       batch reads            No-code EL
      │                  │                 │                       │
      └──────────────────┴─────────────────┴───────────────────────┘
                                     │
                        ┌────────────▼────────────┐
                        │   MinIO  (bronze/)       │
                        │  Parquet / Delta Lake    │
                        └──────────────────────────┘
```

See [docs/architecture.md](docs/architecture.md) for the full diagram.

---

## Repo structure

```
data-platform-ingestion/
│
├── dlt/                      ← Python-native incremental EL
│   ├── pipelines/
│   │   ├── postgres_pipeline.py    CDC: orders/customers/products
│   │   └── api_pipeline.py         REST: GitHub/OpenMeteo/FX rates
│   ├── config/sources.yml
│   ├── .dlt/config.toml            Filesystem → MinIO
│   └── requirements.txt
│
├── spark_sdp/                ← PySpark 4.1 Declarative Pipelines
│   ├── pipelines/
│   │   ├── ecommerce/        ← JDBC: MySQL sales DB (customers, orders)
│   │   │   ├── pipeline.yml
│   │   │   └── bronze_ingestion.py
│   │   └── manage/           ← JDBC: PostgreSQL manage DB (users, projects, tasks, issues)
│   │       ├── pipeline.yml
│   │       └── bronze_ingestion.py
│   └── sdp/runner.py         ← local dev helper → calls spark-pipelines CLI
│
├── airbyte/                  ← Connection templates (infra in data-platform-infra)
│   ├── connections/
│   │   ├── github_events.json
│   │   ├── openmeteo_hourly.json
│   │   └── exchangerates_daily.json
│   └── destination/
│       └── minio_s3.json
│
├── docs/
│   ├── architecture.md
│   ├── when-to-use.md
│   └── bronze-convention.md
│
├── .env.example
└── README.md
```

---

## Quick start

```bash
# 1. Start infrastructure (data-platform-infra repo)
cd data-platform-infra
make up          # PostgreSQL :5432 | MySQL :3306 | MinIO :9000/:9001
make build-spark # Build Spark 4.1 image (one-time)

# 2. Configure ingestion
cd data-platform-ingestion
cp .env.example .env
# Edit .env — set GITHUB_TOKEN

# 3. dlt — PostgreSQL CDC + REST APIs
cd dlt
pip install -r requirements.txt
python pipelines/postgres_pipeline.py
python pipelines/api_pipeline.py

# 4. Spark SDP — batch ingestion (uses platform-spark:4.1 image from infra)
python spark_sdp/sdp/runner.py ecommerce --dry-run
python spark_sdp/sdp/runner.py manage    --dry-run

# 5. Airbyte — configure connections via Airbyte UI (templates in airbyte/)
#   Triggers via AirbyteTriggerSyncOperator in data-platform-orchestration
```

---

## Bronze output paths

| Layer | Path | Format |
|---|---|---|
| dlt PostgreSQL CDC | `bronze/dlt/postgres/<table>/_dlt_load_id=*/` | Parquet |
| dlt REST APIs | `bronze/dlt/api/<stream>/_dlt_load_id=*/` | Parquet |
| Spark MySQL/ecommerce | `bronze/spark_sdp/mysql/sales/bronze_<table>/` | Delta |
| Spark PostgreSQL/manage | `bronze/spark_sdp/postgresql/manage/bronze_<table>/` | Delta |
| Airbyte | `bronze/airbyte/<source>/<stream>/YYYY/MM/DD/` | Parquet |

---

## Tool decision guide

| Source | Scale | Tool |
|---|---|---|
| SaaS API (GitHub, Stripe…) | Any | **Airbyte** |
| PostgreSQL CDC (real-time changes) | Medium | **dlt** |
| Custom REST with pagination | Medium | **dlt** |
| MySQL / PostgreSQL full-table dump | Large (1M+ rows) | **Spark SDP** |

→ Full decision matrix: [docs/when-to-use.md](docs/when-to-use.md)

---

## Part of data-platform series

| Repo | Purpose |
|---|---|
| [data-platform-infra](https://github.com/thanhphat2609/data-platform-infra) | Shared infrastructure: databases, MinIO, Spark image, Airbyte |
| **data-platform-ingestion** (this) | EL pipelines: dlt + Spark SDP + Airbyte templates |
| data-platform-transformation | dbt: bronze → silver → gold |
| data-platform-orchestration | Airflow: schedule + monitor |
| data-platform-visualization | Metabase / Superset: dashboards |
