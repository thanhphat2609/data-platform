# data-platform

Mono-repo for the full data engineering stack — ingestion, orchestration, and infrastructure in one place.

[![apache-spark](https://img.shields.io/badge/apache--spark-4.1-orange)]()
[![dagster](https://img.shields.io/badge/dagster-latest-blue)]()
[![dlt](https://img.shields.io/badge/dlt-0.5-green)]()
[![delta-lake](https://img.shields.io/badge/delta--lake-4.0-teal)]()
[![medallion](https://img.shields.io/badge/medallion--architecture-grey)]()

---

## Architecture

```
PostgreSQL (CDC)   REST APIs    MySQL (JDBC)   PostgreSQL (JDBC)
      │                │              │                │
      ▼                ▼              ▼                ▼
   [ dlt ]          [ dlt ]     [ Spark SDP ]   [ Spark SDP ]
      │                │              │                │
      └────────────────┴──────────────┴────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    MinIO  (bronze/)      │
                    │   Parquet / Delta Lake   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │       Dagster            │
                    │  schedules · sensors     │
                    │  software-defined assets │
                    └─────────────────────────┘
```

---

## Repo structure

```
data-platform/
│
├── ingestion/                    ← EL pipelines
│   ├── dlt/                      Python-native CDC + REST APIs
│   │   └── demo/                 Demo: MinIO + PostgreSQL + dlt runners + seed data
│   └── sdp/                      Spark Declarative Pipelines (batch JDBC)
│       ├── pipelines/            spark-pipelines specs + @dp.materialized_view
│       └── demo/                 Self-contained demo: MySQL + PostgreSQL + Azurite + Spark
│           └── Dockerfile.spark  Spark image with Delta + JDBC + Azure JARs
│
├── orchestration/                ← Dagster software-defined assets
│   ├── assets/                   Asset definitions (sales, sap)
│   ├── jobs/                     Job wrappers
│   ├── schedules/                Cron schedules
│   ├── resources/                IO managers
│   └── src/                      Business logic
│
└── infrastructure/               ← Deployment + all Dockerfiles
    ├── dagster/                  Docker Compose — webserver, daemon, workers
    │   ├── control/              Master VM (postgres + redis + UI + daemon)
    │   └── worker/               Worker VMs (Celery workers)
    ├── spark/                    Spark 4.1 image (Delta + JDBC + Azure Blob)
    └── dlt/                      dlt runner image (Python 3.11 + dlt deps)
```

---

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, DAGSTER_POSTGRES_PASSWORD
```

### 2. Ingestion — dlt (Docker demo)

```bash
cd ingestion/dlt/demo
docker compose up --build
```

Pipelines exit on completion. Browse results in Azure Storage Explorer at `http://localhost:10000` (Azurite).

```bash
# Re-run individual pipelines
docker compose run --rm dlt-postgres
docker compose run --rm dlt-api
```

### 3. Ingestion — Spark SDP (Docker demo)

```bash
cd ingestion/sdp/demo
docker compose up --build -d

# Run ecommerce pipeline (MySQL → Azurite)
docker exec sdp_demo spark-pipelines run \
  --spec /opt/spark/pipeline/ecommerce/pipeline.yml

# Run manage pipeline (PostgreSQL → Azurite)
docker exec sdp_demo spark-pipelines run \
  --spec /opt/spark/pipeline/manage/pipeline.yml
```

### 4. Orchestration — Dagster (demo, single-node)

```bash
# Set Azure credentials (used by dlt assets)
export AZURE_STORAGE_ACCOUNT_NAME=<your-account>
export AZURE_STORAGE_ACCOUNT_KEY=<your-key>

cd infrastructure/dagster/demo
docker compose up --build -d
```

Open **http://localhost:3000** for the Dagster UI.

> **Production (multi-VM + Celery):**
> ```bash
> docker compose -f infrastructure/dagster/control/docker-compose.yml up -d   # master
> docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d    # workers
> ```

---

## Bronze output paths

| Source | Path | Format |
|--------|------|--------|
| dlt CRM (PostgreSQL platform DB) | `bronze/dlt/crm/<table>/ingestion_date=YYYY-MM-DD/` | Parquet |
| dlt Events (REST APIs) | `bronze/dlt/events/<stream>/ingestion_date=YYYY-MM-DD/` | Parquet |
| Spark MySQL/ecommerce | `bronze/sdp/ecommerce/bronze_<table>/ingestion_date=YYYY-MM-DD/` | Delta |
| Spark PostgreSQL/manage | `bronze/sdp/manage/bronze_<table>/ingestion_date=YYYY-MM-DD/` | Delta |

---

## Layer READMEs

| Layer | README |
|-------|--------|
| Ingestion | [ingestion/README.md](ingestion/README.md) |
| dlt pipelines | [ingestion/dlt/README.md](ingestion/dlt/README.md) |
| Spark SDP pipelines | [ingestion/sdp/README.md](ingestion/sdp/README.md) |
| Orchestration | [orchestration/README.md](orchestration/README.md) |
| Infrastructure | [infrastructure/README.md](infrastructure/README.md) |
