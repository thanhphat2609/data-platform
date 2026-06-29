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
  container        container     container        container
      │                │              │                │
      └────────────────┴──────────────┴────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Azure Blob  (bronze/)  │
                    │   Parquet / Delta Lake   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │         Dagster          │
                    │  schedules · sensors     │
                    │  docker run per pipeline │
                    └─────────────────────────┘
```

Each pipeline runs as a **short-lived Docker container** triggered by Dagster via `docker run`.
The image is the runtime (libraries + pipeline code). No long-running sidecar processes.

---

## Execution model

| Pipeline type | Image | Dagster triggers |
|--------------|-------|-----------------|
| dlt CRM | `dlt-pipeline:latest` | `docker run dlt-pipeline:latest pipelines/postgres_pipeline.py` |
| dlt Events | `dlt-pipeline:latest` | `docker run dlt-pipeline:latest pipelines/api_pipeline.py` |
| Spark SDP ecommerce | `spark-pipeline:latest` | `docker run spark-pipeline:latest spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml` |
| Spark SDP manage | `spark-pipeline:latest` | `docker run spark-pipeline:latest spark-pipelines run --spec /opt/spark/pipeline/manage/pipeline.yml` |

Adding a new pipeline = add a script (dlt) or a spec folder (SDP) and rebuild the image. No orchestration code changes needed.

---

## Repo structure

```
data-platform/
│
├── ingestion/                    ← EL pipelines (pipeline code only — no orchestration logic)
│   ├── dlt/                      Python-native CDC + REST APIs
│   │   └── pipelines/            postgres_pipeline.py, api_pipeline.py (each has __main__)
│   └── sdp/                      Spark Declarative Pipelines (batch JDBC)
│       └── pipelines/            spark-pipelines specs + @dp.materialized_view modules
│
├── orchestration/                ← Dagster software-defined assets
│   ├── assets/                   Asset definitions (dlt, sdp, sales, sap)
│   ├── jobs/                     Job wrappers
│   ├── schedules/                Cron schedules
│   ├── resources/                IO managers
│   └── src/
│       ├── docker_runner.py      Shared helper: subprocess docker run + log streaming
│       ├── sales_processor.py
│       └── sap_processor.py
│
└── infrastructure/               ← Dockerfiles + Docker Compose configs
    ├── dagster/                  Dagster image + control/worker/demo Compose files
    │   ├── control/              Master VM (postgres + redis + webserver + daemon)
    │   └── worker/               Worker VMs (Celery workers)
    ├── spark/                    Spark 4.1 image (Delta + JDBC + Azure Blob + SDP specs)
    └── dlt/                      dlt runtime image (Python 3.11 + dlt deps)
```

---

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY
```

### 2. Build pipeline images (once, or on pipeline changes)

```bash
# From repo root
docker build -f infrastructure/dlt/Dockerfile   -t dlt-pipeline:latest   .
docker build -f infrastructure/spark/Dockerfile.spark -t spark-pipeline:latest .
```

### 3. Run a pipeline manually

```bash
# DLT — CRM full load
docker run --rm \
  -e DESTINATION__FILESYSTEM__BUCKET_URL=az://bronze/dlt \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=<key> \
  -e POSTGRES_CONNECTION_STRING=postgresql://... \
  dlt-pipeline:latest pipelines/postgres_pipeline.py

# Spark SDP — ecommerce
docker run --rm \
  -e MYSQL_HOST=<host> -e MYSQL_USER=admin -e MYSQL_PASSWORD=admin123 \
  spark-pipeline:latest \
  spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml
```

### 4. Orchestration — Dagster demo (single-node)

```bash
# Build the Dagster image
docker build -f infrastructure/dagster/Dockerfile -t dagster:latest .

# Set Azure credentials (used by dlt assets at runtime)
export AZURE_STORAGE_ACCOUNT_NAME=<your-account>
export AZURE_STORAGE_ACCOUNT_KEY=<your-key>

cd infrastructure/dagster/demo
docker compose up -d
```

Open **http://localhost:3000** for the Dagster UI. Dagster will trigger pipeline containers
via `docker run` using the socket mounted at `/var/run/docker.sock`.

> **Production (multi-VM + Celery):**
> ```bash
> docker compose -f infrastructure/dagster/control/docker-compose.yml up -d   # master
> docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d    # workers
> ```
> Set `DLT_IMAGE` and `SDP_IMAGE` env vars to point at your registry, e.g.
> `DLT_IMAGE=myregistry.azurecr.io/dlt-pipeline:v1.2`

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
