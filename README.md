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
│   ├── dlt/                      Python-native CDC + REST
│   └── sdp/                      Spark Declarative Pipelines (batch)
│
├── orchestration/                ← Dagster software-defined assets
│   ├── assets/                   Asset definitions (sales, sap)
│   ├── jobs/                     Job wrappers
│   ├── schedules/                Cron schedules
│   ├── resources/                IO managers
│   └── src/                      Business logic
│
└── infrastructure/               ← Deployment
    ├── dagster/                  Docker Compose — webserver, daemon, workers
    │   ├── control/              Master VM (postgres + redis + UI + daemon)
    │   └── worker/               Worker VMs (Celery workers)
    └── spark/                    Custom Spark 4.1 image (Delta + JDBC + Azure Blob)
```

---

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, DAGSTER_POSTGRES_PASSWORD
```

### 2. Ingestion — dlt

```bash
cd ingestion/dlt
pip install -r requirements.txt
python pipelines/postgres_pipeline.py
python pipelines/api_pipeline.py
```

### 3. Ingestion — Spark SDP

```bash
cd infrastructure/spark
docker compose up --build -d

docker exec spark_demo spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml
docker exec spark_demo spark-pipelines run --spec /opt/spark/pipeline/manage/pipeline.yml
```

### 4. Orchestration — Dagster

```bash
# Master VM
docker compose -f infrastructure/dagster/control/docker-compose.yml up -d

# Worker VMs (set MASTER_VM_IP in .env)
docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d
```

Open **http://MASTER_VM_IP:3000** for the Dagster UI.

---

## Bronze output paths

| Source | Path | Format |
|--------|------|--------|
| dlt PostgreSQL CDC | `bronze/dlt/postgres/<table>/_dlt_load_id=*/` | Parquet |
| dlt REST APIs | `bronze/dlt/api/<stream>/_dlt_load_id=*/` | Parquet |
| Spark MySQL/ecommerce | `bronze/sdp/mysql/sales/bronze_<table>/` | Delta |
| Spark PostgreSQL/manage | `bronze/sdp/postgresql/manage/bronze_<table>/` | Delta |

---

## Layer READMEs

| Layer | README |
|-------|--------|
| Ingestion | [ingestion/README.md](ingestion/README.md) |
| Orchestration | [orchestration/README.md](orchestration/README.md) |
| Infrastructure | [infrastructure/README.md](infrastructure/README.md) |
