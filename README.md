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
PostgreSQL CRM     REST APIs      MySQL ecommerce   PostgreSQL manage
      │                │                 │                  │
      ▼                ▼                 ▼                  ▼
 [ dlt CRM ]     [ dlt API ]    [ Spark SDP eco ]  [ Spark SDP mgmt ]
  container        container       container           container
      │                │                 │                  │
      └────────────────┴─────────────────┴──────────────────┘
                                    │
                       ┌────────────▼─────────────┐
                       │    Azure Blob  (bronze/)  │
                       │    Parquet / Delta Lake   │
                       └────────────┬─────────────┘

                  ┌─────────────────────────────────────────┐
                  │                Dagster                   │
                  │  ┌──────────┐        ┌───────────────┐  │
                  │  │ daemon   │──Redis──│  worker-1     │  │
                  │  │scheduler │        │  dlt queue    │  │
                  │  └──────────┘        ├───────────────┤  │
                  │  ┌──────────┐        │  worker-2     │  │
                  │  │webserver │        │  SDP queue    │  │
                  │  │    UI    │        └───────────────┘  │
                  │  └──────────┘                           │
                  └─────────────────────────────────────────┘
```

Each pipeline runs as a **short-lived Docker container** launched by Dagster via `docker run`.
Celery routes each run to the worker subscribed to that pipeline's queue — dlt runs go to
`worker-1`, SDP/Spark runs go to `worker-2`.

---

## Execution model

| Pipeline | Image | Queue | Dagster triggers |
|----------|-------|-------|-----------------|
| dlt CRM | `dlt-pipeline:latest` | `worker-1` | `docker run dlt-pipeline:latest python pipelines/postgres_pipeline.py` |
| dlt Events | `dlt-pipeline:latest` | `worker-1` | `docker run dlt-pipeline:latest python pipelines/api_pipeline.py` |
| Spark SDP ecommerce | `spark-pipeline:latest` | `worker-2` | `docker run spark-pipeline:latest spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml` |
| Spark SDP manage | `spark-pipeline:latest` | `worker-2` | `docker run spark-pipeline:latest spark-pipelines run --spec /opt/spark/pipeline/manage/pipeline.yml` |

Adding a new pipeline = one new file in `orchestration/pipelines/` + one import line in `orchestration/definitions.py`.

---

## Repo structure

```
data-platform/
│
├── ingestion/                         ← EL pipeline code — no orchestration logic here
│   ├── dlt/
│   │   └── pipelines/                 postgres_pipeline.py, api_pipeline.py
│   └── sdp/
│       └── pipelines/                 spark-pipelines specs + @dp.materialized_view modules
│
├── orchestration/                     ← Dagster definitions
│   ├── definitions.py                 Registers all assets, jobs, and schedules
│   ├── pipelines/                     One file per pipeline — asset + job + schedule co-located
│   │   ├── dlt_crm.py                 op_tags: worker-1 | schedule: 01:00 ICT
│   │   ├── dlt_api.py                 op_tags: worker-1 | schedule: 01:30 ICT
│   │   ├── sdp_ecommerce.py           op_tags: worker-2 | schedule: 02:00 ICT
│   │   └── sdp_manage.py              op_tags: worker-2 | schedule: 02:30 ICT
│   └── src/
│       └── docker_runner.py           Shared helper: docker run + log streaming into Dagster
│
└── infrastructure/
    ├── dagster/
    │   ├── dagster.yaml               Shared config: Celery executor + PostgreSQL + Azure Blob logs
    │   ├── workspace.yaml             Code location: orchestration/definitions.py
    │   ├── Dockerfile                 Dagster image (webserver, daemon, and workers all use this)
    │   ├── demo/                      NON-PROD: all services on one VM
    │   │   ├── docker-compose.yml     9 containers including 2 Celery workers
    │   │   └── DEMO.md                Step-by-step demo walkthrough
    │   ├── control/                   PROD: control VM (webserver + daemon + Redis + DB)
    │   │   └── docker-compose.yml
    │   └── worker/                    PROD: worker VMs (one compose per worker)
    │       ├── docker-compose-worker1.yml   dlt queue
    │       └── docker-compose-worker2.yml   SDP/Spark queue
    ├── spark/                         Spark 4.1 image (Delta + JDBC + Azure + SDP specs)
    └── dlt/                           dlt runtime image (Python 3.11 + dlt + adlfs)
```

---

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY
```

### 2. Build pipeline images

```bash
docker build -f infrastructure/dlt/Dockerfile          -t dlt-pipeline:latest   .
docker build -f infrastructure/spark/Dockerfile.spark  -t spark-pipeline:latest .
```

### 3. Run a pipeline manually (no Dagster)

```bash
# dlt — CRM full load
docker run --rm \
  -e DESTINATION__FILESYSTEM__BUCKET_URL=az://bronze/dlt \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=<key> \
  -e POSTGRES_CONNECTION_STRING=postgresql://admin:<pw>@<host>:5432/crm \
  dlt-pipeline:latest python pipelines/postgres_pipeline.py

# Spark SDP — ecommerce
docker run --rm \
  -e MYSQL_HOST=<host> -e MYSQL_USER=admin -e MYSQL_PASSWORD=<pw> \
  -e AZURE_STORAGE_ACCOUNT_NAME=<account> -e AZURE_STORAGE_ACCOUNT_KEY=<key> \
  spark-pipeline:latest \
  spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml
```

### 4. Demo — Dagster single-VM (NON-PROD)

All 9 containers (control plane + 2 Celery workers + 3 source DBs) on one machine.

```bash
cp infrastructure/dagster/demo/.env.demo infrastructure/dagster/demo/.env
# Edit .env: set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY

docker compose \
  -f infrastructure/dagster/demo/docker-compose.yml \
  --env-file infrastructure/dagster/demo/.env \
  up --build -d
```

Open **http://localhost:3000**. See [infrastructure/dagster/demo/DEMO.md](infrastructure/dagster/demo/DEMO.md) for the full walkthrough.

### 5. Production — multi-VM with Celery (CI/CD)

Push to `main` — GitHub Actions deploys to all three VMs automatically:

```
push → deploy-control (Control VM)
             ↓
     ┌───────┴────────┐
deploy-worker1    deploy-worker2
 (Worker VM 1)   (Worker VM 2)
```

Each job: SSH → `git pull` → `docker build` → write `.env` from GitHub Secrets → `docker compose up`.

**One-time setup:** add the following secrets in **Settings → Secrets and variables → Actions**:

| Secret | Description |
|--------|-------------|
| `CTRL_VM_IP` | Control VM IP |
| `WORKER1_VM_IP` | Worker VM 1 IP |
| `WORKER2_VM_IP` | Worker VM 2 IP |
| `VM_SSH_USER` | SSH username (e.g. `azureuser`) |
| `CTRL_VM_SSH_KEY` | Control VM private SSH key |
| `WORKER1_VM_SSH_KEY` | Worker VM 1 private SSH key |
| `WORKER2_VM_SSH_KEY` | Worker VM 2 private SSH key |
| `DAGSTER_DB_PASSWORD` | Dagster metadata DB password |
| `AZURE_STORAGE_ACCOUNT_NAME` | Azure Blob account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | Azure Blob account key |
| `POSTGRES_CONNECTION_STRING` | CRM DB (worker-1) |
| `MYSQL_HOST` / `MYSQL_USER` / `MYSQL_PASSWORD` | Ecommerce DB (worker-2) |
| `POSTGRES_HOST` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | Manage DB (worker-2) |

Manual deploy to one VM: **Actions → Deploy Data Platform → Run workflow → choose target**.

See [.github/workflows/deploy.yml](.github/workflows/deploy.yml) and
[infrastructure/dagster/control/README.md](infrastructure/dagster/control/README.md) /
[infrastructure/dagster/worker/README.md](infrastructure/dagster/worker/README.md) for full setup.

---

## Bronze output paths

| Source | Path | Format |
|--------|------|--------|
| dlt CRM (PostgreSQL CRM DB) | `bronze/dlt/crm/<table>/ingestion_date=YYYY-MM-DD/` | Parquet |
| dlt Events (REST APIs) | `bronze/dlt/events/<stream>/ingestion_date=YYYY-MM-DD/` | Parquet |
| Spark SDP ecommerce (MySQL) | `bronze/sdp/ecommerce/bronze_<table>/ingestion_date=YYYY-MM-DD/` | Delta |
| Spark SDP manage (PostgreSQL) | `bronze/sdp/manage/bronze_<table>/ingestion_date=YYYY-MM-DD/` | Delta |

---

## Layer READMEs

| Layer | README |
|-------|--------|
| Ingestion | [ingestion/README.md](ingestion/README.md) |
| dlt pipelines | [ingestion/dlt/README.md](ingestion/dlt/README.md) |
| Spark SDP pipelines | [ingestion/sdp/README.md](ingestion/sdp/README.md) |
| Orchestration | [orchestration/README.md](orchestration/README.md) |
| Infrastructure | [infrastructure/README.md](infrastructure/README.md) |
| Demo walkthrough | [infrastructure/dagster/demo/DEMO.md](infrastructure/dagster/demo/DEMO.md) |
| Control VM setup | [infrastructure/dagster/control/README.md](infrastructure/dagster/control/README.md) |
| Worker VM setup | [infrastructure/dagster/worker/README.md](infrastructure/dagster/worker/README.md) |
