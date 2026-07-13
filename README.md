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

| Pipeline | Image | Queue | Schedule (ICT) |
|----------|-------|-------|----------------|
| dlt CRM | `dlt-pipeline:latest` | `worker-1` | 01:00 |
| dlt Events | `dlt-pipeline:latest` | `worker-1` | 01:30 |
| Spark SDP ecommerce | `spark-pipeline:latest` | `worker-2` | 02:00 |
| Spark SDP manage | `spark-pipeline:latest` | `worker-2` | 02:30 |

Spark SDP pipelines substitute Azure credentials into `pipeline.yml` at runtime via `envsubst`
before calling `spark-pipelines run` — this is the only way to pass dynamic account names into
the ABFS Hadoop filesystem driver inside declarative Spark pipelines.

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
│           ├── ecommerce/             pipeline.yml (envsubst placeholders) + bronze_ingestion.py
│           └── manage/                pipeline.yml (envsubst placeholders) + bronze_ingestion.py
│
├── orchestration/                     ← Dagster definitions
│   ├── definitions.py                 Registers all assets, jobs, schedules, and Celery executor
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
    │   ├── dagster.yaml               Instance config: PostgreSQL storage + Azure Blob compute logs
    │   ├── workspace.yaml             Code location: orchestration/definitions.py
    │   ├── Dockerfile                 Dagster image (webserver, daemon, and workers all use this)
    │   ├── demo/                      Source databases for local testing (non-prod)
    │   │   ├── docker-compose.yml     3 source DB containers (CRM:5431, ecommerce:3306, manage:5433)
    │   │   └── DEMO.md                Step-by-step demo walkthrough
    │   ├── control/                   PROD: control VM (webserver + daemon + Redis + DB)
    │   │   └── docker-compose.yml
    │   └── worker/                    PROD: worker VMs (one compose per worker)
    │       ├── docker-compose-worker1.yml   dlt queue
    │       └── docker-compose-worker2.yml   SDP/Spark queue
    ├── spark/                         Spark 4.1 image (Delta + JDBC + Azure + SDP specs + envsubst)
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

# Spark SDP — ecommerce (envsubst injects account name/key into pipeline.yml at runtime)
docker run --rm \
  -e MYSQL_HOST=<host> -e MYSQL_USER=admin -e MYSQL_PASSWORD=<pw> \
  -e AZURE_STORAGE_ACCOUNT_NAME=<account> -e AZURE_STORAGE_ACCOUNT_KEY=<key> \
  spark-pipeline:latest \
  bash -c "envsubst '\${AZURE_STORAGE_ACCOUNT_NAME} \${AZURE_STORAGE_ACCOUNT_KEY}' \
    < /opt/spark/pipeline/ecommerce/pipeline.yml > /tmp/pipeline.yml \
    && spark-pipelines run --spec /tmp/pipeline.yml"
```

### 4. Start source databases (testing / demo)

The `demo/` compose contains only the 3 source databases used by the ingestion pipelines.
Start them on the control VM before triggering any pipeline runs:

```bash
# Via GitHub Actions (recommended)
# Actions → Deploy Source Databases → Run workflow

# Or manually on the control VM
docker compose -f infrastructure/dagster/demo/docker-compose.yml up -d
```

| Container | Port | DB | Credentials |
|-----------|------|----|-------------|
| `demo_postgres_crm` | `5431` | `crm` | `admin` / `admin123` |
| `demo_mysql_ecommerce` | `3306` | `ecommerce` | `admin` / `admin123` |
| `demo_postgres_manage` | `5433` | `manage` | `admin` / `admin123` |

See [infrastructure/dagster/demo/DEMO.md](infrastructure/dagster/demo/DEMO.md) for the full walkthrough.

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

**One-time setup:** add the following 11 secrets in **Settings → Secrets and variables → Actions**:

| # | Secret | Description |
|---|--------|-------------|
| 1 | `GH_TOKEN` | GitHub Personal Access Token (`repo` scope) — VMs use this to `git fetch` |
| 2 | `CTRL_VM_IP` | Master / Control VM public IP |
| 3 | `WORKER1_VM_IP` | Worker VM 1 public IP |
| 4 | `WORKER2_VM_IP` | Worker VM 2 public IP |
| 5 | `VM_SSH_USER` | SSH username on all VMs (e.g. `azureuser`) |
| 6 | `CTRL_VM_SSH_KEY` | Control VM private SSH key |
| 7 | `WORKER1_VM_SSH_KEY` | Worker VM 1 private SSH key |
| 8 | `WORKER2_VM_SSH_KEY` | Worker VM 2 private SSH key |
| 9 | `DAGSTER_DB_PASSWORD` | Dagster metadata DB password |
| 10 | `AZURE_STORAGE_ACCOUNT_NAME` | Azure Blob Storage account name |
| 11 | `AZURE_STORAGE_ACCOUNT_KEY` | Azure Blob Storage account key |

> Source database credentials are **not secrets** — demo containers use fixed credentials (`admin` / `admin123`).
> The workflow constructs JDBC connection strings from `CTRL_VM_IP` and hardcoded demo credentials inline.

**Workflows:**

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `deploy.yml` | Push to `main` (auto) or manual | Deploy Dagster + Celery workers to all 3 VMs |
| `deploy-sources.yml` | Manual only | Start source database containers on control VM |

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
