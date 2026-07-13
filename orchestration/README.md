# Orchestration

Dagster software-defined assets — Celery-based distributed execution, daily schedules, and Docker container pipeline triggering.

---

## Project structure

```
orchestration/
├── definitions.py          ← Dagster entry point — registers all assets, jobs, schedules, and Celery executor
├── pipelines/              ← One file per pipeline: asset + job + schedule co-located
│   ├── dlt_crm.py          op_tags: worker-1 | schedule: 01:00 ICT
│   ├── dlt_api.py          op_tags: worker-1 | schedule: 01:30 ICT
│   ├── sdp_ecommerce.py    op_tags: worker-2 | schedule: 02:00 ICT
│   └── sdp_manage.py       op_tags: worker-2 | schedule: 02:30 ICT
└── src/
    └── docker_runner.py    Shared helper: docker run + log streaming into Dagster
```

---

## Asset graph

```
dlt_crm_ingestion       ── worker-1  (docker run dlt-pipeline:latest python pipelines/postgres_pipeline.py)
dlt_api_ingestion       ── worker-1  (docker run dlt-pipeline:latest python pipelines/api_pipeline.py)
sdp_ecommerce_ingestion ── worker-2  (docker run spark-pipeline:latest bash -c "envsubst ... && spark-pipelines run")
sdp_manage_ingestion    ── worker-2  (docker run spark-pipeline:latest bash -c "envsubst ... && spark-pipelines run")
                                │
                                ▼
                   Azure Blob Storage (bronze/)
                   Parquet (dlt) / Delta Lake (SDP)
```

Each asset is independent — no upstream/downstream dependencies between them. All four run in parallel across two Celery workers.

---

## Execution model

| Asset | Queue | Image | Schedule (ICT) | Source |
|-------|-------|-------|----------------|--------|
| `dlt_crm_ingestion` | `worker-1` | `dlt-pipeline:latest` | 01:00 | PostgreSQL CRM DB |
| `dlt_api_ingestion` | `worker-1` | `dlt-pipeline:latest` | 01:30 | REST APIs (USGS, Open-Meteo, Exchange Rates) |
| `sdp_ecommerce_ingestion` | `worker-2` | `spark-pipeline:latest` | 02:00 | MySQL ecommerce DB (5 tables) |
| `sdp_manage_ingestion` | `worker-2` | `spark-pipeline:latest` | 02:30 | PostgreSQL manage DB (5 tables) |

---

## How pipeline containers are triggered

`src/docker_runner.py` provides a single helper used by all pipeline assets:

```python
run_docker_pipeline(
    context,
    image="dlt-pipeline:latest",
    cmd=["python", "pipelines/postgres_pipeline.py"],
    env_vars={"POSTGRES_CONNECTION_STRING": "..."},
)
```

This calls `docker run --rm [-e KEY=VAL ...] [--network <net>] <image> <cmd...>` as a subprocess and streams all container stdout/stderr into the Dagster run log. The container starts, executes the pipeline, and exits — no long-running sidecars.

`DOCKER_NETWORK` env var (set in Compose files) attaches pipeline containers to the same bridge network as source databases.

### SDP / Spark pipelines — envsubst pattern

Spark Declarative Pipelines require Azure credentials in `pipeline.yml` at load time. Since the account name is dynamic, credentials are injected via `envsubst` before `spark-pipelines` reads the spec:

```python
cmd=[
    "bash", "-c",
    "cp -r /opt/spark/pipeline/ecommerce /tmp/ecommerce-pipeline"
    " && envsubst '${AZURE_STORAGE_ACCOUNT_NAME} ${AZURE_STORAGE_ACCOUNT_KEY}'"
    " < /opt/spark/pipeline/ecommerce/pipeline.yml"
    " > /tmp/ecommerce-pipeline/pipeline.yml"
    " && spark-pipelines run --spec /tmp/ecommerce-pipeline/pipeline.yml",
]
```

The pipeline directory is copied to `/tmp` first so the substituted spec stays alongside `bronze_ingestion.py` — `spark-pipelines` resolves the `libraries` glob relative to the spec file's directory.

---

## Celery executor

The Celery executor is configured in `definitions.py` (not `dagster.yaml`):

```python
_celery = celery_executor.configured({
    "broker":  {"env": "CELERY_BROKER_URL"},
    "backend": {"env": "CELERY_RESULT_BACKEND"},
})

defs = Definitions(assets=[...], jobs=[...], schedules=[...], executor=_celery)
```

Assets are routed via `op_tags`:

| Tag | Worker | Assets |
|-----|--------|--------|
| `dagster-celery/queue: worker-1` | `dagster-worker-1` | `dlt_crm_ingestion`, `dlt_api_ingestion` |
| `dagster-celery/queue: worker-2` | `dagster-worker-2` | `sdp_ecommerce_ingestion`, `sdp_manage_ingestion` |

---

## Environment variables

| Variable | Used by | Purpose |
|----------|---------|---------|
| `DLT_IMAGE` | `dlt_crm.py`, `dlt_api.py` | dlt pipeline image (default: `dlt-pipeline:latest`) |
| `SDP_IMAGE` | `sdp_ecommerce.py`, `sdp_manage.py` | Spark SDP image (default: `spark-pipeline:latest`) |
| `DOCKER_NETWORK` | `docker_runner.py` | Bridge network for pipeline containers to reach source DBs |
| `DESTINATION__FILESYSTEM__BUCKET_URL` | dlt pipelines | dlt destination bucket (`az://bronze/dlt`) |
| `DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME` | dlt pipelines | Azure storage account |
| `DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY` | dlt pipelines | Azure storage key |
| `POSTGRES_CONNECTION_STRING` | `dlt_crm.py` | CRM source DB |
| `MYSQL_HOST` / `MYSQL_USER` / `MYSQL_PASSWORD` | `sdp_ecommerce.py` | ecommerce source DB |
| `POSTGRES_HOST` / `POSTGRES_MANAGE_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `sdp_manage.py` | manage source DB |
| `AZURE_STORAGE_ACCOUNT_NAME` | SDP pipelines | Azure account name (substituted into pipeline.yml) |
| `AZURE_STORAGE_ACCOUNT_KEY` | SDP pipelines | Azure account key (substituted into pipeline.yml) |
| `DAGSTER_POSTGRES_URL` | Dagster instance | Metadata DB |
| `CELERY_BROKER_URL` | Dagster + Celery | Redis broker |
| `CELERY_RESULT_BACKEND` | Dagster + Celery | Redis result backend |

---

## Deploy

All deployment is managed via `infrastructure/dagster/` — no manual server setup needed.

```bash
# Control VM (webserver + daemon + Redis + metadata DB)
docker compose -f infrastructure/dagster/control/docker-compose.yml up -d

# Worker VM 1 (dlt queue)
docker compose -f infrastructure/dagster/worker/docker-compose-worker1.yml up -d

# Worker VM 2 (SDP/Spark queue)
docker compose -f infrastructure/dagster/worker/docker-compose-worker2.yml up -d
```

See [infrastructure/dagster/README.md](../infrastructure/dagster/README.md) for the full deployment guide.

---

## Adding a new pipeline

**1. Create `orchestration/pipelines/<name>.py`** with asset + job + schedule:

```python
import os
from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job
from src.docker_runner import run_docker_pipeline

_IMAGE = os.getenv("MY_IMAGE", "my-pipeline:latest")

@asset(
    key_prefix=["my"],
    group_name="my_group",
    op_tags={"dagster-celery/queue": "worker-1"},  # or worker-2
    description="...",
)
def my_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(context, image=_IMAGE, cmd=["python", "my_pipeline.py"], env_vars={...})

job = define_asset_job("my_job", selection=AssetSelection.groups("my_group"))
schedule = ScheduleDefinition(name="daily_my_schedule", job=job, cron_schedule="0 3 * * *", execution_timezone="Asia/Ho_Chi_Minh")
```

**2. Register in `definitions.py`**:

```python
from pipelines import my_module

defs = Definitions(
    assets=[..., my_module.my_ingestion],
    jobs=[..., my_module.job],
    schedules=[..., my_module.schedule],
    executor=_celery,
)
```

---

## Dagster vs Airflow

| Airflow | Dagster (this project) |
|---------|------------------------|
| `BashOperator` / `DockerOperator` | `@asset` + `run_docker_pipeline()` |
| `@dag` / `DAG()` | `define_asset_job` |
| Manual task dependency (`>>`) | Function parameter name = upstream asset key |
| No built-in data lineage | Full asset lineage in UI |
| `queue=` on operator | `op_tags={"dagster-celery/queue": ...}` on asset |

---

## Project Demo

Watch the full orchestration demo on YouTube: [Watch here](https://youtu.be/KRVlAa_19vI)
