# Orchestration

Dagster software-defined assets — schedules, Celery-based distributed execution, and Azure Blob asset persistence.

## Project structure

```
orchestration/
├── definitions.py          ← Dagster entry point — registers all assets, jobs, schedules, resources
├── assets/
│   ├── dlt_assets.py       dlt_crm_ingestion, dlt_events_ingestion   (docker run dlt-pipeline)
│   ├── sdp_assets.py       sdp_ecommerce_ingestion, sdp_manage_ingestion  (docker run spark-pipeline)
│   ├── sales_assets.py     sales_raw → sales_transformed  (routed to worker-2)
│   └── sap_assets.py       sap_raw → sap_transformed      (routed to worker-1)
├── jobs/
│   └── __init__.py         sales_job, sap_job, dlt_ingestion_job, sdp_ingestion_job
├── schedules/
│   └── __init__.py         daily_sales_schedule, daily_sap_schedule, daily_dlt_schedule, daily_sdp_schedule
├── resources/
│   └── blob_io_manager.py  AzureBlobParquetIOManager
└── src/
    ├── docker_runner.py    Shared helper for pipeline container execution
    ├── sales_processor.py  extract + transform business logic
    └── sap_processor.py    extract + transform business logic
```

## Asset graph

```
dlt_crm_ingestion       ─┐
dlt_events_ingestion    ─┤  (docker run dlt-pipeline:latest python <script>)
                         │
sdp_ecommerce_ingestion ─┤  (docker run spark-pipeline:latest spark-pipelines run --spec ...)
sdp_manage_ingestion    ─┘

sap_raw   ──► sap_transformed      (worker-1, queue: worker-1)
sales_raw ──► sales_transformed    (worker-2, queue: worker-2)
```

`sap_transformed` and `sales_transformed` are persisted as Parquet in Azure Blob automatically via `AzureBlobParquetIOManager`. Downstream assets receive the previous output as a `pd.DataFrame` — no manual read/write needed.

## How pipeline containers are triggered

`src/docker_runner.py` provides a single helper used by all ingestion assets:

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

## Execution model

| Asset | Trigger | Image |
|-------|---------|-------|
| `dlt_crm_ingestion` | `docker run dlt-pipeline:latest python pipelines/postgres_pipeline.py` | `dlt-pipeline:latest` |
| `dlt_events_ingestion` | `docker run dlt-pipeline:latest python pipelines/api_pipeline.py` | `dlt-pipeline:latest` |
| `sdp_ecommerce_ingestion` | `docker run spark-pipeline:latest spark-pipelines run --spec .../ecommerce/pipeline.yml` | `spark-pipeline:latest` |
| `sdp_manage_ingestion` | `docker run spark-pipeline:latest spark-pipelines run --spec .../manage/pipeline.yml` | `spark-pipeline:latest` |

## IO manager

`AzureBlobParquetIOManager` in `resources/blob_io_manager.py`:
- Writes every asset output as `<base_folder>/<asset_key>.parquet` in Azure Blob
- Reads it back and injects as `pd.DataFrame` into downstream assets
- Configured via `AZURE_STORAGE_CONNECTION_STRING` and `AZURE_STORAGE_CONTAINER` env vars

```python
def sap_transformed(sap_raw: pd.DataFrame)  ← IO manager loads sap_raw.parquet automatically
    ...
    return pd.DataFrame(...)                 ← IO manager uploads sap_transformed.parquet automatically
```

## Celery worker routing

Assets are tagged with `op_tags={"dagster-celery/queue": "worker-N"}`. Dagster routes each op to the correct Celery worker — no manual wiring needed.

| Queue | Worker | Assets |
|-------|--------|--------|
| `worker-1` | `dagster-worker-1` | `sap_raw`, `sap_transformed` |
| `worker-2` | `dagster-worker-2` | `sales_raw`, `sales_transformed` |
| `default` | daemon | `dlt_*`, `sdp_*` |

## Environment variables

| Variable | Purpose |
|----------|---------|
| `DLT_IMAGE` | dlt pipeline image reference (default: `dlt-pipeline:latest`) |
| `SDP_IMAGE` | Spark SDP pipeline image reference (default: `spark-pipeline:latest`) |
| `DOCKER_NETWORK` | Docker network to attach pipeline containers to (set in Compose files) |
| `DESTINATION__FILESYSTEM__BUCKET_URL` | dlt destination bucket URL |
| `DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME` | Azure storage account |
| `DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY` | Azure storage key |
| `POSTGRES_CONNECTION_STRING` | dlt CRM source DB |
| `MYSQL_HOST` / `MYSQL_USER` / `MYSQL_PASSWORD` | SDP ecommerce source |
| `POSTGRES_HOST` / `POSTGRES_MANAGE_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | SDP manage source |
| `AZURE_STORAGE_CONNECTION_STRING` | Blob IO manager — read/write asset Parquet outputs |
| `AZURE_STORAGE_CONTAINER` | Container name for asset outputs |
| `DAGSTER_POSTGRES_URL` | Dagster metadata DB (injected by infrastructure/dagster) |
| `CELERY_BROKER_URL` | Redis broker (injected by infrastructure/dagster) |
| `CELERY_RESULT_BACKEND` | Redis result backend (injected by infrastructure/dagster) |

## Deploy

All deployment is managed via `infrastructure/dagster/` — no manual server setup needed.

```bash
# Master VM
docker compose -f infrastructure/dagster/control/docker-compose.yml up -d

# Worker VM (set MASTER_VM_IP in .env)
docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d
```

See [infrastructure/dagster/README.md](../infrastructure/dagster/README.md) for the full deployment guide.

## Adding a new pipeline

### New dlt pipeline

**1. Pipeline file** — add `ingestion/dlt/pipelines/<name>_pipeline.py` with a `__main__` block:

```python
@dlt.source(name="my_source")
def my_source(): ...

if __name__ == "__main__":
    pipeline = dlt.pipeline("my_ingestion", destination="filesystem", dataset_name="my")
    print(pipeline.run(my_source()))
```

**2. Rebuild dlt image** — `docker build -f infrastructure/dlt/Dockerfile -t dlt-pipeline:latest .`

**3. Dagster asset** — add to `assets/dlt_assets.py`:

```python
@asset(key_prefix=["dlt"], group_name="dlt_ingestion")
def dlt_my_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(context, image=_DLT_IMAGE, cmd=["python", "pipelines/my_pipeline.py"], env_vars=_dlt_env())
```

**4. Register** in `jobs/__init__.py`, `schedules/__init__.py`, and `definitions.py`.

### New Spark SDP pipeline

**1. Pipeline spec** — add `ingestion/sdp/pipelines/<name>/pipeline.yml` + `bronze_ingestion.py`

**2. Rebuild Spark image** — `docker build -f infrastructure/spark/Dockerfile.spark -t spark-pipeline:latest .`

**3. Dagster asset** — add to `assets/sdp_assets.py`:

```python
@asset(key_prefix=["sdp"], group_name="sdp_ingestion")
def sdp_my_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(context, image=_SDP_IMAGE,
        cmd=["spark-pipelines", "run", "--spec", f"{_SPEC_BASE}/my/pipeline.yml"],
        env_vars=_sdp_env())
```

**4. Register** in `jobs/__init__.py`, `schedules/__init__.py`, and `definitions.py`.

## Dagster vs Airflow

| Airflow | Dagster (this project) |
|---------|------------------------|
| `BashOperator` / `DockerOperator` | `@asset` + `run_docker_pipeline()` |
| `@dag` / `DAG()` | `define_asset_job` |
| XCom for passing data | Function parameter (IO manager loads from blob) |
| Manual task dependency (`>>`) | Function parameter name = upstream asset name |
| No built-in data lineage | Full asset lineage in UI |
| `queue=` on operator | `op_tags={"dagster-celery/queue": ...}` on asset |
