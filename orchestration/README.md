# Orchestration

Dagster software-defined assets — schedules, Celery-based distributed execution, and Azure Blob asset persistence.

## Project structure

```
orchestration/
├── definitions.py          ← Dagster entry point — registers all assets, jobs, schedules, resources
├── assets/
│   ├── sales_assets.py     sales_raw → sales_transformed  (routed to worker-2)
│   └── sap_assets.py       sap_raw → sap_transformed      (routed to worker-1)
├── jobs/
│   └── __init__.py         sales_job, sap_job
├── schedules/
│   └── __init__.py         daily_sales_schedule, daily_sap_schedule
├── resources/
│   └── blob_io_manager.py  AzureBlobParquetIOManager
└── src/
    ├── sales_processor.py  extract + transform business logic
    └── sap_processor.py    extract + transform business logic
```

## Asset graph

```
sap_raw   ──► sap_transformed      (worker-1, queue: worker-1)
sales_raw ──► sales_transformed    (worker-2, queue: worker-2)
```

Each asset is persisted as Parquet in Azure Blob Storage automatically via `AzureBlobParquetIOManager`. Downstream assets receive the previous asset's output as a `pd.DataFrame` parameter — no manual read/write needed.

## IO manager

`AzureBlobParquetIOManager` in `resources/blob_io_manager.py`:
- Writes every asset output as `<base_folder>/<asset_key>.parquet` in Azure Blob
- Reads it back and injects as `pd.DataFrame` into downstream assets
- Configured via `AZURE_STORAGE_CONNECTION_STRING` and `AZURE_STORAGE_CONTAINER` env vars

```
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

## Environment variables

| Variable | Purpose |
|----------|---------|
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

**1. Business logic** — add `src/<name>_processor.py`

**2. Assets** — add `assets/<name>_assets.py`, set `op_tags` to route to the right queue:

```python
@asset(key_prefix=["crm"], group_name="crm", op_tags={"dagster-celery/queue": "worker-2"})
def crm_raw() -> pd.DataFrame: ...

@asset(key_prefix=["crm"], group_name="crm", op_tags={"dagster-celery/queue": "worker-2"})
def crm_qualified(crm_raw: pd.DataFrame) -> pd.DataFrame: ...
```

**3. Register assets** in `assets/__init__.py`

**4. Add job** in `jobs/__init__.py` using `define_asset_job`

**5. Add schedule** in `schedules/__init__.py` using `ScheduleDefinition`

**6. Wire** all three into `definitions.py` under `assets=`, `jobs=`, `schedules=`

## Dagster vs Airflow

| Airflow | Dagster (this project) |
|---------|------------------------|
| `@task` / `PythonOperator` | `@asset` |
| `@dag` / `DAG()` | `define_asset_job` |
| XCom for passing data | Function parameter (IO manager loads from blob) |
| Manual task dependency (`>>`) | Function parameter name = upstream asset name |
| No built-in data lineage | Full asset lineage in UI |
| `queue=` on operator | `op_tags={"dagster-celery/queue": ...}` on asset |
