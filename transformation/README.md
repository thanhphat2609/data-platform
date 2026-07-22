# Transformation

T layer — dbt + DuckDB, reading bronze Parquet/Delta straight off Azure Blob and writing
silver/gold back out the same way. No warehouse, no cluster.

## Why not a distributed DuckDB cluster

DuckDB is an embedded, single-process OLAP engine — there's no shuffle, no distributed query
planner, no clustering. Its speed comes from vectorized execution and automatic multi-core
parallelism *within one process*, plus reading Parquet/Delta with predicate/projection
pushdown straight off Blob storage. A "master + worker" DuckDB setup (mirroring the
[Dagster control/worker VMs](../infrastructure/dagster/README.md)) has nothing for the master
to coordinate — there's no distributed executor underneath to route work to.

So this scales **vertically** (cores + RAM on one box) and runs as an ephemeral container
launched by Dagster, same as `dlt-pipeline` / `spark-pipeline` — see
[infrastructure/dbt/](../infrastructure/dbt/README.md). If a model set ever outgrows one VM,
that's the signal to route it to Spark (already in this stack), not to stretch DuckDB past
what it's built for.

## Model layout

Organized **domain → entity → layer**, not by layer first. Each entity folder is
self-contained: source definition, staging, and (where real transformation logic exists)
intermediate + mart models all live together.

```
models/
├── crm/                        ← dlt, Parquet, bronze/dlt/crm/
│   ├── customers/
│   ├── orders/
│   └── products/
├── events/                     ← dlt, Parquet, bronze/dlt/events/ (append-only time series)
│   ├── earthquakes/
│   ├── weather/
│   └── exchange_rates/
├── ecommerce/                  ← Spark SDP, Delta, bronze/sdp/ecommerce/
│   ├── customers/
│   ├── categories/
│   ├── products/
│   ├── order_items/
│   └── orders/                 ← reference: stg -> int -> mart (external, writes gold/)
└── manage/                     ← Spark SDP, Delta, bronze/sdp/manage/
    ├── users/
    ├── projects/
    ├── tasks/
    ├── issues/
    └── comments/
```

`ecommerce/orders/` is the full pattern to copy when an entity needs it:

| File | Layer | Materialization |
|------|-------|-----------------|
| `stg_ecommerce__orders.sql` | staging | `view` (default) — 1:1 rename/cast off the bronze source |
| `int_ecommerce__orders.sql` | intermediate | `table` — rolls up `order_items` onto each order |
| `mart_ecommerce__fct_orders.sql` | mart | `external` — joins in customer attributes, writes Parquet to `gold/ecommerce/fct_orders` |

Every other entity currently has staging only — add `int_`/`mart_` files the same way once
there's real business logic to put there. Don't add empty intermediate layers just to match
the shape; a passthrough `int_` model that does nothing is dead weight.

## Reading bronze without a warehouse

Sources point directly at bronze files via `meta.external_location` — `dbt-duckdb` turns
that into a view over `read_parquet(...)` (dlt output, Hive-partitioned by `ingestion_date`)
or `delta_scan(...)` (Spark SDP output, already current-state). See any `_*.yml` file next to
a staging model, e.g. [`models/ecommerce/orders/_ecommerce__orders.yml`](models/ecommerce/orders/_ecommerce__orders.yml).

## Filtering by ingestion_date

[`macros/filter_ingestion_date.sql`](macros/filter_ingestion_date.sql) is used in every staging
model's `where` clause (except `events/*`, which is append-only — every partition is a real
historical record, not a snapshot to replace):

```sql
with source as (
    select * from {{ source('ecommerce', 'orders') }}
)

select ...
from source
where {{ filter_ingestion_date('ingestion_date') }}
```

- No `--vars` → latest partition only (correct default: CRM/ecommerce/manage bronze is a
  full-refresh snapshot on every run, so "latest" is "current state").
- `--vars '{"start_date": "...", "end_date": "..."}'` → filters `ingestion_date` to that
  range instead, for backfills/reprocessing.

## Running locally

```bash
cd transformation
pip install -r requirements.txt
AZURE_STORAGE_ACCOUNT_NAME=<account> AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dbt build --profiles-dir profiles
```

Or via the container image — see [infrastructure/dbt/README.md](../infrastructure/dbt/README.md).

## Orchestration

Split into two Dagster assets that run **in parallel on the workers that already own their
bronze data**, instead of one combined job waiting on both ingestion tools:

| Asset | `dbt build --select` | Queue | Schedule (ICT) |
|-------|----------------------|-------|-----------------|
| `dbt_transform_dlt` | `crm events` | `worker-1` | 02:00, after dlt (01:00/01:30) |
| `dbt_transform_sdp` | `ecommerce manage` | `worker-2` | 03:00, after Spark SDP (02:00/02:30) |

Neither runs on a new VM — each shares the worker its source ingestion tool already runs on
(`dlt` + `dbt_transform_dlt` on worker-1, Spark SDP + `dbt_transform_sdp` on worker-2). See
[orchestration/pipelines/dbt_transform_dlt.py](../orchestration/pipelines/dbt_transform_dlt.py)
and [orchestration/pipelines/dbt_transform_sdp.py](../orchestration/pipelines/dbt_transform_sdp.py).

Note `--profiles-dir /app/profiles` there is an absolute path — the container's `WORKDIR` is
`/app` (see [infrastructure/dbt/Dockerfile](../infrastructure/dbt/Dockerfile)), so this doesn't
depend on `docker run` inheriting a particular cwd. Running locally from this folder, the
relative `profiles` path above works because your shell's cwd already is `transformation/`.
