# sdp — Spark Declarative Pipelines (PySpark 4.1)

Batch ingestion from MySQL and PostgreSQL → Azure Blob Storage bronze using **PySpark 4.1 Declarative Pipelines** (`spark-pipelines` CLI).

YAML-driven spec. Python modules with `@dp.materialized_view`. No custom runner code.

> **Note:** Production Spark runs are triggered by **Dagster** in [`orchestration/`](../../orchestration/README.md).  
> The Spark runtime image lives in [`infrastructure/spark/`](../../infrastructure/spark/README.md) (`Dockerfile.spark`).

---

## Architecture

```
MySQL `ecommerce` DB            PostgreSQL `manage` DB
  ├─ customers                    ├─ users
  ├─ orders                       ├─ projects
  ├─ products       JDBC query    ├─ tasks
  ├─ order_items  ────────────►   ├─ issues
  └─ categories                   └─ comments
           │                             │
    @dp.materialized_view         @dp.materialized_view
    (schema + path + partition    (schema + path + partition
     + incremental query)          + incremental query)
           │                             │
           ▼                             ▼
  wasb://bronze/.../               wasb://bronze/.../
  ingestion/ecommerce/             ingestion/manage/
  partitioned by ingestion_date    partitioned by ingestion_date
```

---

## Pipeline structure

```
sdp/
├── pipelines/
│   ├── schema/
│   │   ├── __init__.py           ← package marker
│   │   ├── ecommerce.py          ← StructType schemas for MySQL ecommerce (5 tables)
│   │   └── manage.py             ← StructType schemas for PostgreSQL manage (5 tables)
│   ├── ecommerce/
│   │   ├── pipeline.yml          ← spark-pipelines spec (Azure Blob Storage)
│   │   └── bronze_ingestion.py   ← 5 @dp.materialized_view with incremental queries
│   └── manage/
│       ├── pipeline.yml          ← spark-pipelines spec (Azure Blob Storage)
│       └── bronze_ingestion.py   ← 5 @dp.materialized_view with incremental queries
└── demo/
    ├── Dockerfile.spark          ← Spark image with hadoop-azure + JDBC JARs
    ├── docker-compose.yml        ← MySQL + PostgreSQL + Azurite + Spark
    └── seed/
        ├── mysql/01_init.sql     ← DDL + seed data for ecommerce DB (5 tables)
        └── postgres/01_init.sql  ← DDL + seed data for manage DB (5 tables)
```

---

## Schema files

Each domain has one schema file that exports `StructType` definitions directly.
`bronze_ingestion.py` imports them via wildcard:

```python
# schema/ecommerce.py — pure StructType definitions, no env vars or config
_customers_schema = StructType([
    StructField("id",             IntegerType(),   True),
    StructField("name",           StringType(),    True),
    ...
    StructField("ingestion_date", DateType(),      True),  # derived — not in MySQL
])
```

```python
# ecommerce/bronze_ingestion.py
from schema.ecommerce import *

_INGESTION_PATH = "wasb://bronze@devstoreaccount1.blob.core.windows.net/ingestion/ecommerce"
_PARTITION_COLS  = ["ingestion_date"]
```

---

## Incremental query pattern

Each `@dp.materialized_view` passes a SQL query to `_read_table` instead of a full
table scan. The query is wrapped as a JDBC subquery: `(SELECT ...) AS t`.

```python
def _read_table(source_table: str, query: str | None = None):
    dbtable = f"({query}) AS t" if query else source_table
    return (
        spark.read.format("jdbc")
        .option("dbtable", dbtable)
        ...
        .withColumn("ingestion_date", F.current_date())  # derived partition column
    )
```

Rolling 30-day window examples:

```python
# MySQL (ecommerce)
def bronze_orders():
    return _read_table(
        "orders",
        query="SELECT * FROM orders WHERE order_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)",
    )

# PostgreSQL (manage)
def bronze_tasks():
    return _read_table(
        "tasks",
        query="SELECT * FROM tasks WHERE created_at >= NOW() - INTERVAL '30 days'",
    )
```

Static reference tables (e.g. `categories`) use `SELECT * FROM table` — still goes
through `_read_table` so the pattern is consistent, no date filter needed.

---

## Derived partition column

`ingestion_date` **does not exist in MySQL or PostgreSQL**. It is added inside
`_read_table()` after the JDBC load:

```python
.withColumn("ingestion_date", F.current_date())
```

Delta partitions files on disk as `ingestion_date=YYYY-MM-DD/`.  
Rule: the column must exist in the DataFrame **at write time**, not in the source system.

---

## Demo (local Docker)

Sources: MySQL (`ecommerce` DB) + PostgreSQL (`manage` DB) with seed data.  
Destination: Azurite (Azure Blob Storage emulator).

### 1. Start the stack

```bash
cd sdp/demo
docker compose up --build -d
```

| Container | Role |
|---|---|
| `sdp_mysql` | MySQL 8.4 — `ecommerce` DB, 5 tables + seed data |
| `sdp_postgres` | PostgreSQL 15 — `manage` DB, 5 tables + seed data |
| `sdp_azurite` | Azure Blob emulator — `bronze` container on host port 10000 |
| `sdp_demo` | Spark 4.1 with hadoop-azure — kept alive for exec |

### 2. Run pipelines

```bash
# MySQL ecommerce → Azurite (5 tables, incremental queries)
docker exec sdp_demo spark-pipelines run \
  --spec /opt/spark/pipeline/ecommerce/pipeline.yml

# PostgreSQL manage → Azurite (5 tables, incremental queries)
docker exec sdp_demo spark-pipelines run \
  --spec /opt/spark/pipeline/manage/pipeline.yml
```

Use `dry-run` instead of `run` to validate the plan without writing data.

---

## Bronze output

```
wasb://bronze@devstoreaccount1.blob.core.windows.net/
└── ingestion/
    ├── ecommerce/                    (MySQL ecommerce DB)
    │   ├── bronze_customers/
    │   ├── bronze_orders/
    │   ├── bronze_categories/
    │   ├── bronze_products/
    │   └── bronze_order_items/
    │       └── ingestion_date=YYYY-MM-DD/
    └── manage/                       (PostgreSQL manage DB)
        ├── bronze_users/
        ├── bronze_projects/
        ├── bronze_tasks/
        ├── bronze_issues/
        └── bronze_comments/
            └── ingestion_date=YYYY-MM-DD/
```

---

## Key packages (in Dockerfile.spark)

| Package | Version | Purpose |
|---|---|---|
| `apache/spark` | 4.1.0-python3 | Base image |
| `pyspark[pipelines]` | 4.1.x | spark-pipelines CLI + @dp.materialized_view |
| `delta-spark_2.13` | 4.0.0 | Delta Lake for Spark 4.1 |
| `hadoop-azure` | 3.4.1 | Azure Blob Storage (wasb://) filesystem |
| `azure-storage` | 8.6.6 | Azure SDK for hadoop-azure |
| `mysql-connector-j` | 8.4.0 | MySQL JDBC driver |
| `postgresql` | 42.7.3 | PostgreSQL JDBC driver |
