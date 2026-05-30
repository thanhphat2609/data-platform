# Bronze Layer — Folder Convention & Partition Strategy

All raw data lands in `s3://bronze/` with no transformations applied.  
Path structure encodes **tool → source system → stream → partition**.

---

## Folder structure

```
bronze/
│
├── airbyte/                              ← Airbyte CE connector output
│   ├── github/
│   │   └── events/
│   │       └── 2026/05/30/<epoch>/
│   │           └── *.parquet
│   ├── openmeteo/
│   │   └── hourly/
│   │       └── 2026/05/30/<epoch>/
│   │           └── *.parquet
│   └── exchangerates/
│       └── rates/
│           └── 2026/05/30/<epoch>/
│               └── *.parquet
│
├── dlt/                                  ← dlt filesystem destination
│   ├── postgres/                         ←   dataset_name = "postgres"
│   │   ├── orders/
│   │   │   └── _dlt_load_id=abc123/
│   │   │       └── *.parquet
│   │   ├── customers/
│   │   │   └── _dlt_load_id=abc124/
│   │   │       └── *.parquet
│   │   └── products/
│   │       └── _dlt_load_id=abc125/
│   │           └── *.parquet
│   └── api/                              ←   dataset_name = "api"
│       ├── github_events/
│       │   └── _dlt_load_id=abc126/
│       ├── openmeteo_hourly/
│       │   └── _dlt_load_id=abc127/
│       └── exchange_rates/
│           └── _dlt_load_id=abc128/
│
└── spark_sdp/                            ← Delta tables managed by spark-pipelines
    ├── mysql/
    │   └── sales/                        ←   storage: "s3a://bronze/spark_sdp/mysql/sales"
    │       ├── bronze_customers/         ←   @dp.materialized_view(name="bronze_customers")
    │       │   ├── _delta_log/
    │       │   └── part-*.parquet
    │       └── bronze_orders/
    │           ├── _delta_log/
    │           └── part-*.parquet
    └── postgresql/
        └── manage/                       ←   storage: "s3a://bronze/spark_sdp/postgresql/manage"
            ├── bronze_users/             ←   @dp.materialized_view(name="bronze_users")
            │   ├── _delta_log/
            │   └── part-*.parquet
            ├── bronze_projects/
            ├── bronze_tasks/
            └── bronze_issues/
```

---

## Partition key rationale

| Tool | Partition strategy | Rationale |
|---|---|---|
| **airbyte** | `YYYY/MM/DD/<epoch>/` (path, set by destination config) | Per-run snapshot; epoch prevents overwrites on same-day reruns |
| **dlt** | `_dlt_load_id=<uuid>` (managed by dlt) | Each load is atomic; query by load ID to get exactly one consistent snapshot |
| **spark_sdp** | None — spark-pipelines manages Delta state internally | Full table Delta; time-travel via `VERSION AS OF` or `TIMESTAMP AS OF` |

---

## Naming rules

1. **Lowercase** directory names — consistent across all tools
2. **Underscores**, not hyphens, in logical names (`bronze_orders`, not `bronze-orders`)
3. **Date format in paths**: `YYYY/MM/DD` (Airbyte convention) or `_dlt_load_id=` (dlt)
4. **No credentials, PII, or runtime secrets** in paths or filenames
5. **Tool prefix first** (`dlt/`, `spark_sdp/`, `airbyte/`) — makes provenance obvious in MinIO UI

---

## Reading bronze from downstream

### dlt tables (Parquet via Hive partitioning)
```sql
-- Latest load of postgres orders (Trino / Athena)
SELECT * FROM hive.bronze."dlt/postgres/orders"
WHERE "$path" LIKE '%_dlt_load_id=abc123%';

-- All orders regardless of load
SELECT * FROM hive.bronze."dlt/postgres/orders";
```

### Spark SDP tables (Delta Lake)
```sql
-- MySQL ecommerce — current state
SELECT * FROM delta.`s3a://bronze/spark_sdp/mysql/sales/bronze_orders`;

-- PostgreSQL manage — current state
SELECT * FROM delta.`s3a://bronze/spark_sdp/postgresql/manage/bronze_tasks`;

-- Point-in-time read (Delta time-travel)
SELECT * FROM delta.`s3a://bronze/spark_sdp/mysql/sales/bronze_orders`
TIMESTAMP AS OF '2026-05-30 08:00:00';

-- Check history
DESCRIBE HISTORY delta.`s3a://bronze/spark_sdp/mysql/sales/bronze_customers`;
```

### Airbyte files (plain Parquet)
```sql
-- Latest run (Trino)
SELECT * FROM hive.bronze."airbyte/exchangerates/rates/2026/05/30/1748563200/";

-- All exchange rate data (all dates)
SELECT * FROM hive.bronze."airbyte/exchangerates/rates/**";
```
