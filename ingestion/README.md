# Ingestion

EL layer — dlt for CDC and REST APIs, Spark SDP for large-scale JDBC batch.

## Tools at a glance

| Tool | Best for | Output format |
|------|----------|---------------|
| [dlt](dlt/README.md) | PostgreSQL CDC · custom REST APIs | Parquet |
| [Spark SDP](sdp/README.md) | Large-scale JDBC batch (MySQL, PostgreSQL) | Delta Lake |

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
                    └─────────────────────────┘
```

---

## Tool decision matrix

| Criteria | dlt | Spark SDP |
|----------|-----|-----------|
| **Source type** | Databases (CDC) + custom REST APIs | Relational DBs via JDBC |
| **Code required** | Python (50–100 lines) | Python `@dp.materialized_view` + YAML spec |
| **Scale** | Medium (streaming rows) | Large (millions of rows, parallel JDBC reads) |
| **Schema handling** | Auto-inferred + evolvable | Explicit StructType in schema file |
| **Incremental state** | `pipeline_state` per-resource cursor | spark-pipelines checkpoint |
| **Output format** | Parquet | Delta Lake (ACID, time-travel) |
| **When to choose** | CDC or custom pagination logic | Full table dumps, Delta Lake downstream |

### Anti-patterns

| Don't | Do instead |
|-------|-----------|
| dlt for a 100M-row MySQL full dump | Spark SDP |
| Spark SDP for a small REST API response | dlt |
| `python main.py` to run Spark SDP | `spark-pipelines run --spec pipeline.yml` |

### Source → tool → bronze mapping

```
Source               Tool        Bronze path
────────────────────────────────────────────────────────
PostgreSQL CDC       dlt         bronze/dlt/postgres/
REST APIs            dlt         bronze/dlt/api/
MySQL batch          Spark SDP   bronze/sdp/ecommerce/
PostgreSQL batch     Spark SDP   bronze/sdp/manage/
```

---

## Bronze layer convention

All raw data lands in `bronze/` with no transformations. Path structure encodes **tool → source → stream → partition**.

```
bronze/
├── dlt/
│   ├── postgres/
│   │   ├── orders/_dlt_load_id=<uuid>/*.parquet
│   │   ├── customers/_dlt_load_id=<uuid>/*.parquet
│   │   └── products/_dlt_load_id=<uuid>/*.parquet
│   └── api/
│       ├── github_events/_dlt_load_id=<uuid>/
│       └── openmeteo_hourly/_dlt_load_id=<uuid>/
│
└── sdp/
    ├── mysql/sales/
    │   ├── bronze_customers/_delta_log/ + part-*.parquet
    │   └── bronze_orders/_delta_log/   + part-*.parquet
    └── postgresql/manage/
        ├── bronze_users/
        ├── bronze_projects/
        ├── bronze_tasks/
        └── bronze_issues/
```

### Partition strategy

| Tool | Partition key | Rationale |
|------|--------------|-----------|
| **dlt** | `_dlt_load_id=<uuid>` | Each load is atomic — query by load ID for a consistent snapshot |
| **Spark SDP** | None (Delta-managed) | Full Delta table — use `VERSION AS OF` or `TIMESTAMP AS OF` for time-travel |

### Naming rules

1. Lowercase directory names throughout
2. Underscores, not hyphens (`bronze_orders`, not `bronze-orders`)
3. Tool prefix first (`dlt/`, `sdp/`) — provenance is visible in MinIO UI
4. No credentials, PII, or secrets in paths

### Reading bronze downstream

```sql
-- dlt (Parquet, Hive partitioning)
SELECT * FROM hive.bronze."dlt/postgres/orders";

-- Spark SDP (Delta Lake — current state)
SELECT * FROM delta.`s3a://bronze/sdp/mysql/sales/bronze_orders`;

-- Spark SDP (Delta time-travel)
SELECT * FROM delta.`s3a://bronze/sdp/mysql/sales/bronze_orders`
TIMESTAMP AS OF '2026-05-30 08:00:00';
```
