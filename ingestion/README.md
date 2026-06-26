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
                    │  Azure Blob (bronze/)    │
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
Source                     Tool        Bronze path
──────────────────────────────────────────────────────────────
PostgreSQL platform DB     dlt         bronze/dlt/crm/
REST APIs                  dlt         bronze/dlt/events/
MySQL ecommerce DB         Spark SDP   bronze/sdp/ecommerce/
PostgreSQL manage DB       Spark SDP   bronze/sdp/manage/
```

---

## Bronze layer convention

All raw data lands in `bronze/` with no transformations.  
Both tools use **Hive-style `ingestion_date=YYYY-MM-DD` partitioning** for consistent downstream reads.

```
bronze/
├── dlt/
│   ├── platform/
│   │   ├── orders/
│   │   │   └── ingestion_date=YYYY-MM-DD/
│   │   │       └── <load_id>.<file_id>.parquet
│   │   ├── customers/
│   │   │   └── ingestion_date=YYYY-MM-DD/
│   │   └── products/
│   │       └── ingestion_date=YYYY-MM-DD/
│   └── api/
│       ├── github_events/
│       │   └── ingestion_date=YYYY-MM-DD/
│       ├── openmeteo_hourly/
│       │   └── ingestion_date=YYYY-MM-DD/
│       └── exchange_rates/
│           └── ingestion_date=YYYY-MM-DD/
│
└── sdp/
    ├── ecommerce/
    │   ├── bronze_customers/
    │   ├── bronze_orders/
    │   ├── bronze_categories/
    │   ├── bronze_products/
    │   └── bronze_order_items/
    │       └── ingestion_date=YYYY-MM-DD/   ← Delta partition
    └── manage/
        ├── bronze_users/
        ├── bronze_projects/
        ├── bronze_tasks/
        ├── bronze_issues/
        └── bronze_comments/
            └── ingestion_date=YYYY-MM-DD/   ← Delta partition
```

### Partition strategy

| Tool | Partition key | Set by |
|------|--------------|--------|
| **dlt** | `ingestion_date=YYYY-MM-DD` | `layout` in `.dlt/config.toml` (`{curr_date}`) |
| **Spark SDP** | `ingestion_date=YYYY-MM-DD` | `F.current_date()` added in `_read_table()` |

### Naming rules

1. Lowercase directory names throughout
2. Underscores, not hyphens (`bronze_orders`, not `bronze-orders`)
3. Tool prefix first (`dlt/`, `sdp/`) — provenance is visible in Azure Storage Explorer
4. No credentials, PII, or secrets in paths

### Reading bronze downstream

```sql
-- dlt (Parquet, date-partitioned)
SELECT * FROM read_parquet(
  'az://bronze/dlt/crm/orders/ingestion_date=2026-06-26/*.parquet'
);

-- Spark SDP (Delta Lake — current state)
SELECT * FROM delta.`abfss://bronze@<account>.dfs.core.windows.net/sdp/ecommerce/bronze_orders`;

-- Spark SDP (Delta time-travel)
SELECT * FROM delta.`abfss://bronze@<account>.dfs.core.windows.net/sdp/ecommerce/bronze_orders`
TIMESTAMP AS OF '2026-06-26 08:00:00';
```
