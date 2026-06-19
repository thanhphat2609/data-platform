# When to Use: Airbyte vs dlt vs Spark SDP

## Decision matrix

| Criteria | Airbyte CE | dlt | Spark SDP |
|---|---|---|---|
| **Source type** | SaaS APIs (GitHub, Stripe, Salesforce…) | Databases (CDC) + custom REST | Relational DBs via JDBC |
| **Code required** | None — config JSON only | Python (50–100 lines) | Python `@dp.materialized_view` + YAML spec |
| **Scale** | Medium (API rate-limited) | Medium (streaming rows) | Large (millions of rows, parallel JDBC reads) |
| **Schema handling** | Auto (connector-managed) | Auto-inferred + evolvable | Explicit in `@dp.materialized_view` function |
| **Incremental state** | Airbyte connection state API | dlt `pipeline_state` (per-resource cursor) | spark-pipelines checkpoint in `storage:` path |
| **Output format** | Parquet via S3 destination | Parquet (filesystem destination) | Delta Lake (ACID, time-travel, schema enforcement) |
| **Infrastructure** | Server VM + N Worker VMs | Single Python process | Single Spark container (local) or cluster (prod) |
| **Orchestration** | `AirbyteTriggerSyncOperator` | `BashOperator` / `PythonOperator` | `BashOperator` → `spark-pipelines run` |
| **When to choose** | Connector exists, zero dev time | Need CDC or custom pagination | Full table dumps, need Delta Lake downstream |

---

## Concrete examples

### Use Airbyte when…
- The source is a SaaS tool with an existing connector (GitHub, Jira, Shopify, Notion, 300+ others)
- You want raw data landed fast with zero Python code
- Schema evolves over time — Airbyte handles it transparently
- You need scalable worker routing (multiple VMs via `DATA_PLANE_ID`)

### Use dlt when…
- The source is a Postgres/MySQL table and you need **row-level CDC** (`updated_at` merge)
- You're calling a REST API with custom pagination, auth headers, or retry logic
- You want **state-aware incremental** loads — re-runs only fetch new/changed rows
- You need schema inference without writing Spark schemas upfront
- Source doesn't have an Airbyte connector

### Use Spark SDP when…
- Full table dumps from MySQL/Oracle/MSSQL at scale (1M+ rows)
- You need **Delta Lake** semantics downstream: ACID, time-travel, `MERGE`, schema evolution
- You have a medallion pipeline (bronze → silver → gold) driven by `@dp.materialized_view`
- Parallel JDBC reads matter — partition by auto-increment column across multiple tasks
- Downstream consumers are Trino, Athena, Databricks, or any Delta-compatible engine

---

## Anti-patterns

| Don't do this | Do this instead |
|---|---|
| Use Spark SDP for a 1 000-row API response | Use dlt |
| Use dlt for a 100M-row MySQL full dump | Use Spark SDP |
| Use Airbyte for a source with no connector | Write a dlt custom resource |
| Use Airbyte when you need custom pre-landing logic | Use dlt (transform inside `@dlt.resource`) |
| Run Spark SDP as `python main.py` | Use `spark-pipelines run --spec pipeline.yml` |
| Hardcode MinIO credentials in Python | Use `configuration:` block in pipeline YAML or `--conf` overrides |

---

## Tool + layer mapping

```
Source              Tool          Bronze path
─────────────────────────────────────────────────────────────
PostgreSQL CDC      dlt           bronze/dlt/postgres/
REST APIs           dlt           bronze/dlt/api/
MySQL batch         Spark SDP     bronze/sdp/mysql/sales/
GitHub Events       Airbyte CE    bronze/airbyte/github/
Open-Meteo          Airbyte CE    bronze/airbyte/openmeteo/
Exchange Rates      Airbyte CE    bronze/airbyte/exchangerates/
```
