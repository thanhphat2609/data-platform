# airbyte — Connector-based EL (connection templates)

Airbyte Community Edition connection templates for GitHub Events, Open-Meteo weather, and Exchange Rates → MinIO bronze.

The Airbyte server + worker infrastructure lives in [data-platform-infra](https://github.com/thanhphat2609/data-platform-infra) (`airbyte/server/` and `airbyte/worker/`).  
Connection configurations will be applied via the Airbyte UI or API in a later phase.

---

## Connections

```
airbyte/
├── connections/
│   ├── github_events.json        ← GitHub Events → bronze/airbyte/github/
│   ├── openmeteo_hourly.json     ← Open-Meteo weather → bronze/airbyte/openmeteo/
│   └── exchangerates_daily.json  ← Exchange Rates → bronze/airbyte/exchangerates/
└── destination/
    └── minio_s3.json             ← Shared MinIO S3-compatible destination
```

---

## Source → bronze path mapping

| Connection | Source | Bronze path |
|---|---|---|
| `github_events` | GitHub API | `bronze/airbyte/github/events/YYYY/MM/DD/` |
| `openmeteo_hourly` | Open-Meteo (free) | `bronze/airbyte/openmeteo/hourly/YYYY/MM/DD/` |
| `exchangerates_daily` | Exchange Rates API | `bronze/airbyte/exchangerates/rates/YYYY/MM/DD/` |

All connections write **Parquet + SNAPPY** to MinIO via the shared `minio-bronze` destination.

---

## Sync schedules

| Connection | Schedule | Mode |
|---|---|---|
| `github_events` | Every 6 hours | Incremental append-dedup |
| `openmeteo_hourly` | Daily at 01:00 | Incremental append-dedup |
| `exchangerates_daily` | Daily at 02:00 | Incremental append-dedup |

---

## Deploy

1. Start the Airbyte server from **data-platform-infra**:
   ```bash
   cd data-platform-infra
   make airbyte-server
   ```

2. Open Airbyte UI at `http://<server-ip>:8000`

3. Create the MinIO destination using `destination/minio_s3.json` as reference

4. Create source connections using the `connections/*.json` templates

5. Trigger manually or wait for the cron schedule

---

## Orchestration (coming in data-platform-orchestration)

```python
# Airflow DAG will trigger via:
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

sync_github = AirbyteTriggerSyncOperator(
    task_id="airbyte_github_events",
    airbyte_conn_id="airbyte_server",
    connection_id="<github-events-connection-id>",
    asynchronous=False,
)
```
