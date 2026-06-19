# Airbyte — Connector-based EL (connection templates)

Airbyte Community Edition connection templates for GitHub Events, Open-Meteo weather, and Exchange Rates → MinIO bronze.

The Airbyte server + worker infrastructure is managed separately (self-hosted Airbyte CE).  
The JSON files here serve as config references to recreate connections via the Airbyte UI or API.

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

1. Start your Airbyte CE stack (self-hosted)

2. Open Airbyte UI at `http://localhost:8000`

3. Create the MinIO destination using `destination/minio_s3.json` as reference

4. Create source connections using the `connections/*.json` templates

5. Trigger manually or wait for the cron schedule

---

## Orchestration

Airbyte syncs are triggered by Dagster using the `dagster-airbyte` integration:

```python
from dagster_airbyte import AirbyteSyncCompletionState, build_airbyte_assets

airbyte_assets = build_airbyte_assets(
    connection_id="<github-events-connection-id>",
    destination_tables=["github_events"],
)
```

Trigger and monitor status is tracked in the Dagster UI alongside all other assets.  
See [orchestration/README.md](../../orchestration/README.md) for the full orchestration setup.
