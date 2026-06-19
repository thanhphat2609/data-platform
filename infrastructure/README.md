# Infrastructure

Deployment configs for all platform services.

## What's here

| Folder | Purpose |
|--------|---------|
| [`dagster/`](dagster/README.md) | Dagster webserver, daemon, and Celery workers via Docker Compose |
| [`spark/`](spark/README.md) | Custom Spark 4.1 image bundled with Delta Lake, Azure Blob, and JDBC drivers |

## Architecture

```
┌─── Master VM ──────────────────────────────┐
│  dagster-webserver     :3000               │
│  dagster-daemon                            │
│  postgres              :5432  (metadata)   │ ◄── worker VMs connect here
│  redis                 :6379  (broker)     │ ◄── worker VMs connect here
└────────────────────────────────────────────┘

┌─── Worker VM-1 ─────────┐   ┌─── Worker VM-2 ─────────┐
│  dagster-worker-1        │   │  dagster-worker-2        │
│  queue: worker-1         │   │  queue: worker-2         │
└──────────────────────────┘   └──────────────────────────┘

┌─── Any VM / CI ─────────┐
│  Spark 4.1 container     │   ← used by ingestion/sdp/ pipelines
└──────────────────────────┘
```

## Quick reference

```bash
# Dagster — master VM
docker compose -f dagster/control/docker-compose.yml up -d

# Dagster — worker VM (set MASTER_VM_IP in .env)
docker compose -f dagster/worker/docker-compose.yml up -d

# Spark image — build once
docker build -f spark/Dockerfile.spark -t data-platform/spark:4.1 .
```
