# Dagster Infrastructure

Distributed deployment across two VM types using Docker Compose.

## Architecture

```
┌─── Master VM ──────────────────────────────┐
│  dagster-webserver     :3000  (UI)         │
│  dagster-daemon               (scheduler)  │
│  postgres              :5432  (metadata)   │ ◄── all VMs connect here
│  redis                 :6379  (broker)     │ ◄── all VMs connect here
└────────────────────────────────────────────┘

┌─── Worker VM-1 ─────────┐   ┌─── Worker VM-2 ─────────┐
│  dagster-worker-1        │   │  dagster-worker-2        │
│  queue: worker-1         │   │  queue: worker-2         │
│  → sap assets            │   │  → sales assets          │
└──────────────────────────┘   └──────────────────────────┘
```

## Folders

| Folder | Purpose |
|--------|---------|
| `control/` | Master VM — webserver, daemon, postgres, redis |
| `worker/` | Worker VMs — Celery workers connecting to master |

## Shared files

| File | Purpose |
|------|---------|
| `Dockerfile` | Single image used by all Dagster services |
| `dagster.yaml` | Instance config — storage and compute logs (baked into image) |
| `workspace.yaml` | Code location — points to `orchestration/definitions.py` |

## Quick reference

```bash
# Master VM
docker compose -f infrastructure/dagster/control/docker-compose.yml up -d

# Worker VM
MASTER_VM_IP=10.x.x.x docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d
```

See `control/README.md` and `worker/README.md` for full setup steps.
