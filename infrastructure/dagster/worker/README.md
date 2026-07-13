# Worker VM Setup

Each worker VM runs one Celery worker that connects back to the Control VM for metadata (PostgreSQL) and job routing (Redis). See [../control/](../control/) for the control plane setup.

## Worker routing

| File | Container | Queue | Pipelines |
|------|-----------|-------|-----------|
| `docker-compose-worker1.yml` | `worker1_dagster_worker` | `worker-1` | `dlt_crm_ingestion`, `dlt_api_ingestion` |
| `docker-compose-worker2.yml` | `worker2_dagster_worker` | `worker-2` | `sdp_ecommerce_ingestion`, `sdp_manage_ingestion` |

Each asset op is tagged `{"dagster-celery/queue": "worker-N"}` in `orchestration/pipelines/` — Dagster routes it to the right worker automatically.

---

## Prerequisites

- Docker Engine with `/var/run/docker.sock` accessible
- Pipeline images built on this VM (see steps below)
- **Control VM must be running** (`control/docker-compose.yml` up)
- Firewall/NSG: Worker VM can reach Control VM on ports **5432** (PostgreSQL) and **6379** (Redis)

---

## Worker VM 1 — dlt pipelines

### Step 1 — SSH and clone

```bash
ssh ubuntu@<worker1-vm-ip>
git clone https://github.com/thanhphat2609/data-platform.git
cd data-platform
```

### Step 2 — Build the dlt image

```bash
docker build -t dlt-pipeline:latest -f infrastructure/dlt/Dockerfile .
```

### Step 3 — Create `.env.worker1`

```bash
cp infrastructure/dagster/worker/.env.worker1.demo infrastructure/dagster/worker/.env.worker1
```

Edit `.env.worker1` and replace all `<placeholders>`:

```bash
DAGSTER_POSTGRES_URL=postgresql://dagster:<pw>@<ctrl-vm-ip>:5432/dagster_db
CELERY_BROKER_URL=redis://<ctrl-vm-ip>:6379/0
CELERY_RESULT_BACKEND=redis://<ctrl-vm-ip>:6379/0
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_ACCOUNT_KEY=...
POSTGRES_CONNECTION_STRING=postgresql://admin:<pw>@<crm-db-host>:5432/crm
```

### Step 4 — Start Worker 1

```bash
docker compose \
  -f infrastructure/dagster/worker/docker-compose-worker1.yml \
  --env-file infrastructure/dagster/worker/.env.worker1 \
  up --build -d
```

### Step 5 — Verify

```bash
docker logs -f worker1_dagster_worker
```

Expected output:
```
.> transport:   redis://<ctrl-vm-ip>:6379/0
.> results:     redis://<ctrl-vm-ip>:6379/0
[tasks]
  . dagster_celery.tasks.create_task
[celery@worker1_dagster_worker ready.]
```

---

## Worker VM 2 — SDP / Spark pipelines

### Step 1 — SSH and clone

```bash
ssh ubuntu@<worker2-vm-ip>
git clone https://github.com/thanhphat2609/data-platform.git
cd data-platform
```

### Step 2 — Build the Spark image

```bash
# Takes ~5 min. VM needs at least 6 GB RAM for Spark runs.
docker build -t spark-pipeline:latest -f infrastructure/spark/Dockerfile.spark .
```

### Step 3 — Create `.env.worker2`

```bash
cp infrastructure/dagster/worker/.env.worker2.demo infrastructure/dagster/worker/.env.worker2
```

Edit `.env.worker2` and replace all `<placeholders>`:

```bash
DAGSTER_POSTGRES_URL=postgresql://dagster:<pw>@<ctrl-vm-ip>:5432/dagster_db
CELERY_BROKER_URL=redis://<ctrl-vm-ip>:6379/0
CELERY_RESULT_BACKEND=redis://<ctrl-vm-ip>:6379/0
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_ACCOUNT_KEY=...
MYSQL_HOST=<mysql-db-host>
MYSQL_PASSWORD=...
POSTGRES_HOST=<manage-db-host>
POSTGRES_PASSWORD=...
```

### Step 4 — Start Worker 2

```bash
docker compose \
  -f infrastructure/dagster/worker/docker-compose-worker2.yml \
  --env-file infrastructure/dagster/worker/.env.worker2 \
  up --build -d
```

### Step 5 — Verify

```bash
docker logs -f worker2_dagster_worker
```

Expected output:
```
.> transport:   redis://<ctrl-vm-ip>:6379/0
.> results:     redis://<ctrl-vm-ip>:6379/0
[celery@worker2_dagster_worker ready.]
```

---

## Verify routing in the Dagster UI

1. Open **http://\<ctrl-vm-ip\>:3000**
2. Go to **Jobs → dlt_crm_job** → **Launch run**  
   → Only `worker1_dagster_worker` logs should show activity
3. Go to **Jobs → sdp_ecommerce_job** → **Launch run**  
   → Only `worker2_dagster_worker` logs should show activity

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `amqp://` in worker logs instead of `redis://` | `CELERY_BROKER_URL` not set | Check `.env.worker*` |
| `Connection refused` on 6379 | Redis port blocked | Open port 6379 on Control VM NSG |
| `Connection refused` on 5432 | Postgres port blocked | Open port 5432 on Control VM NSG |
| `dlt container exits immediately` | Azure key wrong or CRM DB unreachable | Check `AZURE_STORAGE_ACCOUNT_KEY` and `POSTGRES_CONNECTION_STRING` |
| `Spark container OOM` | Insufficient RAM | Worker VM 2 needs 6+ GB free RAM |

---

## Useful commands

```bash
# Follow worker-1 logs
docker logs -f worker1_dagster_worker

# Follow worker-2 logs
docker logs -f worker2_dagster_worker

# Restart worker-1 (e.g. after code deploy)
docker compose -f infrastructure/dagster/worker/docker-compose-worker1.yml restart

# Pull latest code and redeploy worker-2
git pull
docker compose -f infrastructure/dagster/worker/docker-compose-worker2.yml up --build -d

# Test Redis reachability from this VM
docker run --rm redis:7-alpine redis-cli -h <ctrl-vm-ip> ping

# Test PostgreSQL reachability from this VM
docker run --rm postgres:15-alpine \
  psql "postgresql://dagster:<pw>@<ctrl-vm-ip>:5432/dagster_db" -c "SELECT 1;"
```

