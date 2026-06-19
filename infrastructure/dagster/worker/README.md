# Worker VM Setup

Each worker VM runs one or more Celery workers that connect back to the Master VM for metadata (Postgres) and job routing (Redis).

## Worker routing

| Worker | Queue | Assets |
|--------|-------|--------|
| `dagster-worker-1` | `worker-1` | `sap/sap_raw`, `sap/sap_transformed` |
| `dagster-worker-2` | `worker-2` | `sales/sales_raw`, `sales/sales_transformed` |

Each asset op is tagged `{"dagster-celery/queue": "worker-N"}` — Dagster routes it to the right worker automatically.

---

## Prerequisites

- Docker + Docker Compose installed on the VM
- The **Master VM must be running** (`control/docker-compose.yml` up) before starting workers
- Worker VM can reach Master VM on ports **5432** (Postgres) and **6379** (Redis)

---

## Step-by-Step

### Step 1 — SSH into the Worker VM and clone the repo

```bash
ssh ubuntu@WORKER_VM_IP
git clone https://github.com/YOUR_ORG/data-platform.git
cd data-platform
```

### Step 2 — Create `.env`

```bash
cp .env.example .env
```

Edit `.env` and set both values:

```bash
MASTER_VM_IP=10.x.x.x              # private IP of your Master VM
DAGSTER_POSTGRES_PASSWORD=your_strong_password   # must match the master's password
```

### Step 3 — Start workers

```bash
docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d
```

### Step 4 — Verify

```bash
# Check containers are running
docker compose -f infrastructure/dagster/worker/docker-compose.yml ps

# Confirm workers connected to Redis and Postgres
docker compose -f infrastructure/dagster/worker/docker-compose.yml logs dagster-worker-1
```

Expected output:
```
.> transport:   redis://10.x.x.x:6379/0     ← Redis on master
.> results:     redis://10.x.x.x:6379/0
...
[celery@dagster-worker-1 ready.]              ← worker is up
```

If you see `amqp://` instead of `redis://` → `MASTER_VM_IP` is not set in `.env`.  
If you see `Connection refused` on 6379 → Redis port not open on Master VM firewall.  
If you see `Connection refused` on 5432 → Postgres port not open on Master VM firewall.

### Step 5 — Verify in Dagster UI

1. Open **http://MASTER_VM_IP:3000**
2. Go to **Deployment → Daemons** — all daemons should be green
3. Go to **Assets** → select the `sap` group → **Materialize** → runs on worker-1
4. Go to **Assets** → select the `sales` group → **Materialize** → runs on worker-2

---

## Useful commands

```bash
# Follow worker logs
docker compose -f infrastructure/dagster/worker/docker-compose.yml logs -f

# Restart a specific worker
docker compose -f infrastructure/dagster/worker/docker-compose.yml restart dagster-worker-1

# Test Redis connection from this VM
docker run --rm redis:7-alpine redis-cli -h $MASTER_VM_IP ping

# Test Postgres connection from this VM
docker run --rm postgres:15-alpine psql "postgresql://dagster:PASSWORD@$MASTER_VM_IP:5432/dagster_db" -c "SELECT 1;"

# Pull latest code and redeploy
git pull
docker compose -f infrastructure/dagster/worker/docker-compose.yml up -d --build
```

---

## Legacy (VM bare-metal, no Docker)

`setup.sh`, `start_worker1.sh`, `start_worker2.sh`, `celery_worker.yaml`, and `systemd/` are the original shell-based setup. Kept as reference.
