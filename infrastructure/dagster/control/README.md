# Control VM Setup

Runs the Dagster control plane: metadata database, Redis broker, webserver UI, and daemon scheduler.
Workers are separate VMs — see [../worker/](../worker/).

## Services

| Container | Port | Role |
|-----------|------|------|
| `ctrl_dagster_db` | 5432 | Dagster metadata DB (run history, schedules, asset materializations) |
| `ctrl_dagster_redis` | 6379 | Celery broker — routes tasks from daemon to workers |
| `ctrl_dagster_webserver` | 3000 | Dagster UI |
| `ctrl_dagster_daemon` | — | Scheduler + sensor + run coordinator |

Ports 5432 and 6379 must be open to worker VM IPs in your firewall / NSG rules.

---

## Configuration

### `dagster.yaml` (shared)

Located at [../dagster.yaml](../dagster.yaml). Mounted read-only into all control plane containers.

Configures:
- **Storage**: PostgreSQL on `dagster-db` (via `DAGSTER_POSTGRES_URL`)
- **Executor**: Celery (via `CELERY_BROKER_URL` + `CELERY_RESULT_BACKEND`)
- **Compute logs**: Azure Blob Storage `dagster-logs/compute-logs/` (via `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_KEY`)

---

## Setup

### Step 1 — SSH into the Control VM and clone the repo

```bash
ssh ubuntu@<ctrl-vm-ip>
git clone https://github.com/thanhphat2609/data-platform.git
cd data-platform
```

### Step 2 — Create `.env`

```bash
cp infrastructure/dagster/control/.env.demo infrastructure/dagster/control/.env
```

Edit `.env` and set:

```bash
DAGSTER_DB_PASSWORD=<strong_password>
AZURE_STORAGE_ACCOUNT_NAME=<your_account>
AZURE_STORAGE_ACCOUNT_KEY=<your_key>
```

### Step 3 — Start the control plane

```bash
docker compose \
  -f infrastructure/dagster/control/docker-compose.yml \
  --env-file infrastructure/dagster/control/.env \
  up --build -d
```

### Step 4 — Verify

```bash
# All containers healthy
docker compose -f infrastructure/dagster/control/docker-compose.yml ps

# Follow logs
docker compose -f infrastructure/dagster/control/docker-compose.yml logs -f
```

Open **http://\<ctrl-vm-ip\>:3000** — Dagster UI should load.

Go to **Deployment → Daemons** and confirm all daemons are green.

---

## Worker connection

Once the control plane is running, note the VM's private IP:

```bash
hostname -I | awk '{print $1}'
```

Workers need:
```bash
DAGSTER_POSTGRES_URL=postgresql://dagster:<pw>@<ctrl-vm-ip>:5432/dagster_db
CELERY_BROKER_URL=redis://<ctrl-vm-ip>:6379/0
CELERY_RESULT_BACKEND=redis://<ctrl-vm-ip>:6379/0
```

Set these in each worker VM's `.env.worker1` / `.env.worker2` before starting workers.

---

## Useful commands

```bash
# Restart the daemon (e.g. after code deploy)
docker compose -f infrastructure/dagster/control/docker-compose.yml restart ctrl_dagster_daemon

# Check DB is up
docker exec ctrl_dagster_db psql -U dagster -d dagster_db -c "SELECT 1;"

# Check Redis
docker exec ctrl_dagster_redis redis-cli ping

# Pull latest code and redeploy
git pull
docker compose -f infrastructure/dagster/control/docker-compose.yml up --build -d
```

