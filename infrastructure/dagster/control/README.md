# Master VM Setup

Runs PostgreSQL, Redis, Dagster webserver, and Dagster daemon.

## Services

| Service | Port | Role |
|---------|------|------|
| `postgres` | 5432 | Shared metadata DB — run history, schedules, asset materializations |
| `redis` | 6379 | Job broker — routes tasks from daemon to Celery workers |
| `dagster-webserver` | 3000 | Dagster UI |
| `dagster-daemon` | — | Scheduler, sensor, and run coordinator |

Postgres and Redis ports are exposed so worker VMs can connect.

---

## Prerequisites

- Docker + Docker Compose installed on the VM
- Port **3000**, **5432**, **6379** open in firewall/NSG rules (allow worker VM IPs)

---

## Step-by-Step

### Step 1 — SSH into the Master VM and clone the repo

```bash
ssh ubuntu@MASTER_VM_IP
git clone https://github.com/YOUR_ORG/data-platform.git
cd data-platform
```

### Step 2 — Create `.env`

```bash
cp .env.example .env
```

Edit `.env` and set a strong password:

```bash
DAGSTER_POSTGRES_PASSWORD=your_strong_password
```

### Step 3 — Start services

```bash
docker compose -f infrastructure/dagster/control/docker-compose.yml up -d
```

Docker will build the Dagster image on first run (takes ~2 minutes).

### Step 4 — Verify

```bash
# Check all containers are running
docker compose -f infrastructure/dagster/control/docker-compose.yml ps

# Follow logs
docker compose -f infrastructure/dagster/control/docker-compose.yml logs -f
```

Open **http://MASTER_VM_IP:3000** — you should see the Dagster UI.

Go to **Deployment → Daemons** and confirm all daemons are green.

---

## Useful commands

```bash
# Restart a single service
docker compose -f infrastructure/dagster/control/docker-compose.yml restart dagster-daemon

# Check postgres
docker exec -it <postgres-container> psql -U dagster -d dagster_db -c "SELECT 1;"

# Check redis
docker exec -it <redis-container> redis-cli ping

# Pull latest code and redeploy
git pull
docker compose -f infrastructure/dagster/control/docker-compose.yml up -d --build
```

---

## Legacy (VM bare-metal, no Docker)

`setup.sh`, `start_control.sh`, and `systemd/` are the original shell-based setup for environments without Docker. Kept as reference.
