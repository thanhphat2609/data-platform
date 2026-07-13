# Data Platform — Demo Walkthrough

Full demo of the Bronze ingestion layer orchestrated by Dagster.

**Stack:** Dagster → dlt (PostgreSQL CRM + REST APIs) + Spark SDP (MySQL ecommerce + PostgreSQL manage) → Azure Blob Storage (Delta Lake)

---

## Architecture

```
infrastructure/dagster/
├── dagster.yaml          ← shared config: Celery executor + PostgreSQL storage + Azure Blob logs
├── workspace.yaml        ← code location: orchestration/definitions.py
├── Dockerfile            ← Dagster image (webserver + daemon + workers all use this)
├── demo/
│   ├── docker-compose.yml   ← NON-PROD: all 9 services on one VM
│   ├── .env.demo            ← credential template — copy to .env
│   └── DEMO.md              ← this file
├── control/              ← PROD: control plane only (webserver + daemon + Redis + DB)
└── worker/               ← PROD: split worker VMs (docker-compose-worker1/2.yml)

orchestration/
├── definitions.py        ← Dagster Definitions: registers all assets, jobs, schedules
├── pipelines/            ← one file per pipeline (asset + job + schedule co-located)
│   ├── dlt_crm.py        ← queue: worker-1
│   ├── dlt_api.py        ← queue: worker-1
│   ├── sdp_ecommerce.py  ← queue: worker-2
│   └── sdp_manage.py     ← queue: worker-2
└── src/
    └── docker_runner.py  ← launches ephemeral pipeline containers via Docker-in-Docker
```

**How a run works:**
1. Schedule fires (or manual trigger) → Dagster daemon submits task to Redis (Celery)
2. The worker subscribed to that queue picks it up
3. Worker calls `docker run <image>` with pipeline credentials as env vars
4. Ephemeral container runs the pipeline, writes to Azure Blob, exits
5. Dagster collects logs and marks the asset materialized

---

## Phase 0 — Prerequisites

Run all commands from the **repo root** unless stated otherwise.

### 0.1 Requirements on the Azure VM

- Docker Engine + Docker Compose v2
- At least 8 GB RAM (Spark needs ~3–4 GB per run)
- Outbound internet access (dlt API pipeline calls external APIs)
- Azure Blob Storage account with two containers:
  - `bronze` — pipeline output (dlt Parquet + SDP Delta Lake)
  - `dagster-logs` — Dagster compute logs (written by all workers via `dagster.yaml`)

### 0.2 Fill in credentials

```bash
cp infrastructure/dagster/demo/.env.demo infrastructure/dagster/demo/.env
```

Edit `.env` and set your real values:

```
AZURE_STORAGE_ACCOUNT_NAME=<your_account_name>
AZURE_STORAGE_ACCOUNT_KEY=<your_storage_account_key>
```

Leave `DLT_IMAGE` and `SDP_IMAGE` as-is — you will build those in Phase 1.

---

## Phase 1 — Build Pipeline Images

The Dagster workers launch **ephemeral** `docker run` containers for each pipeline.
These images must be built before starting the stack.

```bash
# dlt image — Python + dlt 0.5.4 + psycopg2 + adlfs (~2 min)
docker build -t dlt-pipeline:latest -f infrastructure/dlt/Dockerfile .

# Spark SDP image — Spark 4.1.2 + Delta 4.0 + JDBC drivers + hadoop-azure (~5 min)
docker build -t spark-pipeline:latest -f infrastructure/spark/Dockerfile.spark .
```

Verify both images exist:

```bash
docker images | grep -E "dlt-pipeline|spark-pipeline"
```

---

## Phase 2 — Start the Demo Stack

```bash
docker compose \
  -f infrastructure/dagster/demo/docker-compose.yml \
  --env-file infrastructure/dagster/demo/.env \
  up --build -d
```

This starts **9 containers**:

| Container | Role | Port |
|-----------|------|------|
| `demo_dagster_db` | Dagster metadata (PostgreSQL) | 5434 |
| `demo_redis` | Celery broker + result backend | 6379 |
| `demo_dagster_webserver` | Dagster UI | 3000 |
| `demo_dagster_daemon` | Scheduler + sensor runner | — |
| `demo_dagster_worker_1` | Celery worker — dlt queue (worker-1) | — |
| `demo_dagster_worker_2` | Celery worker — SDP queue (worker-2) | — |
| `demo_postgres_crm` | dlt source: CRM data | 5431 |
| `demo_mysql_ecommerce` | SDP source: ecommerce | 3306 |
| `demo_postgres_manage` | SDP source: project management | 5432 |

Wait ~20 seconds for healthchecks to pass:

```bash
docker compose -f infrastructure/dagster/demo/docker-compose.yml ps
```

All containers should show `healthy` or `running`.

Open the Dagster UI: **http://localhost:3000** (or `http://<vm-ip>:3000`)

---

## Phase 3 — Explore the Source Data

Before running any pipeline, show the audience what's in the source systems.

### 3.1 CRM database (dlt source — PostgreSQL)

```bash
docker exec -it demo_postgres_crm psql -U admin -d crm
```

```sql
-- 5 customers from Vietnam
SELECT id, name, email FROM customers;

-- 8 products across categories
SELECT id, name, price FROM products;

-- 8 orders over the last 25 days
SELECT id, customer_id, status, total_amount, order_date FROM orders ORDER BY order_date;

\q
```

### 3.2 Ecommerce database (SDP source — MySQL)

```bash
docker exec -it demo_mysql_ecommerce mysql -u admin -padmin123 ecommerce
```

```sql
-- Category hierarchy: Electronics > Smartphones / Laptops
SELECT id, name, parent_id FROM categories;

-- Products linked to categories
SELECT p.name, c.name AS category, p.price FROM products p JOIN categories c ON p.category_id = c.id;

-- 11 order line items across 8 orders
SELECT o.id, o.status, oi.quantity, oi.unit_price FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 10;

exit
```

### 3.3 Project management database (SDP source — PostgreSQL)

```bash
docker exec -it demo_postgres_manage psql -U admin -d manage
```

```sql
-- 5 users across 3 roles
SELECT username, email, role FROM users;

-- 3 active projects with owners
SELECT p.name, u.username AS owner, p.status FROM projects p JOIN users u ON p.owner_id = u.id;

-- 8 tasks with status and priority
SELECT title, status, priority FROM tasks ORDER BY project_id, priority;

-- 5 issues including a critical one
SELECT title, status, severity FROM issues;

\q
```

---

## Phase 4 — Open Worker Log Terminals

**Do this before triggering any job.** The audience needs to see which worker picks up each run.

```bash
# Terminal 2 — worker-1 (dlt pipelines only)
docker logs -f demo_dagster_worker_1

# Terminal 3 — worker-2 (SDP pipelines only)
docker logs -f demo_dagster_worker_2
```

Leave both terminals visible throughout the demo. When a job runs, activity appears in **exactly one** worker — whichever queue that pipeline's op is tagged with.

---

## Phase 5 — Tour the Dagster UI

Open **http://localhost:3000** and walk through:

1. **Assets** tab → show 4 assets grouped by pipeline:
   - `dlt / dlt_crm_ingestion` (group: `dlt_crm`, queue: `worker-1`)
   - `dlt / dlt_api_ingestion` (group: `dlt_api`, queue: `worker-1`)
   - `sdp / sdp_ecommerce_ingestion` (group: `sdp_ecommerce`, queue: `worker-2`)
   - `sdp / sdp_manage_ingestion` (group: `sdp_manage`, queue: `worker-2`)

2. **Jobs** tab → 4 independent jobs, one per pipeline:
   - `dlt_crm_job`, `dlt_api_job`, `sdp_ecommerce_job`, `sdp_manage_job`

3. **Schedules** tab → 4 daily schedules staggered to avoid resource contention:
   - `01:00 ICT` dlt CRM → `01:30` dlt API → `02:00` SDP ecommerce → `02:30` SDP manage

---

## Phase 6 — Run dlt CRM Pipeline

Ingests **orders, customers, products** from PostgreSQL CRM → `bronze/dlt/crm/` on Azure Blob.

### Trigger from UI

1. Go to **Jobs → dlt_crm_job** → **Launch run**
2. Watch the run log — Dagster dispatches to `worker-1` via Celery
3. Worker-1 terminal shows: `docker run dlt-pipeline:latest python pipelines/postgres_pipeline.py`
4. Worker-2 terminal shows **nothing** — it is on a different queue

### Or trigger from CLI

```bash
docker exec demo_dagster_webserver \
  dagster job execute -m definitions -j dlt_crm_job
```

### What to show during the run

- Logs show: `Extracting table orders...` → row counts yielded per table
- dlt state file written to `bronze/dlt/crm/_dlt_pipeline_state/`
- Run completes with **Success** and row counts per table

### Verify output on Azure Blob

```bash
docker run --rm \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=${AZURE_STORAGE_ACCOUNT_NAME} \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=${AZURE_STORAGE_ACCOUNT_KEY} \
  dlt-pipeline:latest python pipelines/verify_output.py
```

Expected:
```
bronze/dlt/crm/customers/ingestion_date=YYYY-MM-DD/...parquet   rows=5
bronze/dlt/crm/products/ingestion_date=YYYY-MM-DD/...parquet    rows=8
bronze/dlt/crm/orders/ingestion_date=YYYY-MM-DD/...parquet      rows=8
```

---

## Phase 7 — Run dlt API Pipeline

Ingests **USGS Earthquakes, Open-Meteo weather, Exchange Rates** → `bronze/dlt/events/` on Azure Blob.

### Trigger from UI

1. **Jobs → dlt_api_job** → **Launch run**
2. Worker-1 terminal picks it up — worker-2 stays idle
3. Show three incremental sources loading in sequence

### What to show during the run

- Each source uses **append** disposition — only new records since the last run are added
- USGS: real earthquake events worldwide (min magnitude 1.0)
- Open-Meteo: hourly weather data for Ho Chi Minh City (lat 10.8231, lon 106.6297)
- Exchange Rates: daily FX rates from frankfurter.app

### Verify output

```bash
docker run --rm \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=${AZURE_STORAGE_ACCOUNT_NAME} \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=${AZURE_STORAGE_ACCOUNT_KEY} \
  dlt-pipeline:latest python pipelines/verify_output.py
```

Expected additions:
```
bronze/dlt/events/earthquake_events/ingestion_date=YYYY-MM-DD/...parquet
bronze/dlt/events/openmeteo_hourly/ingestion_date=YYYY-MM-DD/...parquet
bronze/dlt/events/exchange_rates/ingestion_date=YYYY-MM-DD/...parquet
```

---

## Phase 8 — Run SDP Ecommerce Pipeline

Ingests **categories, products, customers, orders, order_items** from MySQL ecommerce → `bronze/sdp/ecommerce/` as **Delta Lake** on Azure Blob.

### Trigger from UI

1. **Jobs → sdp_ecommerce_job** → **Launch run**
2. Worker-2 terminal picks it up — worker-1 stays idle (this is the routing proof)
3. Dagster runs `docker run spark-pipeline:latest spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml`

### What to show during the run

- Spark startup takes ~60–90 seconds (JVM init + Delta Lake bootstrap)
- Each table logs a `WRITE` with row count and `ingestion_date` partition
- Delta Lake: output is **versioned** — supports time-travel and ACID guarantees

### Expected output on Azure Blob

```
bronze/sdp/ecommerce/
  bronze_categories/   ingestion_date=YYYY-MM-DD/
  bronze_customers/    ingestion_date=YYYY-MM-DD/
  bronze_orders/       ingestion_date=YYYY-MM-DD/
  bronze_order_items/  ingestion_date=YYYY-MM-DD/
  bronze_products/     ingestion_date=YYYY-MM-DD/
```

---

## Phase 9 — Run SDP Manage Pipeline

Ingests **users, projects, tasks, issues, comments** from PostgreSQL manage → `bronze/sdp/manage/` as **Delta Lake** on Azure Blob.

### Trigger from UI

1. **Jobs → sdp_manage_job** → **Launch run**
2. Worker-2 picks it up again — 5 tables written as parallel materialized views

### What to show

- The `tasks` table includes "Set up Spark SDP pipeline" with status `done` — the pipeline is ingesting metadata about its own creation
- The `issues` table has a `critical` severity login bug — a realistic data quality scenario to discuss

### Expected output on Azure Blob

```
bronze/sdp/manage/
  bronze_users/     ingestion_date=YYYY-MM-DD/
  bronze_projects/  ingestion_date=YYYY-MM-DD/
  bronze_tasks/     ingestion_date=YYYY-MM-DD/
  bronze_issues/    ingestion_date=YYYY-MM-DD/
  bronze_comments/  ingestion_date=YYYY-MM-DD/
```

---

## Phase 10 — Show the Full Asset Graph

After all 4 pipelines succeed:

1. **Assets** → **View global asset graph** — all 4 assets green (materialized)
2. Click any asset → **Metadata** tab → last materialization timestamp
3. **Runs** tab → 4 completed runs with durations

**Key talking points:**
- Each pipeline is fully independent — one failure doesn't block the others
- Celery routing is real: dlt jobs never touch worker-2, SDP jobs never touch worker-1
- Schedules are staggered to avoid resource contention on a shared VM
- Adding a new pipeline = one new file in `orchestration/pipelines/` + one line in `definitions.py`

---

## Phase 11 — Teardown

```bash
docker compose \
  -f infrastructure/dagster/demo/docker-compose.yml \
  down -v
```

The `-v` flag removes named volumes (source DB data is wiped). Azure Blob data persists.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Dagster UI shows no assets | Wait 30s for daemon to load code location; check `docker logs demo_dagster_daemon` |
| dlt run fails with Azure auth error | Verify `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` in `.env` |
| Spark container exits immediately | Same Azure credential check; also verify `bronze` container exists in your storage account |
| Compute logs missing in UI | Verify `dagster-logs` container exists in Azure Blob Storage — this is where `dagster.yaml` writes logs |
| `docker run` permission denied | Docker socket not mounted — check `volumes: /var/run/docker.sock` in compose |
| MySQL healthcheck fails | First-boot init takes ~30s; run `docker compose ps` again after a minute |
| Worker picks up wrong job | Verify `op_tags={"dagster-celery/queue": "worker-N"}` in the relevant `orchestration/pipelines/*.py` file |

---

## Appendix — Real Multi-VM Deployment

The demo runs everything on one VM via `docker-compose.yml` (workers are Docker containers simulating separate VMs — Celery routing is real, hardware isolation is not). For real distribution, use the prod compose files.

### File layout

| File | Role | Deploy to |
|------|------|-----------|
| `infrastructure/dagster/control/docker-compose.yml` | Control plane: webserver + daemon + Redis + DB | Control VM |
| `infrastructure/dagster/worker/docker-compose-worker1.yml` | Celery worker-1 — dlt pipelines | Worker VM 1 |
| `infrastructure/dagster/worker/docker-compose-worker2.yml` | Celery worker-2 — SDP/Spark pipelines | Worker VM 2 |
| `infrastructure/dagster/dagster.yaml` | Shared config — mounted by all prod containers | All VMs |

Full setup steps: [control/README.md](../control/README.md) and [worker/README.md](../worker/README.md)

### Container naming

| Prefix | Stack |
|--------|-------|
| `demo_*` | `demo/docker-compose.yml` — single-VM demo |
| `ctrl_*` | `control/docker-compose.yml` — Control VM |
| `worker1_*` | `worker/docker-compose-worker1.yml` — Worker VM 1 |
| `worker2_*` | `worker/docker-compose-worker2.yml` — Worker VM 2 |

### 1. Start the control plane (Control VM)

```bash
cp infrastructure/dagster/control/.env.demo infrastructure/dagster/control/.env
# Edit .env: set DAGSTER_DB_PASSWORD, AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY

docker compose \
  -f infrastructure/dagster/control/docker-compose.yml \
  --env-file infrastructure/dagster/control/.env \
  up --build -d

# Note the private IP — workers need it
hostname -I | awk '{print $1}'
```

### 2. Start Worker VM 1 (dlt)

```bash
cp infrastructure/dagster/worker/.env.worker1.demo infrastructure/dagster/worker/.env.worker1
# Edit .env.worker1: replace all <placeholders>

docker build -t dlt-pipeline:latest -f infrastructure/dlt/Dockerfile .

docker compose \
  -f infrastructure/dagster/worker/docker-compose-worker1.yml \
  --env-file infrastructure/dagster/worker/.env.worker1 \
  up --build -d

docker logs -f worker1_dagster_worker
```

### 3. Start Worker VM 2 (SDP/Spark — needs 6+ GB RAM)

```bash
cp infrastructure/dagster/worker/.env.worker2.demo infrastructure/dagster/worker/.env.worker2
# Edit .env.worker2: replace all <placeholders>

docker build -t spark-pipeline:latest -f infrastructure/spark/Dockerfile.spark .

docker compose \
  -f infrastructure/dagster/worker/docker-compose-worker2.yml \
  --env-file infrastructure/dagster/worker/.env.worker2 \
  up --build -d

docker logs -f worker2_dagster_worker
```

### 4. Verify routing

Trigger `dlt_crm_job` from the UI → only `worker1_dagster_worker` shows activity.  
Trigger `sdp_ecommerce_job` → only `worker2_dagster_worker` shows activity.
