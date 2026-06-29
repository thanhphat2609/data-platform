# Infrastructure

Deployment configs for all platform services.

## What's here

| Folder | Purpose |
|--------|---------|
| [`dagster/`](dagster/README.md) | Dagster webserver, daemon, and Celery workers via Docker Compose |
| [`spark/`](spark/README.md) | Spark 4.1 runtime image — Delta Lake, Azure Blob, JDBC drivers, SDP pipeline specs |
| [`dlt/`](dlt/README.md) | dlt runtime image — Python 3.11, dlt, psycopg2, pipeline scripts |

## Architecture

```
┌─── Master VM ──────────────────────────────┐
│  dagster-webserver     :3000               │
│  dagster-daemon                            │
│  postgres              :5432  (metadata)   │ ◄── worker VMs connect here
│  redis                 :6379  (broker)     │ ◄── worker VMs connect here
│                                            │
│  Docker socket mounted → triggers          │
│  pipeline containers via `docker run`      │
└────────────────────────────────────────────┘
         │ docker run dlt-pipeline:latest ...
         │ docker run spark-pipeline:latest ...
         ▼
┌─── Pipeline containers (ephemeral) ────────┐
│  dlt-pipeline:latest                        │  ← runs, writes to Azure Blob, exits
│  spark-pipeline:latest                      │  ← runs, writes to Azure Blob, exits
└─────────────────────────────────────────────┘

┌─── Worker VM-1 ─────────┐   ┌─── Worker VM-2 ─────────┐
│  dagster-worker-1        │   │  dagster-worker-2        │
│  queue: worker-1  (SAP)  │   │  queue: worker-2 (Sales) │
└──────────────────────────┘   └──────────────────────────┘
```

## Pipeline images

Images are the **runtime environment** — all libraries and pipeline code baked in, no dispatcher logic.
The command passed to `docker run` selects which pipeline to execute.

| Image | Dockerfile | Built from |
|-------|-----------|------------|
| `dlt-pipeline:latest` | `infrastructure/dlt/Dockerfile` | repo root as context |
| `spark-pipeline:latest` | `infrastructure/spark/Dockerfile.spark` | repo root as context |
| `dagster:latest` | `infrastructure/dagster/Dockerfile` | repo root as context |

### Build all images

```bash
# From repo root
docker build -f infrastructure/dlt/Dockerfile           -t dlt-pipeline:latest    .
docker build -f infrastructure/spark/Dockerfile.spark   -t spark-pipeline:latest  .
docker build -f infrastructure/dagster/Dockerfile       -t dagster:latest         .
```

### Push to a container registry (production)

```bash
# Example: Azure Container Registry
az acr login --name myregistry
docker tag dlt-pipeline:latest    myregistry.azurecr.io/dlt-pipeline:v1.0
docker tag spark-pipeline:latest  myregistry.azurecr.io/spark-pipeline:v1.0
docker push myregistry.azurecr.io/dlt-pipeline:v1.0
docker push myregistry.azurecr.io/spark-pipeline:v1.0
```

Then set `DLT_IMAGE` and `SDP_IMAGE` env vars in your Compose file or `.env`:
```
DLT_IMAGE=myregistry.azurecr.io/dlt-pipeline:v1.0
SDP_IMAGE=myregistry.azurecr.io/spark-pipeline:v1.0
```

## Quick reference

```bash
# Dagster — master VM
docker compose -f dagster/control/docker-compose.yml up -d

# Dagster — worker VM (set MASTER_VM_IP in .env)
docker compose -f dagster/worker/docker-compose.yml up -d

# Dagster — single-node demo
docker compose -f dagster/demo/docker-compose.yml up -d

# Run a pipeline manually (no Dagster)
docker run --rm \
  -e POSTGRES_CONNECTION_STRING=postgresql://admin:admin123@localhost:5432/platform \
  -e DESTINATION__FILESYSTEM__BUCKET_URL=az://bronze/dlt \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=<key> \
  dlt-pipeline:latest pipelines/postgres_pipeline.py

docker run --rm \
  -e MYSQL_HOST=localhost -e MYSQL_USER=admin -e MYSQL_PASSWORD=admin123 \
  spark-pipeline:latest \
  spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml
```
