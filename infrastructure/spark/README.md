# Spark Infrastructure

Custom Spark 4.1 Docker image used by `ingestion/sdp/` pipelines.

## What's bundled in the image

| Component | Version | Purpose |
|-----------|---------|---------|
| `apache/spark` | 4.1.0-python3 | Base Spark + PySpark |
| `pyspark[pipelines]` | ≥ 4.1.0 | `spark-pipelines` CLI for declarative pipelines |
| `delta-spark` | 4.0.0 | Delta Lake read/write, ACID transactions, time-travel |
| `delta-storage` | 4.0.0 | Delta log storage layer |
| `hadoop-azure` | 3.4.1 | Azure Blob Storage connector (`abfss://`) |
| `azure-storage` | 8.6.6 | Azure SDK for Blob auth |
| `postgresql` JDBC | 42.7.3 | Read from PostgreSQL via JDBC |
| `mysql-connector-j` | 8.4.0 | Read from MySQL via JDBC |

## Build

Build context must be the **project root** so the Dockerfile is found:

```bash
# From project root
docker build -f infrastructure/spark/Dockerfile.spark -t data-platform/spark:4.1 .
```

## Usage

The container stays alive with `tail -f /dev/null` — trigger pipelines via `docker exec`:

```bash
docker exec <container> spark-pipelines run --spec /opt/spark/pipeline/ecommerce/pipeline.yml
docker exec <container> spark-pipelines run --spec /opt/spark/pipeline/manage/pipeline.yml
```

For a local demo stack (Spark + MySQL + PostgreSQL + Azurite), see `ingestion/sdp/README.md`.
