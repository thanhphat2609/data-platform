# spark_sdp — Spark Declarative Pipelines (PySpark 4.1)

Batch ingestion from MySQL and PostgreSQL → MinIO bronze using **PySpark 4.1 Declarative Pipelines** (`spark-pipelines` CLI).

YAML-driven spec. Python modules with `@dp.materialized_view`. No custom runner code.

> **Note:** Production Spark runs are triggered by **Airflow** in `data-platform-orchestration`.  
> The Spark runtime image lives in [data-platform-infra](https://github.com/thanhphat2609/data-platform-infra) (`spark/Dockerfile`).

---

## Architecture

```
MySQL `sales` DB          PostgreSQL `manage` DB
  └─ JDBC                   └─ JDBC
       └─ @dp.materialized_view (PySpark 4.1 pipelines)
               │                        │
      bronze_customers          bronze_users
      bronze_orders             bronze_projects
                                bronze_tasks
                                bronze_issues
               │                        │
  s3a://bronze/spark_sdp/mysql/sales/  s3a://bronze/spark_sdp/postgresql/manage/
```

---

## Pipeline structure

```
spark_sdp/
├── pipelines/
│   ├── ecommerce/
│   │   ├── pipeline.yml          ← spark-pipelines spec (MySQL sales DB)
│   │   └── bronze_ingestion.py   ← bronze_customers, bronze_orders
│   └── manage/
│       ├── pipeline.yml          ← spark-pipelines spec (PostgreSQL manage DB)
│       └── bronze_ingestion.py   ← bronze_users, bronze_projects, bronze_tasks, bronze_issues
└── sdp/
    └── runner.py                 ← local dev helper (calls spark-pipelines CLI)
```

---

## Run commands

### ecommerce (MySQL — dry-run)

```bash
spark-pipelines dry-run \
  --spec /opt/spark/pipeline/ecommerce/pipeline.yml \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.plugins=io.dataflint.spark.SparkDataflintPlugin \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.warehouse.dir=s3a://bronze/spark_sdp/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://infra-minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --driver-class-path /opt/spark/jars/postgresql-42.7.3.jar:/opt/spark/jars/mysql-connector-j-8.4.0.jar
```

### manage (PostgreSQL — dry-run)

```bash
spark-pipelines dry-run \
  --spec /opt/spark/pipeline/manage/pipeline.yml \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.plugins=io.dataflint.spark.SparkDataflintPlugin \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.warehouse.dir=s3a://bronze/spark_sdp/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://infra-minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --driver-class-path /opt/spark/jars/postgresql-42.7.3.jar:/opt/spark/jars/mysql-connector-j-8.4.0.jar
```

---

## Via runner.py (local dev)

```bash
# Requires Spark image from data-platform-infra: make build-spark
python sdp/runner.py ecommerce [--dry-run]
python sdp/runner.py manage [--dry-run]
```

---

## Bronze output

```
s3a://bronze/spark_sdp/
├── mysql/
│   └── sales/
│       ├── bronze_customers/     ← Delta table (managed by spark-pipelines)
│       └── bronze_orders/        ← Delta table (managed by spark-pipelines)
└── postgresql/
    └── manage/
        ├── bronze_users/         ← Delta table
        ├── bronze_projects/      ← Delta table
        ├── bronze_tasks/         ← Delta table
        └── bronze_issues/        ← Delta table
```

---

## Key packages (in data-platform-infra Spark image)

| Package | Version | Purpose |
|---|---|---|
| `apache/spark` | 4.1.0 | Base image |
| `pyspark[pipelines]` | 4.1.x | spark-pipelines CLI + @dp.materialized_view |
| `delta-spark_2.13` | 4.0.0 | Delta Lake for Spark 4.1 |
| `hadoop-aws` | 3.4.1 | S3A filesystem |
| `aws-sdk-bundle` | 2.23.19 | AWS SDK v2 (for Hadoop 3.4.x) |
| `mysql-connector-j` | 8.4.0 | MySQL JDBC driver |
| `postgresql` | 42.7.3 | PostgreSQL JDBC driver |
| `dataflint-spark` | 0.8.8 | Spark observability plugin |
