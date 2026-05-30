"""
Local dev helper — calls spark-pipelines CLI for a given pipeline.

The data-platform-orchestration (Airflow) layer will call spark-pipelines directly.
This script is only for local testing / dry-runs.

Usage:
  python sdp/runner.py ecommerce [--dry-run]
  python sdp/runner.py manage [--dry-run]
"""
import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

ROOT      = Path(__file__).parent.parent
JARS_DIR  = os.getenv("SPARK_JARS_DIR", "/opt/spark/jars")

PIPELINES = {
    "ecommerce": ROOT / "pipelines" / "ecommerce" / "pipeline.yml",
    "manage":    ROOT / "pipelines" / "manage"    / "pipeline.yml",
}

DRIVER_CLASSPATH = ":".join([
    f"{JARS_DIR}/postgresql-42.7.3.jar",
    f"{JARS_DIR}/mysql-connector-j-8.4.0.jar",
])


def run(pipeline_name: str, dry: bool = False) -> int:
    if pipeline_name not in PIPELINES:
        log.error("Unknown pipeline: %s. Available: %s", pipeline_name, list(PIPELINES))
        return 1

    spec  = str(PIPELINES[pipeline_name])
    mode  = "dry-run" if dry else "run"
    minio = os.getenv("MINIO_ENDPOINT", "http://infra-minio:9000")

    cmd = [
        "spark-pipelines", mode,
        "--spec", spec,
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", "spark.plugins=io.dataflint.spark.SparkDataflintPlugin",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.sql.warehouse.dir=s3a://bronze/spark_sdp/warehouse",
        "--conf", f"spark.hadoop.fs.s3a.endpoint={minio}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={os.getenv('MINIO_SECRET_KEY', 'minioadmin123')}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--driver-class-path", DRIVER_CLASSPATH,
    ]

    log.info("spark-pipelines %s: %s", mode, pipeline_name)
    return subprocess.run(cmd, check=False).returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline", choices=list(PIPELINES))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sys.exit(run(args.pipeline, dry=args.dry_run))
