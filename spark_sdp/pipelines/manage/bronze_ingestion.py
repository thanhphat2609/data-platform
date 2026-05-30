"""
Bronze layer — raw ingestion from PostgreSQL `manage` DB via JDBC.
Each function produces a materialized_view registered in the pipeline catalog.

spark-pipelines discovers this file via the pipeline.yml glob:
  libraries:
    - glob: { include: bronze/** }
"""
import os

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

_JDBC_URL = (
    "jdbc:postgresql://{host}:{port}/{db}"
).format(
    host=os.getenv("POSTGRES_HOST", "infra-postgres"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    db=os.getenv("POSTGRES_MANAGE_DB", "manage"),
)

_JDBC_OPTIONS = {
    "driver":    "org.postgresql.Driver",
    "user":      os.getenv("POSTGRES_USER",     "admin"),
    "password":  os.getenv("POSTGRES_PASSWORD", "admin123"),
    "fetchsize": "10000",
}


def _read_table(table: str):
    spark = SparkSession.getActiveSession()
    return (
        spark.read.format("jdbc")
        .option("url", _JDBC_URL)
        .option("dbtable", table)
        .options(**_JDBC_OPTIONS)
        .load()
    )


@dp.materialized_view(name="bronze_users")
def bronze_users():
    return _read_table("users")


@dp.materialized_view(name="bronze_projects")
def bronze_projects():
    return _read_table("projects")


@dp.materialized_view(name="bronze_tasks")
def bronze_tasks():
    return _read_table("tasks")


@dp.materialized_view(name="bronze_issues")
def bronze_issues():
    return _read_table("issues")
