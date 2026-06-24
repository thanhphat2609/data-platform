"""
Bronze layer — raw ingestion from PostgreSQL `manage` DB via JDBC.
Each function produces a materialized_view registered in the pipeline catalog.

spark-pipelines discovers this file via the pipeline.yml glob:
  libraries:
    - glob: { include: bronze_ingestion.py }
"""
import os
import sys

# Make the schema package importable when spark-pipelines executes this file directly
sys.path.append("/opt/spark/pipeline")

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema.manage import *

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

_INGESTION_PATH = "abfss://bronze@dataplatformstrorage.dfs.core.windows.net/sdp/manage"

_PARTITION_COLS = ["ingestion_date"]  # derived in _read_table(), not in PostgreSQL source


def _read_table(source_table: str, query: str | None = None):
    """Read a PostgreSQL table via JDBC.

    Pass `query` to run a custom SQL instead of a full table scan — useful for
    incremental loads.  The query is wrapped as a subquery so JDBC treats it as
    a table: ``(SELECT ...) AS t``.
    """
    spark = SparkSession.getActiveSession()
    dbtable = f"({query}) AS t" if query else source_table
    return (
        spark.read.format("jdbc")
        .option("url", _JDBC_URL)
        .option("dbtable", dbtable)
        .options(**_JDBC_OPTIONS)
        .load()
        # ingestion_date does not exist in PostgreSQL — derived here so Delta can partition by it
        .withColumn("ingestion_date", F.current_date())
    )


@dp.materialized_view(
    name="bronze_users",
    comment="Raw users from PostgreSQL manage DB — rolling 30-day window on created_at",
    table_properties={
        "location": f"{_INGESTION_PATH}/bronze_users",
    },
    partition_cols=_PARTITION_COLS,
    schema=_users_schema,
)
def bronze_users():
    return _read_table(
        "users",
        query="SELECT * FROM users",
    )


@dp.materialized_view(
    name="bronze_projects",
    comment="Raw projects from PostgreSQL manage DB — rolling 30-day window on created_at",
    table_properties={
        "location": f"{_INGESTION_PATH}/bronze_projects",
    },
    partition_cols=_PARTITION_COLS,
    schema=_projects_schema,
)
def bronze_projects():
    return _read_table(
        "projects",
        query="SELECT * FROM projects",
    )


@dp.materialized_view(
    name="bronze_tasks",
    comment="Raw tasks from PostgreSQL manage DB — rolling 30-day window on created_at",
    table_properties={
        "location": f"{_INGESTION_PATH}/bronze_tasks",
    },
    partition_cols=_PARTITION_COLS,
    schema=_tasks_schema,
)
def bronze_tasks():
    return _read_table(
        "tasks",
        query="SELECT * FROM tasks",
    )


@dp.materialized_view(
    name="bronze_issues",
    comment="Raw issues from PostgreSQL manage DB — rolling 30-day window on created_at",
    table_properties={
        "location": f"{_INGESTION_PATH}/bronze_issues",
    },
    partition_cols=_PARTITION_COLS,
    schema=_issues_schema,
)
def bronze_issues():
    return _read_table(
        "issues",
        query="SELECT * FROM issues",
    )


@dp.materialized_view(
    name="bronze_comments",
    comment="Raw comments from PostgreSQL manage DB — rolling 30-day window on created_at",
    table_properties={
        "location": f"{_INGESTION_PATH}/bronze_comments",
    },
    partition_cols=_PARTITION_COLS,
    schema=_comments_schema,
)
def bronze_comments():
    return _read_table(
        "comments",
        query="SELECT * FROM comments",
    )
