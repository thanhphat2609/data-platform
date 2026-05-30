"""
Bronze layer — raw ingestion from MySQL `sales` DB via JDBC.
Each function produces a materialized_view registered in the pipeline catalog.

spark-pipelines discovers this file via the pipeline.yml glob:
  libraries:
    - glob: { include: bronze/** }
"""
import os

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

_JDBC_URL = (
    "jdbc:mysql://{host}:{port}/{db}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
).format(
    host=os.getenv("MYSQL_HOST", "infra-mysql"),
    port=os.getenv("MYSQL_PORT", "3306"),
    db=os.getenv("MYSQL_DB", "sales"),
)

_JDBC_OPTIONS = {
    "driver":    "com.mysql.cj.jdbc.Driver",
    "user":      os.getenv("MYSQL_USER",     "admin"),
    "password":  os.getenv("MYSQL_PASSWORD", "admin123"),
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


@dp.materialized_view(name="bronze_customers")
def bronze_customers():
    return _read_table("customers")


@dp.materialized_view(name="bronze_orders")
def bronze_orders():
    return _read_table("orders")
