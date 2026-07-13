"""
Bronze layer — raw ingestion from MySQL `ecommerce` DB via JDBC.
Each function produces a materialized_view registered in the pipeline catalog.

spark-pipelines discovers this file via the pipeline.yml glob:
  libraries:
    - glob: { include: bronze_ingestion.py }
"""
import os
import sys

sys.path.append("/opt/spark/pipeline")

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema.ecommerce import *

_JDBC_URL = (
    "jdbc:mysql://{host}:{port}/{db}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
).format(
    host=os.getenv("MYSQL_HOST", "infra-mysql"),
    port=os.getenv("MYSQL_PORT", "3306"),
    db=os.getenv("MYSQL_DB", "ecommerce"),
)

_JDBC_OPTIONS = {
    "driver":    "com.mysql.cj.jdbc.Driver",
    "user":      os.getenv("MYSQL_USER",     "admin"),
    "password":  os.getenv("MYSQL_PASSWORD", "admin123"),
    "fetchsize": "10000",
}

_ACCOUNT_NAME   = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
_INGESTION_PATH = f"abfss://bronze@{_ACCOUNT_NAME}.dfs.core.windows.net/sdp/ecommerce"

_PARTITION_COLS = ["ingestion_date"]


def _read_table(source_table: str, query: str | None = None):
    spark = SparkSession.getActiveSession()
    dbtable = f"({query}) AS t" if query else source_table
    return (
        spark.read.format("jdbc")
        .option("url", _JDBC_URL)
        .option("dbtable", dbtable)
        .options(**_JDBC_OPTIONS)
        .load()
        .withColumn("ingestion_date", F.current_date())
    )


@dp.materialized_view(
    name="bronze_customers",
    comment="Raw customers from MySQL ecommerce DB",
    table_properties={"location": f"{_INGESTION_PATH}/bronze_customers"},
    partition_cols=_PARTITION_COLS,
    schema=_customers_schema,
)
def bronze_customers():
    return _read_table("customers", query="SELECT * FROM customers")


@dp.materialized_view(
    name="bronze_orders",
    comment="Raw orders from MySQL ecommerce DB",
    table_properties={"location": f"{_INGESTION_PATH}/bronze_orders"},
    partition_cols=_PARTITION_COLS,
    schema=_orders_schema,
)
def bronze_orders():
    return _read_table("orders", query="SELECT * FROM orders")


@dp.materialized_view(
    name="bronze_categories",
    comment="Raw product categories from MySQL ecommerce DB",
    table_properties={"location": f"{_INGESTION_PATH}/bronze_categories"},
    partition_cols=_PARTITION_COLS,
    schema=_categories_schema,
)
def bronze_categories():
    return _read_table("categories", query="SELECT * FROM categories")


@dp.materialized_view(
    name="bronze_products",
    comment="Raw products from MySQL ecommerce DB",
    table_properties={"location": f"{_INGESTION_PATH}/bronze_products"},
    partition_cols=_PARTITION_COLS,
    schema=_products_schema,
)
def bronze_products():
    return _read_table("products", query="SELECT * FROM products")


@dp.materialized_view(
    name="bronze_order_items",
    comment="Raw order line items from MySQL ecommerce DB",
    table_properties={"location": f"{_INGESTION_PATH}/bronze_order_items"},
    partition_cols=_PARTITION_COLS,
    schema=_order_items_schema,
)
def bronze_order_items():
    return _read_table("order_items", query="SELECT * FROM order_items")
