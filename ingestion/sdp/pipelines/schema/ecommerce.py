from pyspark.sql.types import (
    DateType, DecimalType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)

# ingestion_date is derived via withColumn("ingestion_date", current_date())
# in bronze_ingestion.py — it does NOT exist in MySQL, but must appear last in every schema here.

_customers_schema = StructType([
    StructField("id",             IntegerType(),      True),
    StructField("name",           StringType(),       True),
    StructField("email",          StringType(),       True),
    StructField("phone",          StringType(),       True),
    StructField("address",        StringType(),       True),
    StructField("created_at",     TimestampType(),    True),
    StructField("updated_at",     TimestampType(),    True),
    StructField("ingestion_date", DateType(),         True),
])

_orders_schema = StructType([
    StructField("id",             IntegerType(),      True),
    StructField("customer_id",    IntegerType(),      True),
    StructField("status",         StringType(),       True),
    StructField("total_amount",   DecimalType(10, 2), True),
    StructField("order_date",     TimestampType(),    True),
    StructField("created_at",     TimestampType(),    True),
    StructField("updated_at",     TimestampType(),    True),
    StructField("ingestion_date", DateType(),         True),
])

_categories_schema = StructType([
    StructField("id",             IntegerType(),      True),
    StructField("name",           StringType(),       True),
    StructField("description",    StringType(),       True),
    StructField("parent_id",      IntegerType(),      True),
    StructField("created_at",     TimestampType(),    True),
    StructField("ingestion_date", DateType(),         True),
])

_products_schema = StructType([
    StructField("id",             IntegerType(),      True),
    StructField("category_id",    IntegerType(),      True),
    StructField("name",           StringType(),       True),
    StructField("description",    StringType(),       True),
    StructField("price",          DecimalType(10, 2), True),
    StructField("stock_qty",      IntegerType(),      True),
    StructField("created_at",     TimestampType(),    True),
    StructField("updated_at",     TimestampType(),    True),
    StructField("ingestion_date", DateType(),         True),
])

_order_items_schema = StructType([
    StructField("id",             IntegerType(),      True),
    StructField("order_id",       IntegerType(),      True),
    StructField("product_id",     IntegerType(),      True),
    StructField("quantity",       IntegerType(),      True),
    StructField("unit_price",     DecimalType(10, 2), True),
    StructField("subtotal",       DecimalType(10, 2), True),
    StructField("created_at",     TimestampType(),    True),
    StructField("ingestion_date", DateType(),         True),
])
