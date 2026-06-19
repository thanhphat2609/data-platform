"""
PostgreSQL full-copy pipeline → MinIO bronze/dlt/postgres/

Reads all rows on every run (replace disposition — no incremental state needed).
Output path: s3://bronze/dlt/postgres/<table>/_dlt_load_id=<id>/*.parquet
"""
import os
from typing import Iterator

import dlt
import psycopg2
from psycopg2.extras import RealDictCursor

_CONN_STR = os.getenv(
    "POSTGRES_CONNECTION_STRING",
    "postgresql://admin:admin123@localhost:5432/platform",
)


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(_CONN_STR, cursor_factory=RealDictCursor)


@dlt.source(name="postgres")
def postgres_source():
    return [orders(), customers(), products()]


@dlt.resource(name="orders", write_disposition="replace")
def orders() -> Iterator[dict]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM orders ORDER BY id ASC")
        yield from (dict(row) for row in cur)


@dlt.resource(name="customers", write_disposition="replace")
def customers() -> Iterator[dict]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM customers ORDER BY id ASC")
        yield from (dict(row) for row in cur)


@dlt.resource(name="products", write_disposition="replace")
def products() -> Iterator[dict]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM products ORDER BY id ASC")
        yield from (dict(row) for row in cur)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="postgres",
        destination="filesystem",
        dataset_name="postgres",
    )
    load_info = pipeline.run(postgres_source())
    print(load_info)
