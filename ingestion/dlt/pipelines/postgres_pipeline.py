"""
CRM full-copy pipeline → Azure Blob bronze/dlt/crm/

Reads all rows on every run (replace disposition — no incremental state needed).
Output path: az://bronze/dlt/crm/<table>/ingestion_date=<date>/<load_id>.<file_id>.parquet
"""
import os
from datetime import date
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


@dlt.source(name="crm")
def crm_source():
    return [orders(), customers(), products()]


@dlt.resource(name="orders", write_disposition="replace")
def orders() -> Iterator[dict]:
    today = date.today().isoformat()
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM orders ORDER BY id ASC")
        yield from ({**dict(row), "ingestion_date": today} for row in cur)


@dlt.resource(name="customers", write_disposition="replace")
def customers() -> Iterator[dict]:
    today = date.today().isoformat()
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM customers ORDER BY id ASC")
        yield from ({**dict(row), "ingestion_date": today} for row in cur)


@dlt.resource(name="products", write_disposition="replace")
def products() -> Iterator[dict]:
    today = date.today().isoformat()
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM products ORDER BY id ASC")
        yield from ({**dict(row), "ingestion_date": today} for row in cur)


if __name__ == "__main__":
    from dlt.destinations import filesystem

    account_name = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY")
    bucket_url = os.getenv("DESTINATION__FILESYSTEM__BUCKET_URL", "az://bronze/dlt")

    # When env vars are present (Docker / CI), inject them directly so dlt's
    # AzureCredentialsWithoutDefaults resolver doesn't fail to find the key.
    # When they're absent (local dev), fall back to .dlt/secrets.toml resolution.
    dest = (
        filesystem(bucket_url=bucket_url, credentials={"azure_storage_account_name": account_name, "azure_storage_account_key": account_key})
        if account_name and account_key
        else filesystem(bucket_url=bucket_url)
    )

    pipeline = dlt.pipeline(
        pipeline_name="crm_ingestion",
        destination=dest,
        dataset_name="crm",
    )
    load_info = pipeline.run(crm_source(), loader_file_format="parquet")
    print(load_info)
