"""
Read and preview dlt output from Azure Blob Storage.

Usage:
  docker run --rm \
    -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=<account> \
    -e DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY=<key> \
    [-e DESTINATION__FILESYSTEM__BUCKET_URL=az://bronze/dlt] \
    dlt-pipeline:latest python pipelines/verify_output.py
"""
import os

import adlfs
import pyarrow.parquet as pq

account_name = os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"]
account_key  = os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"]
bucket_url   = os.getenv("DESTINATION__FILESYSTEM__BUCKET_URL", "az://bronze/dlt")

base_path = bucket_url.removeprefix("az://")  # e.g. "bronze/dlt"

fs = adlfs.AzureBlobFileSystem(account_name=account_name, account_key=account_key)

print(f"Scanning {bucket_url} ...\n")

entries = fs.glob(f"{base_path}/**")
files   = [e for e in entries if not fs.isdir(e)]

if not files:
    print("No files found.")
    raise SystemExit(0)

for path in sorted(files):
    size = fs.info(path)["size"]
    print(f"{path}  ({size / 1024:.1f} KiB)")
    try:
        with fs.open(path, "rb") as fh:
            table = pq.read_table(fh)
        cols = table.schema.names
        print(f"  rows={table.num_rows}  cols={cols[:6]}{'...' if len(cols) > 6 else ''}")
        sample = table.slice(0, 2).to_pydict()
        for col, vals in sample.items():
            print(f"    {col}: {vals}")
    except Exception as exc:
        print(f"  [not readable as parquet: {exc}]")
    print()
