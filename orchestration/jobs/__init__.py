from dagster import AssetSelection, define_asset_job

sales_job = define_asset_job(
    name="sales_job",
    selection=AssetSelection.groups("sales"),
    description="Sales pipeline: extract → transform → persist to Azure Blob Storage.",
)

sap_job = define_asset_job(
    name="sap_job",
    selection=AssetSelection.groups("sap"),
    description="SAP pipeline: extract → validate → convert VND → persist to Azure Blob Storage.",
)

dlt_ingestion_job = define_asset_job(
    name="dlt_ingestion_job",
    selection=AssetSelection.groups("dlt_ingestion"),
    description="dlt ingestion: PostgreSQL full load + REST API incremental → Azure Blob bronze/dlt/",
)

sdp_ingestion_job = define_asset_job(
    name="sdp_ingestion_job",
    selection=AssetSelection.groups("sdp_ingestion"),
    description="Spark SDP ingestion: MySQL ecommerce + PostgreSQL manage → Azure Blob bronze/sdp/",
)
