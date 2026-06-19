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
