import os

from dagster import AssetExecutionContext, asset

from src.docker_runner import run_docker_pipeline

_DLT_IMAGE = os.getenv("DLT_IMAGE", "dlt-pipeline:latest")


def _dlt_env() -> dict[str, str]:
    """Forward dlt credential env vars from the Dagster process into the container."""
    keys = [
        "DESTINATION__FILESYSTEM__BUCKET_URL",
        "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME",
        "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY",
        "POSTGRES_CONNECTION_STRING",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


@asset(
    key_prefix=["dlt"],
    group_name="dlt_ingestion",
    description="Full load of orders, customers, products from CRM (platform DB) → Azure Blob bronze/dlt/crm/",
)
def dlt_crm_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_DLT_IMAGE,
        cmd=["python", "pipelines/postgres_pipeline.py"],
        env_vars=_dlt_env(),
    )


@asset(
    key_prefix=["dlt"],
    group_name="dlt_ingestion",
    description="Incremental load of USGS Earthquake Events, Open-Meteo, Exchange Rates → Azure Blob bronze/dlt/events/",
)
def dlt_events_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_DLT_IMAGE,
        cmd=["python", "pipelines/api_pipeline.py"],
        env_vars=_dlt_env(),
    )
