import os

from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job

from src.docker_runner import run_docker_pipeline

_DLT_IMAGE = os.getenv("DLT_IMAGE", "dlt-pipeline:latest")


def _env() -> dict[str, str]:
    keys = [
        "DESTINATION__FILESYSTEM__BUCKET_URL",
        "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME",
        "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


@asset(
    key_prefix=["dlt"],
    group_name="dlt_api",
    op_tags={"dagster-celery/queue": "worker-1"},
    description="Incremental load of USGS Earthquake Events, Open-Meteo, Exchange Rates → Azure Blob bronze/dlt/events/",
)
def dlt_api_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_DLT_IMAGE,
        cmd=["python", "pipelines/api_pipeline.py"],
        env_vars=_env(),
    )


job = define_asset_job(
    name="dlt_api_job",
    selection=AssetSelection.groups("dlt_api"),
    description="dlt API ingestion: incremental load of USGS Earthquake, Open-Meteo, Exchange Rates → Azure Blob bronze/dlt/events/",
)

schedule = ScheduleDefinition(
    name="daily_dlt_api_schedule",
    job=job,
    cron_schedule="30 1 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
