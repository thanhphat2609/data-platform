import os

from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job

from src.docker_runner import run_docker_pipeline

_DLT_IMAGE = os.getenv("DLT_IMAGE", "dlt-pipeline:latest")


def _env() -> dict[str, str]:
    keys = [
        "DESTINATION__FILESYSTEM__BUCKET_URL",
        "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME",
        "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY",
        "POSTGRES_CONNECTION_STRING",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


@asset(
    key_prefix=["dlt"],
    group_name="dlt_crm",
    op_tags={"dagster-celery/queue": "worker-1"},
    description="Full load of orders, customers, products from CRM PostgreSQL → Azure Blob bronze/dlt/crm/",
)
def dlt_crm_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_DLT_IMAGE,
        cmd=["python", "pipelines/postgres_pipeline.py"],
        env_vars=_env(),
    )


job = define_asset_job(
    name="dlt_crm_job",
    selection=AssetSelection.groups("dlt_crm"),
    description="dlt CRM ingestion: full load of orders, customers, products from PostgreSQL → Azure Blob bronze/dlt/crm/",
)

schedule = ScheduleDefinition(
    name="daily_dlt_crm_schedule",
    job=job,
    cron_schedule="0 1 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
