import os

from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job

from src.docker_runner import run_docker_pipeline

_DBT_IMAGE = os.getenv("DBT_IMAGE", "dbt-pipeline:latest")


def _env() -> dict[str, str]:
    keys = ["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT_KEY"]
    return {k: v for k in keys if (v := os.getenv(k))}


# Transforms the domains dlt owns (crm, events) — runs on worker-1 right after
# dlt_crm/dlt_api land bronze there, in parallel with dbt_transform_sdp on
# worker-2. Split by domain rather than one combined job so each half starts
# as soon as its own bronze is ready instead of waiting on the other tool.
@asset(
    key_prefix=["dbt"],
    group_name="dbt_transform_dlt",
    op_tags={"dagster-celery/queue": "worker-1"},
    description="dbt build --select crm events: bronze (Parquet on Azure Blob) -> staging via DuckDB",
)
def dbt_transform_dlt(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_DBT_IMAGE,
        cmd=["dbt", "build", "--profiles-dir", "/app", "--target", "prod", "--select", "crm", "events"],
        env_vars=_env(),
    )


job = define_asset_job(
    name="dbt_transform_dlt_job",
    selection=AssetSelection.groups("dbt_transform_dlt"),
    description="dbt build for crm + events domains (dlt-sourced bronze)",
)

# 02:00 ICT — after dlt (01:00/01:30). Independent of Spark SDP's schedule.
schedule = ScheduleDefinition(
    name="daily_dbt_transform_dlt_schedule",
    job=job,
    cron_schedule="0 2 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
