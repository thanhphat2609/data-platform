import os

from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job

from src.docker_runner import run_docker_pipeline

_DBT_IMAGE = os.getenv("DBT_IMAGE", "dbt-pipeline:latest")


def _env() -> dict[str, str]:
    keys = [
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "GOLD_ECOMMERCE_ORDERS_PATH",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


# Transforms the domains Spark SDP owns (ecommerce, manage) — runs on
# worker-2 right after sdp_ecommerce/sdp_manage land bronze there, in
# parallel with dbt_transform_dlt on worker-1. Shares worker-2 with Spark
# rather than a dedicated VM — DuckDB scales vertically (cores/RAM on one
# box), not by adding nodes, so there's no distributed-compute reason to
# stand up a third worker for it.
@asset(
    key_prefix=["dbt"],
    group_name="dbt_transform_sdp",
    op_tags={"dagster-celery/queue": "worker-2"},
    description="dbt build --select ecommerce manage: bronze (Delta on Azure Blob) -> staging/intermediate/mart via DuckDB",
)
def dbt_transform_sdp(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_DBT_IMAGE,
        cmd=["dbt", "build", "--profiles-dir", "/app/profiles", "--select", "ecommerce", "manage"],
        env_vars=_env(),
    )


job = define_asset_job(
    name="dbt_transform_sdp_job",
    selection=AssetSelection.groups("dbt_transform_sdp"),
    description="dbt build for ecommerce + manage domains (Spark SDP-sourced bronze)",
)

# 03:00 ICT — after Spark SDP (02:00/02:30).
schedule = ScheduleDefinition(
    name="daily_dbt_transform_sdp_schedule",
    job=job,
    cron_schedule="0 3 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
