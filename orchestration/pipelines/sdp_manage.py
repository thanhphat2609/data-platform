import os

from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job

from src.docker_runner import run_docker_pipeline

_SDP_IMAGE = os.getenv("SDP_IMAGE", "spark-pipeline:latest")
_SPEC = "/opt/spark/pipeline/manage/pipeline.yml"


def _env() -> dict[str, str]:
    keys = [
        "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_MANAGE_DB",
        "POSTGRES_USER", "POSTGRES_PASSWORD",
        "AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT_KEY",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


@asset(
    key_prefix=["sdp"],
    group_name="sdp_manage",
    op_tags={"dagster-celery/queue": "worker-2"},
    description="Ingest PostgreSQL manage DB (5 tables) → Azure Blob bronze/sdp/manage/ via Spark SDP",
)
def sdp_manage_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_SDP_IMAGE,
        cmd=[
            "bash", "-c",
            f"envsubst '${{AZURE_STORAGE_ACCOUNT_NAME}} ${{AZURE_STORAGE_ACCOUNT_KEY}}'"
            f" < {_SPEC} > /tmp/pipeline.yml"
            f" && spark-pipelines run --spec /tmp/pipeline.yml",
        ],
        env_vars=_env(),
    )


job = define_asset_job(
    name="sdp_manage_job",
    selection=AssetSelection.groups("sdp_manage"),
    description="Spark SDP manage ingestion: PostgreSQL manage (5 tables) → Azure Blob bronze/sdp/manage/",
)

schedule = ScheduleDefinition(
    name="daily_sdp_manage_schedule",
    job=job,
    cron_schedule="30 2 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
