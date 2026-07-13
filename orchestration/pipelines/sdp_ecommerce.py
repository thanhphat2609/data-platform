import os

from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job

from src.docker_runner import run_docker_pipeline

_SDP_IMAGE = os.getenv("SDP_IMAGE", "spark-pipeline:latest")


def _env() -> dict[str, str]:
    keys = [
        "MYSQL_HOST", "MYSQL_PORT", "MYSQL_DB", "MYSQL_USER", "MYSQL_PASSWORD",
        "AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT_KEY",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


@asset(
    key_prefix=["sdp"],
    group_name="sdp_ecommerce",
    op_tags={"dagster-celery/queue": "worker-2"},
    description="Ingest MySQL ecommerce DB (5 tables) → Azure Blob bronze/sdp/ecommerce/ via Spark SDP",
)
def sdp_ecommerce_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_SDP_IMAGE,
        cmd=[
            "bash", "-c",
            "cp -r /opt/spark/pipeline/ecommerce /tmp/ecommerce-pipeline"
            " && envsubst '${AZURE_STORAGE_ACCOUNT_NAME} ${AZURE_STORAGE_ACCOUNT_KEY}'"
            " < /opt/spark/pipeline/ecommerce/pipeline.yml"
            " > /tmp/ecommerce-pipeline/pipeline.yml"
            " && spark-pipelines run --spec /tmp/ecommerce-pipeline/pipeline.yml",
        ],
        env_vars=_env(),
    )


job = define_asset_job(
    name="sdp_ecommerce_job",
    selection=AssetSelection.groups("sdp_ecommerce"),
    description="Spark SDP ecommerce ingestion: MySQL ecommerce (5 tables) → Azure Blob bronze/sdp/ecommerce/",
)

schedule = ScheduleDefinition(
    name="daily_sdp_ecommerce_schedule",
    job=job,
    cron_schedule="0 2 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
