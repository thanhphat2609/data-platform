import os

from dagster import AssetExecutionContext, asset

from src.docker_runner import run_docker_pipeline

_SDP_IMAGE = os.getenv("SDP_IMAGE", "spark-pipeline:latest")

_SPEC_BASE = "/opt/spark/pipeline"


def _sdp_env() -> dict[str, str]:
    """Forward SDP credential env vars from the Dagster process into the container."""
    keys = [
        "MYSQL_HOST", "MYSQL_PORT", "MYSQL_DB", "MYSQL_USER", "MYSQL_PASSWORD",
        "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_MANAGE_DB",
        "POSTGRES_USER", "POSTGRES_PASSWORD",
    ]
    return {k: v for k in keys if (v := os.getenv(k))}


@asset(
    key_prefix=["sdp"],
    group_name="sdp_ingestion",
    description="Ingest MySQL ecommerce DB (5 tables) → Azure Blob bronze/sdp/ecommerce/ via Spark SDP",
)
def sdp_ecommerce_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_SDP_IMAGE,
        cmd=["spark-pipelines", "run", "--spec", f"{_SPEC_BASE}/ecommerce/pipeline.yml"],
        env_vars=_sdp_env(),
    )


@asset(
    key_prefix=["sdp"],
    group_name="sdp_ingestion",
    description="Ingest PostgreSQL manage DB (5 tables) → Azure Blob bronze/sdp/manage/ via Spark SDP",
)
def sdp_manage_ingestion(context: AssetExecutionContext) -> None:
    run_docker_pipeline(
        context,
        image=_SDP_IMAGE,
        cmd=["spark-pipelines", "run", "--spec", f"{_SPEC_BASE}/manage/pipeline.yml"],
        env_vars=_sdp_env(),
    )
