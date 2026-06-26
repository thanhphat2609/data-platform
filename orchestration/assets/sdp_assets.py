import os

import docker
from dagster import OpExecutionContext, asset

_SPARK_CONTAINER = os.getenv("SPARK_CONTAINER_NAME", "demo_spark")


def _exec_spark(context: OpExecutionContext, spec: str) -> None:
    client = docker.from_env()
    container = client.containers.get(_SPARK_CONTAINER)
    exit_code, output = container.exec_run(
        f"spark-pipelines run --spec {spec}",
        demux=False,
    )
    context.log.info(output.decode("utf-8", errors="replace"))
    if exit_code != 0:
        raise RuntimeError(f"spark-pipelines exited {exit_code} for spec={spec}")


@asset(
    key_prefix=["sdp"],
    group_name="sdp_ingestion",
    description="Ingest MySQL ecommerce DB (5 tables) → Azure Blob bronze/sdp/ecommerce/ via Spark SDP",
)
def sdp_ecommerce_ingestion(context: OpExecutionContext) -> None:
    _exec_spark(context, "/opt/spark/pipeline/ecommerce/pipeline.yml")


@asset(
    key_prefix=["sdp"],
    group_name="sdp_ingestion",
    description="Ingest PostgreSQL manage DB (5 tables) → Azure Blob bronze/sdp/manage/ via Spark SDP",
)
def sdp_manage_ingestion(context: OpExecutionContext) -> None:
    _exec_spark(context, "/opt/spark/pipeline/manage/pipeline.yml")
