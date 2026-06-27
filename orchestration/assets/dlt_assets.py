import os

import dlt
from dagster import OpExecutionContext, asset

_POSTGRES_CONN = os.getenv(
    "POSTGRES_CONNECTION_STRING",
    "postgresql://admin:admin123@postgres-platform:5432/platform",
)


def _pipeline(name: str, dataset: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=name,
        destination="filesystem",
        dataset_name=dataset,
    )


@asset(
    key_prefix=["dlt"],
    group_name="dlt_ingestion",
    description="Full load of orders, customers, products from CRM (platform DB) → Azure Blob bronze/dlt/crm/",
)
def dlt_crm_ingestion(context: OpExecutionContext) -> None:
    import sys
    sys.path.insert(0, "/opt/dagster/app/ingestion/dlt")
    from pipelines.postgres_pipeline import crm_source

    os.environ.setdefault("POSTGRES_CONNECTION_STRING", _POSTGRES_CONN)
    pipeline = _pipeline("crm_ingestion", "crm")
    info = pipeline.run(crm_source())
    context.log.info(str(info))


@asset(
    key_prefix=["dlt"],
    group_name="dlt_ingestion",
    description="Incremental load of USGS Earthquake Events, Open-Meteo, Exchange Rates → Azure Blob bronze/dlt/events/",
)
def dlt_events_ingestion(context: OpExecutionContext) -> None:
    import sys
    sys.path.insert(0, "/opt/dagster/app/ingestion/dlt")
    from pipelines.api_pipeline import events_source

    pipeline = _pipeline("events_ingestion", "events")
    info = pipeline.run(events_source())
    context.log.info(str(info))
