from dagster import Definitions

from assets import dlt, sales, sap, sdp
from jobs import dlt_ingestion_job, sales_job, sap_job, sdp_ingestion_job
from resources import blob_io_manager
from schedules import (
    daily_dlt_schedule,
    daily_sales_schedule,
    daily_sap_schedule,
    daily_sdp_schedule,
)

defs = Definitions(
    assets=[*sales, *sap, *dlt, *sdp],
    jobs=[sales_job, sap_job, dlt_ingestion_job, sdp_ingestion_job],
    schedules=[
        daily_sales_schedule,
        daily_sap_schedule,
        daily_dlt_schedule,
        daily_sdp_schedule,
    ],
    resources={
        "io_manager": blob_io_manager,
    },
)
