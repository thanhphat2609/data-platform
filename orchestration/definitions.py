from dagster import Definitions

from assets import sales, sap
from jobs import sales_job, sap_job
from resources import blob_io_manager
from schedules import daily_sales_schedule, daily_sap_schedule

defs = Definitions(
    assets=[*sales, *sap],
    jobs=[sales_job, sap_job],
    schedules=[daily_sales_schedule, daily_sap_schedule],
    resources={
        "io_manager": blob_io_manager,
    },
)
