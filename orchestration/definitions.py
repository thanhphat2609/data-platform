from dagster import Definitions

from pipelines import dlt_api, dlt_crm, sdp_ecommerce, sdp_manage

defs = Definitions(
    assets=[
        dlt_crm.dlt_crm_ingestion,
        dlt_api.dlt_api_ingestion,
        sdp_ecommerce.sdp_ecommerce_ingestion,
        sdp_manage.sdp_manage_ingestion,
    ],
    jobs=[
        dlt_crm.job,
        dlt_api.job,
        sdp_ecommerce.job,
        sdp_manage.job,
    ],
    schedules=[
        dlt_crm.schedule,
        dlt_api.schedule,
        sdp_ecommerce.schedule,
        sdp_manage.schedule,
    ],
)
