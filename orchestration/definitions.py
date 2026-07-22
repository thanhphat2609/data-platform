from dagster import Definitions
from dagster_celery import celery_executor

from pipelines import dbt_transform_dlt, dbt_transform_sdp, dlt_api, dlt_crm, sdp_ecommerce, sdp_manage

_celery = celery_executor.configured({
    "broker": {"env": "CELERY_BROKER_URL"},
    "backend": {"env": "CELERY_RESULT_BACKEND"},
})

defs = Definitions(
    assets=[
        dlt_crm.dlt_crm_ingestion,
        dlt_api.dlt_api_ingestion,
        sdp_ecommerce.sdp_ecommerce_ingestion,
        sdp_manage.sdp_manage_ingestion,
        dbt_transform_dlt.dbt_transform_dlt,
        dbt_transform_sdp.dbt_transform_sdp,
    ],
    jobs=[
        dlt_crm.job,
        dlt_api.job,
        sdp_ecommerce.job,
        sdp_manage.job,
        dbt_transform_dlt.job,
        dbt_transform_sdp.job,
    ],
    schedules=[
        dlt_crm.schedule,
        dlt_api.schedule,
        sdp_ecommerce.schedule,
        sdp_manage.schedule,
        dbt_transform_dlt.schedule,
        dbt_transform_sdp.schedule,
    ],
    executor=_celery,
)
