from dagster import ScheduleDefinition

from jobs import dlt_ingestion_job, sap_job, sales_job, sdp_ingestion_job

daily_sales_schedule = ScheduleDefinition(
    name="daily_sales_schedule",
    job=sales_job,
    cron_schedule="15 22 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)

daily_sap_schedule = ScheduleDefinition(
    name="daily_sap_schedule",
    job=sap_job,
    cron_schedule="15 22 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)

daily_dlt_schedule = ScheduleDefinition(
    name="daily_dlt_schedule",
    job=dlt_ingestion_job,
    cron_schedule="0 1 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)

daily_sdp_schedule = ScheduleDefinition(
    name="daily_sdp_schedule",
    job=sdp_ingestion_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
)
