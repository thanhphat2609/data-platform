from dagster import DefaultScheduleStatus, ScheduleDefinition

from jobs import sales_job, sap_job

daily_sales_schedule = ScheduleDefinition(
    name="daily_sales_schedule",
    job=sales_job,
    cron_schedule="15 22 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    # default_status=DefaultScheduleStatus.RUNNING,
)

daily_sap_schedule = ScheduleDefinition(
    name="daily_sap_schedule",
    job=sap_job,
    cron_schedule="15 22 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    # default_status=DefaultScheduleStatus.RUNNING,
)
