{{ config(
    materialized = 'external',
    location     = dest_location('manage', 'int_tasks'),
    format       = 'parquet',
    options={
            "partition_by": "ingestion_date",
            "overwrite": True
    }
) }}

with tasks as (
    select * from {{ ref('stg_manage__tasks') }}
),

projects as (
    select * from {{ ref('stg_manage__projects') }}
)

select
    tasks.task_id,
    tasks.project_id,
    projects.project_name,
    projects.project_status,
    tasks.title       as task_title,
    tasks.description,
    tasks.assignee_id,
    tasks.task_status,
    tasks.priority,
    tasks.due_date,
    tasks.created_at,
    tasks.ingestion_date
from tasks
left join projects using (project_id)
