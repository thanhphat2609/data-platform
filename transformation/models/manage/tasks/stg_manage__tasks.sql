with source as (
    select * from {{ source('manage', 'tasks') }}
)

select
    id             as task_id,
    project_id,
    title,
    description,
    assignee_id,
    status         as task_status,
    priority,
    due_date,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
