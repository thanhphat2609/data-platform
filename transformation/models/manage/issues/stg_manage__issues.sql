with source as (
    select * from {{ source('manage', 'issues') }}
)

select
    id             as issue_id,
    project_id,
    title,
    description,
    reporter_id,
    assignee_id,
    status         as issue_status,
    severity,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
