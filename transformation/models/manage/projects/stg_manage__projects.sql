with source as (
    select * from {{ source('manage', 'projects') }}
)

select
    id             as project_id,
    name           as project_name,
    description,
    owner_id,
    status         as project_status,
    start_date,
    end_date,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
