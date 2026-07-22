with source as (
    select * from {{ source('manage', 'comments') }}
)

select
    id             as comment_id,
    project_id,
    author_id,
    entity_type,
    entity_id,
    body,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
