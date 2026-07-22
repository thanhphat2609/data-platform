with source as (
    select * from {{ source('manage', 'users') }}
)

select
    id             as user_id,
    username,
    email,
    role,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
