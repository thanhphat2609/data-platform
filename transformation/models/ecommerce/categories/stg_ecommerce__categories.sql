with source as (
    select * from {{ source('ecommerce', 'categories') }}
)

select
    id             as category_id,
    name           as category_name,
    description,
    parent_id      as parent_category_id,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
