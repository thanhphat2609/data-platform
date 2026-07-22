with source as (
    select * from {{ source('crm', 'products') }}
)

select
    id             as product_id,
    name           as product_name,
    description,
    price,
    stock_qty,
    created_at,
    updated_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
