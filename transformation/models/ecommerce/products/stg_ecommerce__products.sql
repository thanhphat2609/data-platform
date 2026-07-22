with source as (
    select * from {{ source('ecommerce', 'products') }}
)

select
    id             as product_id,
    category_id,
    name           as product_name,
    description,
    price,
    stock_qty,
    created_at,
    updated_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
