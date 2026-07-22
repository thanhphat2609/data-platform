with source as (
    select * from {{ source('ecommerce', 'order_items') }}
)

select
    id             as order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    subtotal,
    created_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
