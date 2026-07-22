with source as (
    select * from {{ source('ecommerce', 'orders') }}
)

select
    id             as order_id,
    customer_id,
    status         as order_status,
    total_amount,
    order_date,
    created_at,
    updated_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
