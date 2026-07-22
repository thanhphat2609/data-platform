with source as (
    select * from {{ source('ecommerce', 'customers') }}
)

select
    id          as customer_id,
    name        as customer_name,
    email,
    phone,
    address,
    created_at,
    updated_at,
    ingestion_date
from source
where {{ filter_ingestion_date('ingestion_date') }}
