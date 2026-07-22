with source as (
    select * from {{ source('events', 'exchange_rates') }}
)

select
    date,
    base            as base_currency,
    target          as target_currency,
    rate,
    ingestion_date
from source
