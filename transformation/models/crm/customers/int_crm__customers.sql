{{ config(
    materialized = 'external',
    location     = dest_location('crm', 'int_customers'),
    format       = 'parquet'
) }}

with customers as (
    select * from {{ ref('stg_crm__customers') }}
),

orders as (
    select * from {{ ref('stg_crm__orders') }}
),

order_rollup as (
    select
        customer_id,
        count(*)          as order_count,
        sum(total_amount) as total_spend
    from orders
    group by customer_id
)

select
    customers.customer_id,
    customers.customer_name,
    customers.email,
    customers.phone,
    customers.address,
    coalesce(order_rollup.order_count, 0) as order_count,
    coalesce(order_rollup.total_spend, 0) as total_spend,
    customers.ingestion_date
from customers
left join order_rollup using (customer_id)
