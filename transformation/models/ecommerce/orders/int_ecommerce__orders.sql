{{ config(
    materialized = 'external',
    location     = dest_location('ecommerce', 'int_orders'),
    format       = 'parquet'
) }}

with orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
),

order_items as (
    select * from {{ ref('stg_ecommerce__order_items') }}
),

item_rollup as (
    select
        order_id,
        count(*)      as item_count,
        sum(quantity) as total_quantity,
        sum(subtotal) as items_subtotal
    from order_items
    group by order_id
)

select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.total_amount,
    orders.order_date,
    item_rollup.item_count,
    item_rollup.total_quantity,
    item_rollup.items_subtotal,
    orders.ingestion_date
from orders
left join item_rollup using (order_id)
