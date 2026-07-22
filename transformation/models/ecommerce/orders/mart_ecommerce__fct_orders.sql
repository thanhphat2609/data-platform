{{ config(
    materialized = 'external',
    location = env_var('GOLD_ECOMMERCE_ORDERS_PATH', 'az://gold/ecommerce/fct_orders'),
    format = 'parquet'
) }}

with orders as (
    select * from {{ ref('int_ecommerce__orders') }}
),

customers as (
    select * from {{ ref('stg_ecommerce__customers') }}
)

select
    orders.order_id,
    orders.customer_id,
    customers.customer_name,
    customers.email          as customer_email,
    orders.order_status,
    orders.total_amount,
    orders.item_count,
    orders.total_quantity,
    orders.items_subtotal,
    orders.order_date,
    orders.ingestion_date
from orders
left join customers using (customer_id)
