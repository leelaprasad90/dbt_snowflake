{{ config(materialized="view") }}

select
    order_id,
    TO_DATE(order_date,'DD/MM/YYYY') as order_date,
    customer_id,
    customer_name,
    product_id,
    product_name,
    category,
    sub_category,
    sales,
    quantity,
    profit
from {{ source("superstore", "superstore_orders") }}
where sales is not null --and quantity > 0
