{{ config(materialized="table") }}

select
    date_trunc('month', order_date) as order_month,
    category,
    sub_category,
    sum(sales) as total_sales,
    sum(profit) as total_profit,
    count(distinct order_id) as num_orders
from {{ ref("stg_orders") }}
group by 1, 2, 3
order by 1, 2, 3
