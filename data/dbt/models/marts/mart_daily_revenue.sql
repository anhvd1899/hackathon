-- Tier-0 mart: doanh thu theo ngày. Dashboard Finance đọc lúc 08:00 hằng ngày.
-- Giữ ĐỒNG BỘ với MART_SQL['mart_daily_revenue'] trong data/warehouse.py (bản fallback
-- khi không có dbt).
{{ config(materialized='table') }}

select
    order_date,
    count(*)                                             as order_count,
    count(distinct customer_id)                          as unique_customers,
    sum(total_amount)                                    as gross_revenue,
    sum(case when customer_id is null then 1 else 0 end)  as orphan_orders
from {{ ref('stg_orders') }}
group by order_date
order by order_date
