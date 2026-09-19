-- Tier-0 mart: doanh thu theo ngay.
-- Giu dong bo voi MART_SQL trong data/warehouse.py de ca 2 duong (Python job va dbt)
-- deu tao ra cung mot ket qua.
select
    order_date,
    count(*)                                            as order_count,
    count(distinct customer_id)                         as unique_customers,
    sum(total_amount)                                   as gross_revenue,
    sum(case when customer_id is null then 1 else 0 end) as orphan_orders
from {{ source('main', 'fact_orders') }}
group by order_date
order by order_date
