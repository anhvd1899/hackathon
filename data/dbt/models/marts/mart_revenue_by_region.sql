-- Tier-1 mart: doanh thu theo vùng + segment. Cho thấy sự cố lan xuống nhiều nhánh
-- trong lineage graph, không chỉ một nhánh.
{{ config(materialized='table') }}

select
    coalesce(c.region, 'UNKNOWN')   as region,
    coalesce(c.segment, 'UNKNOWN')  as segment,
    count(*)                        as order_count,
    count(distinct o.customer_id)   as unique_customers,
    sum(o.total_amount)             as gross_revenue
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c using (customer_id)
group by 1, 2
