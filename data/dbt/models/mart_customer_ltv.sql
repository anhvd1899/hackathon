-- Tier-0 mart: lifetime value theo khach hang.
-- LEFT JOIN de giu lai ca cac don co customer_id NULL -> giup phat hien o nhiem
-- tu bang fact lan xuong mart.
select
    f.customer_id,
    c.segment,
    c.region,
    count(*)            as lifetime_orders,
    sum(f.total_amount) as lifetime_value,
    max(f.order_date)   as last_order_date
from {{ source('main', 'fact_orders') }} f
left join {{ source('main', 'dim_customers') }} c using (customer_id)
group by f.customer_id, c.segment, c.region
