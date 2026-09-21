-- Tier-0 mart: lifetime value theo khách hàng. Customer 360 + CRM segmentation đọc.
-- LEFT JOIN để giữ cả đơn có customer_id NULL -> thấy được ô nhiễm lan từ fact xuống mart.
{{ config(materialized='table') }}

select
    o.customer_id,
    c.segment,
    c.region,
    count(*)            as lifetime_orders,
    sum(o.total_amount) as lifetime_value,
    max(o.order_date)   as last_order_date
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c using (customer_id)
group by o.customer_id, c.segment, c.region
