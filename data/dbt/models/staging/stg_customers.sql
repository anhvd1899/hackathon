-- Staging layer: dimension khách hàng.
{{ config(materialized='table') }}

select
    customer_id,
    customer_name,
    segment,
    region,
    signup_date
from {{ source('warehouse', 'dim_customers') }}
