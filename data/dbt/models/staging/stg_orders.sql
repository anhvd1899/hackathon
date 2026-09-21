-- Staging layer: chuẩn hoá nhẹ + gắn cờ chất lượng cho từng dòng.
-- KHÔNG lọc bỏ dòng bẩn ở đây: giữ nguyên để cổng DQ phát hiện và để agent điều tra
-- được pattern lỗi theo source_system / source_version.
{{ config(materialized='table') }}

select
    order_id,
    order_date,
    customer_id,
    customer_email,
    product_sku,
    quantity,
    unit_price,
    total_amount,
    currency,
    order_status,
    upper(trim(order_status))                                   as order_status_normalized,
    payment_method,
    source_system,
    source_version,
    ingested_at,

    -- Cờ chất lượng: mart_order_quality tổng hợp lại từ đây
    case when customer_id is null then 1 else 0 end             as flag_missing_customer,
    case when customer_email is null
              or customer_email not like '%@%' then 1 else 0 end as flag_bad_email,
    case when total_amount < 0 then 1 else 0 end                as flag_negative_amount,
    case when order_status not in ('COMPLETED', 'PENDING', 'CANCELLED', 'REFUNDED')
         then 1 else 0 end                                      as flag_bad_status
from {{ source('warehouse', 'fact_orders') }}
