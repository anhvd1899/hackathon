-- Mart QUAN SÁT CHẤT LƯỢNG: tổng hợp cờ lỗi theo nguồn + phiên bản.
--
-- Đây là bảng mà agent dùng để khoanh vùng nguyên nhân cho BẤT KỲ sự cố nào, không chỉ
-- ca demo: chỉ cần GROUP BY nguồn/phiên bản là thấy lỗi tập trung ở đâu.
{{ config(materialized='table') }}

select
    source_system,
    source_version,
    order_date,
    count(*)                        as rows_total,
    sum(flag_missing_customer)      as missing_customer_rows,
    sum(flag_bad_email)             as bad_email_rows,
    sum(flag_negative_amount)       as negative_amount_rows,
    sum(flag_bad_status)            as bad_status_rows,
    sum(flag_missing_customer
        + flag_bad_email
        + flag_negative_amount
        + flag_bad_status)          as total_defects
from {{ ref('stg_orders') }}
group by 1, 2, 3
order by total_defects desc, order_date desc
