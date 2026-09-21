-- Singular test: doanh thu không được âm.
-- dbt coi test PASS khi câu này trả về 0 dòng.
select
    order_id,
    total_amount,
    source_system,
    source_version
from {{ ref('stg_orders') }}
where total_amount < 0
