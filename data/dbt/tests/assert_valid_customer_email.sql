-- Singular test: email phải đúng định dạng cơ bản (có '@').
-- Cho phép NULL vì đó là phạm vi của test not_null riêng.
select
    order_id,
    customer_email,
    source_system,
    source_version
from {{ ref('stg_orders') }}
where customer_email is not null
  and customer_email not like '%@%'
