-- Singular test: order_id là khoá chính, không được trùng.
-- Viết thành singular test (thay vì generic `unique`) để thông báo lỗi trả về luôn
-- danh sách khoá bị trùng — agent dùng trực tiếp làm bằng chứng.
select
    order_id,
    count(*) as occurrences
from {{ ref('stg_orders') }}
group by order_id
having count(*) > 1
