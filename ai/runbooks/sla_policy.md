# RUNBOOK — SLA & DATA FRESHNESS POLICY (Data Platform)

Phiên bản: 2026.09 · Chủ sở hữu: Data Platform Team · Kênh trực: `#data-oncall`

## 1. SLA theo tier của bảng

| Tier | Bảng | Freshness SLA | Correctness SLA | Thời gian tối đa được phép lỗi (MTTR) |
| --- | --- | --- | --- | --- |
| Tier-0 (Gold) | `mart_daily_revenue`, `mart_customer_ltv` | 06:00 mỗi ngày | 100% hàng khớp `fact_orders` | 2 giờ |
| Tier-1 (Silver) | `fact_orders`, `dim_customers` | 04:00 mỗi ngày | tỉ lệ vi phạm DQ < 0.5% | 4 giờ |
| Tier-2 (Bronze) | `stg_*`, staging layer | 03:00 mỗi ngày | best-effort | 24 giờ |

## 2. Ngưỡng vi phạm DQ (dùng để chấm severity)

| Tỉ lệ dòng vi phạm trên bảng Tier-1 | Severity |
| --- | --- |
| < 0.1% và không lan xuống Tier-0 | LOW |
| 0.1% – 1.0% | MEDIUM |
| 1.0% – 5.0% hoặc làm sai số liệu doanh thu Tier-0 | HIGH |
| > 5.0% hoặc sai lệch doanh thu đã gửi cho C-level / đối tác | CRITICAL |

**Quy tắc nâng bậc (escalation rule):** bất kỳ vi phạm nào làm mart Tier-0
sai lệch số liệu doanh thu hoặc số khách hàng duy nhất đều được nâng lên **HIGH**
tối thiểu, kể cả khi số dòng lỗi nhỏ.

## 3. SLA breach được tính khi nào

Coi là **SLA breach** nếu thoả một trong các điều kiện:

1. Bảng Tier-0 phục vụ dashboard trước 08:00 mà dữ liệu vẫn còn vi phạm DQ.
2. Job `dbt build` fail khiến mart Tier-0 không được refresh trong ngày.
3. Sự cố Tier-1 chưa được xử lý sau 4 giờ kể từ lúc alert.

Job `dbt build` hằng ngày chạy 02:30. Dashboard Finance đọc mart lúc 08:00.
=> Một sự cố phát hiện lúc 02:30 mà chưa vá trước 08:00 là **SLA breach của Tier-0**.

## 4. Cửa sổ được phép thao tác dữ liệu (change window)

- Thao tác quarantine / delete trên bảng Tier-1: cho phép 24/7 **với điều kiện**
  dữ liệu bị loại bỏ phải được ghi sang bảng `quarantine_*` trước.
- `DROP TABLE` trên Tier-0/Tier-1: **cấm** trong mọi trường hợp remediation tự động.
- Mọi remediation phải có `verification_sql` trả về 0 dòng vi phạm sau khi chạy.
