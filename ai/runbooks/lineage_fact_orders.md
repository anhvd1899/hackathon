# RUNBOOK — DATA LINEAGE của `main.fact_orders`

Nguồn: DataHub export ngày 2026-09-14 · Cập nhật tự động từ dbt manifest.

## 1. Upstream (nguồn nạp vào fact_orders)

| Nguồn (`source_system`) | Cơ chế nạp | Tần suất | Owner | Ghi chú |
| --- | --- | --- | --- | --- |
| `erp_core` | CDC qua Debezium | 15 phút | Team ERP | ổn định |
| `web_checkout` | Batch export CSV | 1 giờ | Team Web | ổn định |
| `mobile_app_v3` | REST ingestion job `ingest_mobile_app_v3` | 30 phút | Team Mobile | **Vừa rollout SDK 3.4.1 ngày 2026-09-15**, đổi tên field `user_ref` → `customer_ref` |
| `partner_api` | SFTP + partner API | 1 ngày | Team Partnership | thường trễ |

**Cảnh báo đã biết:** SDK mobile 3.4.1 đổi tên field định danh khách hàng.
Job ingestion vẫn map theo tên cũ (`user_ref`) nên ghi NULL vào `customer_id`.
Cột `source_version` trong `fact_orders` cho biết payload đến từ SDK nào —
dùng `GROUP BY source_system, source_version` để khoanh vùng nhanh.

## 2. Downstream (bảng bị ảnh hưởng khi fact_orders lỗi)

```
main.fact_orders
├── main.mart_daily_revenue        (Tier-0) -> đếm unique_customers, gross_revenue theo ngày
│     └── Dashboard: "Finance Daily Revenue"  (Metabase, gửi mail 08:00 hằng ngày)
│     └── Dashboard: "Executive KPI Overview" (Metabase, C-level xem 09:00)
├── main.mart_customer_ltv         (Tier-0) -> LTV theo customer, JOIN dim_customers
│     └── Dashboard: "Customer 360 & Retention" (Metabase)
│     └── Consumer: CRM segmentation job (đẩy audience sang hệ thống marketing)
└── ML feature store: feature_customer_recency (job train chạy 05:00)
```

## 3. Dashboard / consumer chi tiết

| Dashboard / Consumer | Bảng đọc | Người dùng | Ảnh hưởng khi customer_id NULL |
| --- | --- | --- | --- |
| Finance Daily Revenue | `mart_daily_revenue` | Finance, CFO | `unique_customers` bị đếm thiếu, doanh thu vẫn có nhưng không quy được về khách |
| Executive KPI Overview | `mart_daily_revenue` | Ban điều hành | KPI khách hàng mới sai |
| Customer 360 & Retention | `mart_customer_ltv` | CRM, Growth | phát sinh nhóm customer_id = NULL, LTV bị gom sai |
| CRM segmentation job | `mart_customer_ltv` | Marketing automation | audience nhận sai, có thể gửi campaign lệch |
| feature_customer_recency | `fact_orders` | Data Science | feature bị null-leak vào model |

## 4. Thứ tự refresh lại sau khi vá

1. Vá / quarantine trên `main.fact_orders`.
2. Rebuild `main.mart_daily_revenue` (`CREATE OR REPLACE TABLE ... AS SELECT ...`).
3. Rebuild `main.mart_customer_ltv`.
4. Trigger lại dashboard cache (Metabase auto refresh 15 phút).
