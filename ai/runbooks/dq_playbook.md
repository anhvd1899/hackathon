# RUNBOOK — DQ REMEDIATION PLAYBOOK (mẫu lệnh chuẩn cho DuckDB)

Áp dụng cho mọi remediation tự động do Agent đề xuất. **Bắt buộc** tuân thủ
thứ tự: *quarantine trước → xoá/sửa sau → rebuild mart → verify*.

## 1. Nguyên tắc an toàn (bất di bất dịch)

1. **Không bao giờ xoá dữ liệu mà chưa lưu bản sao** vào bảng `quarantine_<table>`.
2. Không dùng `DROP TABLE` với bảng fact/dim/mart đang phục vụ dashboard.
   Muốn thay nội dung mart thì dùng `CREATE OR REPLACE TABLE ... AS SELECT ...`.
3. Mọi `DELETE`/`UPDATE` phải có `WHERE` khoanh đúng phạm vi lỗi (kèm điều kiện
   `source_system` / `source_version` / khoảng ngày nếu xác định được pattern).
4. Luôn kèm `verification_sql` dạng `SELECT COUNT(*) ...` kỳ vọng kết quả = 0.
5. Ghi lại lý do quarantine vào cột `quarantine_reason` để truy vết.

## 2. Playbook: `not_null` fail (ví dụ `customer_id IS NULL`)

Hành động khuyến nghị: `QUARANTINE_DATA`.

```sql
-- Bước 1: tạo bảng cách ly (idempotent) và nạp các dòng vi phạm
CREATE TABLE IF NOT EXISTS quarantine_fact_orders AS
SELECT *,
       CAST(NULL AS VARCHAR)   AS quarantine_reason,
       CAST(NULL AS TIMESTAMP) AS quarantined_at
FROM fact_orders
WHERE 1 = 0;

INSERT INTO quarantine_fact_orders
SELECT *, 'NULL_CUSTOMER_ID_SDK_3_4_1' AS quarantine_reason, now() AS quarantined_at
FROM fact_orders
WHERE customer_id IS NULL;

-- Bước 2: loại các dòng bẩn khỏi bảng phục vụ (fact_orders)
DELETE FROM fact_orders WHERE customer_id IS NULL;

-- Bước 3: rebuild mart hạ nguồn để dashboard hết sai
CREATE OR REPLACE TABLE mart_daily_revenue AS
SELECT order_date,
       COUNT(*)                    AS order_count,
       COUNT(DISTINCT customer_id) AS unique_customers,
       SUM(total_amount)           AS gross_revenue,
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_orders
FROM fact_orders
GROUP BY order_date;

CREATE OR REPLACE TABLE mart_customer_ltv AS
SELECT f.customer_id, c.segment, c.region,
       COUNT(*) AS lifetime_orders,
       SUM(f.total_amount) AS lifetime_value,
       MAX(f.order_date) AS last_order_date
FROM fact_orders f
LEFT JOIN dim_customers c USING (customer_id)
GROUP BY f.customer_id, c.segment, c.region;
```

Verify:

```sql
SELECT COUNT(*) AS violations FROM fact_orders WHERE customer_id IS NULL;
```

Rollback:

```sql
INSERT INTO fact_orders
SELECT * EXCLUDE (quarantine_reason, quarantined_at) FROM quarantine_fact_orders
WHERE quarantine_reason = 'NULL_CUSTOMER_ID_SDK_3_4_1';
```

## 3. Playbook: `accepted_values` fail (order_status sai định dạng)

Hành động khuyến nghị: `MANUAL_FIX` (chuẩn hoá tại chỗ, không cần xoá dòng).

```sql
UPDATE fact_orders
SET order_status = UPPER(TRIM(order_status))
WHERE order_status IS NOT NULL
  AND order_status <> UPPER(TRIM(order_status));

UPDATE fact_orders
SET order_status = 'COMPLETED'
WHERE UPPER(TRIM(order_status)) IN ('COMPLETE', 'COMPLETD');
```

Verify:

```sql
SELECT COUNT(*) AS violations FROM fact_orders
WHERE order_status NOT IN ('COMPLETED','PENDING','CANCELLED','REFUNDED');
```

## 4. Playbook: `unique` fail (duplicate order_id)

```sql
CREATE OR REPLACE TABLE fact_orders AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
  FROM fact_orders
) WHERE rn = 1;
ALTER TABLE fact_orders DROP COLUMN rn;
```

Verify:

```sql
SELECT COUNT(*) AS violations FROM (
  SELECT order_id FROM fact_orders GROUP BY order_id HAVING COUNT(*) > 1
);
```

## 5. Playbook: `BACKFILL` khi upstream đã fix

Chỉ dùng khi team upstream xác nhận đã sửa mapping và re-publish dữ liệu:

```sql
DELETE FROM fact_orders
WHERE source_system = '<source>' AND order_date BETWEEN '<from>' AND '<to>';
-- sau đó rerun job ingestion cho cửa sổ đó
```
