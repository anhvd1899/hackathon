# RUNBOOK — DQ REMEDIATION PLAYBOOK (mẫu lệnh chuẩn, kiến trúc WAP)

Áp dụng cho mọi remediation tự động do Agent đề xuất. Mọi câu lệnh ghi
**CHỈ được nhắm vào `shadow_X` / `quarantine_X`** — TUYỆT ĐỐI KHÔNG ghi trực
tiếp bảng thật, KHÔNG rebuild mart (việc dựng lại hạ nguồn là của
`dbt run --select <model>` sau khi publish).

Trong mọi mẫu dưới đây, `X` = bảng mục tiêu của incident
(ví dụ `stg_orders`, `fact_orders`, `mart_daily_revenue` — KHÔNG fix cứng).
Luôn `DESCRIBE X` trước khi viết SQL, không giả định tên cột.

## 0. Mẫu quarantine DUY NHẤT (bắt buộc dùng nguyên văn)

```sql
CREATE OR REPLACE TABLE quarantine_X AS
SELECT *,
       'REASON'::VARCHAR AS quarantine_reason,
       CURRENT_TIMESTAMP AS quarantined_at
FROM X
WHERE <điều kiện dòng bẩn>;
```

**CẤM tách thành `CREATE TABLE IF NOT EXISTS` + `INSERT INTO` rời rạc:**
bảng cũ sót lại từ lần chạy trước sẽ giữ nguyên shape cũ, câu `INSERT`
đời sau lệch số cột → Binder Error, rollback cả remediation đúng.
`CREATE OR REPLACE` tự co giãn theo mọi bảng (14 cột hay 19 cột đều đúng).

## 1. Nguyên tắc an toàn (bất di bất dịch)

1. **Không bao giờ xoá dữ liệu mà chưa lưu bản sao** vào bảng `quarantine_X`
   (đúng mẫu mục 0, kèm `quarantine_reason` để truy vết).
2. Không dùng `DROP TABLE` với bảng fact/dim/mart/staging đang phục vụ.
   Muốn thay nội dung mart thì để `dbt run` làm sau publish.
3. Mọi `DELETE`/`UPDATE` **chỉ chạy trên `shadow_X`** và phải có `WHERE`
   khoanh đúng phạm vi lỗi (kèm `source_system` / `source_version` /
   khoảng ngày nếu xác định được pattern).
4. Luôn kèm `verification_sql` dạng `SELECT COUNT(*) ...` trên `shadow_X`,
   kỳ vọng kết quả = 0.
5. Script staging chuẩn chỉ có 3 câu:
   1. `CREATE OR REPLACE TABLE shadow_X AS SELECT * FROM X`
   2. Mẫu quarantine mục 0 (đọc từ `X`)
   3. `DELETE FROM shadow_X WHERE <điều kiện dòng bẩn>`

## 2. Playbook: `not_null` fail (ví dụ `customer_id IS NULL` trên `X`)

Hành động khuyến nghị: `QUARANTINE_DATA`.

```sql
-- Bước 1: dựng bảng bóng từ bảng thật
CREATE OR REPLACE TABLE shadow_X AS SELECT * FROM X;

-- Bước 2: cách ly các dòng vi phạm (1 câu duy nhất, tự co giãn shape)
CREATE OR REPLACE TABLE quarantine_X AS
SELECT *, 'NULL_CUSTOMER_ID'::VARCHAR AS quarantine_reason,
       CURRENT_TIMESTAMP AS quarantined_at
FROM X
WHERE customer_id IS NULL;

-- Bước 3: loại các dòng bẩn khỏi BẢNG BÓNG (bảng thật không bị chạm)
DELETE FROM shadow_X WHERE customer_id IS NULL;
```

Verify (trên bảng bóng, kỳ vọng = 0):

```sql
SELECT COUNT(*) AS violations FROM shadow_X WHERE customer_id IS NULL;
```

Rollback (bảng thật chưa từng bị chạm nên thường không cần; khi cần backfill):

```sql
INSERT INTO X
SELECT * EXCLUDE (quarantine_reason, quarantined_at) FROM quarantine_X
WHERE quarantine_reason = 'NULL_CUSTOMER_ID';
```

## 3. Playbook: `accepted_values` fail (giá trị sai định dạng)

Hành động khuyến nghị: `MANUAL_FIX` (chuẩn hoá tại chỗ **trên bảng bóng**,
không cần xoá dòng).

```sql
CREATE OR REPLACE TABLE shadow_X AS SELECT * FROM X;

UPDATE shadow_X
SET order_status = UPPER(TRIM(order_status))
WHERE order_status IS NOT NULL
  AND order_status <> UPPER(TRIM(order_status));

UPDATE shadow_X
SET order_status = 'COMPLETED'
WHERE UPPER(TRIM(order_status)) IN ('COMPLETE', 'COMPLETD');
```

Verify:

```sql
SELECT COUNT(*) AS violations FROM shadow_X
WHERE order_status NOT IN ('COMPLETED','PENDING','CANCELLED','REFUNDED');
```

## 4. Playbook: `unique` fail (trùng khoá)

Cách ly các bản ghi trùng (giữ lại 1 bản), rồi xoá khỏi **bảng bóng**:

```sql
CREATE OR REPLACE TABLE shadow_X AS SELECT * FROM X;

CREATE OR REPLACE TABLE quarantine_X AS
SELECT *, 'DUPLICATE_ORDER_ID'::VARCHAR AS quarantine_reason,
       CURRENT_TIMESTAMP AS quarantined_at
FROM X
WHERE order_id IN (SELECT order_id FROM X GROUP BY order_id HAVING COUNT(*) > 1)
  AND rowid NOT IN (SELECT MIN(rowid) FROM X GROUP BY order_id);

DELETE FROM shadow_X
WHERE order_id IN (SELECT order_id FROM X GROUP BY order_id HAVING COUNT(*) > 1)
  AND rowid NOT IN (SELECT MIN(rowid) FROM X GROUP BY order_id);
```

Verify:

```sql
SELECT COUNT(*) AS violations FROM (
  SELECT order_id FROM shadow_X GROUP BY order_id HAVING COUNT(*) > 1
);
```

## 5. Playbook: `BACKFILL` khi upstream đã fix

Chỉ dùng khi team upstream xác nhận đã sửa mapping và re-publish dữ liệu.
Khoanh đúng cửa sổ lỗi **trên bảng bóng**, sau đó rerun job ingestion:

```sql
CREATE OR REPLACE TABLE shadow_X AS SELECT * FROM X;

DELETE FROM shadow_X
WHERE source_system = '<source>' AND order_date BETWEEN '<from>' AND '<to>';
-- sau đó rerun job ingestion cho cửa sổ đó, rồi publish shadow như thường lệ
```
