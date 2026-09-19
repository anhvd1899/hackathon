# RUNBOOK — HẠ TẦNG RUNTIME & SỰ CỐ TÀI NGUYÊN

Áp dụng cho các incident `PIPELINE_FAILURE` / `INFRA_RESOURCE` (OOM, timeout, hết disk).

## 1. Cấu hình runtime hiện tại

| Thành phần | Cấu hình | Giới hạn |
| --- | --- | --- |
| Agent runtime (VNG Cloud AgentBase) | 2 vCPU / 4 GB RAM | container restart nếu RSS > 3.6 GB |
| dbt worker | 4 vCPU / 8 GB RAM | timeout mỗi model 900s |
| DuckDB | in-process, dùng RAM của worker | `memory_limit` mặc định 80% RAM |
| Disk volume warehouse | 50 GB | alert khi > 80% |

## 2. Dấu hiệu nhận biết trong log

| Log pattern | Nguyên nhân thường gặp | Xử lý |
| --- | --- | --- |
| `Out of Memory Error: could not allocate` | DuckDB xử lý JOIN/ORDER BY quá lớn | tăng `memory_limit`, hoặc chia batch theo ngày |
| `Killed` / exit code 137 | container bị OOMKilled | nâng RAM lên 8 GB (`SCALE_RESOURCE`) |
| `Query timed out after 900s` | model dbt thiếu partition filter | thêm filter theo `order_date`, tạo index/sort |
| `IO Error: No space left on device` | disk đầy do file tạm | dọn `/tmp`, dọn bảng quarantine cũ |
| `Connection reset by peer` khi gọi MaaS | rate limit / mạng | retry backoff, giảm concurrency |

## 3. Lệnh xử lý nhanh (dùng cho `executable_command` khi incident là INFRA)

```sql
-- Nới hạn mức bộ nhớ và số luồng của DuckDB trong session hiện tại
SET memory_limit = '3GB';
SET threads = 2;
-- Dọn bảng quarantine quá 90 ngày để giải phóng disk
DELETE FROM quarantine_fact_orders WHERE quarantined_at < now() - INTERVAL 90 DAY;
```

```bash
# Nâng tài nguyên container trên AgentBase (chạy phía CI/CD, cần approve)
agentbase scale --service data-reliability-agent --cpu 4 --memory 8Gi
```

## 4. Ngưỡng severity cho sự cố hạ tầng

- Pipeline Tier-0 fail do OOM và chưa có workaround: **HIGH**.
- Fail lặp lại > 3 lần liên tiếp hoặc disk > 95%: **CRITICAL**.
- Fail 1 lần rồi retry thành công: **LOW** (chỉ ghi ticket theo dõi).
