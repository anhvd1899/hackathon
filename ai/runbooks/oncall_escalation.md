# RUNBOOK — ON-CALL & ESCALATION MATRIX

## 1. Ai chịu trách nhiệm

| Domain | Owner | Kênh | Giờ trực |
| --- | --- | --- | --- |
| `fact_orders`, `dim_customers` | Data Platform Team | `#data-oncall` | 24/7 |
| Job `ingest_mobile_app_v3` | Team Mobile Backend | `#mobile-be` | 08:00 – 22:00 |
| `erp_core` CDC | Team ERP | `#erp-integration` | 24/7 |
| Mart Tier-0 + dashboard | Analytics Engineering | `#analytics-eng` | 08:00 – 18:00 |
| Hạ tầng runtime (CPU/RAM/disk) | Cloud Infra | `#cloud-infra` | 24/7 |

## 2. Ma trận leo thang theo severity

| Severity | Thông báo | Deadline phản hồi | Cần duyệt của ai |
| --- | --- | --- | --- |
| LOW | ghi ticket Jira `DATA-` | ngày làm việc kế tiếp | Data Engineer trực |
| MEDIUM | Slack `#data-oncall` | 2 giờ | Data Engineer trực |
| HIGH | Slack + gọi on-call | 30 phút | Data Engineer trực + Owner upstream |
| CRITICAL | Gọi on-call + báo Head of Data | 15 phút | Head of Data (bắt buộc) |

## 3. Quy tắc Human-in-the-loop

- Agent **không được** tự thực thi lệnh ghi dữ liệu khi chưa có Approve từ engineer.
- Sau khi Approve: Agent chạy remediation → verify → cập nhật trạng thái RESOLVED.
- Nếu verify vẫn còn vi phạm: chuyển trạng thái FAILED, không tự thử lại lần 2,
  đẩy sang on-call người thật kèm log lệnh đã chạy.
- Nếu Reject: Agent phải hỏi lý do và đề xuất phương án thay thế (re-plan).

## 4. Việc phải làm sau sự cố (post-incident)

1. Tạo ticket cho team upstream để fix gốc (ví dụ mapping SDK).
2. Thêm test `not_null` ở tầng staging để chặn sớm hơn (fail fast).
3. Thêm alert freshness/volume theo `source_system` + `source_version`.
4. Viết postmortem trong 48 giờ nếu severity >= HIGH.
