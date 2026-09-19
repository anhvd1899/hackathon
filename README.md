# AGENT SPEC KIT: DATA RELIABILITY AGENT (DataOps SRE)

## 1. TỔNG QUAN VỀ AGENT (METADATA)
- **Tên Agent:** Data Reliability Agent
- **Sứ mệnh (Goal):** Tự động phát hiện và nhận diện các sự cố Data Quality (DQ) từ data pipeline, chủ động điều tra nguyên nhân gốc rễ (Root Cause Analysis - RCA), ước lượng phạm vi ảnh hưởng (Blast Radius), đề xuất kế hoạch khắc phục và thực thi lệnh sửa lỗi sau khi có sự phê duyệt của kỹ sư.
- **Actor chính:** Data Engineer / Analytics Engineer (Người giám sát và ra quyết định duyệt remediation).
- **Môi trường triển khai:** GreenNode AI Platform / VNG Cloud AgentBase Runtime (Docker, CPU 2x4GB hoặc 4x4GB).

---

## 2. USE CASE & BỐI CẢNH NGHIỆP VỤ
- **Vấn đề thực tế:** Khi một pipeline dữ liệu gặp sự cố (ví dụ: `dbt test` fail do NULL values, duplicate keys, hoặc schema drift, hoặc lỗi về tài nguyên hạ tầng server, có thể đọc log để dự đoán nguyên nhân, đưa ra kết luận, đọc db nguồn,...), Data Engineer thường mất hàng giờ để đọc log, truy vấn SQL thủ công để tìm các dòng lỗi, truy vết bảng downstream và viết script vá data.
- **Giải pháp:** Agent đóng vai trò là một "SRE trực ban" 24/7. Khi có alert, Agent tự động:
  1. Trích xuất thông tin lỗi.
  2. Dùng DuckDB chạy các câu query điều tra ngay lập tức.
  3. Đưa ra bản báo cáo chẩn đoán nguyên nhân và lệnh khắc phục (Remediation script).
  4. Trình diện lên Web UI / Chainlit để Engineer duyệt (Human-in-the-loop) trước khi ghi dữ liệu sạch trở lại DuckDB (Sink).

---

## 3. KIẾN TRÚC DỮ LIỆU & CÔNG NGHỆ (TECH STACK)
- **Pipeline flow:** 
  `Raw Dataset -> Python Ingestion -> DuckDB (Source) -> dbt test (hoặc mock alert) -> AI Agent -> FastAPI/Chainlit UI -> DuckDB (Sink)`
- **Core Stack:**
  - **Ngôn ngữ:** Python 3.11
  - **Brain (LLM):** GreenNode MaaS API (OpenAI-compatible SDK)
  - **In-Memory OLAP / Engine:** DuckDB (đóng vai trò cả Source dữ liệu để query và Sink dữ liệu sau remediation)
  - **Data Testing Framework:** dbt (dbt-duckdb hoặc mock dbt test run_results)
  - **Data Contract:** Pydantic v2
  - **Giao diện & Human-in-the-loop:** Chainlit + FastAPI (Nhúng iframe trong cùng 1 server)

---

## 4. INPUT & OUTPUT CONTRACT (TỔNG QUAN)

### 4.1. Input Contract (Generic Incident Envelope)
Thiết kế mở để chấp nhận mọi loại sự cố (DQ, Timeout, Schema Drift) mà không bị lỗi schema:
- `incident_id` (str): Mã định danh (vd: `INC-2026-DQ01`).
- `incident_type` (str): `DATA_QUALITY` | `PIPELINE_FAILURE` | `SCHEMA_DRIFT`.
- `target_table` (str): Bảng dữ liệu xảy ra sự cố (vd: `warehouse.fact_orders`).
- `description` (str): Mô tả tóm tắt sự cố từ test runner hoặc pipeline.
- `evidence_payload` (dict): Dữ liệu chi tiết dạng JSON (failed rule, tên cột, mẫu dòng lỗi, log stacktrace).

### 4.2. Output Contract (Structured Agent Report)
Ép LLM trả về cấu trúc JSON nghiêm ngặt phục vụ Web UI:
- **Diagnosis:**
  - `root_cause` (str): Nguyên nhân gốc rễ cụ thể.
  - `confidence_score` (float): Độ tin cậy (0.0 - 1.0).
  - `suspected_source` (str): Nguồn nghi vấn (Upstream API / ETL / Source DB).
- **Impact Assessment:**
  - `severity` (str): `LOW` | `MEDIUM` | `HIGH` | `CRITICAL`.
  - `affected_downstream_tables` (list[str]): Danh sách bảng hạ nguồn bị ảnh hưởng.
  - `affected_dashboards` (list[str]): Báo cáo BI bị ảnh hưởng.
- **Remediation Plan:**
  - `action_type` (str): `QUARANTINE_DATA` | `BACKFILL` | `RERUN_PIPELINE` | `MANUAL_FIX`.
  - `summary` (str): Giải thích hành động khắc phục cho người đọc.
  - `executable_command` (str): Script SQL hoặc CLI command cụ thể để vá lỗi trên DuckDB, hoặc xử lý vấn đề về hạ tầng như thiếu tài nguyên lỗi service.
- **Status:** `INVESTIGATING` | `WAITING_FOR_APPROVAL` | `RESOLVED` | `REJECTED`.

---

## 5. WORKFLOW & STATE MACHINE (CHU KỲ HOẠT ĐỘNG)

Workflow gồm 8 bước tuần tự kết hợp vòng lặp suy luận (ReAct):

```text
[1. DETECT] 
      │ Nhận webhook/incident payload từ dbt test fail.
      ▼
[2. INVESTIGATE] 
      │ Agent tự sinh SQL query gọi DuckDB tool để phân tích pattern dữ liệu lỗi.
      ▼
[3. DIAGNOSE] 
      │ LLM tổng hợp bằng chứng, xác định nguyên nhân và đo lường phạm vi ảnh hưởng.
      ▼
[4. RECOMMEND] 
      │ Tạo ra remediation plan và câu lệnh khắc phục chi tiết (SQL DDL/DML).
      ▼
[5. APPROVE (HITL)] ⏸️ 
      │ DỪNG LẠI (Pause State): Hiển thị báo cáo và nút [Approve] / [Reject] trên Chainlit.
      │ - Nếu Engineer yêu cầu chỉnh sửa/phản hồi: Agent re-plan lại bước 4.
      │ - Nếu Engineer bấm Approve: Chuyển sang bước 6.
      ▼
[6. EXECUTE] 
      │ Chạy lệnh executable_command trên DuckDB (Sink) để cách ly hoặc update dữ liệu.
      ▼
[7. VERIFY] 
      │ Chạy lại test query trên DuckDB để xác nhận vi phạm DQ đã về 0 dòng.
      ▼
[8. RESOLVE] ✅ 
      │ Đổi trạng thái sang "RESOLVED", bắn thông báo hoàn tất lên Dashboard.
