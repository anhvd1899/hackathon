# 🛡️ Data Reliability Squad — Maker · Checker

Hai agent phối hợp xử lý sự cố Data Quality trên DuckDB warehouse, và **anh Engineer
là người ra quyết định** ở giữa.

| Vai | Agent | Việc | Quyền trên DuckDB |
| --- | --- | --- | --- |
| 👷‍♀️ **Maker** | Agent 1 — Data SRE Agent | điều tra RCA, đo blast radius, soạn script vá | đọc + **ghi** (chỉ sau khi anh duyệt) |
| 🕵️‍♀️ **Checker** | Agent 2 — Data Auditor | nghiệm thu độc lập sau khi vá | **chỉ đọc** |

🔀 **Cross-model checking**: hai vai chạy bằng **hai model khác nhau** (cấu hình trong
`.env`). Cùng model thì cùng điểm mù — Checker sẽ có xu hướng đồng thuận với Maker thay vì
bắt lỗi. Model nào đã nghiệm thu được ghi vào `auditor_model` của biên bản để truy vết.

## Cách dùng

1. 📥 Mở phiên chat mới: một incident (`dbt test` fail) được nạp tự động, **Agent 1** bắt
   đầu điều tra — anh thấy từng câu SQL nó chạy trên DuckDB.
2. 📑 Đọc báo cáo: root cause, số liệu chứng minh, bảng/dashboard hạ nguồn, script remediation.
3. 💬 **Chất vấn trước khi duyệt** — cứ gõ tự do:
   - *Tại sao lại lỗi?*
   - *Show thử 5 dòng dữ liệu lỗi*
   - *Lỗi tập trung ở nguồn nào?*
   - *Nếu xoá thì doanh thu giảm bao nhiêu?*
4. ✅ Bấm **Duyệt Remediation** → Agent 1 thực thi + tự verify.
5. 🤔 **Anh chọn: recheck hay chốt luôn?** (human-in-the-loop thứ hai)
   - 🕵️‍♀️ **Recheck độc lập** → Agent 2 vào kiểm. Nên chọn khi lỗi phức tạp, ảnh hưởng
     mart/dashboard, hoặc cần bằng chứng nghiệm thu.
   - ⚡ **Chốt luôn** → đóng incident ngay, không tốn thêm thời gian/token. Nên chọn khi
     lỗi đơn giản anh đã biết rõ. Quyết định bỏ qua được ghi vào `agent_audit_log`.
   - Nếu Agent 1 **vá thất bại**, nút "chốt luôn" bị khoá — đóng incident khi vi phạm còn
     tồn tại là che lỗi.
6. 🧾 Nếu recheck: Agent 2 tự viết SQL kiểm 3 việc rồi cấp biên bản
   `AUDIT_PASSED` / `AUDIT_FAILED`:
   - 🧼 **Cleanliness** — bảng chính còn dòng vi phạm không? (kỳ vọng 0)
   - 🧊 **Data Preservation** — số dòng vào `quarantine_*` có khớp số lỗi ban đầu không?
     (bảo đảm không xoá oan)
   - 🧮 **Row Count Integrity** — tổng số dòng có được bảo toàn không?
7. 💬 Câu hỏi có chữ *nghiệm thu*, *audit*, *mất dữ liệu*… sẽ được chuyển cho Agent 2
   trả lời. Đổi ý sau khi đã chốt luôn thì vẫn bấm nút nghiệm thu được bất cứ lúc nào.

## Vì sao Agent 2 đáng tin hơn là "Agent 1 tự khen mình"

- 🔒 Agent 2 **không được cấp tool ghi** — kể cả LLM có cố gọi cũng bị dispatcher từ chối,
  nên nó không thể sửa dữ liệu cho báo cáo đẹp lên.
- 🧠 Agent 2 có **hội thoại riêng**, không thừa hưởng ngữ cảnh của Agent 1. Báo cáo của
  Agent 1 chỉ vào dưới nhãn *"lời khai cần kiểm chứng"*.
- 📸 Mốc so sánh trước/sau lấy từ bảng `dq_baseline_snapshot` — do **tầng Python** ghi lúc
  anh bấm duyệt, LLM không can thiệp được.
- 🔀 Agent 2 chạy bằng **model khác** Agent 1, nên điểm mù của model này không trùng với
  model kia.
- 🤖 Sau khi LLM trả biên bản, hệ thống **chạy lại 3 check lõi bằng Python** để đối chiếu.
  Lệch nhau thì **máy thắng** và verdict tự động bị hạ xuống `AUDIT_FAILED`.

## Ghi chú

- Agent 1 **không thể** ghi dữ liệu trước khi anh duyệt: tool ghi bị khoá ở tầng code.
- Mọi tool call được ghi vào `agent_audit_log` (xem qua `GET /api/audit-log`).
- Chưa cấu hình `DRA_API_KEY`? App vẫn chạy OFFLINE với số liệu DuckDB thật.
