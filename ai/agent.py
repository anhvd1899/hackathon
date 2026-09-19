"""
agent.py
========
Data Reliability Agent — triển khai NATIVE PYTHON, không dùng LangChain/LangGraph/CrewAI.

Cơ chế: vòng lặp `while True` + OpenAI Python SDK (Tool/Function Calling) trỏ tới
MaaS API (GreenNode / VNG Cloud) hoặc bất kỳ endpoint OpenAI-compatible nào.

Vòng lặp ReAct thuần tay:
    while True:
        resp = client.chat.completions.create(model, messages, tools=TOOLS_SCHEMA)
        msg  = resp.choices[0].message
        if msg.tool_calls:  -> chạy tool thật trên DuckDB, append role="tool", loop tiếp
        else:               -> đó là câu trả lời cuối, thoát vòng lặp

Agent giữ nguyên `self.messages` nên hội thoại có ngữ cảnh liên tục: engineer có thể
chất vấn ("tại sao lỗi?", "show 5 dòng lỗi") giữa lúc chờ duyệt, Agent vẫn nhớ toàn bộ
quá trình điều tra trước đó.

Nếu KHÔNG có API key, Agent tự chuyển sang `OfflineBrain` — một "bộ não" mô phỏng
tool-calling deterministic để demo/UI vẫn chạy được đầu-cuối mà không cần internet.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional

from pydantic import ValidationError

from ai import tools
from ai.llm import (
    LLMSettings,
    MockMessage,
    MockResponse,
    MockToolCall,
    ToolEvent,
    extract_json,
)
from ai.schemas import (
    REPORT_JSON_TEMPLATE,
    AgentReport,
    Diagnosis,
    IncidentInput,
    IncidentStatus,
)

# ---------------------------------------------------------------------------
# 2. SYSTEM PROMPT — "bộ não" hướng dẫn Agent suy nghĩ từng bước
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
Bạn là **Data Reliability Agent** (Agent 1 — vai *Maker*) — một bạn nữ SRE trực ban 24/7
chuyên về Data Quality của đội T24 Data Lake. Bạn làm việc trực tiếp trên data warehouse
DuckDB và trao đổi với Data Engineer bằng **tiếng Việt**.

# CÁCH BẠN NÓI CHUYỆN (rất quan trọng)
- Bạn **tự gọi mình là "em"**, gọi Data Engineer là **"anh"**. Giọng lễ phép, dễ thương,
  nhiệt tình nhưng vẫn cực kỳ chuyên nghiệp và chính xác về số liệu.
- **Dùng nhiều emoji/icon** cho dễ đọc: 🔍 điều tra, 📊 số liệu, 🚨 sự cố, ✅ ổn, ❌ lỗi,
  🛠️ khắc phục, 💡 đề xuất, ⚠️ cảnh báo, 🎯 kết luận, 💚 khi xong việc.
- Giữ nguyên thuật ngữ kỹ thuật tiếng Anh (NULL, quarantine, downstream, SLA breach…).
- Dễ thương nhưng KHÔNG được làm mềm sự thật: số liệu sai là phải nói thẳng ạ.

# LƯU Ý VỀ QUY TRÌNH 2 LỚP (Maker - Checker)
Sau khi bạn vá dữ liệu, sẽ có **Agent 2 (Data Auditor)** nghiệm thu ĐỘC LẬP: bạn ấy tự
query DuckDB để kiểm lại, **không tin** báo cáo của bạn. Vì vậy:
- Tuyệt đối không phóng đại, không bịa số.
- Remediation phải để lại dấu vết kiểm chứng được: quarantine giữ đủ dòng, số dòng cộng
  lại phải khớp, không được xoá mất dữ liệu oan.

# NHIỆM VỤ
Nhận một incident (dbt test fail, pipeline fail, schema drift, sự cố hạ tầng), tự điều
tra bằng SQL, tìm nguyên nhân gốc rễ, đo phạm vi ảnh hưởng, và đề xuất script vá lỗi
an toàn để engineer phê duyệt.

# CÔNG CỤ BẠN CÓ
- `tool_query_duckdb(query)`  : chạy SQL CHỈ ĐỌC để điều tra. Đây là nguồn sự thật duy nhất.
- `tool_read_runbook(topic)`  : đọc runbook nội bộ (sla_policy, lineage_fact_orders,
  dq_playbook, oncall_escalation, infra_resources).
- `tool_execute_remediation(sql_command)` : ghi/vá dữ liệu — CHỈ dùng khi được thông báo
  rõ ràng rằng engineer ĐÃ phê duyệt. Gọi trước khi được duyệt sẽ bị hệ thống từ chối.
- `tool_verify_health(table_name, check_sql)` : verify lại sau khi vá.

# QUY TRÌNH BẮT BUỘC (suy nghĩ từng bước)
1. **DETECT** — Đọc kỹ incident envelope: bảng nào, test nào fail, cột nào, log nói gì.
2. **INVESTIGATE** — Gọi `tool_query_duckdb` NHIỀU LẦN, mỗi lần một câu hỏi cụ thể:
   a. Đếm chính xác số dòng vi phạm và tổng số dòng (tính tỉ lệ %).
   b. Tìm PATTERN: `GROUP BY source_system, source_version`, theo `order_date`,
      theo giờ `ingested_at`... để biết lỗi tập trung ở đâu (một batch? một nguồn?
      một phiên bản SDK?) chứ không phải rải rác.
   c. Xem sample vài dòng lỗi thật để mô tả cụ thể.
   d. Kiểm tra bảng hạ nguồn (mart_*) đã bị nhiễm dữ liệu bẩn chưa.
   e. Kiểm tra `dq_test_results` xem còn test nào khác fail cùng lúc.
3. **DIAGNOSE** — Tổng hợp bằng chứng thành root cause. CẤM phỏng đoán số liệu:
   mọi con số bạn viết ra phải xuất phát từ kết quả query. Nếu chưa query thì phải query.
4. **IMPACT** — Đọc `lineage_fact_orders` để biết bảng/dashboard hạ nguồn, đọc
   `sla_policy` để chấm severity và xác định có SLA breach hay không.
5. **RECOMMEND** — Đọc `dq_playbook` rồi soạn `executable_command` là SQL DuckDB CHẠY ĐƯỢC.
   Nguyên tắc an toàn tuyệt đối:
   - Luôn QUARANTINE (INSERT sang bảng `quarantine_<table>`) TRƯỚC khi DELETE.
   - `DELETE` bắt buộc có `WHERE` khoanh đúng phạm vi lỗi.
   - Không `DROP` bảng lõi; muốn thay nội dung mart thì `CREATE OR REPLACE TABLE ... AS SELECT`.
   - Sau khi vá fact table thì rebuild lại các mart hạ nguồn trong cùng script.
   - Luôn kèm `verification_sql` dạng `SELECT COUNT(*) ...` kỳ vọng bằng 0.
6. **ĐỢI PHÊ DUYỆT** — Bạn KHÔNG được tự ý ghi dữ liệu. Trình bày kế hoạch và dừng lại.
7. **EXECUTE + VERIFY** — Khi được thông báo đã approve: chạy remediation, verify, kết luận.

# KHI ENGINEER CHAT HỎI THÊM
Trong lúc chờ duyệt, engineer sẽ chất vấn bạn ("tại sao lại lỗi?", "show 5 dòng dữ liệu",
"nếu xoá thì doanh thu giảm bao nhiêu?"). Hãy:
- Nếu câu hỏi cần số liệu -> GỌI `tool_query_duckdb` để lấy dữ liệu thật rồi mới trả lời.
- Trả lời ngắn gọn, có số liệu, có bảng markdown khi liệt kê dữ liệu.
- Nếu engineer chỉ ra bạn sai hoặc yêu cầu đổi phương án -> điều tra lại và re-plan.

# PHONG CÁCH
- Ngắn gọn, đi thẳng vào số liệu, giọng một bạn SRE đang trực sự cố — xưng "em" với anh.
- Không bịa tên bảng/cột: nếu không chắc, chạy `SHOW TABLES` hoặc `DESCRIBE <table>`.
- Không hứa hẹn suông; mọi kết luận đều gắn với bằng chứng cụ thể.

# SCHEMA THẬT CỦA WAREHOUSE (DuckDB)
{catalog}

# RUNBOOK KHẢ DỤNG
{runbooks}
"""

INVESTIGATE_INSTRUCTION = """\
Một incident vừa được đẩy vào hàng đợi trực ban. Hãy bắt đầu quy trình
DETECT → INVESTIGATE → DIAGNOSE → IMPACT → RECOMMEND.

{incident_block}

Yêu cầu cho lượt này:
- Chạy ÍT NHẤT 3 câu `tool_query_duckdb` khác nhau (đếm vi phạm, tìm pattern theo
  source_system/source_version/ngày, xem sample dòng lỗi, kiểm tra mart hạ nguồn).
- Đọc runbook `lineage_fact_orders` và `sla_policy` (và `dq_playbook` trước khi soạn SQL vá).
- Sau khi đã đủ bằng chứng, viết bản tóm tắt điều tra bằng tiếng Việt cho engineer:
  root cause, số liệu chứng minh, phạm vi ảnh hưởng, phương án vá đề xuất.
- TUYỆT ĐỐI chưa gọi `tool_execute_remediation` ở bước này.
"""

REPORT_INSTRUCTION = """\
Bây giờ hãy đóng gói toàn bộ kết quả điều tra ở trên thành MỘT object JSON duy nhất,
đúng theo schema sau (không thêm chữ nào ngoài JSON, không dùng markdown fence):

{template}

Ràng buộc:
- `incident_id` = "{incident_id}", `target_table` = "{target_table}", `status` = "WAITING_FOR_APPROVAL".
- `affected_row_count` phải là con số THẬT lấy từ query đã chạy.
- `evidence_summary` liệt kê 3-6 bằng chứng, mỗi bằng chứng kèm số liệu cụ thể.
- `investigation_queries` liệt kê các câu SQL bạn đã thực sự chạy.
- `executable_command` là SQL DuckDB hợp lệ, chạy được ngay, nhiều câu tách bằng ';',
  theo đúng thứ tự: quarantine -> delete/update có WHERE -> rebuild mart hạ nguồn.
- `verification_sql` là 1 câu SELECT COUNT(*) kỳ vọng trả về 0 sau khi vá.
"""

POST_EXECUTION_INSTRUCTION = """\
Engineer ĐÃ PHÊ DUYỆT và hệ thống đã thực thi remediation. Đây là kết quả thật:

[KẾT QUẢ THỰC THI REMEDIATION]
{execution_result}

[KẾT QUẢ VERIFY]
{verify_result}

Hãy viết thông báo kết thúc sự cố bằng tiếng Việt cho engineer, gồm:
- Những gì đã thực sự thay đổi trên DuckDB (bảng nào, bao nhiêu dòng).
- Kết quả verify (số dòng vi phạm còn lại) và kết luận RESOLVED hay FAILED.
- 2-3 hành động phòng ngừa để sự cố không tái diễn (fix upstream, thêm test ở staging...).
Nếu cần số liệu để đối chiếu, hãy gọi `tool_query_duckdb` trước khi kết luận.
"""


# ---------------------------------------------------------------------------
# 3. OFFLINE BRAIN — mô phỏng tool-calling khi không có API key
# ---------------------------------------------------------------------------


def _fold(text: str) -> str:
    """
    Bỏ dấu tiếng Việt + lowercase để so khớp từ khoá không phụ thuộc cách gõ
    ("Tại sao" và "Tai sao" đều khớp). Chỉ dùng cho bộ não OFFLINE.
    """
    import unicodedata

    normalized = unicodedata.normalize("NFD", (text or "").lower())
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return stripped.replace("đ", "d")


class _OfflineCompletions:
    """
    Bộ não offline: đọc lịch sử `messages` để biết đang ở bước nào rồi phát ra
    tool_call/nội dung tương ứng. Deterministic -> demo luôn ra cùng kết quả.
    """

    # Kịch bản điều tra: các bước tool sẽ gọi lần lượt
    INVESTIGATION_STEPS: List[Dict[str, Any]] = [
        {
            "name": "tool_query_duckdb",
            "args": {
                "query": (
                    "SELECT COUNT(*) AS total_rows, "
                    "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_rows, "
                    "ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 3) "
                    "AS null_pct FROM fact_orders"
                )
            },
        },
        {
            "name": "tool_query_duckdb",
            "args": {
                "query": (
                    "SELECT source_system, source_version, COUNT(*) AS rows_total, "
                    "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_rows "
                    "FROM fact_orders GROUP BY source_system, source_version "
                    "ORDER BY null_rows DESC, rows_total DESC"
                )
            },
        },
        {
            "name": "tool_query_duckdb",
            "args": {
                "query": (
                    "SELECT order_id, order_date, customer_id, total_amount, order_status, "
                    "source_system, source_version, ingested_at FROM fact_orders "
                    "WHERE customer_id IS NULL ORDER BY ingested_at LIMIT 5"
                ),
                "max_rows": 5,
            },
        },
        {
            "name": "tool_query_duckdb",
            "args": {
                "query": (
                    "SELECT order_date, order_count, unique_customers, gross_revenue, orphan_orders "
                    "FROM mart_daily_revenue WHERE orphan_orders > 0 ORDER BY order_date"
                )
            },
        },
        {"name": "tool_read_runbook", "args": {"topic": "lineage_fact_orders"}},
        {"name": "tool_read_runbook", "args": {"topic": "sla_policy"}},
        {"name": "tool_read_runbook", "args": {"topic": "dq_playbook"}},
    ]

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _last_user_index(messages: List[Dict[str, Any]]) -> int:
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "user":
                return i
        return 0

    @classmethod
    def _tools_used_this_turn(cls, messages: List[Dict[str, Any]]) -> List[str]:
        start = cls._last_user_index(messages)
        return [m.get("name", "") for m in messages[start:] if m.get("role") == "tool"]

    @staticmethod
    def _last_tool_payload(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for m in reversed(messages):
            if m.get("role") == "tool":
                try:
                    return json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    return None
        return None

    @staticmethod
    def _scalar(query: str, default: Any = 0) -> Any:
        res = tools.tool_query_duckdb(query, max_rows=1)
        if res.get("ok") and res.get("rows"):
            return list(res["rows"][0].values())[0]
        return default

    @staticmethod
    def _render_rows(payload: Optional[Dict[str, Any]], limit: int = 10) -> str:
        """Render kết quả query thành bảng markdown."""
        if not payload or not payload.get("ok") or not payload.get("rows"):
            return "_(không có dữ liệu trả về)_"
        rows = payload["rows"][:limit]
        cols = list(rows[0].keys())
        head = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join("---" for _ in cols) + " |"
        body = [
            "| " + " | ".join("NULL" if r.get(c) is None else str(r.get(c)) for c in cols) + " |"
            for r in rows
        ]
        return "\n".join([head, sep, *body])

    # -- các phase ---------------------------------------------------------

    def _build_report_json(self, messages: List[Dict[str, Any]]) -> str:
        """Sinh AgentReport JSON với số liệu THẬT lấy từ DuckDB."""
        total = self._scalar("SELECT COUNT(*) FROM fact_orders", 0)
        nulls = self._scalar("SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL", 0)
        pct = round(nulls * 100.0 / total, 3) if total else 0.0
        bad_days = self._scalar(
            "SELECT COUNT(*) FROM mart_daily_revenue WHERE orphan_orders > 0", 0
        )
        broken_batch = self._scalar(
            "SELECT COUNT(*) FROM fact_orders "
            "WHERE source_system = 'mobile_app_v3' AND source_version = '3.4.1'",
            0,
        )
        # Lấy incident_id / target_table từ system+user messages nếu có
        incident_id = "INC-2026-DQ01"
        target_table = "main.fact_orders"
        for m in messages:
            content = m.get("content") or ""
            if isinstance(content, str):
                found_id = re.search(r"incident_id\s*[:=]\s*\"?([A-Za-z0-9\-_]+)", content)
                if found_id:
                    incident_id = found_id.group(1)
                found_tbl = re.search(r"target_table\s*[:=]\s*\"?([A-Za-z0-9_.]+)", content)
                if found_tbl:
                    target_table = found_tbl.group(1)

        report = {
            "incident_id": incident_id,
            "target_table": target_table,
            "status": "WAITING_FOR_APPROVAL",
            "diagnosis": {
                "root_cause": (
                    f"Job ingestion `ingest_mobile_app_v3` map sai field định danh khách hàng sau khi "
                    f"mobile SDK nâng lên 3.4.1 (field `user_ref` đổi tên thành `customer_ref`), khiến "
                    f"{nulls} dòng của batch ingest 2026-09-15 02:15 được ghi vào fact_orders với "
                    f"customer_id = NULL. Toàn bộ {nulls} dòng lỗi đều thuộc source_system="
                    f"'mobile_app_v3' và source_version='3.4.1' — không có dòng lỗi nào ở nguồn khác, "
                    f"chứng tỏ đây là lỗi mapping của một nguồn/phiên bản cụ thể chứ không phải lỗi ngẫu nhiên."
                ),
                "confidence_score": 0.92,
                "suspected_source": "Upstream ingestion job ingest_mobile_app_v3 (mobile SDK 3.4.1)",
                "evidence_summary": [
                    f"fact_orders có {total} dòng, trong đó {nulls} dòng customer_id IS NULL "
                    f"(tỉ lệ {pct}%).",
                    f"100% dòng lỗi tập trung ở source_system='mobile_app_v3', "
                    f"source_version='3.4.1' ({broken_batch} dòng thuộc phiên bản này).",
                    "Các nguồn erp_core / web_checkout / partner_api không có dòng NULL nào.",
                    f"mart_daily_revenue đang có {bad_days} ngày với orphan_orders > 0 -> "
                    f"dashboard Finance đếm thiếu unique_customers.",
                    "Log ingestion 02:30:01 ghi rõ: field 'user_ref' missing in 15 payloads (sdk 3.4.1).",
                ],
                "investigation_queries": [
                    step["args"]["query"]
                    for step in self.INVESTIGATION_STEPS
                    if step["name"] == "tool_query_duckdb"
                ],
            },
            "impact": {
                "severity": "HIGH",
                "affected_row_count": nulls,
                "affected_downstream_tables": [
                    "main.mart_daily_revenue",
                    "main.mart_customer_ltv",
                    "feature_customer_recency (ML feature store)",
                ],
                "affected_dashboards": [
                    "Finance Daily Revenue (Metabase, gửi mail 08:00)",
                    "Executive KPI Overview (Metabase)",
                    "Customer 360 & Retention (Metabase)",
                ],
                "sla_breach": True,
                "business_impact": (
                    f"{nulls} đơn hàng không quy được về khách hàng: Finance Daily Revenue đếm thiếu "
                    "unique_customers, Customer 360 sinh nhóm khách NULL làm lệch LTV, và CRM "
                    "segmentation có thể gửi campaign sai đối tượng. Theo sla_policy, mart Tier-0 "
                    "phục vụ dashboard lúc 08:00 nên sự cố phát hiện 02:30 mà chưa vá là SLA breach."
                ),
            },
            "remediation": {
                "action_type": "QUARANTINE_DATA",
                "summary": (
                    "Cách ly 15 dòng lỗi sang bảng quarantine_fact_orders (giữ nguyên bản gốc để "
                    "backfill khi team Mobile fix mapping), xoá chúng khỏi fact_orders bằng DELETE có "
                    "WHERE khoanh đúng nguồn/phiên bản, rồi rebuild 2 mart Tier-0 để dashboard hết sai. "
                    "Đây là phương án đảo ngược được: dữ liệu gốc vẫn nằm trong bảng quarantine."
                ),
                "executable_command": (
                    "CREATE TABLE IF NOT EXISTS quarantine_fact_orders AS "
                    "SELECT *, CAST(NULL AS VARCHAR) AS quarantine_reason, "
                    "CAST(NULL AS TIMESTAMP) AS quarantined_at FROM fact_orders WHERE 1 = 0; "
                    "INSERT INTO quarantine_fact_orders SELECT *, "
                    "'NULL_CUSTOMER_ID_MOBILE_SDK_3_4_1' AS quarantine_reason, now() AS quarantined_at "
                    "FROM fact_orders WHERE customer_id IS NULL; "
                    "DELETE FROM fact_orders WHERE customer_id IS NULL; "
                    "CREATE OR REPLACE TABLE mart_daily_revenue AS SELECT order_date, "
                    "COUNT(*) AS order_count, COUNT(DISTINCT customer_id) AS unique_customers, "
                    "SUM(total_amount) AS gross_revenue, "
                    "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_orders "
                    "FROM fact_orders GROUP BY order_date; "
                    "CREATE OR REPLACE TABLE mart_customer_ltv AS SELECT f.customer_id, c.segment, "
                    "c.region, COUNT(*) AS lifetime_orders, SUM(f.total_amount) AS lifetime_value, "
                    "MAX(f.order_date) AS last_order_date FROM fact_orders f "
                    "LEFT JOIN dim_customers c USING (customer_id) "
                    "GROUP BY f.customer_id, c.segment, c.region"
                ),
                "verification_sql": (
                    "SELECT COUNT(*) AS violations FROM fact_orders WHERE customer_id IS NULL"
                ),
                "rollback_hint": (
                    "INSERT INTO fact_orders SELECT * EXCLUDE (quarantine_reason, quarantined_at) "
                    "FROM quarantine_fact_orders "
                    "WHERE quarantine_reason = 'NULL_CUSTOMER_ID_MOBILE_SDK_3_4_1'; "
                    "sau đó rebuild lại 2 mart."
                ),
                "risk_level": "MEDIUM",
                "requires_human_approval": True,
            },
            "next_steps": [
                "Mở ticket cho Team Mobile Backend: sửa mapping user_ref -> customer_ref trong "
                "job ingest_mobile_app_v3 (SDK 3.4.1).",
                "Backfill lại 15 đơn từ bảng quarantine sau khi upstream fix xong.",
                "Thêm test not_null ở tầng staging để chặn sớm, kèm alert theo source_version.",
                "Thông báo Finance rằng số unique_customers ngày 2026-09-15 đã được tính lại.",
            ],
            "agent_notes": (
                "Đang chạy ở chế độ OFFLINE (chưa cấu hình MaaS API key) — kết quả điều tra là số "
                "liệu THẬT truy vấn từ DuckDB, phần diễn giải dùng kịch bản mẫu deterministic."
            ),
        }
        return json.dumps(report, ensure_ascii=False)

    def _answer_chat(self, messages: List[Dict[str, Any]], question: str) -> str:
        """Trả lời câu chất vấn của engineer (offline)."""
        # Chỉ render bảng dữ liệu nếu tool vừa được gọi TRONG lượt này
        payload = (
            self._last_tool_payload(messages) if self._tools_used_this_turn(messages) else None
        )
        q = _fold(question)
        parts: List[str] = []

        if payload and payload.get("ok") and payload.get("rows"):
            parts.append("Dữ liệu vừa truy vấn từ DuckDB:")
            parts.append("")
            parts.append(self._render_rows(payload))
            parts.append("")

        if any(k in q for k in ("tai sao", "vi sao", "why", "root cause", "nguyen nhan", "do dau")):
            nulls = self._scalar(
                "SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL", 0
            )
            parts.append(
                f"**Vì sao lỗi:** {nulls} dòng NULL đều đến từ `mobile_app_v3` phiên bản "
                "`3.4.1`, dồn trong đúng một batch ingest lúc 02:15 ngày 2026-09-15. SDK 3.4.1 "
                "đổi tên field định danh (`user_ref` → `customer_ref`) nhưng job ingestion vẫn "
                "đọc theo tên cũ nên ghi NULL. Các nguồn khác không có dòng lỗi nào, nên đây là "
                "lỗi mapping của một nguồn cụ thể, không phải lỗi dữ liệu ngẫu nhiên."
            )
        elif any(k in q for k in ("doanh thu", "revenue", "tien", "amount", "money")):
            amount = self._scalar(
                "SELECT COALESCE(SUM(total_amount), 0) FROM fact_orders WHERE customer_id IS NULL", 0
            )
            parts.append(
                f"**Ảnh hưởng tiền:** tổng `total_amount` của các dòng lỗi là "
                f"**{float(amount):,.0f} VND**. Nếu quarantine, doanh thu ngày 2026-09-15 sẽ giảm "
                "đúng phần này — vì vậy cần thông báo Finance và backfill lại sau khi upstream fix."
            )
        elif any(k in q for k in ("quarantine", "lenh", "sql", "script", "va ", "remediation", "plan")):
            parts.append(
                "**Kế hoạch vá:** tạo `quarantine_fact_orders` → INSERT 15 dòng lỗi kèm "
                "`quarantine_reason` → `DELETE FROM fact_orders WHERE customer_id IS NULL` → "
                "rebuild `mart_daily_revenue` và `mart_customer_ltv`. Dữ liệu gốc vẫn nằm trong "
                "bảng quarantine nên có thể rollback/backfill bất cứ lúc nào."
            )
        else:
            parts.append(
                "Tôi đã tổng hợp từ dữ liệu thật trong DuckDB. Bạn có thể hỏi thêm: "
                "*“tại sao lại lỗi?”*, *“show 5 dòng dữ liệu lỗi”*, "
                "*“ảnh hưởng doanh thu bao nhiêu?”*, *“lệnh vá là gì?”* — hoặc bấm "
                "**✅ Duyệt Remediation** để tôi thực thi."
            )
        return "\n".join(parts)

    def _investigation_summary(self, messages: List[Dict[str, Any]]) -> str:
        total = self._scalar("SELECT COUNT(*) FROM fact_orders", 0)
        nulls = self._scalar("SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL", 0)
        pct = round(nulls * 100.0 / total, 3) if total else 0.0
        return (
            f"Đã điều tra xong `fact_orders`. Tổng {total} dòng, phát hiện **{nulls} dòng "
            f"customer_id IS NULL** (tỉ lệ {pct}%). Toàn bộ dòng lỗi thuộc "
            f"`source_system='mobile_app_v3'`, `source_version='3.4.1'`, cùng một batch ingest "
            f"02:15 ngày 2026-09-15 → root cause là mapping field định danh bị sai sau khi nâng SDK. "
            f"Hai mart Tier-0 (`mart_daily_revenue`, `mart_customer_ltv`) đã nhiễm dữ liệu bẩn nên "
            f"dashboard Finance/Customer 360 đang sai. Phương án đề xuất: QUARANTINE_DATA rồi "
            f"rebuild mart. Đang chờ bạn phê duyệt."
        )

    def _post_execution(self, messages: List[Dict[str, Any]]) -> str:
        remaining = self._scalar(
            "SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL", 0
        )
        try:
            quarantined = self._scalar("SELECT COUNT(*) FROM quarantine_fact_orders", 0)
        except Exception:  # noqa: BLE001
            quarantined = 0
        verdict = "✅ **RESOLVED**" if remaining == 0 else f"❌ **FAILED** (còn {remaining} dòng)"
        return (
            f"{verdict}\n\n"
            f"- Đã cách ly **{quarantined} dòng** sang `quarantine_fact_orders` (kèm "
            f"`quarantine_reason` và `quarantined_at` để truy vết).\n"
            f"- Đã xoá các dòng NULL khỏi `fact_orders` và rebuild `mart_daily_revenue`, "
            f"`mart_customer_ltv`.\n"
            f"- Verify: `SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL` = "
            f"**{remaining}**.\n\n"
            "**Phòng ngừa:**\n"
            "1. Ticket cho Team Mobile: sửa mapping `user_ref` → `customer_ref` trong "
            "`ingest_mobile_app_v3` (SDK 3.4.1).\n"
            "2. Thêm test `not_null` ở tầng staging + alert theo `source_version` để chặn từ sớm.\n"
            "3. Backfill 15 đơn từ bảng quarantine sau khi upstream fix, rồi thông báo Finance."
        )

    # -- API giống OpenAI SDK ---------------------------------------------

    def create(
        self,
        *,
        model: str = "",
        messages: Optional[List[Dict[str, Any]]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,  # noqa: A002 - giữ đúng tên param OpenAI
        tool_choice: Any = None,
        response_format: Any = None,
        temperature: float = 0.0,
        max_tokens: int = 0,
        **_: Any,
    ) -> _MockResponse:
        messages = messages or []
        last_user = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                last_user = str(m.get("content") or "")
                break

        # Phase: yêu cầu trả JSON có cấu trúc
        wants_json = isinstance(response_format, dict) and response_format.get("type") == "json_object"
        if wants_json or "object JSON duy nhất" in last_user:
            return MockResponse(MockMessage(content=self._build_report_json(messages)))

        used = self._tools_used_this_turn(messages)

        # Phase: sau khi approve + đã thực thi
        if "KẾT QUẢ THỰC THI REMEDIATION" in last_user:
            return MockResponse(MockMessage(content=self._post_execution(messages)))

        # Phase: điều tra ban đầu
        if "INCIDENT ENVELOPE" in last_user:
            step_idx = len(used)
            if step_idx < len(self.INVESTIGATION_STEPS):
                step = self.INVESTIGATION_STEPS[step_idx]
                return MockResponse(
                    MockMessage(tool_calls=[MockToolCall(step["name"], step["args"])])
                )
            return MockResponse(MockMessage(content=self._investigation_summary(messages)))

        # Phase: engineer chat chất vấn
        q = _fold(last_user)
        needs_data = any(
            k in q
            for k in (
                "show", "xem", "sample", "dong", "row", "du lieu", "bao nhieu",
                "list", "liet ke", "query", "select", "count", "thong ke", "kiem tra",
            )
        )
        if needs_data and not used:
            if any(k in q for k in ("mart", "downstream", "dashboard")):
                query = (
                    "SELECT order_date, order_count, unique_customers, gross_revenue, orphan_orders "
                    "FROM mart_daily_revenue WHERE orphan_orders > 0 ORDER BY order_date"
                )
            elif any(k in q for k in ("nguon", "source", "pattern", "version", "sdk")):
                query = (
                    "SELECT source_system, source_version, COUNT(*) AS rows_total, "
                    "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_rows "
                    "FROM fact_orders GROUP BY source_system, source_version ORDER BY null_rows DESC"
                )
            else:
                query = (
                    "SELECT order_id, order_date, customer_id, total_amount, order_status, "
                    "source_system, source_version, ingested_at FROM fact_orders "
                    "WHERE customer_id IS NULL ORDER BY ingested_at LIMIT 5"
                )
            return MockResponse(
                MockMessage(tool_calls=[MockToolCall("tool_query_duckdb", {"query": query})])
            )

        return MockResponse(MockMessage(content=self._answer_chat(messages, last_user)))


class _OfflineChat:
    def __init__(self) -> None:
        self.completions = _OfflineCompletions()


class OfflineBrain:
    """Client giả lập, cùng interface `client.chat.completions.create(...)`."""

    def __init__(self) -> None:
        self.chat = _OfflineChat()


# ---------------------------------------------------------------------------
# 4. AGENT
# ---------------------------------------------------------------------------


class DataReliabilityAgent:
    """
    Agent điều tra & khắc phục sự cố dữ liệu.

    Ví dụ dùng:
        agent = DataReliabilityAgent()
        agent.load_incident(incident)
        report = agent.investigate()          # bước 1-4 của workflow
        answer = agent.ask("show 5 dòng lỗi") # engineer chất vấn (bước 5)
        result = agent.approve()              # bước 6-8 sau khi bấm Approve
    """

    def __init__(
        self,
        incident: Optional[IncidentInput] = None,
        settings: Optional[LLMSettings] = None,
        client: Any = None,
        on_tool_event: Optional[Callable[[ToolEvent], None]] = None,
        max_history_messages: int = 60,
    ) -> None:
        self.settings = settings or LLMSettings.from_env()
        self.on_tool_event = on_tool_event
        self.max_history_messages = max_history_messages

        self.messages: List[Dict[str, Any]] = []
        self.tool_events: List[ToolEvent] = []
        self.incident: Optional[IncidentInput] = None
        self.report: Optional[AgentReport] = None
        self.status: IncidentStatus = IncidentStatus.INVESTIGATING
        self.execution_result: Optional[Dict[str, Any]] = None
        self.verify_result: Optional[Dict[str, Any]] = None
        # Ảnh chụp trạng thái trước khi vá — bàn giao cho Agent 2 đối chiếu
        self.baseline: Optional[Dict[str, Any]] = None

        self.client, self.mode = self._build_client(client)

        if incident is not None:
            self.load_incident(incident)

    # -- khởi tạo client ---------------------------------------------------

    def _build_client(self, client: Any) -> tuple[Any, str]:
        """Tạo OpenAI client trỏ tới MaaS; fallback OfflineBrain nếu thiếu API key."""
        if client is not None:
            return client, "custom"
        if not self.settings.api_key:
            print(
                "[DataReliabilityAgent] ⚠️  Chưa có DRA_API_KEY/OPENAI_API_KEY -> "
                "chạy chế độ OFFLINE (bộ não mô phỏng, DuckDB vẫn thật)."
            )
            return OfflineBrain(), "offline"
        try:
            from openai import OpenAI  # import trễ để môi trường offline vẫn khởi động được
        except ImportError:
            print("[DataReliabilityAgent] ⚠️  Chưa cài package `openai` -> chạy OFFLINE.")
            return OfflineBrain(), "offline"

        client = OpenAI(
            api_key=self.settings.api_key,
            base_url=self.settings.base_url,
            timeout=self.settings.request_timeout,
            max_retries=2,
        )
        print(
            f"[DataReliabilityAgent] ✅ MaaS: {self.settings.base_url} "
            f"| model={self.settings.describe()}"
        )
        return client, "maas"

    @property
    def is_offline(self) -> bool:
        return self.mode == "offline"

    # -- quản lý hội thoại -------------------------------------------------

    def _system_prompt(self) -> str:
        return SYSTEM_PROMPT.format(
            catalog=tools.get_catalog_snapshot(),
            runbooks=", ".join(tools.list_runbook_topics()) or "(không có)",
        )

    def load_incident(self, incident: IncidentInput) -> None:
        """Nạp incident mới, reset hội thoại."""
        self.incident = incident
        self.report = None
        self.status = IncidentStatus.INVESTIGATING
        self.execution_result = None
        self.verify_result = None
        self.baseline = None
        self.tool_events = []
        tools.set_current_incident(incident.incident_id)
        tools.lock_remediation()  # đảm bảo luôn khoá khi bắt đầu
        self.messages = [{"role": "system", "content": self._system_prompt()}]

    def _trim_history(self) -> None:
        """
        Giữ context không phình to: luôn giữ system + user đầu tiên,
        cắt bớt các cặp assistant/tool ở giữa khi vượt ngưỡng.
        """
        if len(self.messages) <= self.max_history_messages:
            return
        head = self.messages[:3]  # system + incident + phản hồi đầu
        tail = self.messages[-(self.max_history_messages - 4) :]
        # Không được để message đầu của tail là role="tool" (mồ côi tool_call_id)
        while tail and tail[0].get("role") == "tool":
            tail = tail[1:]
        self.messages = head + [
            {
                "role": "system",
                "content": "(… lược bớt phần giữa của lịch sử điều tra để tiết kiệm context …)",
            }
        ] + tail

    @staticmethod
    def _assistant_to_dict(msg: Any) -> Dict[str, Any]:
        """Chuyển message object của SDK (hoặc mock) về dict để append vào history."""
        out: Dict[str, Any] = {"role": "assistant", "content": getattr(msg, "content", None)}
        tool_calls = getattr(msg, "tool_calls", None)
        if tool_calls:
            out["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments or "{}",
                    },
                }
                for tc in tool_calls
            ]
        return out

    # -- gọi LLM -----------------------------------------------------------

    def _call_llm(self, use_tools: bool, json_mode: bool) -> Any:
        """Một lần gọi chat.completions, có hạ cấp tham số nếu endpoint không hỗ trợ."""
        kwargs: Dict[str, Any] = {
            "model": self.settings.model,
            "messages": self.messages,
            "temperature": self.settings.temperature,
            "max_tokens": self.settings.max_tokens,
        }
        if use_tools:
            kwargs["tools"] = tools.TOOLS_SCHEMA
            kwargs["tool_choice"] = "auto"
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        try:
            return self.client.chat.completions.create(**kwargs)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            # Một số endpoint MaaS không hỗ trợ response_format / tool_choice -> thử lại gọn hơn
            if any(
                token in msg
                for token in ("response_format", "tool_choice", "unsupported", "invalid_request")
            ):
                kwargs.pop("response_format", None)
                kwargs.pop("tool_choice", None)
                return self.client.chat.completions.create(**kwargs)
            raise

    def _completion(self, use_tools: bool = True, json_mode: bool = False) -> Any:
        """
        Gọi LLM, có **failover sang model khác trong pool** khi gặp lỗi tạm thời
        (rate limit, 5xx, timeout, model không tồn tại).
        """
        self._trim_history()
        attempts = 1 + min(len(self.settings.alternatives), 2)
        last_exc: Optional[Exception] = None
        for _ in range(attempts):
            try:
                return self._call_llm(use_tools, json_mode)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                switched = self.settings.failover(exc)
                if not switched:
                    raise
                print(
                    f"[DataReliabilityAgent] ⚠️  Model lỗi ({str(exc)[:80]}) "
                    f"-> chuyển sang `{switched}`"
                )
        raise last_exc  # type: ignore[misc]

    def _emit(self, event: ToolEvent) -> None:
        self.tool_events.append(event)
        if self.on_tool_event is not None:
            try:
                self.on_tool_event(event)
            except Exception:  # noqa: BLE001 - callback UI không được làm sập agent
                pass

    def _run_tool_call(self, tool_call: Any) -> Dict[str, Any]:
        """Thực thi 1 tool call và append kết quả vào history."""
        name = tool_call.function.name
        raw_args = tool_call.function.arguments or "{}"
        try:
            parsed_args = json.loads(raw_args) if isinstance(raw_args, str) else dict(raw_args)
        except json.JSONDecodeError:
            parsed_args = {"_raw": raw_args}

        result = tools.execute_tool(name, parsed_args)
        payload = json.dumps(result, ensure_ascii=False, default=str)
        # Chặn tool output quá lớn làm vỡ context window
        if len(payload) > 12000:
            payload = payload[:12000] + '... (đã cắt bớt)"}'

        self.messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": name,
                "content": payload,
            }
        )
        self._emit(
            ToolEvent(
                name=name,
                arguments=parsed_args if isinstance(parsed_args, dict) else {},
                result=result,
                ok=bool(result.get("ok", False)),
            )
        )
        return result

    # -- VÒNG LẶP AGENT (native while loop) --------------------------------

    def _agent_loop(self, max_iterations: Optional[int] = None) -> str:
        """
        Vòng lặp ReAct lõi: gọi LLM -> nếu có tool_calls thì chạy tool rồi quay lại,
        nếu không có tool_calls thì đó là câu trả lời cuối cùng.
        """
        limit = max_iterations or self.settings.max_iterations
        iteration = 0

        while True:
            iteration += 1
            if iteration > limit:
                # Hết ngân sách bước: ép LLM chốt kết luận, không cho gọi tool nữa
                self.messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Bạn đã dùng hết {limit} bước điều tra. KHÔNG gọi thêm tool. "
                            "Hãy kết luận ngay dựa trên dữ liệu đã có."
                        ),
                    }
                )
                final = self._completion(use_tools=False)
                content = getattr(final.choices[0].message, "content", "") or ""
                self.messages.append({"role": "assistant", "content": content})
                return content

            response = self._completion(use_tools=True)
            message = response.choices[0].message
            self.messages.append(self._assistant_to_dict(message))

            tool_calls = getattr(message, "tool_calls", None)
            if not tool_calls:
                return getattr(message, "content", "") or ""

            for tool_call in tool_calls:
                self._run_tool_call(tool_call)

    # -- BƯỚC 1-4: điều tra & lập kế hoạch ---------------------------------

    def investigate(self) -> AgentReport:
        """
        Chạy toàn bộ chuỗi DETECT -> INVESTIGATE -> DIAGNOSE -> IMPACT -> RECOMMEND
        và trả về AgentReport đã validate bằng Pydantic.
        """
        if self.incident is None:
            raise ValueError("Chưa nạp incident. Gọi load_incident() trước.")

        self.status = IncidentStatus.INVESTIGATING
        self.messages.append(
            {
                "role": "user",
                "content": INVESTIGATE_INSTRUCTION.format(
                    incident_block=self.incident.to_prompt_block()
                ),
            }
        )
        narrative = self._agent_loop()
        report = self._request_structured_report(narrative)
        self.report = report
        self.status = report.status
        return report

    def replan(self, feedback: str) -> AgentReport:
        """Engineer phản hồi/yêu cầu đổi phương án -> quay lại bước RECOMMEND."""
        self.messages.append(
            {
                "role": "user",
                "content": (
                    "Engineer KHÔNG duyệt phương án hiện tại. Phản hồi của họ:\n"
                    f"\"{feedback}\"\n\n"
                    "Hãy điều tra thêm nếu cần (dùng tool_query_duckdb / tool_read_runbook) và "
                    "đề xuất LẠI kế hoạch remediation phù hợp với phản hồi này. "
                    "Chưa được thực thi bất cứ thứ gì."
                ),
            }
        )
        narrative = self._agent_loop()
        report = self._request_structured_report(narrative)
        self.report = report
        self.status = IncidentStatus.WAITING_FOR_APPROVAL
        return report

    def _request_structured_report(self, narrative: str = "") -> AgentReport:
        """Ép LLM đóng gói kết quả thành JSON và validate bằng Pydantic (có retry)."""
        assert self.incident is not None
        self.messages.append(
            {
                "role": "user",
                "content": REPORT_INSTRUCTION.format(
                    template=REPORT_JSON_TEMPLATE,
                    incident_id=self.incident.incident_id,
                    target_table=self.incident.target_table,
                ),
            }
        )

        last_error = ""
        for attempt in range(3):
            try:
                response = self._completion(use_tools=False, json_mode=True)
                raw = getattr(response.choices[0].message, "content", "") or ""
            except Exception as exc:  # noqa: BLE001
                last_error = f"Lỗi gọi LLM: {exc}"
                break

            self.messages.append({"role": "assistant", "content": raw})
            data = extract_json(raw)
            if data is None:
                last_error = "Output không chứa JSON hợp lệ."
            else:
                data.setdefault("incident_id", self.incident.incident_id)
                data.setdefault("target_table", self.incident.target_table)
                try:
                    report = AgentReport.model_validate(data)
                    return self._post_process_report(report, narrative)
                except ValidationError as exc:
                    last_error = f"Sai schema: {exc.errors()[:3]}"

            if attempt < 2:
                self.messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"JSON vừa rồi KHÔNG dùng được ({last_error}). "
                            "Hãy trả lại DUY NHẤT một object JSON đúng schema, "
                            "không kèm chữ nào khác, không markdown fence."
                        ),
                    }
                )

        # Fallback: LLM không trả nổi JSON -> vẫn dựng report từ narrative + tool events
        return self._fallback_report(narrative, last_error)

    def _post_process_report(self, report: AgentReport, narrative: str) -> AgentReport:
        """Bổ khuyết những field mà LLM hay để trống."""
        assert self.incident is not None
        report.incident_id = report.incident_id or self.incident.incident_id
        report.target_table = report.target_table or self.incident.target_table
        report.status = IncidentStatus.WAITING_FOR_APPROVAL

        if not report.diagnosis.investigation_queries:
            report.diagnosis.investigation_queries = self.executed_queries()
        if not report.remediation.verification_sql:
            report.remediation.verification_sql = self._fallback_verification_sql()
        if not report.impact.affected_row_count:
            guessed = self.incident.evidence_payload.get("failures")
            if isinstance(guessed, int):
                report.impact.affected_row_count = guessed
        if narrative and not report.agent_notes:
            report.agent_notes = " ".join(narrative.split())[:600]
        report.remediation.requires_human_approval = True
        return report

    def _fallback_report(self, narrative: str, error: str) -> AgentReport:
        """Report tối thiểu nhưng hợp contract khi LLM không trả được JSON."""
        assert self.incident is not None
        rows = self.incident.evidence_payload.get("failures")
        return AgentReport(
            incident_id=self.incident.incident_id,
            target_table=self.incident.target_table,
            status=IncidentStatus.WAITING_FOR_APPROVAL,
            diagnosis=Diagnosis(
                root_cause=(
                    narrative.strip()[:1500]
                    or f"Chưa xác định được root cause tự động ({error})."
                ),
                confidence_score=0.3,
                suspected_source="UNKNOWN",
                evidence_summary=[e.short_label for e in self.tool_events][:6],
                investigation_queries=self.executed_queries(),
            ),
            impact={
                "severity": "MEDIUM",
                "affected_row_count": rows if isinstance(rows, int) else 0,
                "business_impact": "Chưa đánh giá được tự động, cần engineer xem xét.",
            },
            remediation={
                "action_type": "MANUAL_FIX",
                "summary": (
                    "Agent chưa sinh được script an toàn tự động. Đề nghị engineer xem lại "
                    "bằng chứng điều tra ở trên và xử lý thủ công theo dq_playbook."
                ),
                "executable_command": "",
                "verification_sql": self._fallback_verification_sql(),
                "risk_level": "HIGH",
            },
            next_steps=["Xem lại log Agent", "Xử lý thủ công theo runbook dq_playbook"],
            agent_notes=f"Fallback report do LLM không trả JSON hợp lệ: {error}",
        )

    def _fallback_verification_sql(self) -> str:
        """Suy ra câu verify từ evidence khi LLM để trống."""
        if self.incident is None:
            return ""
        table = self.incident.bare_table_name
        ev = self.incident.evidence_payload
        column = ev.get("column") or ev.get("column_name")
        test_type = str(ev.get("test_type") or ev.get("failed_test") or "").lower()
        if column and "not_null" in test_type:
            return f"SELECT COUNT(*) AS violations FROM {table} WHERE {column} IS NULL"
        if column and "unique" in test_type:
            return (
                f"SELECT COUNT(*) AS violations FROM (SELECT {column} FROM {table} "
                f"GROUP BY {column} HAVING COUNT(*) > 1)"
            )
        compiled = ev.get("compiled_sql")
        if isinstance(compiled, str) and compiled.strip().lower().startswith("select"):
            return compiled.strip().rstrip(";")
        return f"SELECT COUNT(*) AS row_count FROM {table}"

    def executed_queries(self) -> List[str]:
        """Danh sách SQL Agent đã chạy (phục vụ audit/UI)."""
        return [
            str(e.arguments.get("query", ""))
            for e in self.tool_events
            if e.name == "tool_query_duckdb" and e.arguments.get("query")
        ]

    # -- BƯỚC 5: engineer chất vấn -----------------------------------------

    def ask(self, question: str) -> str:
        """
        Engineer chat hỏi tự do. Agent giữ nguyên ngữ cảnh điều tra, được phép
        query DuckDB tương tác để trả lời (nhưng vẫn không được ghi dữ liệu).
        """
        self.messages.append({"role": "user", "content": question})
        return self._agent_loop(max_iterations=max(4, self.settings.max_iterations // 2))

    # -- BƯỚC 6-8: execute -> verify -> resolve ----------------------------

    def approve(self) -> Dict[str, Any]:
        """
        Được gọi khi engineer bấm [✅ Duyệt Remediation].

        Luồng: mở khoá tool ghi -> chạy remediation -> verify -> khoá lại ->
        nhờ LLM viết thông báo kết thúc sự cố.
        """
        if self.report is None:
            raise ValueError("Chưa có báo cáo để duyệt.")

        plan = self.report.remediation
        if not plan.executable_command.strip():
            self.status = IncidentStatus.FAILED
            return {
                "ok": False,
                "status": self.status.value,
                "summary": "Không có `executable_command` để thực thi. Cần xử lý thủ công.",
                "execution": {},
                "verification": {},
            }

        self.status = IncidentStatus.EXECUTING
        verify_sql = plan.verification_sql or self._fallback_verification_sql()
        table = self.report.target_table.split(".")[-1]

        # ---- BASELINE cho Agent 2 (Checker) ----------------------------------
        # Chụp trạng thái TRƯỚC khi vá bằng Python thuần, KHÔNG qua LLM.
        # Nhờ mốc này, Agent 2 mới kiểm được "có xoá mất dữ liệu oan không"
        # mà không phải tin vào bất cứ con số nào do Agent 1 tự khai.
        try:
            self.baseline = tools.capture_baseline(
                incident_id=self.report.incident_id,
                target_table=self.report.target_table,
                violation_sql=verify_sql,
                related_tables=self.report.impact.affected_downstream_tables,
            )
        except Exception as exc:  # noqa: BLE001 - không được chặn remediation
            self.baseline = {"ok": False, "error": str(exc)}

        # ---- EXECUTE (mở khoá đúng 1 lần, đóng lại ngay sau khi xong) ----
        tools.unlock_remediation()
        try:
            execution = tools.execute_tool(
                "tool_execute_remediation",
                {
                    "sql_command": plan.executable_command,
                    "reason": f"Approved by engineer for {self.report.incident_id}",
                },
            )
            self._emit(
                ToolEvent(
                    name="tool_execute_remediation",
                    arguments={"sql_command": plan.executable_command},
                    result=execution,
                    ok=bool(execution.get("ok")),
                )
            )

            # ---- VERIFY ----
            verification = tools.execute_tool(
                "tool_verify_health", {"table_name": table, "check_sql": verify_sql}
            )
            self._emit(
                ToolEvent(
                    name="tool_verify_health",
                    arguments={"table_name": table, "check_sql": verify_sql},
                    result=verification,
                    ok=bool(verification.get("ok")),
                )
            )
        finally:
            tools.lock_remediation()

        self.execution_result = execution
        self.verify_result = verification
        verify_sql_used = verify_sql

        healthy = bool(execution.get("ok")) and bool(verification.get("healthy"))
        self.status = IncidentStatus.RESOLVED if healthy else IncidentStatus.FAILED
        self.report.status = self.status

        # ---- RESOLVE: nhờ LLM viết closing note dựa trên kết quả THẬT ----
        self.messages.append(
            {
                "role": "user",
                "content": POST_EXECUTION_INSTRUCTION.format(
                    execution_result=json.dumps(execution, ensure_ascii=False, default=str)[:3000],
                    verify_result=json.dumps(verification, ensure_ascii=False, default=str)[:2000],
                ),
            }
        )
        try:
            summary = self._agent_loop(max_iterations=4)
        except Exception as exc:  # noqa: BLE001
            summary = (
                f"(Không gọi được LLM để viết tổng kết: {exc})\n"
                f"Kết quả verify: violations={verification.get('violations')}"
            )

        return {
            "ok": healthy,
            "status": self.status.value,
            "summary": summary,
            "execution": execution,
            "verification": verification,
            "baseline": self.baseline,
            "verification_sql": verify_sql_used,
        }

    def reject(self, reason: str = "") -> str:
        """Engineer bấm [❌ Từ chối]. Agent ghi nhận và đề nghị phương án khác."""
        self.status = IncidentStatus.REJECTED
        if self.report is not None:
            self.report.status = IncidentStatus.REJECTED
        tools.lock_remediation()
        reason_text = reason.strip() or "(không nêu lý do)"
        self.messages.append(
            {
                "role": "user",
                "content": (
                    f"Engineer đã TỪ CHỐI phương án remediation. Lý do: {reason_text}. "
                    "Hãy ghi nhận, KHÔNG thực thi gì, và gợi ý ngắn gọn 2-3 phương án thay thế "
                    "hoặc thông tin bạn cần thêm để lập lại kế hoạch."
                ),
            }
        )
        try:
            return self._agent_loop(max_iterations=3)
        except Exception as exc:  # noqa: BLE001
            return (
                f"Đã ghi nhận từ chối (lý do: {reason_text}). "
                f"Không thực thi thay đổi nào trên DuckDB. (LLM lỗi: {exc})"
            )


# ---------------------------------------------------------------------------
# 5. Helper
# ---------------------------------------------------------------------------


def run_headless(incident: IncidentInput, auto_approve: bool = False) -> Dict[str, Any]:
    """
    Chạy Agent không cần UI (dùng cho REST API / cron / test).

    auto_approve=True sẽ tự duyệt remediation — CHỈ dùng cho môi trường dev/test,
    production phải đi qua Human-in-the-loop trên Chainlit.
    """
    agent = DataReliabilityAgent(incident=incident)
    report = agent.investigate()
    payload: Dict[str, Any] = {
        "mode": agent.mode,
        "report": json.loads(report.model_dump_json()),
        "tool_calls": [
            {"name": e.name, "arguments": e.arguments, "ok": e.ok} for e in agent.tool_events
        ],
    }
    if auto_approve:
        payload["remediation_result"] = agent.approve()
        payload["report"] = json.loads(agent.report.model_dump_json())  # type: ignore[union-attr]
    return payload


__all__ = [
    "LLMSettings",
    "ToolEvent",
    "DataReliabilityAgent",
    "OfflineBrain",
    "run_headless",
    "SYSTEM_PROMPT",
]


# ---------------------------------------------------------------------------
# 6. Chạy thử nhanh từ CLI:  python -m ai.agent
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _incident = IncidentInput(**tools.build_sample_incident())
    _agent = DataReliabilityAgent(incident=_incident)

    print("\n" + "=" * 78)
    print(f"🔍 BẮT ĐẦU ĐIỀU TRA {_incident.incident_id} (mode={_agent.mode})")
    print("=" * 78)

    _report = _agent.investigate()
    for _event in _agent.tool_events:
        print(f"  [tool] {'✅' if _event.ok else '❌'} {_event.short_label}")

    print("\n" + _report.to_markdown())
    print("\n" + "=" * 78)
    print("JSON contract:")
    print(_report.to_json())
