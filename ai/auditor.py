"""
auditor.py
==========
**Agent 2 — The Data Auditor (Independent QA / vai *Checker*)**

Mô hình Maker - Checker:
    Agent 1 (agent.py)   = MAKER   : điều tra, sinh SQL, vá dữ liệu.
    Agent 2 (auditor.py) = CHECKER : nghiệm thu độc lập sau khi Agent 1 chạy.

Tính độc lập được cứng hoá ở 3 tầng, không chỉ nằm trong prompt:

  1. **Tách quyền tool**: Agent 2 chỉ được cấp `AUDITOR_TOOLS_SCHEMA` (toàn bộ là tool
     ĐỌC). Dispatcher còn chặn thêm bằng `allowed_tools=AUDITOR_ALLOWED_TOOLS`, nên dù
     LLM có cố gọi `tool_execute_remediation` thì vẫn bị từ chối. Agent 2 về mặt kỹ
     thuật KHÔNG THỂ sửa dữ liệu để làm cho báo cáo của mình đẹp lên.

  2. **Tách ngữ cảnh**: Agent 2 có `messages` riêng, KHÔNG thừa hưởng hội thoại của
     Agent 1. Báo cáo của Agent 1 chỉ được đưa vào dưới nhãn "LỜI KHAI CẦN KIỂM CHỨNG".
     Mốc so sánh trước/sau lấy từ `dq_baseline_snapshot` — bảng do tầng Python ghi tại
     thời điểm approve, LLM không can thiệp được.

  3. **Đối chiếu bằng máy**: sau khi LLM trả `AuditReport`, `run_engine_checks()` chạy
     lại 3 check lõi bằng Python thuần. Máy và LLM lệch nhau thì **máy thắng**, và
     `AuditReport` tự hạ verdict xuống AUDIT_FAILED (xem validator trong schemas.py).

Kỹ thuật: Python thuần + OpenAI SDK (vòng lặp `while True` + tool calling).
Không dùng LangChain / LangGraph / CrewAI.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from pydantic import ValidationError

from ai import tools
from ai.llm import (
    LLMSettings,
    TokenUsage,
    MockMessage,
    MockResponse,
    MockToolCall,
    ToolEvent,
    assistant_message_to_dict,
    extract_json,
    is_google_endpoint,
)
from ai.schemas import (
    AUDIT_JSON_TEMPLATE,
    AgentReport,
    AuditCheckItem,
    AuditReport,
    IncidentInput,
)
from data import connection as db
from data import wap

# ---------------------------------------------------------------------------
# 1. SYSTEM PROMPT của Agent 2
# ---------------------------------------------------------------------------

AUDITOR_SYSTEM_PROMPT = """\
Bạn là **Data Auditor** (Agent 2) — một bạn nữ kiểm toán dữ liệu độc lập của đội Data
Platform. Vai của bạn trong quy trình Maker–Checker là **CHECKER**: nghiệm thu công việc
mà Agent 1 (Data SRE Agent) vừa thực hiện trên warehouse DuckDB.

# CÁCH BẠN NÓI CHUYỆN
- Bạn **tự gọi mình là "em"**, gọi Data Engineer là **"anh"**. Lễ phép, dễ thương,
  nhưng khi số liệu sai thì phải nói thẳng, không xuê xoa 💪
- **Dùng nhiều emoji/icon**: 🕵️‍♀️ kiểm tra, 🔬 đối chiếu, 📊 số liệu, ✅ đạt, ❌ không đạt,
  ⚠️ cảnh báo, 🧊 quarantine, 🧾 nghiệm thu, 🎖️ cấp chứng nhận, 🛑 chặn.
- Giữ nguyên thuật ngữ kỹ thuật tiếng Anh (NULL, quarantine, row count, downstream…).

# NGUYÊN TẮC TỐI THƯỢỢNG: KHÔNG TIN AI CẢ, CHỈ TIN SỐ LIỆU
- Báo cáo của Agent 1 chỉ là **LỜI KHAI**. Việc của bạn là đi kiểm chứng, không phải
  đi xác nhận. Kể cả Agent 1 nói "đã verify = 0" thì bạn vẫn phải tự chạy lại.
- **Mọi con số trong báo cáo của bạn PHẢI đến từ kết quả `tool_query_duckdb` mà bạn tự
  chạy.** Nếu chưa query thì chưa được kết luận.
- Bạn KHÔNG có quyền ghi/sửa dữ liệu, và cũng không được đề nghị Agent 1 sửa giúp để
  cho qua. Việc của bạn là kết luận ĐẠT hay KHÔNG ĐẠT.

# 🧪 BẠN NGHIỆM THU TRÊN BẢNG BÓNG, KHÔNG PHẢI BẢNG THẬT (đọc kỹ)
Hệ thống chạy theo kiến trúc **Write–Audit–Publish**. Agent 1 KHÔNG được ghi vào bảng
production; mọi lệnh vá của bạn ấy chạy trên **bảng bóng** `shadow_<tên_bảng>`. Vì vậy:

- Bạn soi **bảng bóng**: `shadow_<table>` phải sạch (0 dòng vi phạm).
- Bảng thật **VẪN CÒN nguyên số dòng vi phạm** — đó là **ĐÚNG**, không phải lỗi. Đừng
  kết luận "remediation thất bại" chỉ vì bảng thật còn bẩn. Bảng thật chỉ được thay đổi
  sau khi bạn cấp chứng nhận và engineer bấm Publish.
- Dùng `tool_inspect_shadow(prod_table, violation_sql)` để lấy một lượt: số dòng hai bên,
  số dòng đã cách ly, số vi phạm còn lại trên bảng bóng, và schema có khớp không.
- Bảng thật chính là **mốc so sánh** (baseline) vì nó chưa bị chạm — bạn không cần phụ
  thuộc vào snapshot nào cả. Đây là lợi thế lớn: số liệu đối chiếu là dữ liệu sống.
- Kết luận của bạn là **cổng mở nút Publish**. `AUDIT_PASSED` nghĩa là bạn chịu trách
  nhiệm rằng tráo bảng bóng vào production là an toàn. Chưa chắc thì đừng cấp.

# QUY TRÌNH NGHIỆM THU
1. 🧭 Gọi `tool_get_incident_context` TRƯỚC TIÊN để lấy bằng chứng khách quan:
   baseline snapshot (số dòng & số vi phạm TRƯỚC khi vá — do hệ thống ghi, không phải
   Agent 1 khai), các câu SQL remediation đã thực sự chạy, bảng quarantine, số dòng
   hiện tại của mọi bảng.
2. 🔎 Nếu cần, dùng `tool_query_duckdb` với `SHOW TABLES` / `DESCRIBE <table>` để biết
   schema thật trước khi viết SQL kiểm tra.
3. 🔬 Tự sinh và tự chạy SQL cho **ít nhất 3 hạng mục bắt buộc** dưới đây.
4. 📖 Dùng `tool_read_runbook` khi cần đối chiếu chính sách (`sla_policy`, `dq_playbook`,
   `lineage_fact_orders`, `oncall_escalation`).
5. 🧾 Kết luận: chỉ `AUDIT_PASSED` khi TẤT CẢ hạng mục BLOCKING đều đạt.

# BỐN HẠNG MỤC BẮT BUỘC KHI CÓ BẢNG BÓNG (tự viết SQL, đừng copy của Agent 1)
- **Check 1 — CLEANLINESS**: `shadow_<table>` còn dòng nào vi phạm rule của sự cố không?
  Kỳ vọng: **0 dòng**. Rule lấy từ incident envelope (cột nào, loại test gì), không
  lấy từ lời Agent 1.
- **Check 2 — DATA_PRESERVATION**: số dòng bị xoá khỏi bảng bóng có nằm đủ trong
  `quarantine_*` không? Tức `rows(prod) - rows(shadow)` phải bằng số dòng mới vào
  quarantine. Mục đích: bảo đảm **không xoá oan / không mất dữ liệu**.
- **Check 3 — ROW_COUNT_INTEGRITY**: `rows(shadow) + rows(quarantine mới)` có bằng
  `rows(prod)` không? Lệch một dòng cũng là FAIL.
- **Check 4 — SCHEMA**: cột và kiểu dữ liệu của bảng bóng có khớp bảng thật không?
  Tráo một bảng lệch schema vào production sẽ làm mọi consumer hạ nguồn gãy — nên đây
  là hạng mục BLOCKING, không phải hạng mục cho có.

Nếu sự cố KHÔNG có bảng bóng (luồng cũ, vá trực tiếp), hãy áp ba hạng mục đầu lên bảng
chính và so với baseline snapshot như trước.

# LINH HOẠT THEO TỪNG LOẠI SỰ CỐ (quan trọng)
Đừng đóng khung vào một use case. Hãy tự suy luận theo `action_type` thật sự đã xảy ra:
- Remediation kiểu **UPDATE/chuẩn hoá tại chỗ** (không xoá dòng, không có quarantine):
  Check 2 chuyển thành "không có dòng nào bị mất" (total_rows phải GIỮ NGUYÊN), và ghi
  rõ `quarantine_table` là rỗng. Đánh dấu severity WARNING/INFO nếu hạng mục không áp
  dụng được, kèm giải thích — **không được tự ý cho `passed = true` khi chưa kiểm được**.
- Remediation kiểu **BACKFILL/RERUN**: kiểm số dòng có tăng đúng kỳ vọng, không nhân bản
  (duplicate key), không lệch khoảng ngày.
- Sự cố **hạ tầng (OOM/timeout)**: kiểm dữ liệu có bị nạp thiếu/nạp trùng sau khi retry.
- **Không có baseline snapshot**: vẫn kiểm được Check 1, còn Check 2/3 phải ghi rõ giới
  hạn "thiếu mốc so sánh" và hạ verdict nếu không thể khẳng định an toàn.
Bạn được khuyến khích thêm hạng mục ngoài 3 cái trên khi thấy cần, ví dụ:
DOWNSTREAM_CONSISTENCY (mart hạ nguồn đã rebuild khớp bảng chính chưa),
SCHEMA (số cột của quarantine có khớp bảng gốc không), hoặc kiểm các DQ rule còn lại.

# SCHEMA THẬT CỦA WAREHOUSE (DuckDB)
{catalog}

# RUNBOOK KHẢ DỤNG
{runbooks}
"""

AUDIT_INSTRUCTION = """\
🧾 Anh Engineer vừa yêu cầu em nghiệm thu độc lập một ca remediation. Em bắt đầu nhé.

=== HỒ SƠ SỰ CỐ (nguồn: hệ thống monitoring, đây là dữ kiện gốc đáng tin) ===
{incident_block}

=== LỜI KHAI CỦA AGENT 1 — CẦN KIỂM CHỨNG, KHÔNG ĐƯỢC TIN NGAY ===
{agent1_claims}

Yêu cầu cho lượt này:
1. Gọi `tool_get_incident_context` để lấy baseline + danh sách SQL đã thực sự chạy.
2. Tự chạy `tool_query_duckdb` cho ít nhất 3 hạng mục bắt buộc (CLEANLINESS,
   DATA_PRESERVATION, ROW_COUNT_INTEGRITY) — mỗi hạng mục một câu SQL riêng, tự viết.
3. Thêm hạng mục khác nếu em thấy cần (ví dụ mart hạ nguồn đã khớp chưa).
4. Sau khi có đủ số liệu, viết bản tóm tắt nghiệm thu bằng tiếng Việt cho anh Engineer:
   hạng mục nào đạt, hạng mục nào không, và em có dám cấp chứng nhận hay không.
"""

AUDIT_REPORT_INSTRUCTION = """\
Bây giờ hãy đóng gói kết quả nghiệm thu ở trên thành MỘT object JSON duy nhất, đúng
schema sau (không kèm chữ nào ngoài JSON, không dùng markdown fence):

{template}

Ràng buộc:
- `audited_incident_id` = "{incident_id}", `target_table` = "{target_table}".
- `checks` phải có ÍT NHẤT 3 phần tử, phủ đủ 3 category: CLEANLINESS,
  DATA_PRESERVATION, ROW_COUNT_INTEGRITY.
- `query_executed` là SQL em ĐÃ THỰC SỰ chạy qua tool (copy đúng nguyên văn).
- `actual_result` là con số THẬT từ kết quả tool, không được suy đoán.
- `passed` chỉ true khi actual khớp expected.
- `verdict` = "AUDIT_PASSED" chỉ khi mọi hạng mục BLOCKING đều passed.
"""


# ---------------------------------------------------------------------------
# 2. Suy ra rule vi phạm từ HỒ SƠ SỰ CỐ (không lấy từ Agent 1)
# ---------------------------------------------------------------------------


def derive_violation_sql(incident: Optional[IncidentInput], fallback: str = "") -> str:
    """
    Sinh câu SQL đếm số dòng vi phạm dựa trên incident envelope — tức là từ dữ kiện
    của dbt/monitoring, KHÔNG phải từ `verification_sql` mà Agent 1 tự viết.
    Nhờ vậy Check 1 của Agent 2 mới thực sự độc lập.
    """
    if incident is None:
        return fallback
    table = incident.bare_table_name
    ev = incident.evidence_payload or {}
    column = ev.get("column") or ev.get("column_name") or ev.get("failed_column")
    test_type = str(ev.get("test_type") or ev.get("failed_test") or "").lower()
    accepted = ev.get("accepted_values") or ev.get("allowed_values")

    if column and ("not_null" in test_type or "notnull" in test_type):
        return f"SELECT COUNT(*) AS violations FROM {table} WHERE {column} IS NULL"
    if column and "unique" in test_type:
        return (
            f"SELECT COUNT(*) AS violations FROM (SELECT {column} FROM {table} "
            f"GROUP BY {column} HAVING COUNT(*) > 1)"
        )
    if column and "accepted_values" in test_type and isinstance(accepted, (list, tuple)) and accepted:
        values = ", ".join("'" + str(v).replace("'", "''") + "'" for v in accepted)
        return (
            f"SELECT COUNT(*) AS violations FROM {table} "
            f"WHERE {column} NOT IN ({values})"
        )
    if column and ("positive" in test_type or "non_negative" in test_type):
        return f"SELECT COUNT(*) AS violations FROM {table} WHERE {column} < 0"

    compiled = ev.get("compiled_sql")
    if isinstance(compiled, str) and compiled.strip().lower().startswith("select"):
        return compiled.strip().rstrip(";")
    if column:
        return f"SELECT COUNT(*) AS violations FROM {table} WHERE {column} IS NULL"
    return fallback


def _scalar_from_result(result: Dict[str, Any]) -> Optional[int]:
    """Bóc giá trị số đầu tiên từ kết quả tool_query_duckdb."""
    if not result.get("ok") or not result.get("rows"):
        return None
    first = list(result["rows"][0].values())[0]
    if isinstance(first, bool):
        return None
    if isinstance(first, (int, float)):
        return int(first)
    return None


# ---------------------------------------------------------------------------
# 3. Bộ não OFFLINE cho Agent 2 (khi chưa cấu hình MaaS)
# ---------------------------------------------------------------------------


class _OfflineAuditorCompletions:
    """
    Bộ não mô phỏng cho Agent 2: phát ra đúng chuỗi tool call của một lượt nghiệm thu
    (lấy context -> 3 câu SQL kiểm tra), rồi dựng JSON báo cáo TỪ KẾT QUẢ THẬT của
    `run_engine_checks()` — nên dù chạy offline, các con số vẫn là số liệu thật
    trong DuckDB, không phải bịa.
    """

    def __init__(self, auditor: "DataAuditorAgent") -> None:
        self.auditor = auditor

    @staticmethod
    def _tools_used(messages: List[Dict[str, Any]]) -> List[str]:
        start = 0
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "user":
                start = i
                break
        return [m.get("name", "") for m in messages[start:] if m.get("role") == "tool"]

    def create(
        self,
        *,
        model: str = "",
        messages: Optional[List[Dict[str, Any]]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,  # noqa: A002
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

        wants_json = isinstance(response_format, dict) and response_format.get("type") == "json_object"
        if wants_json or "object JSON duy nhất" in last_user:
            checks = self.auditor.run_engine_checks()
            payload = {
                "audit_id": self.auditor.audit_id,
                "audited_incident_id": self.auditor.incident_id,
                "target_table": self.auditor.target_table,
                "quarantine_table": self.auditor.quarantine_table or "",
                "verdict": "AUDIT_PASSED"
                if all(c.passed or c.severity != "BLOCKING" for c in checks)
                else "AUDIT_FAILED",
                "checks": [c.model_dump() for c in checks],
                "certification_summary": (
                    "Em đã tự chạy lại toàn bộ hạng mục bằng SQL trực tiếp trên DuckDB "
                    "(chế độ OFFLINE: số liệu là số liệu thật, phần diễn giải dùng "
                    "kịch bản mẫu deterministic ạ)."
                ),
                "recommended_action": "ACCEPT"
                if all(c.passed or c.severity != "BLOCKING" for c in checks)
                else "ROLLBACK",
                "auditor_notes": "Chạy ở chế độ OFFLINE (chưa cấu hình MaaS API key).",
            }
            return MockResponse(MockMessage(content=json.dumps(payload, ensure_ascii=False)))

        used = self._tools_used(messages)
        plan: List[Tuple[str, Dict[str, Any]]] = [
            ("tool_get_incident_context", {"incident_id": self.auditor.incident_id}),
        ]
        for name, sql in self.auditor.engine_check_queries():
            plan.append(("tool_query_duckdb", {"query": sql}))

        if len(used) < len(plan):
            name, args = plan[len(used)]
            return MockResponse(MockMessage(tool_calls=[MockToolCall(name, args)]))

        checks = self.auditor.run_engine_checks()
        lines = [
            "🕵️‍♀️ Em đã nghiệm thu độc lập xong rồi ạ! Em tự chạy SQL trên DuckDB, "
            "không dùng lại con số nào của anh SRE Agent hết 💪",
            "",
        ]
        for c in checks:
            lines.append(f"- {c.icon} **{c.check_name}**: kỳ vọng `{c.expected_result}`, "
                         f"thực tế `{c.actual_result}`")
        return MockResponse(MockMessage(content="\n".join(lines)))


class _OfflineAuditorChat:
    def __init__(self, auditor: "DataAuditorAgent") -> None:
        self.completions = _OfflineAuditorCompletions(auditor)


class OfflineAuditorBrain:
    """Client giả lập cho Agent 2, cùng interface `client.chat.completions.create`."""

    def __init__(self, auditor: "DataAuditorAgent") -> None:
        self.chat = _OfflineAuditorChat(auditor)


# ---------------------------------------------------------------------------
# 4. AGENT 2
# ---------------------------------------------------------------------------


class DataAuditorAgent:
    """
    Agent nghiệm thu độc lập.

    Ví dụ dùng:
        auditor = DataAuditorAgent()
        report  = auditor.audit(incident=incident, remediation_report=agent1_report)
        print(report.verdict, report.passed_count, "/", len(report.checks))
        answer  = auditor.ask("Em chắc chắn không mất dòng nào chứ?")
    """

    def __init__(
        self,
        settings: Optional[LLMSettings] = None,
        client: Any = None,
        on_tool_event: Optional[Callable[[ToolEvent], None]] = None,
    ) -> None:
        # Dùng CHUNG api_key/base_url/model với Agent 1 (xem LLMSettings.for_auditor).
        # Đặt DRA_AUDITOR_MODEL nếu muốn Checker chạy bằng model khác Maker.
        # Ngân sách token nằm trong LLMSettings (xem PROFILES trong ai/llm.py);
        # Checker mặc định rẻ hơn Maker vì chỉ nghiệm thu, không điều tra sâu.
        self.settings = settings or LLMSettings.for_auditor()
        self.on_tool_event = on_tool_event

        self.messages: List[Dict[str, Any]] = []
        self.tool_events: List[ToolEvent] = []
        #: Token đã tiêu của Agent 2 (tách riêng khỏi Agent 1 để so sánh chi phí)
        self.usage = TokenUsage()
        self.report: Optional[AuditReport] = None

        # Ngữ cảnh ca nghiệm thu
        self.audit_id = f"AUD-{datetime.now(timezone.utc):%Y%m%d}-{uuid.uuid4().hex[:6].upper()}"
        self.incident: Optional[IncidentInput] = None
        self.incident_id: str = ""
        self.target_table: str = ""
        self.quarantine_table: Optional[str] = None
        self.violation_sql: str = ""
        self.violation_sql_source: str = "incident_envelope"
        self.engine_inconclusive: set[str] = set()
        self.context: Dict[str, Any] = {}
        #: Bảng bóng đang nghiệm thu (rỗng = luồng cũ, soi trực tiếp bảng thật)
        self.shadow_table: str = ""
        #: Kết quả đối chiếu shadow vs prod gần nhất (UI đọc lại để vẽ bảng diff)
        self.shadow_diff: Optional[Dict[str, Any]] = None

        self.client, self.mode = self._build_client(client)

    # -- client -------------------------------------------------------------

    def _build_client(self, client: Any) -> tuple[Any, str]:
        if client is not None:
            return client, "custom"
        if not self.settings.api_key:
            print(
                "[DataAuditorAgent] ⚠️  Chưa có API key -> Agent 2 chạy OFFLINE "
                "(số liệu DuckDB vẫn thật, lấy từ engine checks)."
            )
            return OfflineAuditorBrain(self), "offline"
        try:
            from openai import OpenAI
        except ImportError:
            print("[DataAuditorAgent] ⚠️  Chưa cài package `openai` -> chạy OFFLINE.")
            return OfflineAuditorBrain(self), "offline"

        client = OpenAI(
            api_key=self.settings.api_key,
            base_url=self.settings.base_url,
            timeout=self.settings.request_timeout,
            max_retries=2,
        )
        print(
            f"[DataAuditorAgent] ✅ MaaS: {self.settings.base_url} "
            f"| model={self.settings.describe()} (role=auditor)"
        )
        return client, "maas"

    @property
    def is_offline(self) -> bool:
        return self.mode == "offline"

    # -- nạp ca nghiệm thu --------------------------------------------------

    def load_case(
        self,
        incident: Optional[IncidentInput] = None,
        remediation_report: Optional[AgentReport] = None,
        shadow_table: str = "",
    ) -> None:
        """
        Nạp ca cần nghiệm thu và reset hội thoại.

        Agent 2 KHÔNG nhận `messages` của Agent 1 — chỉ nhận hồ sơ sự cố (dữ kiện gốc)
        và bản báo cáo của Agent 1 dưới nhãn "lời khai cần kiểm chứng".

        `shadow_table` để rỗng thì tự suy: ưu tiên plan của Agent 1, sau đó là quy ước
        `shadow_<table>` nếu bảng đó có thật. Không suy bừa ra tên bảng không tồn tại —
        nghiệm thu một bảng không có thật thì tệ hơn là nói thẳng rằng thiếu bảng.
        """
        self.incident = incident
        self.report = None
        self.tool_events = []
        self.shadow_diff = None

        self.incident_id = (
            (incident.incident_id if incident else "")
            or (remediation_report.incident_id if remediation_report else "")
        )
        self.target_table = (
            (incident.target_table if incident else "")
            or (remediation_report.target_table if remediation_report else "")
            or "unknown"
        )
        self.violation_sql = derive_violation_sql(
            incident,
            fallback=(remediation_report.remediation.verification_sql if remediation_report else ""),
        )

        # Bảng bóng cần nghiệm thu (kiến trúc WAP)
        candidate = (
            wap.bare_name(shadow_table)
            or (
                wap.bare_name(remediation_report.remediation.shadow_table_name)
                if remediation_report
                else ""
            )
            or wap.shadow_name(self.target_table)
        )
        self.shadow_table = candidate if db.table_exists(candidate) else ""

        # Lấy bằng chứng khách quan ngay từ đầu (Python gọi, không qua LLM)
        self.context = tools.tool_get_incident_context(self.incident_id)
        self.quarantine_table = self.context.get("detected_quarantine_table") or (
            tools.detect_quarantine_table(self.target_table)
        )

        claims = self._format_agent1_claims(remediation_report)
        self.messages = [
            {
                "role": "system",
                "content": AUDITOR_SYSTEM_PROMPT.format(
                    catalog=tools.get_catalog_snapshot(),
                    runbooks=", ".join(tools.list_runbook_topics()) or "(không có)",
                ),
            }
        ]
        self._pending_case = AUDIT_INSTRUCTION.format(
            incident_block=(
                incident.to_prompt_block()
                if incident
                else f"(không có incident envelope; bảng cần nghiệm thu: {self.target_table})"
            ),
            agent1_claims=claims,
        )

    @staticmethod
    def _format_agent1_claims(report: Optional[AgentReport]) -> str:
        if report is None:
            return "(Không có báo cáo của Agent 1 — hãy nghiệm thu dựa hoàn toàn vào dữ liệu thật.)"
        claim = {
            "root_cause_claimed": report.diagnosis.root_cause,
            "confidence_claimed": report.diagnosis.confidence_score,
            "affected_row_count_claimed": report.impact.affected_row_count,
            "action_type": report.remediation.action_type.value,
            "executable_command_claimed": report.remediation.executable_command,
            "verification_sql_claimed": report.remediation.verification_sql,
            "status_claimed": report.status.value,
        }
        return json.dumps(claim, ensure_ascii=False, indent=2, default=str)

    # -- vòng lặp agent (native while loop) --------------------------------

    def _trim_history(self) -> None:
        window = self.settings.max_history_messages
        keep_full = max(4, window // 2)
        # Nén (không xoá) output tool cũ — message role="tool" phải luôn đi kèm
        # assistant.tool_calls tương ứng, xoá sẽ làm request không hợp lệ.
        if len(self.messages) > keep_full:
            for message in self.messages[:-keep_full]:
                if message.get("role") != "tool":
                    continue
                content = message.get("content") or ""
                if len(content) > 280:
                    message["content"] = content[:280] + "… (đã nén để tiết kiệm token)"
        if len(self.messages) <= window:
            return
        head = self.messages[:3]
        tail = self.messages[-(window - 4) :]
        while tail and tail[0].get("role") == "tool":
            tail = tail[1:]
        self.messages = head + [
            {"role": "system", "content": "(… lược bớt phần giữa lịch sử nghiệm thu …)"}
        ] + tail

    def _assistant_to_dict(self, msg: Any) -> Dict[str, Any]:
        """Giữ nguyên `extra_content` (thought_signature của Gemini) khi replay history."""
        return assistant_message_to_dict(
            msg, gemini=is_google_endpoint(self.settings.base_url)
        )

    def _call_llm(self, use_tools: bool, json_mode: bool) -> Any:
        kwargs: Dict[str, Any] = {
            "model": self.settings.model,
            "messages": self.messages,
            "temperature": self.settings.temperature,
            "max_tokens": self.settings.max_tokens,
        }
        if use_tools:
            kwargs["tools"] = tools.AUDITOR_TOOLS_SCHEMA  # <- chỉ tool ĐỌC
            kwargs["tool_choice"] = "auto"
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        try:
            return self.client.chat.completions.create(**kwargs)
        except Exception as exc:  # noqa: BLE001
            if any(
                t in str(exc).lower()
                for t in ("response_format", "tool_choice", "unsupported", "invalid_request")
            ):
                kwargs.pop("response_format", None)
                kwargs.pop("tool_choice", None)
                return self.client.chat.completions.create(**kwargs)
            raise

    def _completion(self, use_tools: bool = True, json_mode: bool = False) -> Any:
        """Gọi LLM, có failover sang model khác trong pool khi lỗi tạm thời."""
        self._trim_history()
        attempts = 1 + min(len(self.settings.alternatives), 2)
        last_exc: Optional[Exception] = None
        for _ in range(attempts):
            try:
                response = self._call_llm(use_tools, json_mode)
                self.usage.add(response)
                return response
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                switched = self.settings.failover(exc)
                if not switched:
                    raise
                print(
                    f"[DataAuditorAgent] ⚠️  Model lỗi ({str(exc)[:80]}) "
                    f"-> chuyển sang `{switched}`"
                )
        raise last_exc  # type: ignore[misc]

    def _emit(self, event: ToolEvent) -> None:
        self.tool_events.append(event)
        if self.on_tool_event is not None:
            try:
                self.on_tool_event(event)
            except Exception:  # noqa: BLE001
                pass

    def _run_tool_call(self, tool_call: Any) -> Dict[str, Any]:
        name = tool_call.function.name
        raw = tool_call.function.arguments or "{}"
        try:
            args = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except json.JSONDecodeError:
            args = {}

        # Chốt cửa lần 2: Agent 2 chỉ được dùng tool ĐỌC
        result = tools.execute_tool(name, args, allowed_tools=tools.AUDITOR_ALLOWED_TOOLS)
        payload = json.dumps(result, ensure_ascii=False, default=str)
        limit = self.settings.max_tool_chars
        if len(payload) > limit:
            payload = payload[:limit] + f'… (đã cắt {len(payload) - limit} ký tự)"}}'

        self.messages.append(
            {"role": "tool", "tool_call_id": tool_call.id, "name": name, "content": payload}
        )
        self._emit(
            ToolEvent(name=name, arguments=args, result=result, ok=bool(result.get("ok", False)))
        )
        return result

    def _agent_loop(self, max_iterations: Optional[int] = None) -> str:
        limit = max_iterations or self.settings.max_iterations
        iteration = 0
        while True:
            iteration += 1
            budget = self.settings.token_budget
            over_budget = bool(budget) and self.usage.total_tokens >= budget
            if iteration > limit or over_budget:
                self.messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Em đã dùng hết ngân sách ({limit} bước / "
                            f"{self.usage.total_tokens:,} tokens). KHÔNG gọi thêm tool nữa, "
                            "hãy kết luận ngay dựa trên số liệu đã thu được."
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

    # -- ENGINE CHECKS: đối chiếu bằng Python thuần -------------------------

    def resolve_violation_sql(self) -> str:
        """
        Xác định câu SQL đếm vi phạm cho engine, theo thứ tự ưu tiên về độ độc lập:

          1. Suy từ **incident envelope** (dữ kiện gốc của dbt/monitoring) — độc lập nhất.
          2. Lấy từ **baseline snapshot** đã đóng băng lúc approve (cột `detail` của
             metric `violation_rows_before`). SQL này xuất phát từ kế hoạch của Agent 1
             nhưng đã bị hệ thống ghi cứng TRƯỚC khi vá, nên Agent 1 không sửa lại được.

        Trả về "" nếu không xác định được — khi đó engine sẽ báo *inconclusive*
        (thiếu bằng chứng) chứ không kết luận là FAIL.
        """
        if self.violation_sql:
            return self.violation_sql
        for row in (self.context.get("baseline_detail") or []):
            if str(row.get("metric")) == "violation_rows_before":
                candidate = str(row.get("detail") or "").strip()
                if candidate.lower().startswith(("select", "with")):
                    self.violation_sql_source = "baseline_snapshot"
                    return candidate.rstrip(";")
        return ""

    def engine_check_queries(self) -> List[Tuple[str, str]]:
        """Các câu SQL mà engine sẽ dùng (cũng là kịch bản tool call cho chế độ offline)."""
        bare = self.target_table.split(".")[-1]
        queries: List[Tuple[str, str]] = []
        resolved = self.resolve_violation_sql()
        if resolved:
            queries.append(("cleanliness", resolved))
        if bare:
            queries.append(("main_row_count", f"SELECT COUNT(*) AS rows FROM {bare}"))
        if self.quarantine_table:
            queries.append(
                ("quarantine_row_count", f"SELECT COUNT(*) AS rows FROM {self.quarantine_table}")
            )
        return queries

    def run_engine_checks(self) -> List[AuditCheckItem]:
        """
        Chạy lại các hạng mục lõi bằng Python thuần, độc lập hoàn toàn với LLM.
        Đây là "trọng tài": nếu LLM khai khác kết quả ở đây thì kết quả ở đây thắng.

        Có bảng bóng thì nghiệm thu trên bảng bóng (kiến trúc WAP); không có thì rơi về
        cách cũ là soi bảng thật và đối chiếu baseline snapshot.
        """
        if self.shadow_table and db.table_exists(self.shadow_table):
            return self.run_shadow_engine_checks()

        bare = self.target_table.split(".")[-1]
        context = tools.tool_get_incident_context(self.incident_id)
        self.context = context
        if not self.quarantine_table:
            self.quarantine_table = context.get("detected_quarantine_table")

        baseline: Dict[str, Any] = context.get("baseline_metrics") or {}
        has_baseline = bool(context.get("baseline_available"))

        total_before = baseline.get("total_rows_before")
        violations_before = baseline.get("violation_rows_before")
        quarantine_before = baseline.get("quarantine_rows_before") or 0

        checks: List[AuditCheckItem] = []
        # Category nào engine KHÔNG kết luận được (thiếu dữ kiện, khác với "kết luận FAIL").
        # Category nằm trong tập này sẽ không được phép ghi đè kết luận của LLM.
        self.engine_inconclusive: set[str] = set()

        # ---- Check 1: CLEANLINESS -----------------------------------------
        violation_sql = self.resolve_violation_sql()
        if violation_sql:
            res = tools.tool_query_duckdb(violation_sql)
            remaining = _scalar_from_result(res)
            if remaining is None and res.get("ok"):
                remaining = res.get("row_count", 0)
            checks.append(
                AuditCheckItem(
                    check_name=(
                        "Check 1 — Cleanliness: bảng chính không còn dòng vi phạm"
                        + (
                            " (rule lấy từ baseline)"
                            if self.violation_sql_source == "baseline_snapshot"
                            else ""
                        )
                    ),
                    category="CLEANLINESS",
                    severity="BLOCKING",
                    query_executed=violation_sql,
                    expected_result="0 dòng vi phạm",
                    actual_result=(
                        f"{remaining} dòng vi phạm"
                        if res.get("ok")
                        else f"LỖI khi query: {res.get('error')}"
                    ),
                    passed=bool(res.get("ok")) and remaining == 0,
                    finding=(
                        "🎉 Rule của sự cố đã sạch hoàn toàn ạ."
                        if res.get("ok") and remaining == 0
                        else f"🛑 Vẫn còn {remaining} dòng vi phạm — remediation chưa xử lý hết."
                    ),
                    verified_by_engine=True,
                )
            )
        else:
            self.engine_inconclusive.add("CLEANLINESS")
            checks.append(
                AuditCheckItem(
                    check_name="Check 1 — Cleanliness: engine không suy được rule vi phạm",
                    category="CLEANLINESS",
                    severity="INFO",
                    expected_result="có rule (cột + loại test) để engine kiểm chéo",
                    actual_result=(
                        "không xác định được từ incident envelope lẫn baseline snapshot"
                    ),
                    passed=False,
                    finding=(
                        "ℹ️ Engine không kiểm chéo được hạng mục này (thiếu định nghĩa rule). "
                        "Kết luận sẽ dựa vào SQL do em tự viết; nếu em cũng không có bằng "
                        "chứng thì hệ thống sẽ KHÔNG cấp chứng nhận đâu ạ."
                    ),
                    verified_by_engine=None,
                )
            )

        # ---- Số dòng hiện tại ---------------------------------------------
        main_now: Optional[int] = None
        if bare:
            res_main = tools.tool_query_duckdb(f"SELECT COUNT(*) AS rows FROM {bare}")
            main_now = _scalar_from_result(res_main)

        quarantine_now: Optional[int] = None
        if self.quarantine_table:
            res_q = tools.tool_query_duckdb(
                f"SELECT COUNT(*) AS rows FROM {self.quarantine_table}"
            )
            quarantine_now = _scalar_from_result(res_q)

        moved = (
            quarantine_now - quarantine_before
            if isinstance(quarantine_now, int) and isinstance(quarantine_before, int)
            else None
        )

        # ---- Check 2: DATA_PRESERVATION -----------------------------------
        if not has_baseline:
            self.engine_inconclusive.add("DATA_PRESERVATION")
            checks.append(
                AuditCheckItem(
                    check_name="Check 2 — Data Preservation: thiếu baseline để đối chiếu",
                    category="DATA_PRESERVATION",
                    severity="INFO",
                    expected_result="có baseline snapshot trước khi vá",
                    actual_result="không tìm thấy baseline cho incident này",
                    passed=False,
                    finding=(
                        "ℹ️ Không có mốc trước-khi-vá nên engine không đối chiếu được. "
                        "Nếu em cũng không tự chứng minh được thì hệ thống sẽ không "
                        "cấp chứng nhận ạ."
                    ),
                    verified_by_engine=None,
                )
            )
        elif self.quarantine_table:
            sql = (
                f"SELECT COUNT(*) AS quarantined_now FROM {self.quarantine_table}  "
                f"-- so với baseline: quarantine_rows_before={quarantine_before}, "
                f"violation_rows_before={violations_before}"
            )
            ok = (
                isinstance(moved, int)
                and isinstance(violations_before, int)
                and moved == violations_before
            )
            checks.append(
                AuditCheckItem(
                    check_name="Check 2 — Data Preservation: số dòng cách ly khớp số vi phạm ban đầu",
                    category="DATA_PRESERVATION",
                    severity="BLOCKING",
                    query_executed=sql,
                    expected_result=f"{violations_before} dòng được chuyển vào quarantine",
                    actual_result=(
                        f"{moved} dòng mới vào quarantine "
                        f"(hiện tại {quarantine_now}, trước đó {quarantine_before})"
                    ),
                    passed=bool(ok),
                    finding=(
                        "✅ Không mất dòng nào — dữ liệu bẩn được giữ nguyên trong quarantine "
                        "để backfill sau ạ."
                        if ok
                        else f"🛑 Lệch {moved} vs {violations_before}: có khả năng xoá oan hoặc "
                        "cách ly thừa. Anh nên kiểm lại WHERE của script remediation."
                    ),
                    verified_by_engine=True,
                )
            )
        else:
            # Remediation kiểu chuẩn hoá tại chỗ: không được mất dòng nào
            ok = isinstance(main_now, int) and isinstance(total_before, int) and main_now == total_before
            checks.append(
                AuditCheckItem(
                    check_name="Check 2 — Data Preservation: không có quarantine, số dòng phải giữ nguyên",
                    category="DATA_PRESERVATION",
                    severity="BLOCKING",
                    query_executed=f"SELECT COUNT(*) AS rows FROM {bare}",
                    expected_result=f"{total_before} dòng (giữ nguyên như trước khi vá)",
                    actual_result=f"{main_now} dòng",
                    passed=bool(ok),
                    finding=(
                        "✅ Remediation sửa tại chỗ, không xoá dòng nào — hợp lệ ạ."
                        if ok
                        else "🛑 Số dòng thay đổi mà không có bảng quarantine nào giữ lại "
                        "=> nghi ngờ mất dữ liệu không thể phục hồi."
                    ),
                    verified_by_engine=True,
                )
            )

        # ---- Check 3: ROW_COUNT_INTEGRITY ---------------------------------
        if has_baseline and isinstance(main_now, int) and isinstance(total_before, int):
            moved_safe = moved if isinstance(moved, int) else 0
            total_after = main_now + moved_safe
            ok = total_after == total_before
            sql = (
                f"SELECT (SELECT COUNT(*) FROM {bare}) AS main_rows"
                + (
                    f", (SELECT COUNT(*) FROM {self.quarantine_table}) AS quarantine_rows"
                    if self.quarantine_table
                    else ""
                )
                + f"  -- baseline total_rows_before={total_before}"
            )
            checks.append(
                AuditCheckItem(
                    check_name="Check 3 — Row Count Integrity: tổng số dòng được bảo toàn",
                    category="ROW_COUNT_INTEGRITY",
                    severity="BLOCKING",
                    query_executed=sql,
                    expected_result=f"main + mới-cách-ly = {total_before}",
                    actual_result=f"{main_now} + {moved_safe} = {total_after}",
                    passed=bool(ok),
                    finding=(
                        "✅ Tổng số dòng khớp tuyệt đối, không bốc hơi dòng nào 💚"
                        if ok
                        else f"🛑 Lệch {total_after - total_before} dòng so với trước khi vá."
                    ),
                    verified_by_engine=True,
                )
            )
        else:
            self.engine_inconclusive.add("ROW_COUNT_INTEGRITY")
            checks.append(
                AuditCheckItem(
                    check_name="Check 3 — Row Count Integrity: engine không đủ dữ kiện đối chiếu",
                    category="ROW_COUNT_INTEGRITY",
                    severity="INFO",
                    expected_result="có baseline total_rows_before và đếm được bảng chính",
                    actual_result=f"total_before={total_before}, main_now={main_now}",
                    passed=False,
                    finding="ℹ️ Thiếu mốc so sánh nên engine không kết luận được ạ.",
                    verified_by_engine=None,
                )
            )

        return checks

    # -- Trọng tài cho kiến trúc WAP ---------------------------------------

    def run_shadow_engine_checks(self) -> List[AuditCheckItem]:
        """
        Nghiệm thu **bảng bóng** bằng Python thuần. Bốn hạng mục BLOCKING.

        Điểm khác biệt quan trọng so với cách cũ: không cần baseline snapshot. Bảng
        production chưa bị chạm nên **chính nó là mốc so sánh** — số liệu sống, không
        phải ảnh chụp có thể lệch thời điểm. Khi Agent 1 vá trực tiếp bảng thật, mốc
        trước-khi-vá là thứ đã mất và chỉ còn snapshot để tin; ở WAP thì mốc đó vẫn nằm
        nguyên trong database.

        Hạng mục 4 (schema) là điều kiện tiên quyết của atomic swap: tráo một bảng lệch
        cột vào production sẽ biến sự cố dữ liệu thành sự cố toàn hệ thống.
        """
        prod = wap.bare_name(self.target_table)
        shadow = wap.bare_name(self.shadow_table)
        violation_sql = self.resolve_violation_sql()

        diff = wap.diff_shadow_vs_prod(prod, shadow, violation_sql=violation_sql)
        self.shadow_diff = diff
        self.engine_inconclusive = set()
        checks: List[AuditCheckItem] = []

        if not diff.get("ok"):
            # Không có bảng bóng để soi thì không thể cấp chứng nhận. Nói thẳng, và để
            # severity BLOCKING để verdict chắc chắn là FAILED.
            return [
                AuditCheckItem(
                    check_name="Check 0 — Staging: không tìm thấy bảng bóng để nghiệm thu",
                    category="CLEANLINESS",
                    severity="BLOCKING",
                    expected_result=f"bảng `{shadow}` tồn tại và có dữ liệu",
                    actual_result=str(diff.get("error") or "không đọc được bảng bóng"),
                    passed=False,
                    finding=(
                        "🛑 Phase Write chưa chạy hoặc bảng bóng đã bị dọn — em không có gì "
                        "để nghiệm thu nên không thể cấp chứng nhận ạ."
                    ),
                    verified_by_engine=True,
                )
            ]

        rows_prod = diff.get("rows_prod")
        rows_shadow = diff.get("rows_shadow")
        rows_removed = diff.get("rows_removed")
        rows_quarantine = diff.get("rows_quarantine")
        violations_shadow = diff.get("violations_shadow")
        violations_prod = diff.get("violations_prod")
        quarantine_tbl = diff.get("quarantine_table") or self.quarantine_table

        # ---- Check 1: CLEANLINESS trên bảng bóng --------------------------
        if violation_sql:
            clean = violations_shadow == 0
            checks.append(
                AuditCheckItem(
                    check_name=f"Check 1 — Cleanliness: bảng bóng `{shadow}` không còn dòng vi phạm",
                    category="CLEANLINESS",
                    severity="BLOCKING",
                    query_executed=diff.get("violation_sql_shadow") or violation_sql,
                    expected_result="0 dòng vi phạm trên bảng bóng",
                    actual_result=(
                        f"{violations_shadow} dòng vi phạm trên `{shadow}` "
                        f"(bảng thật `{prod}` còn {violations_prod} dòng — chưa bị chạm, "
                        "đúng thiết kế)"
                    ),
                    passed=bool(clean),
                    finding=(
                        "🎉 Bảng bóng đã sạch hoàn toàn ạ — tráo sang production là an toàn."
                        if clean
                        else f"🛑 Bảng bóng VẪN còn {violations_shadow} dòng vi phạm. "
                        "Điều kiện WHERE trong script chưa bắt hết — chưa thể publish."
                    ),
                    verified_by_engine=True,
                )
            )
        else:
            self.engine_inconclusive.add("CLEANLINESS")
            checks.append(
                AuditCheckItem(
                    check_name="Check 1 — Cleanliness: engine không suy được rule vi phạm",
                    category="CLEANLINESS",
                    severity="INFO",
                    expected_result="có rule (cột + loại test) để engine kiểm chéo",
                    actual_result="không xác định được từ incident envelope",
                    passed=False,
                    finding=(
                        "ℹ️ Engine không kiểm chéo được hạng mục này. Kết luận dựa vào SQL "
                        "em tự viết; không có bằng chứng thì em không cấp chứng nhận ạ."
                    ),
                    verified_by_engine=None,
                )
            )

        # ---- Check 2: DATA_PRESERVATION -----------------------------------
        if quarantine_tbl and isinstance(rows_removed, int) and isinstance(rows_quarantine, int):
            # Số dòng biến mất khỏi bảng bóng phải nằm đủ trong quarantine.
            preserved = rows_quarantine >= rows_removed
            exact = rows_quarantine == rows_removed
            checks.append(
                AuditCheckItem(
                    check_name="Check 2 — Data Preservation: dòng bị loại đều nằm trong quarantine",
                    category="DATA_PRESERVATION",
                    severity="BLOCKING",
                    query_executed=(
                        f"SELECT (SELECT COUNT(*) FROM {prod}) AS prod_rows, "
                        f"(SELECT COUNT(*) FROM {shadow}) AS shadow_rows, "
                        f"(SELECT COUNT(*) FROM {quarantine_tbl}) AS quarantine_rows"
                    ),
                    expected_result=f"{rows_removed} dòng bị loại đều có trong `{quarantine_tbl}`",
                    actual_result=(
                        f"loại {rows_removed} dòng, quarantine đang giữ {rows_quarantine} dòng"
                        + ("" if exact else " (quarantine có dữ liệu tích luỹ từ lần trước)")
                    ),
                    passed=bool(preserved),
                    finding=(
                        "✅ Không mất dòng nào — dữ liệu bẩn còn nguyên trong quarantine để "
                        "backfill sau ạ."
                        if preserved
                        else f"🛑 Loại {rows_removed} dòng nhưng quarantine chỉ giữ "
                        f"{rows_quarantine} dòng => có dòng bị xoá không thể phục hồi."
                    ),
                    verified_by_engine=True,
                )
            )
        elif isinstance(rows_removed, int) and rows_removed == 0:
            # Remediation kiểu chuẩn hoá tại chỗ (UPDATE), không xoá dòng nào.
            checks.append(
                AuditCheckItem(
                    check_name="Check 2 — Data Preservation: sửa tại chỗ, không xoá dòng nào",
                    category="DATA_PRESERVATION",
                    severity="BLOCKING",
                    query_executed=(
                        f"SELECT (SELECT COUNT(*) FROM {prod}) AS prod_rows, "
                        f"(SELECT COUNT(*) FROM {shadow}) AS shadow_rows"
                    ),
                    expected_result=f"bảng bóng giữ đủ {rows_prod} dòng",
                    actual_result=f"{rows_shadow} dòng",
                    passed=True,
                    finding="✅ Remediation sửa tại chỗ, số dòng giữ nguyên — hợp lệ ạ.",
                    verified_by_engine=True,
                )
            )
        else:
            self.engine_inconclusive.add("DATA_PRESERVATION")
            checks.append(
                AuditCheckItem(
                    check_name="Check 2 — Data Preservation: không tìm thấy bảng quarantine",
                    category="DATA_PRESERVATION",
                    severity="BLOCKING",
                    query_executed=(
                        f"SELECT (SELECT COUNT(*) FROM {prod}) AS prod_rows, "
                        f"(SELECT COUNT(*) FROM {shadow}) AS shadow_rows"
                    ),
                    expected_result="có bảng quarantine giữ các dòng bị loại",
                    actual_result=(
                        f"loại {rows_removed} dòng nhưng không thấy bảng "
                        f"`{wap.quarantine_name(prod)}`"
                    ),
                    passed=False,
                    finding=(
                        "🛑 Có dòng bị loại mà không có bảng quarantine nào giữ lại => dữ liệu "
                        "sẽ mất vĩnh viễn sau khi publish. Em không cấp chứng nhận ạ."
                    ),
                    verified_by_engine=True,
                )
            )

        # ---- Check 3: ROW_COUNT_INTEGRITY ---------------------------------
        if isinstance(rows_prod, int) and isinstance(rows_shadow, int):
            removed = rows_removed if isinstance(rows_removed, int) else 0
            total_back = rows_shadow + removed
            ok = total_back == rows_prod
            checks.append(
                AuditCheckItem(
                    check_name="Check 3 — Row Count Integrity: tổng số dòng được bảo toàn",
                    category="ROW_COUNT_INTEGRITY",
                    severity="BLOCKING",
                    query_executed=(
                        f"SELECT (SELECT COUNT(*) FROM {shadow}) AS shadow_rows, "
                        f"(SELECT COUNT(*) FROM {prod}) AS prod_rows"
                    ),
                    expected_result=f"shadow + đã-loại = {rows_prod} (số dòng bảng thật)",
                    actual_result=f"{rows_shadow} + {removed} = {total_back}",
                    passed=bool(ok),
                    finding=(
                        "✅ Tổng số dòng khớp tuyệt đối, không bốc hơi dòng nào 💚"
                        if ok
                        else f"🛑 Lệch {total_back - rows_prod} dòng so với bảng thật."
                    ),
                    verified_by_engine=True,
                )
            )
        else:
            self.engine_inconclusive.add("ROW_COUNT_INTEGRITY")
            checks.append(
                AuditCheckItem(
                    check_name="Check 3 — Row Count Integrity: không đếm được số dòng",
                    category="ROW_COUNT_INTEGRITY",
                    severity="INFO",
                    expected_result="đếm được cả bảng thật và bảng bóng",
                    actual_result=f"prod={rows_prod}, shadow={rows_shadow}",
                    passed=False,
                    finding="ℹ️ Thiếu số liệu nên engine không kết luận được ạ.",
                    verified_by_engine=None,
                )
            )

        # ---- Check 4: SCHEMA — điều kiện tiên quyết của atomic swap -------
        schema_match = diff.get("schema_match")
        schema_diff = diff.get("schema_diff") or []
        checks.append(
            AuditCheckItem(
                check_name="Check 4 — Schema: bảng bóng khớp schema bảng thật (điều kiện atomic swap)",
                category="SCHEMA",
                severity="BLOCKING",
                query_executed=f"DESCRIBE {shadow}  /* đối chiếu với */  DESCRIBE {prod}",
                expected_result="cột và kiểu dữ liệu khớp hoàn toàn, đúng thứ tự",
                actual_result=(
                    "khớp hoàn toàn"
                    if schema_match
                    else f"lệch {len(schema_diff)} điểm: "
                    + json.dumps(schema_diff, ensure_ascii=False)[:300]
                ),
                passed=bool(schema_match),
                finding=(
                    "✅ Schema khớp nên tráo bảng sẽ không làm hạ nguồn gãy ạ."
                    if schema_match
                    else "🛑 Schema lệch — nếu publish thì mọi consumer hạ nguồn sẽ gãy. "
                    "Nguy hiểm hơn cả sự cố ban đầu."
                ),
                verified_by_engine=True,
            )
        )

        # ---- Check 5: bằng chứng Zero Blast Radius ------------------------
        # Không phải hạng mục chặn theo nghĩa chất lượng dữ liệu, nhưng là bằng chứng
        # kiểm toán quan trọng: chứng minh phase Write đã không chạm production.
        untouched = (
            isinstance(violations_prod, int)
            and isinstance(violations_shadow, int)
            and violations_prod >= violations_shadow
        )
        checks.append(
            AuditCheckItem(
                check_name="Check 5 — Zero Blast Radius: bảng production chưa bị thay đổi",
                category="CUSTOM",
                severity="WARNING",
                query_executed=diff.get("violation_sql_prod") or "",
                expected_result="bảng thật vẫn còn nguyên dữ liệu bẩn (chưa bị vá)",
                actual_result=(
                    f"`{prod}`: {rows_prod} dòng / {violations_prod} vi phạm — "
                    f"`{shadow}`: {rows_shadow} dòng / {violations_shadow} vi phạm"
                ),
                passed=bool(untouched),
                finding=(
                    "✅ Xác nhận bảng thật chưa bị chạm ở phase Write — đúng nguyên tắc "
                    "Zero Blast Radius ạ."
                    if untouched
                    else "⚠️ Số liệu bảng thật trông như đã bị thay đổi — cần điều tra vì "
                    "phase Write không được phép làm việc đó."
                ),
                verified_by_engine=True,
            )
        )
        return checks

    # -- API chính ---------------------------------------------------------

    def audit(
        self,
        incident: Optional[IncidentInput] = None,
        remediation_report: Optional[AgentReport] = None,
        shadow_table: str = "",
    ) -> AuditReport:
        """Chạy trọn một lượt nghiệm thu độc lập và trả về `AuditReport` đã validate."""
        if (
            incident is not None
            or remediation_report is not None
            or shadow_table
            or not self.messages
        ):
            self.load_case(incident, remediation_report, shadow_table=shadow_table)

        self.messages.append({"role": "user", "content": self._pending_case})
        narrative = self._agent_loop()
        report = self._request_structured_audit(narrative)

        # ---- TRỌNG TÀI: đối chiếu với engine checks ----------------------
        report = self._reconcile_with_engine(report)
        # Ghi lại model đã nghiệm thu (phục vụ truy vết cross-model checking)
        report.auditor_model = self.settings.model if not self.is_offline else "offline-engine"
        # `is_ready_for_production` do validator của AuditReport tự suy từ `checks`,
        # gán lại shadow_table rồi validate lần nữa để cổng publish tính đúng ngữ cảnh.
        report.shadow_table = self.shadow_table
        report = AuditReport.model_validate(report.model_dump())
        report.auditor_model = self.settings.model if not self.is_offline else "offline-engine"
        self.report = report
        return report

    def evidence_digest(self, max_chars: int = 220) -> str:
        """
        Bản tóm tắt bằng chứng: mỗi tool call -> 1 dòng (SQL + kết quả rút gọn).

        Dùng cho bước đóng gói JSON: đủ để điền `query_executed` / `actual_result` mà
        KHÔNG phải gửi lại toàn bộ history (tiết kiệm phần lớn token của Agent 2).
        """
        lines: List[str] = []
        for event in self.tool_events:
            result = json.dumps(event.result, ensure_ascii=False, default=str)
            if len(result) > max_chars:
                result = result[:max_chars] + "…"
            if event.name == "tool_query_duckdb":
                sql = " ".join(str(event.arguments.get("query", "")).split())
                lines.append(f"- SQL: {sql}\n  -> {result}")
            elif event.name == "tool_get_incident_context":
                lines.append(f"- tool_get_incident_context -> {result}")
            else:
                lines.append(f"- {event.name}({event.arguments}) -> {result}")
        return "\n".join(lines) or "(chưa chạy tool nào)"

    def _request_structured_audit(self, narrative: str = "") -> AuditReport:
        """
        Ép LLM đóng gói biên bản thành JSON (có retry).

        Tiết kiệm token: KHÔNG gửi lại toàn bộ history nghiệm thu. Chỉ gửi system prompt
        + digest bằng chứng (SQL đã chạy kèm kết quả rút gọn) + tóm tắt + template.
        """
        report_messages: List[Dict[str, Any]] = [
            self.messages[0],  # system prompt
            {
                "role": "user",
                "content": (
                    f"Sự cố: {self.incident_id} · bảng chính: {self.target_table} · "
                    f"bảng cách ly: {self.quarantine_table or '(không có)'}\n\n"
                    "=== BẰNG CHỨNG EM ĐÃ THU (SQL + kết quả thật) ===\n"
                    f"{self.evidence_digest()}\n\n"
                    "=== TÓM TẮT NGHIỆM THU CỦA EM ===\n"
                    f"{narrative.strip() or '(không có)'}\n\n"
                    + AUDIT_REPORT_INSTRUCTION.format(
                        template=AUDIT_JSON_TEMPLATE,
                        incident_id=self.incident_id,
                        target_table=self.target_table,
                    )
                ),
            },
        ]
        full_history = self.messages
        self.messages = report_messages

        last_error = ""
        report: Optional[AuditReport] = None
        try:
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
                    # audit_id do hệ thống cấp, KHÔNG để LLM tự đặt (nó hay copy nguyên
                    # mã ví dụ trong template, làm trùng mã giữa các lần nghiệm thu).
                    data["audit_id"] = self.audit_id
                    data.setdefault("audited_incident_id", self.incident_id)
                    data.setdefault("target_table", self.target_table)
                    data.setdefault("quarantine_table", self.quarantine_table or "")
                    try:
                        report = AuditReport.model_validate(data)
                        if narrative and not report.auditor_notes:
                            report.auditor_notes = " ".join(narrative.split())[:600]
                        break
                    except ValidationError as exc:
                        last_error = f"Sai schema: {exc.errors()[:3]}"

                if attempt < 2:
                    self.messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"JSON vừa rồi không dùng được ({last_error}). Trả lại DUY NHẤT "
                                "một object JSON đúng schema, không kèm chữ nào khác."
                            ),
                        }
                    )
        finally:
            # Trả lại history đầy đủ để engineer vẫn chất vấn Agent 2 được sau đó
            self.messages = full_history

        if report is not None:
            return report

        # Fallback: LLM không trả nổi JSON -> vẫn có giấy nghiệm thu từ engine checks
        engine = self.run_engine_checks()
        return AuditReport(
            audit_id=self.audit_id,
            audited_incident_id=self.incident_id,
            target_table=self.target_table,
            quarantine_table=self.quarantine_table or "",
            verdict="AUDIT_FAILED",
            checks=engine,
            certification_summary=(
                "⚠️ Em không dựng được báo cáo từ LLM nên em dùng kết quả kiểm tra bằng "
                f"engine (Python thuần) ạ. Lý do: {last_error}"
            ),
            recommended_action="INVESTIGATE",
            auditor_notes=f"Fallback do LLM lỗi: {last_error}",
        )

    def _reconcile_with_engine(self, report: AuditReport) -> AuditReport:
        """
        Hợp nhất kết luận của LLM với kết quả engine.

        Quy tắc: **engine là trọng tài**.
          - Cùng category mà LLM nói PASS nhưng engine nói FAIL -> lấy engine,
            gắn cờ `verified_by_engine=False` để UI hiện cảnh báo lệch.
          - Category nào LLM bỏ sót -> chèn thẳng kết quả engine vào.
        Sau đó validator của `AuditReport` tự tính lại `verdict` từ danh sách checks.
        """
        engine_checks = self.run_engine_checks()
        by_category: Dict[str, AuditCheckItem] = {c.category: c for c in engine_checks}
        inconclusive = getattr(self, "engine_inconclusive", set())
        mismatches: List[str] = []

        for check in report.checks:
            engine = by_category.get(check.category)
            if engine is None:
                continue
            if check.category in inconclusive:
                # Engine thiếu dữ kiện -> "không kết luận được" KHÁC "kết luận là sai".
                # Không được ghi đè kết luận của LLM trong trường hợp này.
                continue
            if check.passed == engine.passed:
                check.verified_by_engine = True
            else:
                mismatches.append(
                    f"{check.category}: LLM nói {'PASS' if check.passed else 'FAIL'} "
                    f"nhưng engine đo được {'PASS' if engine.passed else 'FAIL'} "
                    f"({engine.actual_result})"
                )
                check.verified_by_engine = False
                check.passed = engine.passed  # engine thắng
                check.actual_result = engine.actual_result
                check.finding = (
                    f"⚠️ Kết quả này đã bị hệ thống chỉnh lại theo số liệu đo bằng Python. "
                    f"{engine.finding}"
                )

        covered = {c.category for c in report.checks}
        for category, engine in by_category.items():
            if category not in covered:
                engine.check_name = f"{engine.check_name} (engine bổ sung)"
                report.checks.append(engine)
            elif category in inconclusive:
                # Vẫn đính kèm ghi chú minh bạch về giới hạn kiểm chéo của engine
                engine.check_name = f"{engine.check_name} (ghi chú giới hạn kiểm chéo)"
                report.checks.append(engine)

        # ---- Luật bằng chứng tối thiểu ------------------------------------
        # Ba hạng mục lõi PHẢI có ít nhất một bằng chứng ĐẠT, và bằng chứng đó phải
        # kèm câu SQL thật (do engine đo, hoặc do LLM chạy qua tool). Thiếu bằng chứng
        # thì không được cấp chứng nhận — "không kiểm được" không đồng nghĩa "đạt".
        for category in ("CLEANLINESS", "DATA_PRESERVATION", "ROW_COUNT_INTEGRITY"):
            has_evidence = any(
                c.category == category
                and c.passed
                and (c.verified_by_engine is True or (c.query_executed and c.actual_result))
                for c in report.checks
            )
            if not has_evidence:
                report.checks.append(
                    AuditCheckItem(
                        check_name=f"Bằng chứng nghiệm thu cho {category}",
                        category=category,
                        severity="BLOCKING",
                        expected_result="có ít nhất 1 hạng mục ĐẠT kèm SQL chứng minh",
                        actual_result="không có bằng chứng nào đạt",
                        passed=False,
                        finding=(
                            "🛑 Em không đủ bằng chứng để nghiệm thu hạng mục này nên em "
                            "không dám cấp chứng nhận ạ."
                        ),
                        verified_by_engine=True,
                    )
                )

        if mismatches:
            note = "🚨 Phát hiện lệch giữa kết luận của LLM và số liệu đo bằng máy: " + "; ".join(
                mismatches
            )
            report.auditor_notes = (report.auditor_notes + " " + note).strip()
            report.recommended_action = report.recommended_action or "INVESTIGATE"

        # Bắt validator chạy lại để chốt verdict theo bằng chứng
        return AuditReport.model_validate(report.model_dump())

    def ask(self, question: str) -> str:
        """Engineer chất vấn Agent 2 (vẫn chỉ được dùng tool đọc)."""
        if not self.messages:
            self.load_case()
            self.messages.append({"role": "user", "content": self._pending_case})
        self.messages.append({"role": "user", "content": question})
        return self._agent_loop(max_iterations=max(4, self.settings.max_iterations // 2))

    def executed_queries(self) -> List[str]:
        return [
            str(e.arguments.get("query", ""))
            for e in self.tool_events
            if e.name == "tool_query_duckdb" and e.arguments.get("query")
        ]


# ---------------------------------------------------------------------------
# 5. Helper headless
# ---------------------------------------------------------------------------


#: Trạng thái đóng mà chạy audit lại là vô nghĩa VÀ sai lệch (shadow đã bị
#: swap/xoá sau publish nên Agent 2 không còn gì để soi — chạy sẽ báo FAIL oan).
AUDIT_LOCKED_STATUSES = ("PUBLISHED", "PUBLISHED_RESOLVED")

AUDIT_SKIPPED_SUMMARY = (
    "Sự cố này đã được Publish vào Production thành công. "
    "Bảng Staging đã hoàn tất tráo đổi nên không thể và không cần Audit lại."
)


def audit_locked_status(incident_id: str) -> str:
    """Trả về status hiện tại nếu incident đang bị khoá audit, ngược lại ""."""
    if not incident_id:
        return ""
    try:
        from data import incident_store

        st = incident_store.current_status(incident_id) or ""
    except Exception:  # noqa: BLE001 - không đọc được DB thì không chặn
        return ""
    return st if st in AUDIT_LOCKED_STATUSES else ""


def build_audit_skipped(incident_id: str, status: str) -> Dict[str, Any]:
    """
    Payload SKIPPED khi audit bị khoá sau publish. Cùng shape với
    `run_audit_headless` để mọi caller (REST/chat) xử lý chung không vỡ.
    """
    return {
        "ok": True,
        "skipped": True,
        "mode": "locked",
        "verdict": "SKIPPED",
        "status": status,
        "incident_id": incident_id,
        "is_ready_for_production": False,
        "certification_summary": AUDIT_SKIPPED_SUMMARY,
        "audit_report": None,
        "shadow_table": "",
        "shadow_diff": None,
        "failed_details": None,
        "tool_calls": [],
    }


def run_audit_headless(
    incident: Optional[IncidentInput] = None,
    remediation_report: Optional[AgentReport] = None,
    shadow_table: str = "",
) -> Dict[str, Any]:
    """Chạy Agent 2 không cần UI (dùng cho REST API / cron / test)."""
    iid = (getattr(incident, "incident_id", "") or "") or (
        getattr(remediation_report, "incident_id", "") or ""
    )
    locked = audit_locked_status(iid)
    if locked:
        return build_audit_skipped(iid, locked)
    auditor = DataAuditorAgent()
    report = auditor.audit(
        incident=incident, remediation_report=remediation_report, shadow_table=shadow_table
    )
    return {
        "mode": auditor.mode,
        "audit_report": json.loads(report.to_json()),
        "shadow_table": auditor.shadow_table,
        "shadow_diff": auditor.shadow_diff,
        "is_ready_for_production": report.is_ready_for_production,
        "failed_details": report.failed_details,
        "tool_calls": [
            {"name": e.name, "arguments": e.arguments, "ok": e.ok} for e in auditor.tool_events
        ],
    }


__all__ = [
    "DataAuditorAgent",
    "OfflineAuditorBrain",
    "derive_violation_sql",
    "run_audit_headless",
    "AUDITOR_SYSTEM_PROMPT",
    "AUDIT_LOCKED_STATUSES",
    "AUDIT_SKIPPED_SUMMARY",
    "audit_locked_status",
    "build_audit_skipped",
]


# ---------------------------------------------------------------------------
# 6. Chạy thử nhanh:  python -m ai.auditor
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _incident = IncidentInput(**tools.build_sample_incident())
    _auditor = DataAuditorAgent()

    print("\n" + "=" * 78)
    print(f"🕵️‍♀️ AGENT 2 NGHIỆM THU {_incident.incident_id} (mode={_auditor.mode})")
    print("=" * 78)

    _report = _auditor.audit(incident=_incident)
    for _event in _auditor.tool_events:
        print(f"  [tool] {'✅' if _event.ok else '❌'} {_event.short_label}")

    print("\n" + _report.to_markdown())
