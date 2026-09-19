"""
ai/tools.py — Bộ công cụ agent được phép gọi (OpenAI Function Calling)
======================================================================

Tầng này là **cầu nối AI ↔ DATA**: nó không tự mở DuckDB, mọi truy cập đều đi qua
`data/` (connection dùng chung, audit log, baseline). Việc của nó là:

  1. Khai báo schema tool theo format OpenAI.
  2. **Cứng hoá luật an toàn** — không tin LLM:
     - `tool_query_duckdb` chặn mọi câu lệnh ghi.
     - `tool_execute_remediation` bị KHOÁ mặc định, chỉ mở sau khi engineer bấm Approve.
     - Cấm tuyệt đối ATTACH/DETACH/COPY…TO/EXPORT/INSTALL/LOAD, DROP bảng lõi,
       DELETE không có WHERE — dù LLM có sinh ra.
     - Agent 2 chỉ được cấp tool ĐỌC (`AUDITOR_TOOLS_SCHEMA` + `allowed_tools`).
  3. Ghi mọi lời gọi vào `agent_audit_log`.

Phân quyền theo vai:
    Agent 1 (Maker)   -> TOOLS_SCHEMA          : query, runbook, execute_remediation, verify
    Agent 2 (Checker) -> AUDITOR_TOOLS_SCHEMA  : query, runbook, get_incident_context
"""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import duckdb

import config
from data import audit as data_audit
from data import connection as db

# ---------------------------------------------------------------------------
# 0. Hằng số
# ---------------------------------------------------------------------------

RUNBOOK_DIR: Path = config.RUNBOOK_DIR
MAX_RESULT_ROWS: int = config.MAX_RESULT_ROWS

# Re-export từ scope DATA để agent chỉ cần nói chuyện với một module duy nhất.
# Đây là "adapter", không phải logic mới — mọi thao tác DuckDB vẫn do data/ thực hiện.
set_current_incident = data_audit.set_current_incident
detect_quarantine_table = data_audit.detect_quarantine_table
capture_baseline = data_audit.capture_baseline
get_connection = db.get_connection
close_connection = db.close_connection
list_tables = db.list_tables


# ---------------------------------------------------------------------------
# 1. Cơ chế Human-in-the-loop: khoá tool ghi dữ liệu
# ---------------------------------------------------------------------------

_remediation_unlocked = threading.Event()


def unlock_remediation() -> None:
    """Mở khoá tool ghi dữ liệu — CHỈ gọi sau khi engineer bấm [Approve]."""
    _remediation_unlocked.set()


def lock_remediation() -> None:
    """Khoá lại tool ghi dữ liệu (gọi ngay sau khi remediation chạy xong)."""
    _remediation_unlocked.clear()


def is_remediation_unlocked() -> bool:
    return _remediation_unlocked.is_set()


# ---------------------------------------------------------------------------
# 2. Guard SQL
# ---------------------------------------------------------------------------

_WRITE_KEYWORDS = {
    "insert", "update", "delete", "drop", "create", "alter", "truncate",
    "replace", "merge", "upsert", "grant", "revoke", "vacuum", "checkpoint",
}
_DANGEROUS_KEYWORDS = {
    "attach", "detach", "install", "load", "export", "import", "shell", "system",
}
_READ_PREFIXES = (
    "select", "with", "describe", "desc", "show", "explain", "summarize",
    "pragma", "table", "from", "values", "call",
)
_WRITE_PREFIXES = (
    "create", "insert", "update", "delete", "alter", "drop", "with", "select",
    "begin", "commit", "rollback", "set", "pragma", "analyze", "checkpoint",
)
_PROTECTED_TABLES = {
    "fact_orders", "dim_customers", "dq_test_results",
    "agent_audit_log", "dq_baseline_snapshot",
}


def _strip_sql_comments(sql: str) -> str:
    """Bỏ comment để guard không bị lừa bằng '-- DELETE'."""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def _tokens(sql: str) -> List[str]:
    """Tokenize thô, đã loại nội dung string literal để tránh false positive."""
    cleaned = re.sub(r"'[^']*'", " '' ", _strip_sql_comments(sql))
    cleaned = re.sub(r'"[^"]*"', ' "" ', cleaned)
    return re.findall(r"[a-zA-Z_][a-zA-Z_0-9]*", cleaned.lower())


def split_statements(sql: str) -> List[str]:
    """Tách nhiều câu lệnh bằng ';', bỏ qua ';' nằm trong string literal."""
    statements: List[str] = []
    buf: List[str] = []
    in_str = False
    quote = ""
    for char in sql:
        if in_str:
            buf.append(char)
            if char == quote:
                in_str = False
            continue
        if char in ("'", '"'):
            in_str = True
            quote = char
            buf.append(char)
            continue
        if char == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            continue
        buf.append(char)
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def validate_read_only(sql: str) -> Optional[str]:
    """Trả về thông báo lỗi nếu câu lệnh KHÔNG phải chỉ-đọc, None nếu OK."""
    body = _strip_sql_comments(sql).strip()
    if not body:
        return "Câu lệnh rỗng."
    statements = split_statements(body)
    if len(statements) > 1:
        return (
            "tool_query_duckdb chỉ nhận DUY NHẤT 1 câu lệnh SELECT. "
            f"Phát hiện {len(statements)} câu lệnh."
        )
    stmt = statements[0]
    first = stmt.split(None, 1)[0].lower().strip("(")
    if first not in _READ_PREFIXES:
        return (
            f"Câu lệnh bắt đầu bằng '{first.upper()}' không được phép ở tool đọc. "
            "Chỉ cho phép SELECT/WITH/DESCRIBE/SHOW/EXPLAIN/SUMMARIZE/PRAGMA."
        )
    bad = (set(_tokens(stmt)) & _WRITE_KEYWORDS) | (set(_tokens(stmt)) & _DANGEROUS_KEYWORDS)
    if bad:
        return (
            f"Phát hiện từ khoá ghi dữ liệu {sorted(bad)} trong tool đọc. "
            "Hãy dùng tool_execute_remediation (cần approve) nếu muốn ghi."
        )
    return None


def validate_remediation(sql: str) -> Optional[str]:
    """Kiểm tra script remediation. Trả về lỗi (str) hoặc None nếu hợp lệ."""
    body = _strip_sql_comments(sql).strip()
    if not body:
        return "Script remediation rỗng."
    statements = split_statements(body)
    if not statements:
        return "Script remediation rỗng."
    if len(statements) > 20:
        return "Script quá dài (>20 câu lệnh), hãy chia nhỏ để engineer review được."

    for stmt in statements:
        first = stmt.split(None, 1)[0].lower().strip("(")
        if first not in _WRITE_PREFIXES:
            return f"Câu lệnh '{first.upper()}' không nằm trong whitelist remediation."
        toks = set(_tokens(stmt))
        danger = toks & _DANGEROUS_KEYWORDS
        if danger:
            return f"Cấm dùng {sorted(danger)} trong remediation (rủi ro rời khỏi sandbox)."
        if first == "drop":
            for table in _PROTECTED_TABLES:
                if table in toks:
                    return (
                        f"Cấm DROP bảng lõi '{table}'. "
                        "Dùng CREATE OR REPLACE TABLE ... AS SELECT hoặc DELETE có WHERE."
                    )
        if first == "delete" and "where" not in toks:
            return "DELETE bắt buộc phải có WHERE để giới hạn phạm vi."
    return None


# ---------------------------------------------------------------------------
# 3. TOOL 1 — Query DuckDB (read-only)
# ---------------------------------------------------------------------------


def tool_query_duckdb(query: str, max_rows: int = MAX_RESULT_ROWS) -> Dict[str, Any]:
    """
    Chạy 1 câu SQL CHỈ ĐỌC trên DuckDB để điều tra dữ liệu.

    Dùng để: đếm số dòng vi phạm, GROUP BY tìm pattern lỗi, lấy sample rows,
    DESCRIBE schema, SHOW TABLES, kiểm tra bảng hạ nguồn...
    """
    error = validate_read_only(query)
    if error:
        data_audit.log_tool_call("tool_query_duckdb", "BLOCKED", query, "blocked", error)
        return {"ok": False, "error": error, "hint": "Chỉ dùng SELECT/WITH/DESCRIBE/SHOW."}

    try:
        limit = max(1, min(int(max_rows or MAX_RESULT_ROWS), 200))
    except (TypeError, ValueError):
        limit = MAX_RESULT_ROWS

    try:
        result = db.fetch(query.rstrip().rstrip(";"), max_rows=limit)
    except duckdb.Error as exc:
        data_audit.log_tool_call("tool_query_duckdb", "ERROR", query, "error", str(exc))
        return {
            "ok": False,
            "error": f"DuckDB lỗi: {exc}",
            "hint": "Kiểm tra lại tên bảng/cột bằng 'SHOW TABLES' hoặc 'DESCRIBE <table>'.",
        }

    data_audit.log_tool_call(
        "tool_query_duckdb", "QUERY", query, "ok", f"{result['row_count']} dòng trả về"
    )
    return {"ok": True, "query": query, **result}


# ---------------------------------------------------------------------------
# 4. TOOL 2 — Đọc runbook nội bộ
# ---------------------------------------------------------------------------

RUNBOOK_ALIASES: Dict[str, str] = {
    "sla": "sla_policy", "sla_policy": "sla_policy", "freshness": "sla_policy",
    "severity": "sla_policy", "policy": "sla_policy",
    "lineage": "lineage_fact_orders", "lineage_fact_orders": "lineage_fact_orders",
    "downstream": "lineage_fact_orders", "dashboard": "lineage_fact_orders",
    "upstream": "lineage_fact_orders", "fact_orders": "lineage_fact_orders",
    "playbook": "dq_playbook", "dq_playbook": "dq_playbook", "remediation": "dq_playbook",
    "quarantine": "dq_playbook", "fix": "dq_playbook",
    "oncall": "oncall_escalation", "on_call": "oncall_escalation",
    "escalation": "oncall_escalation", "approval": "oncall_escalation",
    "owner": "oncall_escalation",
    "infra": "infra_resources", "infra_resources": "infra_resources",
    "resource": "infra_resources", "oom": "infra_resources",
    "timeout": "infra_resources", "memory": "infra_resources",
}


def list_runbook_topics() -> List[str]:
    """Danh sách topic runbook khả dụng (theo tên file trong ai/runbooks/)."""
    if not RUNBOOK_DIR.exists():
        return []
    return sorted(p.stem for p in RUNBOOK_DIR.glob("*.md"))


def tool_read_runbook(topic: str) -> Dict[str, Any]:
    """
    Đọc runbook nội bộ (SLA, lineage/downstream, playbook remediation, escalation,
    hạ tầng). Agent PHẢI đọc runbook trước khi kết luận severity và trước khi liệt kê
    bảng/dashboard hạ nguồn.
    """
    topics = list_runbook_topics()
    key = (topic or "").strip().lower().replace(" ", "_").replace("-", "_")
    if key.endswith(".md"):
        key = key[:-3]

    target = RUNBOOK_ALIASES.get(key)
    if target is None:
        for name in topics:
            if key and (key in name or name in key):
                target = name
                break
    if target is None:
        for alias, name in RUNBOOK_ALIASES.items():
            if key and key in alias:
                target = name
                break

    if target is None or target not in topics:
        data_audit.log_tool_call(
            "tool_read_runbook", "NOT_FOUND", str(topic), "error", "topic không tồn tại"
        )
        return {
            "ok": False,
            "error": f"Không tìm thấy runbook cho topic '{topic}'.",
            "available_topics": topics,
            "hint": "Gọi lại với một trong các topic: " + ", ".join(topics),
        }

    content = (RUNBOOK_DIR / f"{target}.md").read_text(encoding="utf-8")
    data_audit.log_tool_call(
        "tool_read_runbook", "READ", target, "ok", f"{len(content)} ký tự"
    )
    return {"ok": True, "topic": target, "available_topics": topics, "content": content}


# ---------------------------------------------------------------------------
# 5. TOOL 3 — Thực thi remediation (chỉ chạy được sau khi APPROVE)
# ---------------------------------------------------------------------------


def tool_execute_remediation(sql_command: str, reason: str = "") -> Dict[str, Any]:
    """
    Thực thi script vá dữ liệu (DDL/DML) trên DuckDB Sink.
    Chạy trong 1 transaction: lỗi ở bất kỳ câu nào -> ROLLBACK toàn bộ.

    Tool này bị KHOÁ nếu engineer chưa bấm [Approve] trên UI.
    """
    if not is_remediation_unlocked():
        msg = (
            "TỪ CHỐI THỰC THI: remediation chưa được engineer phê duyệt. "
            "Hãy trình bày kế hoạch và chờ nút [Approve] trên UI (Human-in-the-loop)."
        )
        data_audit.log_tool_call(
            "tool_execute_remediation", "BLOCKED_NO_APPROVAL", sql_command, "blocked", msg
        )
        return {"ok": False, "error": msg, "requires_approval": True}

    error = validate_remediation(sql_command)
    if error:
        data_audit.log_tool_call(
            "tool_execute_remediation", "BLOCKED_UNSAFE", sql_command, "blocked", error
        )
        return {"ok": False, "error": f"Script bị chặn bởi guard an toàn: {error}"}

    statements = split_statements(_strip_sql_comments(sql_command))
    try:
        executed = db.execute_script(statements)
    except duckdb.Error as exc:
        data_audit.log_tool_call(
            "tool_execute_remediation", "EXECUTE", sql_command, "error", str(exc)
        )
        return {
            "ok": False,
            "error": f"Thực thi thất bại, đã ROLLBACK toàn bộ. Chi tiết: {exc}",
            "executed": [],
        }

    data_audit.log_tool_call(
        "tool_execute_remediation",
        "EXECUTE",
        sql_command,
        "ok",
        f"{len(executed)} câu lệnh; lý do: {reason}",
    )
    return {
        "ok": True,
        "statements_executed": len(executed),
        "executed": executed,
        "message": "Remediation đã chạy thành công và được COMMIT vào DuckDB.",
    }


# ---------------------------------------------------------------------------
# 6. TOOL 4 — Verify health sau khi vá
# ---------------------------------------------------------------------------


def tool_verify_health(table_name: str, check_sql: str) -> Dict[str, Any]:
    """
    Chạy lại câu test DQ để xác nhận vi phạm đã về 0.
    `check_sql` nên là `SELECT COUNT(*) ...` trên bảng cần kiểm tra.
    """
    error = validate_read_only(check_sql)
    if error:
        data_audit.log_tool_call("tool_verify_health", "BLOCKED", check_sql, "blocked", error)
        return {"ok": False, "error": f"check_sql phải là câu chỉ đọc. {error}"}

    try:
        result = db.fetch(check_sql.rstrip().rstrip(";"), max_rows=20)
    except duckdb.Error as exc:
        data_audit.log_tool_call("tool_verify_health", "ERROR", check_sql, "error", str(exc))
        return {"ok": False, "error": f"DuckDB lỗi khi verify: {exc}"}

    # Suy ra số vi phạm: ưu tiên giá trị scalar ở dòng đầu, cột đầu
    violations: Optional[int] = None
    if result["rows"]:
        first = list(result["rows"][0].values())[0]
        if isinstance(first, (int, float)) and not isinstance(first, bool):
            violations = int(first)
    if violations is None:
        violations = result["row_count"]

    healthy = violations == 0
    bare = (table_name or "").split(".")[-1].strip()
    table_stats = {"total_rows": db.row_count(bare)} if bare else {}

    data_audit.log_tool_call(
        "tool_verify_health",
        "VERIFY",
        check_sql,
        "ok" if healthy else "violation",
        f"violations={violations}",
    )
    return {
        "ok": True,
        "table": table_name,
        "violations": violations,
        "healthy": healthy,
        "table_stats": table_stats,
        "raw_result": result["rows"],
        "verdict": (
            "PASSED — không còn dòng vi phạm, có thể chuyển trạng thái RESOLVED."
            if healthy
            else f"FAILED — vẫn còn {violations} dòng vi phạm, cần escalate cho on-call."
        ),
    }


# ---------------------------------------------------------------------------
# 7. TOOL 5 — Bằng chứng khách quan (chỉ Agent 2 dùng)
# ---------------------------------------------------------------------------


def tool_get_incident_context(incident_id: str = "") -> Dict[str, Any]:
    """
    Lấy bằng chứng khách quan về sự cố từ chính DuckDB: baseline snapshot trước khi vá,
    các lệnh remediation/verify đã thực sự chạy, bảng quarantine, số dòng hiện tại.
    """
    return data_audit.read_incident_context(incident_id)


# ---------------------------------------------------------------------------
# 8. Catalog snapshot (nhồi vào system prompt để LLM biết schema thật)
# ---------------------------------------------------------------------------


def get_catalog_snapshot() -> str:
    """Sinh mô tả schema thật của DuckDB để agent không phải đoán tên cột."""
    tables = db.list_tables()
    if not tables:
        return "(catalog rỗng)"
    lines: List[str] = []
    for table in tables:
        try:
            desc = db.fetch(f"DESCRIBE {table}", max_rows=100)["rows"]
        except duckdb.Error:
            continue
        cols = ", ".join(f"{d.get('column_name')} {d.get('column_type')}" for d in desc)
        lines.append(f"- {table} ({db.row_count(table)} dòng): {cols}")
    return "\n".join(lines) if lines else "(catalog rỗng)"


def build_sample_incident() -> Dict[str, Any]:
    """
    Incident mẫu cho demo. Uỷ quyền hoàn toàn cho scope `data/` để tránh việc
    tầng AI tự mở DuckDB (DuckDB không cho mở cùng file 2 lần khác cấu hình).
    """
    from data.incidents import build_sample_incident_payload

    return build_sample_incident_payload()


# ---------------------------------------------------------------------------
# 9. OpenAI Function-Calling schemas + dispatcher
# ---------------------------------------------------------------------------

TOOLS_SCHEMA: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "tool_query_duckdb",
            "description": (
                "Chạy một câu SQL CHỈ ĐỌC (SELECT/WITH/DESCRIBE/SHOW/EXPLAIN) trên DuckDB "
                "warehouse để điều tra sự cố: đếm số dòng vi phạm, GROUP BY tìm pattern lỗi "
                "(theo source_system, source_version, ngày, batch ingest), lấy sample rows, "
                "kiểm tra bảng hạ nguồn. Mọi kết luận trong báo cáo PHẢI dựa trên số liệu "
                "lấy từ tool này. Không dùng được để ghi dữ liệu."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Câu SQL chỉ đọc, DUY NHẤT 1 statement, không có ';' cuối.",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Số dòng tối đa trả về (1-200, mặc định 50).",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tool_read_runbook",
            "description": (
                "Đọc runbook nội bộ của Data Platform. Bắt buộc dùng trước khi chấm severity "
                "và trước khi liệt kê bảng/dashboard hạ nguồn. Topic khả dụng: "
                "'sla_policy' (SLA, ngưỡng severity), 'lineage_fact_orders' (upstream/downstream, "
                "dashboard), 'dq_playbook' (mẫu SQL quarantine/backfill an toàn), "
                "'oncall_escalation' (ai duyệt, leo thang), 'infra_resources' (OOM, timeout, scale)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Tên topic runbook, ví dụ 'sla_policy' hoặc 'lineage'.",
                    }
                },
                "required": ["topic"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tool_execute_remediation",
            "description": (
                "Thực thi script SQL vá dữ liệu (DDL/DML) trên DuckDB. CHỈ gọi được SAU KHI "
                "engineer đã bấm [Approve] trên UI; nếu gọi trước sẽ bị từ chối. Chạy trong "
                "transaction, lỗi thì rollback toàn bộ. Luôn quarantine dữ liệu bẩn trước khi xoá."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_command": {
                        "type": "string",
                        "description": "Script SQL, nhiều câu lệnh tách nhau bằng dấu ';'.",
                    },
                    "reason": {
                        "type": "string",
                        "description": "Lý do thực thi (ghi vào audit log).",
                    },
                },
                "required": ["sql_command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tool_verify_health",
            "description": (
                "Chạy lại câu test DQ sau khi vá để xác nhận số dòng vi phạm đã về 0. "
                "Trả về violations, healthy và verdict."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Bảng cần kiểm tra, ví dụ 'fact_orders'.",
                    },
                    "check_sql": {
                        "type": "string",
                        "description": "Câu SELECT COUNT(*) đếm số dòng vi phạm, kỳ vọng = 0.",
                    },
                },
                "required": ["table_name", "check_sql"],
            },
        },
    },
]

_INCIDENT_CONTEXT_SCHEMA: Dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "tool_get_incident_context",
        "description": (
            "Lấy BẰNG CHỨNG KHÁCH QUAN về sự cố từ chính DuckDB: baseline snapshot "
            "(số dòng / số vi phạm TRƯỚC khi vá, do hệ thống ghi chứ không phải LLM khai), "
            "các câu SQL remediation đã thực sự chạy, bảng quarantine phát hiện được, và "
            "số dòng hiện tại của mọi bảng. Hãy gọi tool này ĐẦU TIÊN để có mốc so sánh "
            "thay vì tin vào báo cáo của Agent 1."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "incident_id": {
                    "type": "string",
                    "description": "Mã sự cố cần lấy context, ví dụ 'INC-2026-DQ01'.",
                }
            },
            "required": [],
        },
    },
}

#: Toolset RIÊNG cho Agent 2 (Data Auditor) — chỉ gồm tool ĐỌC.
#: Đây là cách cứng hoá tính độc lập của Maker-Checker: Agent 2 không được cấp tool ghi
#: nên về mặt kỹ thuật KHÔNG THỂ sửa dữ liệu để "làm cho báo cáo đẹp".
AUDITOR_TOOLS_SCHEMA: List[Dict[str, Any]] = [
    schema
    for schema in TOOLS_SCHEMA
    if schema["function"]["name"] in {"tool_query_duckdb", "tool_read_runbook"}
] + [_INCIDENT_CONTEXT_SCHEMA]

AUDITOR_ALLOWED_TOOLS = {"tool_query_duckdb", "tool_read_runbook", "tool_get_incident_context"}

TOOL_FUNCTIONS: Dict[str, Callable[..., Dict[str, Any]]] = {
    "tool_query_duckdb": tool_query_duckdb,
    "tool_read_runbook": tool_read_runbook,
    "tool_execute_remediation": tool_execute_remediation,
    "tool_verify_health": tool_verify_health,
    "tool_get_incident_context": tool_get_incident_context,
}

#: Tham số hợp lệ của từng tool (lọc bớt param lạ do LLM bịa ra)
_ALLOWED_ARGS: Dict[str, set[str]] = {
    "tool_query_duckdb": {"query", "max_rows"},
    "tool_read_runbook": {"topic"},
    "tool_execute_remediation": {"sql_command", "reason"},
    "tool_verify_health": {"table_name", "check_sql"},
    "tool_get_incident_context": {"incident_id"},
}


def execute_tool(
    name: str, arguments: Any, allowed_tools: Optional[set[str]] = None
) -> Dict[str, Any]:
    """
    Dispatcher dùng chung cho agent loop.

    - `arguments` có thể là dict hoặc JSON string (OpenAI trả string).
    - `allowed_tools`: giới hạn tool được phép gọi. Agent 2 truyền
      `AUDITOR_ALLOWED_TOOLS` để chặn triệt tiêu mọi tool ghi dữ liệu, kể cả khi LLM
      cố tình gọi tool không có trong schema của nó.
    - Luôn trả về dict (không raise) để agent loop không bị đứt giữa chừng.
    """
    if allowed_tools is not None and name not in allowed_tools:
        msg = (
            f"TỪ CHỐI: tool '{name}' nằm ngoài quyền hạn của agent này. "
            f"Chỉ được dùng: {sorted(allowed_tools)}."
        )
        data_audit.log_tool_call(
            name, "BLOCKED_OUT_OF_SCOPE", str(arguments)[:500], "blocked", msg
        )
        return {"ok": False, "error": msg, "allowed_tools": sorted(allowed_tools)}

    func = TOOL_FUNCTIONS.get(name)
    if func is None:
        return {
            "ok": False,
            "error": f"Tool '{name}' không tồn tại.",
            "available_tools": list(TOOL_FUNCTIONS),
        }

    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments or "{}")
        except json.JSONDecodeError as exc:
            return {"ok": False, "error": f"Arguments không phải JSON hợp lệ: {exc}"}
    if not isinstance(arguments, dict):
        arguments = {}

    kwargs = {k: v for k, v in arguments.items() if k in _ALLOWED_ARGS.get(name, set())}
    try:
        return func(**kwargs)
    except TypeError as exc:
        return {"ok": False, "error": f"Thiếu/sai tham số cho {name}: {exc}"}
    except Exception as exc:  # noqa: BLE001 - tool không được phép làm sập agent loop
        return {"ok": False, "error": f"Tool {name} lỗi không mong đợi: {exc}"}


def execute_tool_as_json(name: str, arguments: Any) -> str:
    """Bản trả về string JSON để nhét thẳng vào message role='tool'."""
    return json.dumps(execute_tool(name, arguments), ensure_ascii=False, default=str)


__all__ = [
    "tool_query_duckdb",
    "tool_read_runbook",
    "tool_execute_remediation",
    "tool_verify_health",
    "tool_get_incident_context",
    "TOOLS_SCHEMA",
    "AUDITOR_TOOLS_SCHEMA",
    "AUDITOR_ALLOWED_TOOLS",
    "TOOL_FUNCTIONS",
    "execute_tool",
    "execute_tool_as_json",
    "unlock_remediation",
    "lock_remediation",
    "is_remediation_unlocked",
    "get_catalog_snapshot",
    "list_runbook_topics",
    "list_tables",
    "set_current_incident",
    "detect_quarantine_table",
    "capture_baseline",
    "build_sample_incident",
    "get_connection",
    "close_connection",
    "validate_read_only",
    "validate_remediation",
    "split_statements",
]
