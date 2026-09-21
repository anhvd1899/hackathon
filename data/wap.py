"""
data/wap.py — Write / Audit / Publish (WAP) cho DuckDB
=====================================================

Module này là **nơi duy nhất** trong hệ thống được phép thao tác vòng đời bảng bóng
(shadow table). Nó thuộc scope DATA: không biết gì về LLM, không import `ai/`, không
import `web/`. Tầng `ai/` chỉ gọi qua `ai/tools.py`, tầng `web/` chỉ gọi qua API.

TẠI SAO CẦN WAP
---------------
Sự cố thật đã xảy ra: Agent gộp "cách ly dữ liệu bẩn" và "rebuild bảng mart" vào cùng
một transaction dài. Câu rebuild tham chiếu cột `segment` không tồn tại trên
`fact_orders` -> Binder Error -> rollback toàn bộ -> việc cách ly (vốn đúng và cấp
thiết) cũng mất theo. Engineer thì đã bấm duyệt một script sai từ đầu.

WAP cắt đường đó bằng ba tính chất:

1. **Zero Blast Radius.** Mọi lệnh vá chỉ chạy trên `shadow_<table>`. Bảng production
   không bị một câu DML nào chạm vào cho tới bước Publish. Guard `assert_shadow_only()`
   chặn ở tầng code, không dựa vào việc LLM "hứa" sẽ ngoan.
2. **Mỗi bước một transaction độc lập.** Stage, audit, publish, cleanup là bốn lần
   COMMIT riêng biệt. Bước sau fail không kéo đổ kết quả bước trước; bước trước đã
   COMMIT thì trạng thái quan sát được, không treo lơ lửng.
3. **Preflight trước khi trình engineer.** Script phải đi qua `EXPLAIN` từng câu trong
   một transaction thử rồi ROLLBACK. Binder Error bị bắt trước khi engineer nhìn thấy
   script, nên không bao giờ có chuyện duyệt xong mới biết SQL sai.

VÒNG ĐỜI MỘT PHIÊN
------------------
    preflight()          -> không ghi gì, chỉ EXPLAIN + dry-run rồi ROLLBACK
    stage_remediation()  -> transaction #1: tạo shadow + chạy script vá trên shadow
    diff_shadow_vs_prod()-> chỉ đọc, phục vụ Agent 2 nghiệm thu + UI đối chiếu
    atomic_publish()     -> transaction #2: tráo bảng (rename), prod nhận dữ liệu sạch
    cleanup_shadow()     -> transaction #3: dọn shadow khi huỷ hoặc sau publish

Trạng thái phiên nằm ở bảng `wap_sessions`; mỗi lần cập nhật là một transaction riêng
nên registry không bao giờ bị kẹt giữa hai bước.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from data.connection import (
    CONN_LOCK,
    fetch,
    get_connection,
    list_tables,
    row_count,
    scalar,
    table_exists,
)

# ---------------------------------------------------------------------------
# 0. Quy ước tên bảng
# ---------------------------------------------------------------------------

#: Tiền tố bảng bóng. `stg_orders` -> `shadow_stg_orders`.
SHADOW_PREFIX = "shadow_"

#: Tiền tố bảng cách ly. Bảng quarantine là bảng MỚI nên ghi vào đó không có blast
#: radius; guard vì vậy cho phép, miễn là không chạm bảng production.
QUARANTINE_PREFIX = "quarantine_"

#: Hậu tố bảng backup tạm dùng trong lúc tráo bảng ở bước publish.
BACKUP_SUFFIX = "__wap_backup"

#: Trạng thái của một phiên WAP (độc lập với trạng thái incident ở tầng nghiệp vụ).
STATUS_STAGED = "STAGED"              # đã chạy script trên shadow, chờ Agent 2 soi
STATUS_AUDIT_PASSED = "AUDIT_PASSED"  # Agent 2 nghiệm thu đạt, chờ publish
STATUS_AUDIT_FAILED = "AUDIT_FAILED"  # Agent 2 bắt lỗi, chờ engineer chọn hướng
STATUS_PUBLISHED = "PUBLISHED"        # đã tráo sang production
STATUS_CANCELLED = "CANCELLED"        # engineer huỷ, shadow đã bị drop

_DDL: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS wap_sessions (
        session_id       VARCHAR,
        incident_id      VARCHAR,
        created_at       TIMESTAMP,
        updated_at       TIMESTAMP,
        status           VARCHAR,
        prod_table       VARCHAR,
        shadow_table     VARCHAR,
        quarantine_table VARCHAR,
        shadow_script    VARCHAR,
        verification_sql VARCHAR,
        publish_script   VARCHAR,
        retry_count      INTEGER,
        rows_prod_before BIGINT,
        rows_shadow      BIGINT,
        violations_before BIGINT,
        violations_after BIGINT,
        preflight_json   VARCHAR,
        audit_json       VARCHAR,
        error            VARCHAR,
        published_at     TIMESTAMP
    );
    """,
]

_tables_ready = False


def ensure_wap_tables() -> None:
    """Tạo bảng registry nếu chưa có. Idempotent, gọi bao nhiêu lần cũng được."""
    global _tables_ready
    if _tables_ready:
        return
    con = get_connection()
    with CONN_LOCK:
        for ddl in _DDL:
            con.execute(ddl)
    _tables_ready = True


def _now() -> datetime:
    return datetime.now()


def _q(value: Any) -> str:
    """Escape nháy đơn cho SQL literal."""
    return str(value or "").replace("'", "''")


def bare_name(table: str) -> str:
    """`main.stg_orders` -> `stg_orders`. Bỏ schema, bỏ dấu nháy kép."""
    return (table or "").split(".")[-1].strip().strip('"')


def shadow_name(table: str) -> str:
    """Tên bảng bóng tương ứng của một bảng production."""
    name = bare_name(table)
    if not name:
        return ""
    return name if name.startswith(SHADOW_PREFIX) else f"{SHADOW_PREFIX}{name}"


def quarantine_name(table: str) -> str:
    """Tên bảng cách ly tương ứng của một bảng production."""
    name = bare_name(table)
    return f"{QUARANTINE_PREFIX}{name}" if name else ""


def is_shadow_table(table: str) -> bool:
    return bare_name(table).lower().startswith(SHADOW_PREFIX)


def is_quarantine_table(table: str) -> bool:
    return bare_name(table).lower().startswith(QUARANTINE_PREFIX)


# ---------------------------------------------------------------------------
# 1. Guard Zero Blast Radius — bóc bảng đích của từng lệnh ghi
# ---------------------------------------------------------------------------

#: Các lệnh ghi cần soi bảng đích. Thứ tự quan trọng: mẫu dài khớp trước mẫu ngắn.
_WRITE_TARGET_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ("CREATE_TABLE", re.compile(
        r"^\s*create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?table\s+"
        r"(?:if\s+not\s+exists\s+)?([\w.\"]+)", re.IGNORECASE)),
    ("CREATE_VIEW", re.compile(
        r"^\s*create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?view\s+"
        r"(?:if\s+not\s+exists\s+)?([\w.\"]+)", re.IGNORECASE)),
    ("INSERT", re.compile(r"^\s*insert\s+(?:or\s+\w+\s+)?into\s+([\w.\"]+)", re.IGNORECASE)),
    ("DELETE", re.compile(r"^\s*delete\s+from\s+([\w.\"]+)", re.IGNORECASE)),
    ("UPDATE", re.compile(r"^\s*update\s+([\w.\"]+)", re.IGNORECASE)),
    ("MERGE", re.compile(r"^\s*merge\s+into\s+([\w.\"]+)", re.IGNORECASE)),
    ("TRUNCATE", re.compile(r"^\s*truncate\s+(?:table\s+)?([\w.\"]+)", re.IGNORECASE)),
    ("ALTER", re.compile(r"^\s*alter\s+table\s+(?:if\s+exists\s+)?([\w.\"]+)", re.IGNORECASE)),
    ("DROP", re.compile(r"^\s*drop\s+(?:table|view)\s+(?:if\s+exists\s+)?([\w.\"]+)",
                        re.IGNORECASE)),
]

#: Lệnh chỉ đọc thì không cần soi bảng đích.
_READ_HEADS = {
    "select", "with", "explain", "describe", "desc", "show", "summarize", "pragma", "values",
}


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def split_statements(sql: str) -> List[str]:
    """
    Tách script thành từng câu lệnh theo ';', bỏ qua ';' nằm trong string literal.

    Cố ý không dùng `str.split(';')`: một câu `WHERE note = 'a;b'` sẽ bị cắt sai và
    tạo ra hai statement vô nghĩa.
    """
    statements: List[str] = []
    buf: List[str] = []
    in_str = False
    quote = ""
    for char in sql or "":
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
            chunk = "".join(buf).strip()
            if chunk:
                statements.append(chunk)
            buf = []
            continue
        buf.append(char)
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def write_targets(sql: str) -> List[Tuple[str, str]]:
    """
    Danh sách `(operation, table)` của mọi lệnh GHI trong script.

    Lệnh chỉ đọc trả về rỗng. Lệnh ghi mà không bóc được tên bảng sẽ trả
    `(op, "")` để caller coi là không xác định và từ chối — mặc định an toàn.
    """
    found: List[Tuple[str, str]] = []
    for statement in split_statements(_strip_comments(sql)):
        head = statement.split(None, 1)[0].lower() if statement.split() else ""
        if head in _READ_HEADS or head in {"begin", "commit", "rollback"}:
            continue
        matched = False
        for op, pattern in _WRITE_TARGET_PATTERNS:
            match = pattern.match(statement)
            if match:
                found.append((op, bare_name(match.group(1))))
                matched = True
                break
        if not matched:
            found.append(("UNKNOWN", ""))
    return found


def assert_shadow_only(sql: str, shadow_table: str) -> Optional[str]:
    """
    Kiểm tra script CHỈ ghi vào shadow table (hoặc bảng quarantine).

    Trả `None` nếu hợp lệ, hoặc chuỗi mô tả lý do từ chối. Đây là chốt cứng của
    nguyên tắc Zero Blast Radius: dù Agent 1 có sinh ra `DELETE FROM stg_orders`,
    script cũng không bao giờ chạm được bảng thật.
    """
    shadow = bare_name(shadow_table).lower()
    if not shadow:
        return "Thiếu tên shadow table nên không thể kiểm phạm vi ghi."

    targets = write_targets(sql)
    if not targets:
        return "Script không có lệnh ghi nào — không có gì để chạy trên staging."

    for op, target in targets:
        if not target:
            return (
                f"Có lệnh ghi không xác định được bảng đích (op={op}). "
                "Hãy viết lại script bằng câu lệnh tường minh."
            )
        low = target.lower()
        if low == shadow:
            continue
        if low.startswith(QUARANTINE_PREFIX) or low.startswith(SHADOW_PREFIX):
            continue
        return (
            f"TỪ CHỐI: script ghi vào bảng `{target}` (op={op}) nằm ngoài vùng staging. "
            f"Ở phase Write chỉ được ghi vào `{shadow}` hoặc bảng `{QUARANTINE_PREFIX}*`. "
            "Bảng production chỉ được thay đổi ở bước Publish (atomic swap)."
        )
    return None


# ---------------------------------------------------------------------------
# 2. PREFLIGHT — bắt Binder Error trước khi engineer nhìn thấy script
# ---------------------------------------------------------------------------


def explain_statement(sql: str) -> Dict[str, Any]:
    """
    `EXPLAIN` một câu lệnh duy nhất. Không ghi dữ liệu.

    DuckDB phân giải tên bảng/tên cột ngay ở bước lập kế hoạch, nên EXPLAIN đủ để
    bắt Binder Error (thiếu cột, sai bảng) và Parser Error (sai syntax).
    """
    statement = (sql or "").strip().rstrip(";")
    if not statement:
        return {"valid": False, "error": "Câu lệnh rỗng."}
    con = get_connection()
    try:
        with CONN_LOCK:
            con.execute(f"EXPLAIN {statement}").fetchall()
        return {"valid": True, "statement": statement}
    except duckdb.Error as exc:
        return {
            "valid": False,
            "statement": statement,
            "error": str(exc),
            "error_type": type(exc).__name__,
        }


def preflight(sql: str, shadow_table: str = "", prod_table: str = "") -> Dict[str, Any]:
    """
    Kiểm tra script vá TRƯỚC khi trình engineer. Không để lại bất kỳ thay đổi nào.

    Ba tầng, dừng ở tầng đầu tiên phát hiện vấn đề:

    1. **Guard phạm vi** — script có ghi ra ngoài staging không.
    2. **EXPLAIN từng câu** — bắt Binder/Parser Error.
    3. **Dry-run** — chạy thật từng câu trong một transaction rồi ROLLBACK.

    Tầng 3 là tầng không thể bỏ: `DELETE FROM shadow_x` chỉ EXPLAIN được sau khi câu
    `CREATE TABLE shadow_x AS ...` phía trước đã thực thi. Nếu chỉ EXPLAIN rời rạc thì
    mọi câu sau câu tạo bảng đều báo "table không tồn tại" — dương tính giả. Vì vậy
    dry-run chạy tuần tự trong transaction (EXPLAIN rồi execute từng câu) và ROLLBACK
    ở cuối, nên trạng thái database sau preflight y như trước.

    Lưu ý đồng thời: toàn bộ dry-run nằm trong một `CONN_LOCK`, nên không có luồng nào
    khác chen được vào giữa và bị ROLLBACK oan.
    """
    statements = split_statements(_strip_comments(sql or ""))
    result: Dict[str, Any] = {
        "valid": False,
        "statements_total": len(statements),
        "statements_checked": 0,
        "checked_at": _now().isoformat(),
        "shadow_table": bare_name(shadow_table),
        "stage": "scope_guard",
    }
    if not statements:
        result["error"] = "Script rỗng, không có câu lệnh nào để kiểm tra."
        return result

    # ---- Tầng 1: phạm vi ghi -------------------------------------------------
    if shadow_table:
        scope_error = assert_shadow_only(sql, shadow_table)
        if scope_error:
            result["error"] = scope_error
            return result
    if prod_table:
        prod = bare_name(prod_table).lower()
        for op, target in write_targets(sql):
            if target.lower() == prod:
                result["error"] = (
                    f"TỪ CHỐI: script ghi trực tiếp vào bảng production `{target}` (op={op})."
                )
                return result

    # ---- Tầng 2 + 3: EXPLAIN rồi dry-run trong transaction -------------------
    result["stage"] = "explain_dry_run"
    con = get_connection()
    checked: List[Dict[str, Any]] = []
    with CONN_LOCK:
        try:
            con.execute("BEGIN TRANSACTION")
        except duckdb.Error as exc:
            result["error"] = f"Không mở được transaction để dry-run: {exc}"
            return result
        try:
            for statement in statements:
                head = statement.split(None, 1)[0].lower()
                if head in {"begin", "commit", "rollback"}:
                    checked.append({"statement": statement, "skipped": True})
                    continue
                # EXPLAIN trước để lấy đúng thông báo Binder Error
                con.execute(f"EXPLAIN {statement.rstrip(';')}").fetchall()
                # Rồi chạy thật để câu kế tiếp có bảng/dữ liệu mà phân giải
                con.execute(statement)
                checked.append({"statement": statement, "explained": True, "executed": True})
            result["valid"] = True
            result["statements_checked"] = len([c for c in checked if c.get("executed")])
        except duckdb.Error as exc:
            result["error"] = str(exc)
            result["error_type"] = type(exc).__name__
            result["failed_statement"] = statements[len(checked)] if len(checked) < len(
                statements
            ) else statements[-1]
            result["statements_checked"] = len(checked)
        finally:
            # Dù thành công hay thất bại đều ROLLBACK: preflight KHÔNG được để lại dấu vết.
            try:
                con.execute("ROLLBACK")
                result["rolled_back"] = True
            except duckdb.Error as exc:  # pragma: no cover - chỉ xảy ra nếu conn đã chết
                result["rolled_back"] = False
                result["rollback_error"] = str(exc)

    result["details"] = checked
    if result["valid"]:
        result["message"] = (
            f"Preflight PASSED: {result['statements_checked']} câu lệnh hợp lệ về "
            "syntax và schema, đã ROLLBACK sạch."
        )
    return result


# ---------------------------------------------------------------------------
# 3. Registry phiên WAP — mỗi lần cập nhật là một transaction riêng
# ---------------------------------------------------------------------------

_SESSION_COLUMNS = (
    "session_id, incident_id, created_at, updated_at, status, prod_table, shadow_table, "
    "quarantine_table, shadow_script, verification_sql, publish_script, retry_count, "
    "rows_prod_before, rows_shadow, violations_before, violations_after, error, published_at"
)


def _row_to_session(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for key in ("preflight_json", "audit_json"):
        if key in out:
            raw = out.pop(key)
            try:
                out[key.replace("_json", "")] = json.loads(raw) if raw else None
            except (TypeError, ValueError):
                out[key.replace("_json", "")] = None
    return out


def get_session(incident_id: str) -> Optional[Dict[str, Any]]:
    """Phiên WAP mới nhất của một incident (kể cả đã publish/cancel)."""
    ensure_wap_tables()
    rows = fetch(
        f"SELECT {_SESSION_COLUMNS}, preflight_json, audit_json FROM wap_sessions "
        f"WHERE incident_id = '{_q(incident_id)}' ORDER BY created_at DESC LIMIT 1",
        max_rows=1,
    )["rows"]
    return _row_to_session(rows[0]) if rows else None


def active_session(incident_id: str) -> Optional[Dict[str, Any]]:
    """Phiên còn đang mở (chưa publish, chưa cancel) của một incident."""
    session = get_session(incident_id)
    if session and session["status"] in (
        STATUS_STAGED,
        STATUS_AUDIT_PASSED,
        STATUS_AUDIT_FAILED,
    ):
        return session
    return None


def list_sessions(limit: int = 50) -> List[Dict[str, Any]]:
    ensure_wap_tables()
    limit = max(1, min(int(limit), 500))
    return fetch(
        f"SELECT {_SESSION_COLUMNS} FROM wap_sessions ORDER BY created_at DESC LIMIT {limit}",
        max_rows=limit,
    )["rows"]


def update_session(incident_id: str, **fields: Any) -> None:
    """
    Cập nhật một phiên. **Transaction riêng, độc lập** với transaction dữ liệu.

    Cố ý tách khỏi các bước ghi dữ liệu: nếu việc ghi registry lỗi thì dữ liệu đã
    COMMIT vẫn nguyên vẹn, và ngược lại. Registry là siêu dữ liệu quan sát, không
    phải nguồn sự thật của dữ liệu.
    """
    if not fields:
        return
    ensure_wap_tables()
    allowed = {
        "status", "shadow_script", "verification_sql", "publish_script", "retry_count",
        "rows_prod_before", "rows_shadow", "violations_before", "violations_after",
        "quarantine_table", "error", "published_at", "shadow_table", "prod_table",
    }
    sets: List[str] = ["updated_at = ?"]
    params: List[Any] = [_now()]
    for key, value in fields.items():
        if key == "preflight":
            sets.append("preflight_json = ?")
            params.append(json.dumps(value, ensure_ascii=False, default=str) if value else None)
        elif key == "audit":
            sets.append("audit_json = ?")
            params.append(json.dumps(value, ensure_ascii=False, default=str) if value else None)
        elif key in allowed:
            sets.append(f"{key} = ?")
            params.append(value)
    params.append(incident_id)
    con = get_connection()
    with CONN_LOCK:
        con.execute(
            f"UPDATE wap_sessions SET {', '.join(sets)} WHERE incident_id = ?", params
        )


def _insert_session(session: Dict[str, Any]) -> None:
    con = get_connection()
    with CONN_LOCK:
        con.execute(
            "INSERT INTO wap_sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?)",
            [
                session["session_id"], session["incident_id"], session["created_at"],
                session["updated_at"], session["status"], session["prod_table"],
                session["shadow_table"], session["quarantine_table"],
                session["shadow_script"], session["verification_sql"],
                session["publish_script"], session["retry_count"],
                session["rows_prod_before"], session["rows_shadow"],
                session["violations_before"], session["violations_after"],
                session["preflight_json"], session["audit_json"],
                session["error"], session["published_at"],
            ],
        )


# ---------------------------------------------------------------------------
# 4. Sinh script mặc định (dùng khi LLM không tự viết được, hoặc để đối chiếu)
# ---------------------------------------------------------------------------


def build_shadow_script(
    prod_table: str,
    violation_predicate: str,
    quarantine: bool = True,
) -> str:
    """
    Sinh script staging chuẩn cho một rule vi phạm.

    `violation_predicate` là phần WHERE mô tả dòng BẨN, ví dụ `customer_id IS NULL`.
    Script gồm ba việc, tất cả trên vùng staging:

        1. tạo `shadow_<table>` là bản sao đầy đủ của bảng production
        2. đẩy dòng bẩn sang `quarantine_<table>` (giữ dữ liệu gốc, không mất mát)
        3. xoá dòng bẩn khỏi shadow

    Không rebuild bảng mart ở đây. Việc dựng lại hạ nguồn là `dbt run --select ...`
    sau khi publish — để dbt biên dịch SQL theo lineage thật, thay vì để LLM tự bịa
    câu `CREATE OR REPLACE TABLE mart_...` rồi sai tên cột.
    """
    prod = bare_name(prod_table)
    shadow = shadow_name(prod)
    quarantine_tbl = quarantine_name(prod)
    predicate = (violation_predicate or "").strip().rstrip(";")
    if not predicate:
        raise ValueError("Thiếu điều kiện xác định dòng vi phạm (violation_predicate).")

    lines = [
        f"CREATE OR REPLACE TABLE {shadow} AS SELECT * FROM {prod}",
    ]
    if quarantine:
        # CREATE OR REPLACE để số dòng quarantine = đúng số vi phạm lần này
        # (không cộng dồn lần trước) — Auditor đối chiếu exact match.
        lines += [
            f"CREATE OR REPLACE TABLE {quarantine_tbl} AS "
            f"SELECT * FROM {shadow} WHERE {predicate}",
        ]
    lines.append(f"DELETE FROM {shadow} WHERE {predicate}")
    return ";\n".join(lines)


def build_publish_script(prod_table: str, shadow_table: str = "") -> str:
    """
    Sinh script publish để **hiển thị cho engineer xem trước** (không tự chạy).

    Việc chạy thật do `atomic_publish()` đảm nhiệm: nó cần kiểm tra tồn tại bảng, sinh
    tên backup không trùng và quản transaction, nên không thể phó thác cho một chuỗi
    SQL tĩnh.
    """
    prod = bare_name(prod_table)
    shadow = bare_name(shadow_table) or shadow_name(prod)
    backup = f"{prod}{BACKUP_SUFFIX}"
    return (
        "BEGIN TRANSACTION;\n"
        f"ALTER TABLE {prod} RENAME TO {backup};\n"
        f"ALTER TABLE {shadow} RENAME TO {prod};\n"
        f"DROP TABLE IF EXISTS {backup};\n"
        "COMMIT;"
    )


def violation_predicate_from(
    column_name: str, test_type: str, accepted_values: Optional[List[str]] = None
) -> str:
    """
    Suy điều kiện WHERE của dòng bẩn từ metadata dbt test.

    Nhờ đó hệ thống xử lý được test bất kỳ trong dự án, không phải hard-code cho một
    vài rule demo. Không suy được thì trả rỗng để caller tự quyết (thường là nhờ LLM).
    """
    column = (column_name or "").strip()
    kind = (test_type or "").strip().lower()
    if not column:
        return ""
    if kind in {"not_null", "source_not_null"}:
        return f"{column} IS NULL"
    if kind in {"accepted_values", "source_accepted_values"} and accepted_values:
        values = ", ".join(f"'{_q(v)}'" for v in accepted_values)
        return f"({column} IS NULL OR {column} NOT IN ({values}))"
    if kind in {"unique", "source_unique"}:
        # Dòng bẩn = bản ghi thuộc nhóm khoá trùng
        return (
            f"{column} IN (SELECT {column} FROM {{table}} "
            f"GROUP BY {column} HAVING COUNT(*) > 1)"
        )
    return ""


# ---------------------------------------------------------------------------
# 5. WRITE — transaction #1: tạo shadow + chạy script vá trên shadow
# ---------------------------------------------------------------------------


def stage_remediation(
    incident_id: str,
    prod_table: str,
    shadow_script: str,
    verification_sql: str = "",
    preflight_result: Optional[Dict[str, Any]] = None,
    retry_count: int = 0,
    run_preflight: bool = True,
) -> Dict[str, Any]:
    """
    Phase **Write**: chạy script vá trên shadow table. Bảng production KHÔNG bị chạm.

    Trình tự trong một transaction duy nhất (#1):
      - chụp số dòng / số vi phạm của production làm mốc (đọc, trước khi ghi)
      - chạy từng câu trong `shadow_script`
      - COMMIT

    Lỗi bất kỳ -> ROLLBACK toàn bộ phase Write. Vì phase này không chạm production,
    rollback ở đây không thể gây mất dữ liệu thật — đúng tinh thần Zero Blast Radius.

    Registry được ghi bằng transaction RIÊNG sau khi dữ liệu đã COMMIT, nên trạng thái
    phiên không bao giờ mâu thuẫn với dữ liệu thực tế.
    """
    ensure_wap_tables()
    prod = bare_name(prod_table)
    shadow = shadow_name(prod)

    if not prod:
        return {"ok": False, "error": "Thiếu tên bảng production."}
    if not table_exists(prod):
        return {"ok": False, "error": f"Bảng production `{prod}` không tồn tại."}

    scope_error = assert_shadow_only(shadow_script, shadow)
    if scope_error:
        return {"ok": False, "error": scope_error, "stage": "scope_guard"}

    flight = preflight_result
    if run_preflight or flight is None:
        flight = preflight(shadow_script, shadow_table=shadow, prod_table=prod)
    if not flight.get("valid"):
        return {
            "ok": False,
            "stage": "preflight",
            "error": (
                "Preflight FAILED nên không chạy staging: " + str(flight.get("error"))[:400]
            ),
            "preflight": flight,
        }

    # ---- Mốc trước khi vá (đọc, không ghi) --------------------------------
    rows_prod_before = row_count(prod)
    violations_before: Optional[int] = None
    if verification_sql.strip():
        violations_before = _as_int(scalar(verification_sql.rstrip().rstrip(";")))

    # ---- Transaction #1: chỉ ghi vùng staging ------------------------------
    statements = split_statements(_strip_comments(shadow_script))
    con = get_connection()
    executed: List[Dict[str, Any]] = []
    with CONN_LOCK:
        try:
            con.execute("BEGIN TRANSACTION")
            for statement in statements:
                head = statement.split(None, 1)[0].lower()
                if head in {"begin", "commit", "rollback"}:
                    executed.append({"statement": statement, "status": "skipped"})
                    continue
                con.execute(statement)
                executed.append({"statement": statement, "status": "ok"})
            con.execute("COMMIT")
        except duckdb.Error as exc:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            error = (
                f"Staging thất bại ở câu lệnh #{len(executed) + 1}, đã ROLLBACK toàn bộ "
                f"phase Write. Bảng production `{prod}` KHÔNG bị ảnh hưởng. Chi tiết: {exc}"
            )
            _record_session(
                incident_id=incident_id,
                prod_table=prod,
                shadow_table=shadow,
                shadow_script=shadow_script,
                verification_sql=verification_sql,
                status=STATUS_AUDIT_FAILED,
                retry_count=retry_count,
                rows_prod_before=rows_prod_before,
                violations_before=violations_before,
                preflight=flight,
                error=error,
            )
            return {"ok": False, "stage": "execute", "error": error, "executed": executed}

    # ---- Sau COMMIT: đo lại trên shadow ------------------------------------
    rows_shadow = row_count(shadow)
    violations_after = _as_int(
        scalar(_rewrite_for_shadow(verification_sql, prod, shadow).rstrip().rstrip(";"))
    ) if verification_sql.strip() else None

    quarantine_tbl = _detect_quarantine(shadow_script, prod)

    _record_session(
        incident_id=incident_id,
        prod_table=prod,
        shadow_table=shadow,
        shadow_script=shadow_script,
        verification_sql=verification_sql,
        status=STATUS_STAGED,
        retry_count=retry_count,
        rows_prod_before=rows_prod_before,
        rows_shadow=rows_shadow,
        violations_before=violations_before,
        violations_after=violations_after,
        quarantine_table=quarantine_tbl,
        preflight=flight,
        publish_script=build_publish_script(prod, shadow),
    )

    return {
        "ok": True,
        "stage": "staged",
        "incident_id": incident_id,
        "prod_table": prod,
        "shadow_table": shadow,
        "quarantine_table": quarantine_tbl,
        "statements_executed": len([e for e in executed if e["status"] == "ok"]),
        "executed": executed,
        "rows_prod_before": rows_prod_before,
        "rows_shadow": rows_shadow,
        "violations_before": violations_before,
        "violations_after": violations_after,
        "preflight": flight,
        "message": (
            f"Đã dựng `{shadow}` và chạy {len(executed)} câu lệnh vá TRÊN STAGING. "
            f"Bảng production `{prod}` vẫn nguyên vẹn, chờ Agent 2 nghiệm thu."
        ),
    }


def _record_session(**kwargs: Any) -> None:
    """Upsert phiên WAP. Insert nếu chưa có, ngược lại update — luôn transaction riêng."""
    incident_id = kwargs.get("incident_id") or ""
    existing = get_session(incident_id)
    if existing:
        update_session(
            incident_id,
            **{k: v for k, v in kwargs.items() if k != "incident_id"},
        )
        return
    now = _now()
    _insert_session(
        {
            "session_id": f"WAP-{now:%Y%m%d}-{uuid.uuid4().hex[:6].upper()}",
            "incident_id": incident_id,
            "created_at": now,
            "updated_at": now,
            "status": kwargs.get("status") or STATUS_STAGED,
            "prod_table": kwargs.get("prod_table") or "",
            "shadow_table": kwargs.get("shadow_table") or "",
            "quarantine_table": kwargs.get("quarantine_table") or "",
            "shadow_script": kwargs.get("shadow_script") or "",
            "verification_sql": kwargs.get("verification_sql") or "",
            "publish_script": kwargs.get("publish_script") or "",
            "retry_count": int(kwargs.get("retry_count") or 0),
            "rows_prod_before": kwargs.get("rows_prod_before"),
            "rows_shadow": kwargs.get("rows_shadow"),
            "violations_before": kwargs.get("violations_before"),
            "violations_after": kwargs.get("violations_after"),
            "preflight_json": json.dumps(
                kwargs.get("preflight") or {}, ensure_ascii=False, default=str
            ),
            "audit_json": None,
            "error": kwargs.get("error"),
            "published_at": kwargs.get("published_at"),
        }
    )


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _detect_quarantine(script: str, prod_table: str) -> str:
    """Tìm bảng quarantine mà script đã ghi vào (để Agent 2 biết chỗ đối chiếu)."""
    for _op, target in write_targets(script):
        if target and is_quarantine_table(target) and table_exists(target):
            return bare_name(target)
    candidate = quarantine_name(prod_table)
    return candidate if candidate and table_exists(candidate) else ""


def _rewrite_for_shadow(sql: str, prod_table: str, shadow_table: str) -> str:
    """
    Đổi tham chiếu bảng production trong câu verify sang shadow table.

    Agent 2 phải nghiệm thu trên shadow, nhưng câu verify trong plan lại viết theo tên
    bảng thật. Chỉ thay khi tên bảng đứng độc lập (biên từ) để không phá
    `mart_stg_orders_daily` thành `mart_shadow_stg_orders_daily`.
    """
    prod = bare_name(prod_table)
    shadow = bare_name(shadow_table)
    if not sql or not prod or not shadow:
        return sql or ""
    pattern = re.compile(rf"(?<![\w.]){re.escape(prod)}(?![\w])", re.IGNORECASE)
    # Bỏ tiền tố schema trước khi thay để `main.stg_orders` -> `shadow_stg_orders`
    cleaned = re.sub(rf"\bmain\.{re.escape(prod)}\b", prod, sql, flags=re.IGNORECASE)
    return pattern.sub(shadow, cleaned)


def rewrite_for_shadow(sql: str, prod_table: str, shadow_table: str = "") -> str:
    """Bản công khai của `_rewrite_for_shadow` cho tầng `ai/` và `web/` dùng."""
    return _rewrite_for_shadow(sql, prod_table, shadow_table or shadow_name(prod_table))


# ---------------------------------------------------------------------------
# 6. AUDIT — chỉ đọc, phục vụ Agent 2 và bảng đối chiếu trên UI
# ---------------------------------------------------------------------------


def diff_shadow_vs_prod(
    prod_table: str, shadow_table: str = "", violation_sql: str = ""
) -> Dict[str, Any]:
    """
    Đối chiếu shadow với production. **Hoàn toàn chỉ đọc.**

    Trả về đủ số liệu để trả lời ba câu hỏi nghiệm thu:
      - shadow đã sạch chưa (`violations_shadow` phải = 0)
      - mất bao nhiêu dòng (`rows_removed`) và có khớp số dòng cách ly không
      - production có bị thay đổi gì chưa (phải là chưa, ở phase này)
    """
    prod = bare_name(prod_table)
    shadow = bare_name(shadow_table) or shadow_name(prod)
    out: Dict[str, Any] = {
        "ok": True,
        "prod_table": prod,
        "shadow_table": shadow,
        "prod_exists": table_exists(prod),
        "shadow_exists": table_exists(shadow),
        "checked_at": _now().isoformat(),
    }
    if not out["shadow_exists"]:
        out["ok"] = False
        out["error"] = f"Chưa có shadow table `{shadow}` — phase Write chưa chạy."
        return out

    out["rows_prod"] = row_count(prod)
    out["rows_shadow"] = row_count(shadow)
    if isinstance(out["rows_prod"], int) and isinstance(out["rows_shadow"], int):
        out["rows_removed"] = out["rows_prod"] - out["rows_shadow"]

    if violation_sql.strip():
        prod_sql = violation_sql.rstrip().rstrip(";")
        shadow_sql = _rewrite_for_shadow(prod_sql, prod, shadow)
        out["violation_sql_prod"] = prod_sql
        out["violation_sql_shadow"] = shadow_sql
        out["violations_prod"] = _as_int(scalar(prod_sql))
        out["violations_shadow"] = _as_int(scalar(shadow_sql))
        out["rule_rewritten"] = shadow_sql != prod_sql

    quarantine_tbl = quarantine_name(prod)
    if table_exists(quarantine_tbl):
        out["quarantine_table"] = quarantine_tbl
        out["rows_quarantine"] = row_count(quarantine_tbl)

    # Schema phải khớp, nếu không thì atomic swap sẽ làm hỏng hợp đồng dữ liệu
    out["schema_match"], out["schema_diff"] = _compare_schema(prod, shadow)
    return out


def _compare_schema(prod: str, shadow: str) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    So sánh cột (tên + kiểu, theo đúng thứ tự) giữa production và shadow.

    Đây là điều kiện tiên quyết của atomic swap: tráo một bảng lệch schema sang
    production sẽ làm mọi consumer hạ nguồn gãy — tệ hơn cả sự cố ban đầu.
    """
    def describe(table: str) -> List[Tuple[str, str]]:
        try:
            rows = fetch(f"DESCRIBE {table}", max_rows=500)["rows"]
        except duckdb.Error:
            return []
        return [(str(r.get("column_name")), str(r.get("column_type"))) for r in rows]

    prod_cols = describe(prod)
    shadow_cols = describe(shadow)
    if prod_cols == shadow_cols:
        return True, []

    diff: List[Dict[str, Any]] = []
    prod_map = dict(prod_cols)
    shadow_map = dict(shadow_cols)
    for name, dtype in prod_cols:
        if name not in shadow_map:
            diff.append({"column": name, "issue": "thiếu trên shadow", "prod_type": dtype})
        elif shadow_map[name] != dtype:
            diff.append(
                {
                    "column": name,
                    "issue": "lệch kiểu dữ liệu",
                    "prod_type": dtype,
                    "shadow_type": shadow_map[name],
                }
            )
    for name, dtype in shadow_cols:
        if name not in prod_map:
            diff.append({"column": name, "issue": "dư trên shadow", "shadow_type": dtype})
    if not diff and prod_cols != shadow_cols:
        diff.append({"issue": "thứ tự cột khác nhau"})
    return False, diff


def preview_table(table: str, limit: int = 20) -> Dict[str, Any]:
    """Xem trước dữ liệu một bảng (UI dùng cho tab Live / Shadow). Chỉ đọc."""
    name = bare_name(table)
    if not name:
        return {"ok": False, "error": "Thiếu tên bảng."}
    if not table_exists(name):
        return {"ok": False, "error": f"Bảng `{name}` không tồn tại.", "exists": False}
    limit = max(1, min(int(limit), 200))
    try:
        result = fetch(f"SELECT * FROM {name} LIMIT {limit}", max_rows=limit)
    except duckdb.Error as exc:
        return {"ok": False, "error": str(exc), "exists": True}
    return {
        "ok": True,
        "exists": True,
        "table": name,
        "columns": result["columns"],
        "rows": result["rows"],
        "total_rows": row_count(name),
        "preview_rows": len(result["rows"]),
    }


# ---------------------------------------------------------------------------
# 7. PUBLISH — transaction #2: atomic swap sang production
# ---------------------------------------------------------------------------


def atomic_publish(
    prod_table: str,
    shadow_table: str = "",
    incident_id: str = "",
    require_schema_match: bool = True,
    keep_backup: bool = False,
) -> Dict[str, Any]:
    """
    Phase **Publish**: tráo shadow thành production trong MỘT transaction (#2).

        BEGIN;
          ALTER TABLE <prod>   RENAME TO <prod>__wap_backup;
          ALTER TABLE <shadow> RENAME TO <prod>;
          DROP TABLE <prod>__wap_backup;      -- bỏ nếu keep_backup=True
        COMMIT;

    Dùng RENAME chứ không `DELETE`/`INSERT` vì rename là thao tác metadata: không có
    cửa sổ nào mà consumer đọc được bảng nửa vời, và nếu bước hai lỗi thì ROLLBACK
    đưa tên cũ về nguyên trạng.

    Chặn trước khi vào transaction: thiếu bảng, hoặc schema lệch. Tráo một bảng lệch
    cột vào production là cách nhanh nhất để biến một sự cố dữ liệu thành sự cố toàn
    hệ thống, nên mặc định `require_schema_match=True`.
    """
    ensure_wap_tables()
    prod = bare_name(prod_table)
    shadow = bare_name(shadow_table) or shadow_name(prod)
    result: Dict[str, Any] = {"ok": False, "prod_table": prod, "shadow_table": shadow}

    if not prod or not shadow:
        result["error"] = "Thiếu tên bảng production hoặc shadow."
        return result
    if not table_exists(shadow):
        result["error"] = f"Không có shadow table `{shadow}` để publish."
        return result
    if not table_exists(prod):
        result["error"] = f"Bảng production `{prod}` không tồn tại."
        return result

    if require_schema_match:
        match, diff = _compare_schema(prod, shadow)
        if not match:
            result["error"] = (
                f"TỪ CHỐI PUBLISH: schema của `{shadow}` lệch so với `{prod}`. "
                "Tráo bảng lệch cột sẽ làm hạ nguồn gãy."
            )
            result["schema_diff"] = diff
            return result

    rows_prod_before = row_count(prod)
    rows_shadow = row_count(shadow)
    backup = f"{prod}{BACKUP_SUFFIX}"

    con = get_connection()
    with CONN_LOCK:
        try:
            con.execute("BEGIN TRANSACTION")
            # Bảng backup sót lại từ lần publish lỗi trước sẽ làm rename fail -> dọn trước.
            con.execute(f"DROP TABLE IF EXISTS {backup}")
            con.execute(f"ALTER TABLE {prod} RENAME TO {backup}")
            con.execute(f"ALTER TABLE {shadow} RENAME TO {prod}")
            if not keep_backup:
                con.execute(f"DROP TABLE IF EXISTS {backup}")
            con.execute("COMMIT")
        except duckdb.Error as exc:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            result["error"] = (
                f"Publish thất bại, đã ROLLBACK. Bảng `{prod}` giữ nguyên dữ liệu cũ. "
                f"Chi tiết: {exc}"
            )
            update_session(incident_id, error=result["error"]) if incident_id else None
            return result

    published_at = _now()
    result.update(
        {
            "ok": True,
            "rows_prod_before": rows_prod_before,
            "rows_prod_after": row_count(prod),
            "rows_shadow": rows_shadow,
            "backup_table": backup if keep_backup else "",
            "published_at": published_at.isoformat(),
            "message": (
                f"Đã PUBLISH `{shadow}` -> `{prod}` bằng atomic swap. "
                f"{rows_prod_before} dòng -> {row_count(prod)} dòng."
            ),
        }
    )
    if incident_id:
        update_session(
            incident_id,
            status=STATUS_PUBLISHED,
            published_at=published_at,
            rows_shadow=rows_shadow,
            error=None,
        )
    return result


# ---------------------------------------------------------------------------
# 8. CLEANUP — transaction #3: dọn vùng staging
# ---------------------------------------------------------------------------


def cleanup_shadow(shadow_table: str, incident_id: str = "") -> Dict[str, Any]:
    """
    Drop shadow table. Transaction #3, độc lập hoàn toàn với dữ liệu production.

    Guard: chỉ drop bảng có tiền tố `shadow_`. Một lỗi đánh máy truyền vào đây tên
    bảng thật sẽ bị từ chối thay vì xoá mất production.
    """
    ensure_wap_tables()
    shadow = bare_name(shadow_table)
    if not shadow:
        return {"ok": False, "error": "Thiếu tên shadow table."}
    if not is_shadow_table(shadow):
        return {
            "ok": False,
            "error": (
                f"TỪ CHỐI DROP: `{shadow}` không có tiền tố `{SHADOW_PREFIX}` nên không "
                "phải bảng staging. Chỉ được dọn vùng staging ở đây."
            ),
        }

    existed = table_exists(shadow)
    con = get_connection()
    with CONN_LOCK:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DROP TABLE IF EXISTS {shadow}")
            con.execute("COMMIT")
        except duckdb.Error as exc:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            return {"ok": False, "error": f"Không drop được `{shadow}`: {exc}"}

    if incident_id:
        update_session(incident_id, status=STATUS_CANCELLED)
    return {
        "ok": True,
        "shadow_table": shadow,
        "existed": existed,
        "message": (
            f"Đã dọn bảng staging `{shadow}`."
            if existed
            else f"Bảng `{shadow}` không tồn tại, không cần dọn."
        ),
    }


def list_shadow_tables() -> List[Dict[str, Any]]:
    """Mọi bảng staging đang tồn tại — UI dùng để cảnh báo shadow bị bỏ quên."""
    out: List[Dict[str, Any]] = []
    for table in list_tables():
        if is_shadow_table(table):
            prod = bare_name(table)[len(SHADOW_PREFIX):]
            out.append(
                {
                    "shadow_table": bare_name(table),
                    "prod_table": prod,
                    "rows_shadow": row_count(table),
                    "rows_prod": row_count(prod) if table_exists(prod) else None,
                }
            )
    return out


__all__ = [
    "SHADOW_PREFIX",
    "QUARANTINE_PREFIX",
    "BACKUP_SUFFIX",
    "STATUS_STAGED",
    "STATUS_AUDIT_PASSED",
    "STATUS_AUDIT_FAILED",
    "STATUS_PUBLISHED",
    "STATUS_CANCELLED",
    "ensure_wap_tables",
    "bare_name",
    "shadow_name",
    "quarantine_name",
    "is_shadow_table",
    "is_quarantine_table",
    "split_statements",
    "write_targets",
    "assert_shadow_only",
    "explain_statement",
    "preflight",
    "get_session",
    "active_session",
    "list_sessions",
    "update_session",
    "build_shadow_script",
    "build_publish_script",
    "violation_predicate_from",
    "stage_remediation",
    "diff_shadow_vs_prod",
    "preview_table",
    "rewrite_for_shadow",
    "atomic_publish",
    "cleanup_shadow",
    "list_shadow_tables",
]
