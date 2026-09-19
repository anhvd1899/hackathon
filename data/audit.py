"""
data/audit.py — Nhật ký & mốc so sánh (phục vụ Maker–Checker)
=============================================================

Hai bảng do TẦNG CODE ghi, không qua LLM. Đây là nền tảng để Agent 2 nghiệm thu độc lập
mà không phải tin bất cứ con số nào do Agent 1 tự khai:

  - `agent_audit_log`      : mọi tool call của agent (ai/khi nào/chạy SQL gì/kết quả).
  - `dq_baseline_snapshot` : ảnh chụp warehouse TRƯỚC khi vá (số dòng, số vi phạm).
                             Thiết kế long/narrow (metric-value) nên dùng được cho mọi
                             loại sự cố, không fix cứng cột nào.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb

from data.connection import CONN_LOCK, fetch, get_connection, list_tables, row_count

#: Incident đang xử lý — chỉ dùng để gắn nhãn cho audit log
_current_incident_id: str = ""


def set_current_incident(incident_id: str) -> None:
    global _current_incident_id
    _current_incident_id = incident_id or ""


def current_incident() -> str:
    return _current_incident_id


# ---------------------------------------------------------------------------
# 1. agent_audit_log
# ---------------------------------------------------------------------------


def log_tool_call(
    tool_name: str,
    action: str,
    payload: str,
    status: str,
    detail: str = "",
    incident_id: str = "",
) -> None:
    """Ghi vết một hành động của agent. Best-effort: lỗi log không làm sập luồng chính."""
    try:
        con = get_connection()
        with CONN_LOCK:
            next_id = con.execute(
                "SELECT COALESCE(MAX(log_id), 0) + 1 FROM agent_audit_log"
            ).fetchone()[0]
            con.execute(
                "INSERT INTO agent_audit_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    next_id,
                    datetime.now(),
                    incident_id or _current_incident_id,
                    tool_name,
                    action,
                    (payload or "")[:4000],
                    status,
                    (detail or "")[:4000],
                ],
            )
    except duckdb.Error:
        pass


def read_audit_log(limit: int = 50) -> Dict[str, Any]:
    """Đọc nhật ký gần nhất (dùng cho REST /api/audit-log)."""
    limit = max(1, min(int(limit), 500))
    return fetch(
        "SELECT log_id, logged_at, incident_id, tool_name, action, status, "
        "substr(payload, 1, 300) AS payload, substr(detail, 1, 200) AS detail "
        f"FROM agent_audit_log ORDER BY log_id DESC LIMIT {limit}",
        max_rows=limit,
    )


# ---------------------------------------------------------------------------
# 2. Bảng quarantine
# ---------------------------------------------------------------------------


def detect_quarantine_table(target_table: str) -> Optional[str]:
    """
    Suy ra bảng cách ly tương ứng với bảng chính (KHÔNG hardcode tên).
    Ưu tiên `quarantine_<tên bảng>`, sau đó bất kỳ bảng nào có tiền tố
    quarantine/quarantined/_rejected và chứa tên bảng chính.
    """
    bare = (target_table or "").split(".")[-1].strip().lower()
    if not bare:
        return None
    tables = list_tables()
    exact = f"quarantine_{bare}"
    for table in tables:
        if table.lower() == exact:
            return table
    for table in tables:
        low = table.lower()
        if ("quarantine" in low or "quarantined" in low or "_rejected" in low) and bare in low:
            return table
    return None


# ---------------------------------------------------------------------------
# 3. dq_baseline_snapshot
# ---------------------------------------------------------------------------


def capture_baseline(
    incident_id: str,
    target_table: str,
    violation_sql: str = "",
    related_tables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Chụp trạng thái warehouse TRƯỚC khi chạy remediation.

    Được gọi bằng Python tại đúng thời điểm engineer bấm [Approve] — LLM không can
    thiệp được vào số liệu — nên Agent 2 có mốc đối chiếu đáng tin cho
    Check 2 (Data Preservation) và Check 3 (Row Count Integrity).
    """
    bare = (target_table or "").split(".")[-1].strip()
    quarantine = detect_quarantine_table(target_table)

    metrics: List[tuple[str, Optional[int], str]] = []
    if bare:
        metrics.append(("total_rows_before", row_count(bare), f"SELECT COUNT(*) FROM {bare}"))

    if violation_sql.strip():
        try:
            result = fetch(violation_sql.rstrip().rstrip(";"), max_rows=5)
            value: Optional[int] = None
            if result["rows"]:
                first = list(result["rows"][0].values())[0]
                if isinstance(first, (int, float)) and not isinstance(first, bool):
                    value = int(first)
            if value is None:
                value = result["row_count"]
            metrics.append(("violation_rows_before", value, violation_sql.strip()))
        except duckdb.Error as exc:
            metrics.append(("violation_rows_before", None, f"lỗi: {exc}"))

    metrics.append(
        (
            "quarantine_rows_before",
            row_count(quarantine) if quarantine else 0,
            quarantine or "(chưa có bảng quarantine)",
        )
    )

    # Bảng hạ nguồn do LLM liệt kê có thể kèm chú thích ("mart_x (Metabase)") hoặc
    # không phải bảng thật -> chỉ nhận identifier hợp lệ và có tồn tại trong catalog.
    existing = {t.lower() for t in list_tables()}
    for table in related_tables or []:
        short = str(table).split(".")[-1].strip().split(" ")[0].strip("`\"'")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", short or ""):
            continue
        if short.lower() not in existing:
            continue
        metrics.append((f"rows_before:{short}", row_count(short), f"bảng liên quan {short}"))

    now = datetime.now()
    written: Dict[str, Any] = {}
    con = get_connection()
    with CONN_LOCK:
        try:
            next_id = con.execute(
                "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM dq_baseline_snapshot"
            ).fetchone()[0]
        except duckdb.Error:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS dq_baseline_snapshot (
                    snapshot_id BIGINT, captured_at TIMESTAMP, incident_id VARCHAR,
                    target_table VARCHAR, metric VARCHAR, value BIGINT, detail VARCHAR
                )
                """
            )
            next_id = 1
        for offset, (metric, value, detail) in enumerate(metrics):
            con.execute(
                "INSERT INTO dq_baseline_snapshot VALUES (?, ?, ?, ?, ?, ?, ?)",
                [next_id + offset, now, incident_id, target_table, metric, value, detail[:2000]],
            )
            written[metric] = value

    log_tool_call(
        "capture_baseline",
        "SNAPSHOT",
        json.dumps(written, ensure_ascii=False),
        "ok",
        f"quarantine_table={quarantine}",
        incident_id=incident_id,
    )
    return {
        "ok": True,
        "incident_id": incident_id,
        "target_table": target_table,
        "quarantine_table": quarantine,
        "metrics": written,
        "captured_at": now.isoformat(),
    }


def read_incident_context(incident_id: str = "") -> Dict[str, Any]:
    """
    Trả về BẰNG CHỨNG KHÁCH QUAN về một sự cố, lấy từ chính DuckDB:
      - baseline snapshot (trạng thái trước khi vá)
      - các câu SQL remediation / verify đã THỰC SỰ chạy (từ agent_audit_log)
      - bảng quarantine phát hiện được + số dòng hiện tại của mọi bảng

    Agent 2 dùng hàm này (qua tool) thay vì tin vào lời kể của Agent 1.
    """
    where = ""
    if incident_id:
        safe = incident_id.replace("'", "''")
        where = f"WHERE incident_id = '{safe}'"

    try:
        baseline_rows = fetch(
            "SELECT incident_id, target_table, metric, value, detail, captured_at "
            f"FROM dq_baseline_snapshot {where} ORDER BY snapshot_id",
            max_rows=100,
        )["rows"]
    except duckdb.Error:
        baseline_rows = []

    target_table = str(baseline_rows[0].get("target_table") or "") if baseline_rows else ""

    try:
        executed = fetch(
            "SELECT logged_at, incident_id, tool_name, action, status, payload, detail "
            "FROM agent_audit_log "
            + (where + " AND " if where else "WHERE ")
            + "tool_name IN ('tool_execute_remediation', 'tool_verify_health') "
            "ORDER BY log_id",
            max_rows=50,
        )["rows"]
    except duckdb.Error:
        executed = []

    result: Dict[str, Any] = {
        "ok": True,
        "incident_id": incident_id,
        "target_table": target_table,
        "detected_quarantine_table": detect_quarantine_table(target_table) if target_table else None,
        "baseline_available": bool(baseline_rows),
        "baseline_metrics": {
            str(r.get("metric")): r.get("value") for r in baseline_rows if r.get("metric")
        },
        "baseline_detail": baseline_rows,
        "remediation_actually_executed": executed,
        "tables_now": {table: row_count(table) for table in list_tables()},
    }
    if not baseline_rows:
        result["hint"] = (
            "Chưa có baseline snapshot cho incident này (có thể remediation chưa chạy). "
            "Hãy tự suy mốc so sánh từ incident envelope / agent_audit_log và nói rõ "
            "giới hạn đó trong báo cáo."
        )
    log_tool_call(
        "tool_get_incident_context",
        "READ_CONTEXT",
        incident_id or "(all)",
        "ok",
        f"baseline={len(baseline_rows)} metric, executed={len(executed)}",
    )
    return result


__all__ = [
    "set_current_incident",
    "current_incident",
    "log_tool_call",
    "read_audit_log",
    "detect_quarantine_table",
    "capture_baseline",
    "read_incident_context",
]
