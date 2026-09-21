"""
data/incident_store.py — Kho sự cố + thông báo
==============================================

Nơi lưu vòng đời một sự cố, để **UI đọc tức thời mà không phải chờ agent**:

    DETECTED           job fail -> incident được tạo ngay (bởi data/pipeline.py)
    INVESTIGATING      worker nền đang cho Agent 1 điều tra
    WAITING_FOR_APPROVAL  đã có báo cáo (root cause + remediation) -> UI hiện được
    RESOLVED / FAILED / REJECTED

Điểm thiết kế quan trọng: báo cáo điều tra được **ghi sẵn vào DB** (cột `report_json`).
Khi giám khảo bấm vào job lỗi, UI chỉ SELECT ra — không gọi LLM, phản hồi tức thì.

Chống trùng lặp: `open_incident()` dùng khoá logic `(job_id, test_name)`. Job chạy lại
mà lỗi vẫn còn thì **tăng `occurrences`** chứ không tạo incident mới — nhờ vậy 10 flow
chạy mỗi 2-3 phút không làm ngập bảng incident.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from data.connection import CONN_LOCK, fetch, get_connection, scalar

if TYPE_CHECKING:  # tránh import vòng khi chạy thật
    from data.pipeline import PipelineJob

INCIDENT_DDL: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS incidents (
        incident_id     VARCHAR,
        job_id          VARCHAR,
        run_id          VARCHAR,
        created_at      TIMESTAMP,
        updated_at      TIMESTAMP,
        status          VARCHAR,
        severity        VARCHAR,
        target_table    VARCHAR,
        test_name       VARCHAR,
        column_name     VARCHAR,
        test_type       VARCHAR,
        failed_rows     BIGINT,
        occurrences     INTEGER,
        title           VARCHAR,
        envelope_json   VARCHAR,
        report_json     VARCHAR,
        audit_json      VARCHAR,
        error           VARCHAR
    );
    """,
    # --- Migration cho kiến trúc WAP ------------------------------------------
    # Dùng ADD COLUMN IF NOT EXISTS thay vì sửa CREATE TABLE ở trên: warehouse đang chạy
    # đã có bảng `incidents` với dữ liệu thật, và `CREATE TABLE IF NOT EXISTS` sẽ bỏ qua
    # định nghĩa mới. Thêm cột theo cách này là idempotent và không mất dữ liệu.
    "ALTER TABLE incidents ADD COLUMN IF NOT EXISTS shadow_table VARCHAR;",
    "ALTER TABLE incidents ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;",
    "ALTER TABLE incidents ADD COLUMN IF NOT EXISTS ready_for_production BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE incidents ADD COLUMN IF NOT EXISTS published_at TIMESTAMP;",
    """
    CREATE TABLE IF NOT EXISTS notifications (
        notification_id VARCHAR,
        created_at      TIMESTAMP,
        level           VARCHAR,     -- critical | warning | info | success
        title           VARCHAR,
        body            VARCHAR,
        job_id          VARCHAR,
        incident_id     VARCHAR,
        read_at         TIMESTAMP
    );
    """,
    # Bảng 1 dòng: incident đang được chọn để mở phiên HITL trên Chainlit
    """
    CREATE TABLE IF NOT EXISTS ui_state (
        key        VARCHAR,
        value      VARCHAR,
        updated_at TIMESTAMP
    );
    """,
]

#: Trạng thái được coi là "đang mở" (chưa đóng).
#: Gồm cả các trạng thái của kiến trúc WAP và các trạng thái đời cũ — dữ liệu incident
#: đã ghi từ trước vẫn phải được liệt kê đúng.
OPEN_STATUSES = (
    "DETECTED",
    "INVESTIGATING",
    "WAITING_SHADOW_APPROVAL",
    "STAGING_VERIFYING",
    "READY_FOR_PRODUCTION",
    "AUDIT_FAILED_TRIAGE",
    "WAITING_FOR_APPROVAL",
    "EXECUTING",
    "REJECTED",
    "FAILED",
)

#: Trạng thái kết thúc — không có hành động nào tiếp theo.
CLOSED_STATUSES = ("PUBLISHED_RESOLVED", "CANCELLED", "RESOLVED")

# ---------------------------------------------------------------------------
# State machine WAP: chuyển trạng thái nào là hợp lệ
# ---------------------------------------------------------------------------
#
# Vì sao cần bảng này thay vì cứ UPDATE thẳng: nút Publish nằm trên UI, và UI có thể bị
# mở ở hai tab, bị bấm lại sau khi hết hạn session, hoặc bị gọi thẳng qua REST. Không có
# guard thì một request lạc nhịp có thể đưa sự cố từ DETECTED nhảy thẳng sang
# PUBLISHED_RESOLVED mà chưa ai nghiệm thu gì. Đây là bậc bảo vệ cuối, sau guard quyền
# hạn (grant theo phase) và guard phạm vi ghi (chỉ staging).
ALLOWED_TRANSITIONS: Dict[str, frozenset] = {
    "DETECTED": frozenset({"INVESTIGATING", "CANCELLED", "FAILED"}),
    "INVESTIGATING": frozenset(
        {"WAITING_SHADOW_APPROVAL", "WAITING_FOR_APPROVAL", "AUDIT_FAILED_TRIAGE",
         "CANCELLED", "FAILED"}
    ),
    "WAITING_SHADOW_APPROVAL": frozenset(
        {"STAGING_VERIFYING", "WAITING_SHADOW_APPROVAL", "AUDIT_FAILED_TRIAGE",
         "CANCELLED", "REJECTED", "FAILED"}
    ),
    "STAGING_VERIFYING": frozenset(
        {"READY_FOR_PRODUCTION", "AUDIT_FAILED_TRIAGE", "CANCELLED", "FAILED"}
    ),
    "READY_FOR_PRODUCTION": frozenset(
        {"PUBLISHED_RESOLVED", "AUDIT_FAILED_TRIAGE", "CANCELLED", "FAILED"}
    ),
    "AUDIT_FAILED_TRIAGE": frozenset(
        # Ba lựa chọn cứu hộ: re-plan (về chờ duyệt lại), sửa tay (cũng về chờ duyệt),
        # hoặc huỷ. Không có đường nào đi thẳng sang PUBLISHED.
        {"WAITING_SHADOW_APPROVAL", "STAGING_VERIFYING", "CANCELLED", "FAILED"}
    ),
    # --- luồng cũ, giữ để tương thích ---
    "WAITING_FOR_APPROVAL": frozenset(
        {"EXECUTING", "RESOLVED", "FAILED", "REJECTED", "CANCELLED",
         "WAITING_SHADOW_APPROVAL"}
    ),
    "EXECUTING": frozenset({"RESOLVED", "FAILED"}),
    "REJECTED": frozenset({"INVESTIGATING", "WAITING_SHADOW_APPROVAL", "CANCELLED"}),
    "FAILED": frozenset(
        {"INVESTIGATING", "WAITING_SHADOW_APPROVAL", "AUDIT_FAILED_TRIAGE", "CANCELLED"}
    ),
    # Trạng thái kết thúc: không đi đâu nữa
    "PUBLISHED_RESOLVED": frozenset(),
    "CANCELLED": frozenset(),
    "RESOLVED": frozenset(),
}


def can_transition(current: str, target: str) -> bool:
    """Chuyển từ `current` sang `target` có hợp lệ theo state machine hay không."""
    current = (current or "").strip().upper()
    target = (target or "").strip().upper()
    if current == target:
        return True
    if current not in ALLOWED_TRANSITIONS:
        # Trạng thái lạ (dữ liệu cũ, hoặc ghi tay) -> cho phép, nhưng không cho nhảy
        # thẳng vào trạng thái "đã publish" vì đó là thứ duy nhất không thể lùi lại.
        return target != "PUBLISHED_RESOLVED"
    return target in ALLOWED_TRANSITIONS[current]


def next_actions(status: str) -> List[str]:
    """
    Các hành động engineer được phép bấm ở trạng thái hiện tại.

    UI đọc từ đây thay vì tự suy, để backend và frontend không bao giờ lệch nhau về
    việc nút nào đang hợp lệ.
    """
    status = (status or "").strip().upper()
    return {
        "DETECTED": ["investigate"],
        "INVESTIGATING": [],
        "WAITING_SHADOW_APPROVAL": ["approve_shadow", "cancel_shadow", "manual_override"],
        "STAGING_VERIFYING": [],
        "READY_FOR_PRODUCTION": ["publish_prod", "cancel_shadow"],
        "AUDIT_FAILED_TRIAGE": ["replan", "manual_override", "cancel_shadow"],
        "WAITING_FOR_APPROVAL": ["approve_shadow", "cancel_shadow", "manual_override"],
        "PUBLISHED_RESOLVED": [],
        "CANCELLED": [],
        "RESOLVED": [],
    }.get(status, [])

#: Ngưỡng chấm severity theo tỉ lệ dòng vi phạm (đồng bộ runbook sla_policy.md)
def _severity(failed_rows: int, total_rows: Optional[int], tier: str) -> str:
    if not total_rows:
        return "MEDIUM"
    pct = failed_rows * 100.0 / total_rows
    if tier == "Tier-0" or pct >= 5:
        return "CRITICAL" if pct >= 5 else "HIGH"
    if pct >= 1:
        return "HIGH"
    if pct >= 0.1:
        return "MEDIUM"
    return "LOW"


def _q(value: str) -> str:
    return (value or "").replace("'", "''")


def _now() -> datetime:
    return datetime.now()


# ---------------------------------------------------------------------------
# 1. Tạo / cập nhật incident
# ---------------------------------------------------------------------------


def open_incident(
    job: "PipelineJob",
    run_id: str,
    failure: Dict[str, Any],
    all_failures: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Tạo incident cho MỘT test đang fail (hoặc cộng dồn vào incident đang mở của test đó).

    Mỗi test fail = một incident riêng, nên giám khảo hỏi bất kỳ lỗi nào (doanh thu âm,
    email sai định dạng, khoá trùng…) đều có hồ sơ để mở ra xem.

    Chống ngập: khoá logic là **tên test**, không phải (job, test). Cùng một dbt test fail
    thì dù bị nhiều job cùng bắt (job cổng + job model) vẫn chỉ là một sự cố.

    Trả về `incident_id`. Đồng thời tạo notification để UI báo đỏ.
    """
    worst = failure
    failures = all_failures or [failure]
    test_name = str(worst["test_name"])

    existing = fetch(
        "SELECT incident_id, occurrences FROM incidents "
        f"WHERE test_name = '{_q(test_name)}' "
        f"AND status IN {OPEN_STATUSES} ORDER BY created_at DESC LIMIT 1",
        max_rows=1,
    )["rows"]

    con = get_connection()
    total_rows = scalar(f"SELECT COUNT(*) FROM {job.target_table.split('.')[-1]}")
    severity = _severity(int(worst["failures"]), total_rows, job.tier)

    if existing:
        incident_id = str(existing[0]["incident_id"])
        with CONN_LOCK:
            con.execute(
                "UPDATE incidents SET occurrences = occurrences + 1, updated_at = ?, "
                "failed_rows = ?, run_id = ?, severity = ? WHERE incident_id = ?",
                [_now(), int(worst["failures"]), run_id, severity, incident_id],
            )
        return incident_id

    incident_id = f"INC-{_now():%Y%m%d}-{uuid.uuid4().hex[:5].upper()}"
    title = (
        f"{test_name}: {worst['failures']} dòng vi phạm"
        + (f" ở cột `{worst['column']}`" if worst.get("column") else "")
        + f" · phát hiện bởi {job.name}"
    )
    envelope = build_envelope(
        job, run_id, worst, incident_id, total_rows, all_failures=failures
    )

    with CONN_LOCK:
        # Liệt kê cột tường minh, KHÔNG dùng `INSERT INTO incidents VALUES (...)` theo thứ
        # tự. Bảng này được migrate thêm cột (shadow_table, retry_count…) nên INSERT
        # positional sẽ vỡ ngay lần thêm cột kế tiếp — đúng lỗi đã xảy ra một lần.
        con.execute(
            """
            INSERT INTO incidents (
                incident_id, job_id, run_id, created_at, updated_at, status, severity,
                target_table, test_name, column_name, test_type, failed_rows, occurrences,
                title, envelope_json, report_json, audit_json, error,
                shadow_table, retry_count, ready_for_production, published_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                incident_id, job.job_id, run_id, _now(), _now(), "DETECTED", severity,
                job.target_table, test_name, str(worst.get("column") or ""),
                str(worst.get("test_type") or ""), int(worst["failures"]), 1, title,
                json.dumps(envelope, ensure_ascii=False, default=str), None, None, None,
                "", 0, False, None,
            ],
        )
    push_notification(
        level="critical" if severity in ("HIGH", "CRITICAL") else "warning",
        title=f"🔴 Job lỗi: {job.name}",
        body=title,
        job_id=job.job_id,
        incident_id=incident_id,
    )
    return incident_id


def build_envelope(
    job: "PipelineJob",
    run_id: str,
    failure: Dict[str, Any],
    incident_id: str,
    total_rows: Optional[int] = None,
    all_failures: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Đóng gói Incident Envelope cho agent.

    `failure` là ĐÚNG test của incident này (không phải test nặng nhất của job) — nếu lấy
    sai thì agent sẽ đi điều tra một lỗi khác với lỗi mà engineer bấm vào.
    `all_failures` chỉ để làm ngữ cảnh "các test khác cũng fail cùng lúc".

    Giữ đúng contract của `ai/schemas.py::IncidentInput` nhưng KHÔNG import nó —
    scope data không phụ thuộc scope ai.
    """
    from data.incidents import PIPELINE_LOG_TAIL

    worst = failure
    failures = all_failures or [failure]
    # Bảng cần soi là bảng của CHECK (có thể khác bảng đích của job: job build_mart_*
    # fail vì rule trên fact_orders chứ không phải trên chính cái mart).
    bare = str(worst.get("model") or job.target_table.split(".")[-1])
    total = scalar(f"SELECT COUNT(*) FROM {bare}")
    if total is None:
        total = total_rows

    sample: List[Dict[str, Any]] = []
    if worst.get("column") and worst.get("test_type") == "not_null":
        try:
            sample = fetch(
                f"SELECT * FROM {bare} WHERE {worst['column']} IS NULL LIMIT 3", max_rows=3
            )["rows"]
        except Exception:  # noqa: BLE001 - thiếu sample không được làm hỏng incident
            sample = []

    return {
        "incident_id": incident_id,
        "incident_type": "DATA_QUALITY",
        # Bảng mà agent phải vá chính là bảng của rule bị vi phạm
        "target_table": f"main.{bare}",
        "source": f"pipeline:{job.job_id}",
        "description": (
            f"Job `{job.name}` ({job.job_id}) fail ở cổng DQ trong run {run_id}: "
            f"{len(failures)} test không đạt, nặng nhất là `{worst['test_name']}` với "
            f"{worst['failures']} dòng vi phạm trên tổng {total} dòng của {job.target_table}. "
            f"Job thuộc {job.tier}, owner {job.owner}. {job.description}"
        ),
        "evidence_payload": {
            "job_id": job.job_id,
            "job_name": job.name,
            "job_layer": job.layer,
            "job_owner": job.owner,
            "tier": job.tier,
            "run_id": run_id,
            "failed_test": worst["test_name"],
            "test_type": worst.get("test_type"),
            "model": bare,
            "column": worst.get("column"),
            "failures": int(worst["failures"]),
            "total_rows_scanned": total,
            "failure_rate_pct": (
                round(int(worst["failures"]) * 100.0 / total, 3) if total else 0.0
            ),
            "compiled_sql": worst.get("count_sql"),
            "all_failed_tests": failures,
            "depends_on": job.depends_on,
            "sample_failed_rows": sample,
            "pipeline_log_tail": PIPELINE_LOG_TAIL,
        },
    }


def current_status(incident_id: str) -> Optional[str]:
    """Trạng thái hiện tại của một sự cố (None nếu không có sự cố đó)."""
    rows = fetch(
        f"SELECT status FROM incidents WHERE incident_id = '{_q(incident_id)}' LIMIT 1",
        max_rows=1,
    )["rows"]
    return str(rows[0]["status"]) if rows else None


class InvalidTransition(RuntimeError):
    """Chuyển trạng thái không hợp lệ theo state machine WAP."""


def set_status(
    incident_id: str,
    status: str,
    report_json: Optional[str] = None,
    audit_json: Optional[str] = None,
    error: Optional[str] = None,
    shadow_table: Optional[str] = None,
    retry_count: Optional[int] = None,
    ready_for_production: Optional[bool] = None,
    published_at: Optional[datetime] = None,
    enforce: bool = True,
) -> str:
    """
    Cập nhật trạng thái + (tuỳ chọn) báo cáo điều tra / biên bản nghiệm thu.

    `enforce=True` (mặc định) kiểm state machine trước khi ghi và raise
    `InvalidTransition` nếu bước chuyển không hợp lệ. Đặt `enforce=False` chỉ dành cho
    migration hoặc công cụ sửa dữ liệu — không dùng trong luồng nghiệp vụ.

    Trả về trạng thái trước đó, để caller ghi audit log biết đã đi từ đâu đến đâu.
    """
    previous = current_status(incident_id) or ""
    if enforce and previous and not can_transition(previous, status):
        raise InvalidTransition(
            f"Không thể chuyển sự cố {incident_id} từ '{previous}' sang '{status}'. "
            f"Bước hợp lệ từ '{previous}': {sorted(ALLOWED_TRANSITIONS.get(previous, []))}."
        )

    con = get_connection()
    sets = ["status = ?", "updated_at = ?"]
    params: List[Any] = [status, _now()]
    for column, value in (
        ("report_json", report_json),
        ("audit_json", audit_json),
        ("error", error),
        ("shadow_table", shadow_table),
        ("retry_count", retry_count),
        ("ready_for_production", ready_for_production),
        ("published_at", published_at),
    ):
        if value is not None:
            sets.append(f"{column} = ?")
            params.append(value)
    params.append(incident_id)
    with CONN_LOCK:
        con.execute(f"UPDATE incidents SET {', '.join(sets)} WHERE incident_id = ?", params)
    return previous


# ---------------------------------------------------------------------------
# 2. Đọc incident
# ---------------------------------------------------------------------------

_LIST_COLUMNS = (
    "incident_id, job_id, run_id, created_at, updated_at, status, severity, "
    "target_table, test_name, column_name, test_type, failed_rows, occurrences, title, error, "
    "shadow_table, retry_count, ready_for_production, published_at"
)


def list_incidents(status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Danh sách incident (không kèm JSON nặng) cho bảng trên UI."""
    limit = max(1, min(int(limit), 500))
    where = f"WHERE status = '{_q(status)}'" if status else ""
    return fetch(
        f"SELECT {_LIST_COLUMNS} FROM incidents {where} "
        f"ORDER BY created_at DESC LIMIT {limit}",
        max_rows=limit,
    )["rows"]


def get_incident(incident_id: str) -> Optional[Dict[str, Any]]:
    """Chi tiết một incident, đã parse sẵn envelope/report/audit JSON."""
    rows = fetch(
        f"SELECT {_LIST_COLUMNS}, envelope_json, report_json, audit_json FROM incidents "
        f"WHERE incident_id = '{_q(incident_id)}' LIMIT 1",
        max_rows=1,
    )["rows"]
    if not rows:
        return None
    row = dict(rows[0])
    for key in ("envelope_json", "report_json", "audit_json"):
        raw = row.pop(key)
        name = key.replace("_json", "")
        try:
            row[name] = json.loads(raw) if raw else None
        except (TypeError, ValueError):
            row[name] = None
    return row


def incidents_for_job(job_id: str) -> List[Dict[str, Any]]:
    """
    Toàn bộ incident đang mở **liên quan tới một job**, nặng nhất trước.

    Không chỉ tra theo `job_id`: incident được dedup theo tên test trên toàn hệ (một
    test fail = một hồ sơ, tránh ngập khi nhiều job cùng bắt được), nên hồ sơ có thể
    mang `job_id` của job bắt được trước — ví dụ `dbt_test_all` chạy trước và giữ hết
    incident, khiến `build_stg_orders` báo đỏ mà click vào lại không có gì. Vì vậy phần
    fallback tra theo đúng danh sách DQ check là cổng chất lượng của job đó.
    """
    rows = fetch(
        f"SELECT incident_id FROM incidents WHERE job_id = '{_q(job_id)}' "
        f"AND status IN {OPEN_STATUSES} ORDER BY failed_rows DESC, created_at DESC LIMIT 20",
        max_rows=20,
    )["rows"]

    if not rows:
        from data.pipeline import JOBS_BY_ID  # import muộn để tránh vòng import

        job = JOBS_BY_ID.get(job_id)
        checks = list(getattr(job, "dq_checks", []) or [])
        if checks:
            names = ", ".join(f"'{_q(name)}'" for name in checks)
            rows = fetch(
                f"SELECT incident_id FROM incidents WHERE test_name IN ({names}) "
                f"AND status IN {OPEN_STATUSES} "
                "ORDER BY failed_rows DESC, created_at DESC LIMIT 20",
                max_rows=20,
            )["rows"]

    out: List[Dict[str, Any]] = []
    for row in rows:
        incident = get_incident(str(row["incident_id"]))
        if incident:
            out.append(incident)
    return out


def incident_for_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Incident nặng nhất đang mở của một job (UI dùng khi click vào job đỏ)."""
    found = incidents_for_job(job_id)
    return found[0] if found else None


def next_incident_to_investigate() -> Optional[Dict[str, Any]]:
    """Incident tiếp theo cần điều tra (worker nền lấy từ đây)."""
    rows = fetch(
        f"SELECT {_LIST_COLUMNS}, envelope_json FROM incidents WHERE status = 'DETECTED' "
        "ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1 "
        "WHEN 'MEDIUM' THEN 2 ELSE 3 END, created_at LIMIT 1",
        max_rows=1,
    )["rows"]
    if not rows:
        return None
    row = dict(rows[0])
    raw = row.pop("envelope_json")
    try:
        row["envelope"] = json.loads(raw) if raw else None
    except (TypeError, ValueError):
        row["envelope"] = None
    return row


def counts_by_status() -> Dict[str, int]:
    rows = fetch("SELECT status, COUNT(*) AS n FROM incidents GROUP BY status", max_rows=50)["rows"]
    return {str(r["status"]): int(r["n"]) for r in rows}


# ---------------------------------------------------------------------------
# 3. Thông báo
# ---------------------------------------------------------------------------


def push_notification(
    level: str,
    title: str,
    body: str = "",
    job_id: str = "",
    incident_id: str = "",
) -> str:
    """Đẩy một thông báo cho UI (badge đỏ ở mục thông báo)."""
    notification_id = f"NTF-{uuid.uuid4().hex[:10]}"
    con = get_connection()
    with CONN_LOCK:
        con.execute(
            "INSERT INTO notifications (notification_id, created_at, level, title, body, "
            "job_id, incident_id, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [notification_id, _now(), level, title[:300], body[:1000], job_id, incident_id, None],
        )
    return notification_id


def list_notifications(unread_only: bool = False, limit: int = 30) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    where = "WHERE read_at IS NULL" if unread_only else ""
    return fetch(
        "SELECT notification_id, created_at, level, title, body, job_id, incident_id, read_at "
        f"FROM notifications {where} ORDER BY created_at DESC LIMIT {limit}",
        max_rows=limit,
    )["rows"]


def unread_count() -> int:
    return int(scalar("SELECT COUNT(*) FROM notifications WHERE read_at IS NULL", default=0) or 0)


def mark_read(notification_id: Optional[str] = None) -> int:
    """Đánh dấu đã đọc. Không truyền id thì đánh dấu tất cả."""
    con = get_connection()
    with CONN_LOCK:
        if notification_id:
            con.execute(
                "UPDATE notifications SET read_at = ? WHERE notification_id = ? AND read_at IS NULL",
                [_now(), notification_id],
            )
        else:
            con.execute("UPDATE notifications SET read_at = ? WHERE read_at IS NULL", [_now()])
    return unread_count()


# ---------------------------------------------------------------------------
# 4. Incident đang chọn để mở phiên HITL trên Chainlit
# ---------------------------------------------------------------------------

_SELECTED_KEY = "selected_incident_id"


def select_incident(incident_id: str) -> None:
    """
    Ghi nhận incident mà engineer vừa bấm "Mở phiên xử lý" trên dashboard.
    Phiên Chainlit kế tiếp sẽ nạp đúng incident này.
    """
    con = get_connection()
    with CONN_LOCK:
        con.execute("DELETE FROM ui_state WHERE key = ?", [_SELECTED_KEY])
        con.execute("INSERT INTO ui_state VALUES (?, ?, ?)", [_SELECTED_KEY, incident_id, _now()])


def selected_incident_id() -> Optional[str]:
    value = scalar(f"SELECT value FROM ui_state WHERE key = '{_SELECTED_KEY}' LIMIT 1")
    return str(value) if value else None


def clear_selection() -> None:
    con = get_connection()
    with CONN_LOCK:
        con.execute("DELETE FROM ui_state WHERE key = ?", [_SELECTED_KEY])


__all__ = [
    "INCIDENT_DDL",
    "CLOSED_STATUSES",
    "ALLOWED_TRANSITIONS",
    "can_transition",
    "next_actions",
    "current_status",
    "InvalidTransition",
    "OPEN_STATUSES",
    "open_incident",
    "build_envelope",
    "set_status",
    "list_incidents",
    "get_incident",
    "incident_for_job",
    "incidents_for_job",
    "next_incident_to_investigate",
    "counts_by_status",
    "push_notification",
    "list_notifications",
    "unread_count",
    "mark_read",
    "select_incident",
    "selected_incident_id",
    "clear_selection",
]
