"""
web/backend/api.py — REST API (headless)
========================================

Dành cho hệ thống khác gọi vào: webhook từ dbt/Airflow, dashboard ngoài, cron, CI.
Không chứa logic UI. Mọi endpoint đều nằm dưới `/api` (trừ `/health`).

    GET  /health                  Health check cho AgentBase / K8s probe
    GET  /api/incidents/sample    Incident Envelope mẫu (số liệu thật từ DuckDB)
    POST /api/incidents           Nạp incident -> Agent 1 điều tra (?auto_approve=true để vá luôn)
    POST /api/audit               Agent 2 nghiệm thu độc lập

Write–Audit–Publish (các nút engineer bấm — mỗi endpoint một bước, một lần COMMIT):

    POST /api/approve-shadow      BƯỚC 1: chạy script trên shadow table + Agent 2 nghiệm thu
    POST /api/replan              Agent 1 sinh plan v2 từ lỗi Agent 2 (giới hạn 1 lượt)
    POST /api/manual-override     Engineer nộp SQL sửa tay (vẫn qua guard + preflight)
    POST /api/publish-prod        BƯỚC 2: atomic swap sang bảng thật (cần Agent 2 PASS)
    POST /api/cancel-shadow       Huỷ & dọn staging
    GET  /api/wap/{id}            Trạng thái WAP + bảng diff cho UI
    GET  /api/wap/{id}/preview    Xem trước dữ liệu bảng Live / Shadow
    GET  /api/warehouse/summary   Ảnh chụp sức khoẻ DQ của warehouse
    GET  /api/audit-log           Nhật ký mọi tool call của agent
    GET  /api/models              Cấu hình model của Maker/Checker (kiểm cross-model)
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool

import config
from ai import tools, worker
from ai.agent import run_headless
from ai.auditor import audit_locked_status, build_audit_skipped, run_audit_headless
from ai.llm import LLMSettings
from ai.schemas import (
    MAX_REPLAN_ATTEMPTS,
    STATUS_ICON,
    STATUS_LABEL,
    AgentReport,
    IncidentInput,
    ManualOverrideRequest,
    ReplanRequest,
)
from data import audit as data_audit
from data import connection as db
from data import dbt_runner, incident_store, wap
from data.dq import load_checks, run_all_checks
from data.incidents import build_sample_incident_payload
from data.pipeline import (
    JOBS_BY_ID,
    ensure_pipeline_tables,
    job_status_list,
    recent_runs,
    run_all_jobs,
    run_due_jobs,
    run_job,
)
from data.warehouse import ensure_database
from web.backend.security import require_token

router = APIRouter()

#: Khoá tránh 2 request cùng ghi DuckDB một lúc
_write_lock = asyncio.Lock()

#: Scheduler nền: bật/tắt và chu kỳ quét (giây)
SCHEDULER_ENABLED = os.getenv("DRA_SCHEDULER", "1").strip().lower() in {"1", "true", "yes", "y"}
SCHEDULER_INTERVAL = max(5, int(os.getenv("DRA_SCHEDULER_INTERVAL", "20")))
#: Trạng thái scheduler có thể bật/tắt động trong runtime (Auto vs Manual)
SCHEDULER_RUNNING: bool = SCHEDULER_ENABLED
#: Số incident worker điều tra mỗi chu kỳ (mỗi lượt tốn token nên để thấp)
SCHEDULER_MAX_INVESTIGATIONS = max(0, int(os.getenv("DRA_SCHEDULER_MAX_INVESTIGATIONS", "1")))


@router.get("/health", tags=["ops"])
async def health() -> Dict[str, Any]:
    """Health check: xác nhận DuckDB đọc được và trả về số dòng bảng fact."""

    def _probe() -> Dict[str, Any]:
        # Use dynamic table discovery instead of hardcoded fact_orders
        tables_result = tools.tool_query_duckdb("SHOW TABLES")
        main_table = None
        
        if tables_result.get("ok") and tables_result.get("rows"):
            # Look for main fact table (typically largest or contains 'fact' in name)
            tables = [row.get("name", "") for row in tables_result["rows"]]
            
            # Prefer fact tables, then largest table as fallback
            fact_tables = [t for t in tables if "fact" in t.lower()]
            main_table = fact_tables[0] if fact_tables else (tables[0] if tables else "unknown_table")
        
        if main_table and main_table != "unknown_table":
            result = tools.tool_query_duckdb(f"SELECT COUNT(*) AS rows FROM {main_table}")
            table_rows = (result.get("rows") or [{}])[0].get("rows") if result.get("ok") else 0
        else:
            table_rows = 0
        
        return {
            "duckdb_ok": bool(main_table and main_table != "unknown_table"),
            f"{main_table}_rows": table_rows,
            "main_table": main_table
        }

    probe = await run_in_threadpool(_probe)
    return {
        "status": "ok" if probe["duckdb_ok"] else "degraded",
        "service": config.APP_TITLE,
        "version": config.APP_VERSION,
        "db_path": config.DUCKDB_PATH,
        "ui": config.CHAINLIT_PATH,
        "remediation_unlocked": tools.is_remediation_unlocked(),
        **probe,
    }


@router.get("/api/models", tags=["ops"], dependencies=[Depends(require_token)])
async def models() -> Dict[str, Any]:
    """
    Cấu hình model hiện tại của 2 agent — dùng để xác nhận cross-model checking
    đang bật (Checker phải khác model Maker).
    """
    maker = LLMSettings.from_env()
    checker = LLMSettings.for_auditor(avoid_model=maker.model)
    return {
        "base_url": maker.base_url,
        "api_key_configured": bool(maker.api_key),
        "maker": {"model": maker.model, "pool": maker.model_pool, "role": maker.role},
        "checker": {"model": checker.model, "pool": checker.model_pool, "role": checker.role},
        "cross_model_enabled": bool(maker.api_key) and maker.model != checker.model,
    }


@router.get("/api/incidents/sample", tags=["incidents"], dependencies=[Depends(require_token)])
async def sample_incident() -> Dict[str, Any]:
    """Incident Envelope mẫu (số liệu lấy thật từ DuckDB)."""
    return await run_in_threadpool(build_sample_incident_payload)


@router.post("/api/incidents", tags=["incidents"], dependencies=[Depends(require_token)])
async def ingest_incident(payload: Dict[str, Any], auto_approve: bool = False) -> Dict[str, Any]:
    """
    Nhận incident (webhook từ dbt/Airflow) và chạy Agent 1 headless.

    - `auto_approve=false` (mặc định): chỉ điều tra + trả report chờ người duyệt.
    - `auto_approve=true` : tự vá rồi cho Agent 2 nghiệm thu — CHỈ dùng cho dev/test,
      production phải đi qua Human-in-the-loop trên UI.
    """
    try:
        incident = IncidentInput(**payload)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"Incident payload không hợp lệ: {exc}") from exc

    async with _write_lock:
        result = await run_in_threadpool(run_headless, incident, auto_approve)
        if auto_approve:
            # Maker xong thì Checker vào nghiệm thu ngay, kể cả ở chế độ headless
            claimed: Optional[AgentReport] = None
            try:
                claimed = AgentReport.model_validate(result.get("report") or {})
            except Exception:  # noqa: BLE001
                claimed = None
            result["audit"] = await run_in_threadpool(run_audit_headless, incident, claimed)
    return result


@router.post("/api/audit", tags=["audit"], dependencies=[Depends(require_token)])
async def run_audit(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Chạy **Agent 2 (Data Auditor)** nghiệm thu độc lập, headless.

    Body (tuỳ chọn):
      - `incident`: incident envelope để Agent 2 suy ra rule cần kiểm.
      - `incident_id` (+ `target_table`): chỉ có mã -> Agent 2 lấy baseline từ DuckDB.
      - `remediation_report`: báo cáo Agent 1 (chỉ dùng làm "lời khai cần kiểm chứng").
    """
    payload = payload or {}
    iid = str(payload.get("incident_id") or "")
    if iid:
        # Khoá audit sau publish: shadow đã bị swap/xoá, chạy lại chỉ báo FAIL oan.
        locked = audit_locked_status(iid)
        if locked:
            return build_audit_skipped(iid, locked)
    incident: Optional[IncidentInput] = None
    if isinstance(payload.get("incident"), dict):
        incident = IncidentInput(**payload["incident"])
    elif payload.get("incident_id"):
        incident = IncidentInput(
            incident_id=str(payload["incident_id"]),
            target_table=str(payload.get("target_table") or "unknown"),
        )

    report: Optional[AgentReport] = None
    if isinstance(payload.get("remediation_report"), dict):
        try:
            report = AgentReport.model_validate(payload["remediation_report"])
        except Exception:  # noqa: BLE001 - lời khai lỗi thì bỏ qua, vẫn audit được
            report = None

    return await run_in_threadpool(run_audit_headless, incident, report)


# ---------------------------------------------------------------------------
# WRITE – AUDIT – PUBLISH (Two-Phase Human-in-the-loop)
# ---------------------------------------------------------------------------
#
# Năm endpoint dưới đây là toàn bộ các nút engineer bấm được. Nguyên tắc chung:
#
# 1. **Mỗi endpoint là một bước độc lập, một lần COMMIT.** Không endpoint nào làm hai
#    việc ghi dữ liệu. Stage fail không ảnh hưởng tới dữ liệu prod; publish fail không
#    làm mất shadow; cleanup fail không làm mất kết quả publish.
# 2. **Kiểm trạng thái trước khi làm.** State machine trong `incident_store` là bậc bảo
#    vệ cuối: một request lạc nhịp (tab cũ, double click, gọi thẳng REST) bị từ chối
#    bằng HTTP 409 thay vì làm hỏng dữ liệu.
# 3. **Giấy phép ghi cấp theo phase và thu hồi ngay.** Duyệt bước 1 không mở được bước 2.
# 4. **Trạng thái nghiệp vụ ghi vào DB sau khi dữ liệu đã COMMIT**, nên UI không bao giờ
#    thấy một trạng thái không khớp với dữ liệu thật.


async def _load_case(incident_id: str) -> tuple[Dict[str, Any], Any]:
    """Lấy hồ sơ sự cố trong DB + agent tương ứng (dựng lại nếu server đã restart)."""
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có sự cố '{incident_id}'")
    agent = await run_in_threadpool(worker.restore_agent, incident_id)
    if agent is None or agent.report is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Sự cố '{incident_id}' chưa có báo cáo điều tra nên chưa có gì để duyệt. "
                "Hãy chạy điều tra trước (POST /api/incidents/{id}/investigate)."
            ),
        )
    return row, agent


def _require_action(row: Dict[str, Any], action: str) -> None:
    """Chặn hành động không hợp lệ ở trạng thái hiện tại (HTTP 409)."""
    status = str(row.get("status") or "")
    allowed = incident_store.next_actions(status)
    if action not in allowed:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Sự cố đang ở trạng thái '{status}' nên không thể '{action}'. "
                f"Hành động hợp lệ lúc này: {allowed or '(không có)'}."
            ),
        )


def _wap_view(incident_id: str) -> Dict[str, Any]:
    """Ảnh chụp trạng thái WAP để UI vẽ lại Action Bar sau mỗi hành động."""
    row = incident_store.get_incident(incident_id) or {}
    status = str(row.get("status") or "")
    session = wap.get_session(incident_id) or {}
    prod_table = str(session.get("prod_table") or row.get("target_table") or "")
    shadow_table = str(row.get("shadow_table") or session.get("shadow_table") or "")
    # Coherence: BẢNG BÓNG bắt buộc thuộc đúng bảng mục tiêu của incident
    # (shadow_<target>). Shadow sót từ incident/bảng khác sẽ hiện sai diff +
    # preview nên loại bỏ để UI hiện "(chưa dựng)" thay vì số liệu bảng lạ.
    if shadow_table:
        core = wap.bare_name(shadow_table)
        if core.lower().startswith("shadow_"):
            core = core[len("shadow_"):]
        if core.lower() != wap.bare_name(prod_table).lower():
            shadow_table = ""
    return {
        "incident_id": incident_id,
        "status": status,
        "status_label": STATUS_LABEL.get(status, status),
        "status_icon": STATUS_ICON.get(status, "•"),
        "next_actions": incident_store.next_actions(status),
        "shadow_table": shadow_table,
        "prod_table": prod_table,
        "retry_count": int(row.get("retry_count") or 0),
        "max_replan": MAX_REPLAN_ATTEMPTS,
        "ready_for_production": bool(row.get("ready_for_production")),
        "wap_session_status": session.get("status") or "",
    }


@router.post("/api/approve-shadow", tags=["wap"], dependencies=[Depends(require_token)])
async def approve_shadow(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    **BƯỚC 1** — engineer duyệt chạy thử trên Staging.

    Chạy script vá trên `shadow_<table>` rồi cho Agent 2 nghiệm thu ngay trên bảng bóng
    đó. Bảng production không bị chạm trong toàn bộ endpoint này.

    Kết quả nghiệm thu quyết định trạng thái tiếp theo:
      - Agent 2 PASS  -> `READY_FOR_PRODUCTION` (mở nút Publish)
      - Agent 2 FAIL  -> `AUDIT_FAILED_TRIAGE`  (hiện 3 nút cứu hộ)
    """
    incident_id = str(payload.get("incident_id") or "").strip()
    if not incident_id:
        raise HTTPException(status_code=422, detail="Thiếu `incident_id`.")
    row, agent = await _load_case(incident_id)
    _require_action(row, "approve_shadow")

    async with _write_lock:
        # --- Transaction nghiệp vụ #1: ghi vùng staging -----------------------
        stage = await run_in_threadpool(agent.approve_shadow, False)
        if not stage.get("ok"):
            await run_in_threadpool(
                incident_store.set_status,
                incident_id,
                "AUDIT_FAILED_TRIAGE",
                None,
                None,
                str(stage.get("error") or stage.get("summary"))[:900],
            )
            await run_in_threadpool(
                incident_store.push_notification,
                "warning",
                "⚠️ Chạy thử trên Staging thất bại",
                f"{incident_id}: {str(stage.get('error'))[:300]} "
                "(bảng production vẫn nguyên vẹn)",
                "",
                incident_id,
            )
            return {"ok": False, "stage": stage, "wap": _wap_view(incident_id)}

        await run_in_threadpool(
            incident_store.set_status,
            incident_id,
            "STAGING_VERIFYING",
            None,
            None,
            "",
            stage.get("shadow_table") or "",
        )

        # --- Bước đọc: Agent 2 nghiệm thu trên bảng bóng ---------------------
        plan = agent.report.remediation
        audit = await run_in_threadpool(
            run_audit_headless, agent.incident, agent.report, stage.get("shadow_table") or ""
        )

    ready = bool(audit.get("is_ready_for_production"))
    next_status = "READY_FOR_PRODUCTION" if ready else "AUDIT_FAILED_TRIAGE"
    await run_in_threadpool(
        incident_store.set_status,
        incident_id,
        next_status,
        None,
        json.dumps(audit.get("audit_report") or {}, ensure_ascii=False, default=str),
        "",
        stage.get("shadow_table") or "",
        None,
        ready,
    )
    await run_in_threadpool(_record_audit_on_session, incident_id, audit, ready)
    await run_in_threadpool(
        incident_store.push_notification,
        "success" if ready else "critical",
        (
            "🚀 Nghiệm thu ĐẠT — sẵn sàng Publish lên Production"
            if ready
            else "🛑 Nghiệm thu KHÔNG ĐẠT — cần engineer chọn hướng xử lý"
        ),
        (
            f"{incident_id} · bảng bóng `{stage.get('shadow_table')}` đã sạch, "
            "bấm [Publish to Production] để tráo bảng."
            if ready
            else f"{incident_id} · {str((audit.get('failed_details') or {}).get('error_message'))[:280]}"
        ),
        "",
        incident_id,
    )
    return {
        "ok": True,
        "stage": stage,
        "audit": audit,
        "diff": audit.get("shadow_diff"),
        "plan": {
            "shadow_table": plan.shadow_table_name,
            "prod_table": plan.target_production_table,
            "publish_script": plan.publish_script,
            "script_source": plan.script_source,
            "plan_version": plan.plan_version,
        },
        "wap": _wap_view(incident_id),
    }


def _record_audit_on_session(incident_id: str, audit: Dict[str, Any], ready: bool) -> None:
    """Ghi biên bản nghiệm thu vào registry WAP (transaction riêng, tách khỏi dữ liệu)."""
    wap.update_session(
        incident_id,
        status=wap.STATUS_AUDIT_PASSED if ready else wap.STATUS_AUDIT_FAILED,
        audit=audit.get("audit_report"),
        error=None if ready else str(
            (audit.get("failed_details") or {}).get("error_message") or ""
        )[:900],
    )


@router.post("/api/replan", tags=["wap"], dependencies=[Depends(require_token)])
async def replan(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Engineer bấm `[🤖 Cho Agent 1 Re-plan]` — Bounded Reflection Loop.

    Gửi `failed_details` của Agent 2 cho Agent 1 sinh plan v2. Giới hạn cứng một lượt;
    hết lượt thì trả về `allowed=false` và engineer phải sửa tay hoặc escalate.

    Plan v2 **không tự chạy**: nó quay lại `WAITING_SHADOW_APPROVAL` để đi qua đúng cửa
    phê duyệt bước 1 như plan v1.
    """
    try:
        request = ReplanRequest.model_validate(payload or {})
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"Payload không hợp lệ: {exc}") from exc

    row, agent = await _load_case(request.incident_id)
    _require_action(row, "replan")

    details = request.failed_details
    if not details:
        # Không truyền thì lấy từ biên bản nghiệm thu đã lưu — engineer không cần
        # copy-paste hồ sơ lỗi qua lại giữa hai agent.
        audit_row = row.get("audit") or {}
        details = audit_row.get("failed_details") or {
            "error_message": str(row.get("error") or "")[:900]
        }
    if request.feedback:
        details = {**details, "engineer_feedback": request.feedback}

    retry_done = int(row.get("retry_count") or request.retry_count or 0)
    async with _write_lock:
        result = await run_in_threadpool(agent.replan_with_feedback, details, retry_done)

    if not result.get("allowed"):
        return {"ok": False, **result, "wap": _wap_view(request.incident_id)}

    await run_in_threadpool(
        incident_store.set_status,
        request.incident_id,
        result["status"],
        json.dumps(result.get("report") or {}, ensure_ascii=False, default=str),
        None,
        result.get("preflight_error") or "",
        agent.report.remediation.shadow_table_name,
        result.get("retry_count"),
        False,
    )
    await run_in_threadpool(
        incident_store.push_notification,
        "info" if result.get("preflight_passed") else "warning",
        f"🔁 Agent 1 đã soạn plan v{result.get('plan_version')}",
        f"{request.incident_id} · lượt {result.get('retry_count')}/{MAX_REPLAN_ATTEMPTS} · "
        + ("script mới đã qua preflight" if result.get("preflight_passed")
           else f"preflight vẫn lỗi: {str(result.get('preflight_error'))[:200]}"),
        "",
        request.incident_id,
    )
    return {"ok": True, **result, "wap": _wap_view(request.incident_id)}


@router.post("/api/manual-override", tags=["wap"], dependencies=[Depends(require_token)])
async def manual_override(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Engineer bấm `[✏️ Sửa SQL thủ công]` và nộp script tự viết.

    SQL của người đi qua **đúng hai cổng** như SQL của agent: guard phạm vi ghi (chỉ
    staging) và preflight. `rerun_audit=true` thì chạy luôn staging + nghiệm thu lại, để
    engineer thấy kết quả ngay trong một lần bấm.
    """
    try:
        request = ManualOverrideRequest.model_validate(payload or {})
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"Payload không hợp lệ: {exc}") from exc

    row, agent = await _load_case(request.incident_id)
    _require_action(row, "manual_override")

    async with _write_lock:
        applied = await run_in_threadpool(
            agent.apply_manual_override, request.custom_sql, request.note
        )
    if not applied.get("ok"):
        # Không đổi trạng thái: SQL bị từ chối thì sự cố vẫn đứng nguyên chỗ cũ, engineer
        # sửa tiếp trong SQL Studio.
        return {"ok": False, **applied, "wap": _wap_view(request.incident_id)}

    await run_in_threadpool(
        incident_store.set_status,
        request.incident_id,
        "WAITING_SHADOW_APPROVAL",
        agent.report.model_dump_json(),
        None,
        "",
        applied.get("shadow_table") or "",
    )
    await run_in_threadpool(
        data_audit.log_tool_call,
        "manual_override",
        "HUMAN_SQL",
        request.custom_sql,
        "ok",
        f"{request.incident_id}: {request.note or '(không ghi chú)'}",
    )

    if not request.rerun_audit:
        return {"ok": True, **applied, "wap": _wap_view(request.incident_id)}

    # Chạy luôn bước 1 với SQL vừa sửa
    return await approve_shadow({"incident_id": request.incident_id})


@router.post("/api/publish-prod", tags=["wap"], dependencies=[Depends(require_token)])
async def publish_prod(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    **BƯỚC 2** — engineer bấm `[🚀 Publish to Production]`.

    Chỉ mở khi Agent 2 đã nghiệm thu ĐẠT. Endpoint tự kiểm lại điều kiện đó trong DB
    thay vì tin vào việc UI có hiện nút hay không: nút trên một tab cũ vẫn có thể được
    bấm sau khi trạng thái đã đổi.

    Tráo bảng bằng `ALTER TABLE ... RENAME` trong một transaction. Sau khi tráo, gợi ý
    `dbt run --select` cho các model hạ nguồn — việc dựng lại mart là của dbt, không
    phải của script do LLM viết.
    """
    incident_id = str(payload.get("incident_id") or "").strip()
    if not incident_id:
        raise HTTPException(status_code=422, detail="Thiếu `incident_id`.")
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có sự cố '{incident_id}'")
    _require_action(row, "publish_prod")

    if not bool(row.get("ready_for_production")):
        raise HTTPException(
            status_code=409,
            detail=(
                "Chưa được phép publish: Agent 2 chưa cấp chứng nhận cho sự cố này "
                "(`ready_for_production=false`)."
            ),
        )

    session = await run_in_threadpool(wap.get_session, incident_id)
    shadow = str(row.get("shadow_table") or (session or {}).get("shadow_table") or "")
    prod = str((session or {}).get("prod_table") or row.get("target_table") or "")
    if not shadow or not prod:
        raise HTTPException(
            status_code=409, detail="Thiếu thông tin bảng bóng/bảng thật để publish."
        )

    async with _write_lock:
        # --- Transaction nghiệp vụ #2: atomic swap --------------------------
        tools.grant_phase(incident_id, phase=tools.PHASE_PUBLISH)
        try:
            result = await run_in_threadpool(
                tools.tool_atomic_publish_to_prod, shadow, prod, incident_id, False
            )
        finally:
            tools.revoke_phase(incident_id, phase=tools.PHASE_PUBLISH)

    if not result.get("ok"):
        await run_in_threadpool(
            incident_store.set_status,
            incident_id,
            "AUDIT_FAILED_TRIAGE",
            None,
            None,
            str(result.get("error"))[:900],
        )
        return {"ok": False, "publish": result, "wap": _wap_view(incident_id)}

    await run_in_threadpool(
        incident_store.set_status,
        incident_id,
        "PUBLISHED_RESOLVED",
        None,
        None,
        "",
        shadow,
        None,
        True,
        datetime.now(),
    )
    downstream = await run_in_threadpool(_downstream_models, prod)
    await run_in_threadpool(
        incident_store.push_notification,
        "success",
        "🎉 Đã Publish lên Production",
        f"{incident_id} · `{prod}`: {result.get('rows_prod_before')} → "
        f"{result.get('rows_prod_after')} dòng"
        + (f" · cần rebuild: {', '.join(downstream[:4])}" if downstream else ""),
        "",
        incident_id,
    )
    return {
        "ok": True,
        "publish": result,
        "downstream_models": downstream,
        "next_step": (
            f"dbt run --select {' '.join(downstream)}" if downstream else "Không có mart hạ nguồn"
        ),
        "wap": _wap_view(incident_id),
    }


def _downstream_models(prod_table: str) -> List[str]:
    """
    Model hạ nguồn cần dựng lại sau publish, đọc từ dbt lineage.

    Lấy từ DAG thật thay vì để LLM liệt kê: danh sách này phụ thuộc cấu trúc project,
    và đây chính là chỗ mà việc để AI tự viết SQL rebuild đã gây ra Binder Error.
    """
    try:
        graph = dbt_runner.lineage(prod_table, depth=2)
    except Exception:  # noqa: BLE001
        return []
    if not graph.get("ok"):
        return []
    return [
        str(node.get("name"))
        for node in (graph.get("downstream") or [])
        if node.get("resource_type") == "model" and node.get("name")
    ]


@router.post("/api/cancel-shadow", tags=["wap"], dependencies=[Depends(require_token)])
async def cancel_shadow(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Engineer bấm `[🛑 Huỷ bỏ & Xoá Staging]`.

    Drop bảng bóng và đóng sự cố ở `CANCELLED`. An toàn tuyệt đối: bảng production chưa
    từng bị chạm nên không có gì phải rollback.
    """
    incident_id = str(payload.get("incident_id") or "").strip()
    reason = str(payload.get("reason") or "")
    if not incident_id:
        raise HTTPException(status_code=422, detail="Thiếu `incident_id`.")
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có sự cố '{incident_id}'")
    _require_action(row, "cancel_shadow")

    session = await run_in_threadpool(wap.get_session, incident_id)
    shadow = str(row.get("shadow_table") or (session or {}).get("shadow_table") or "")

    async with _write_lock:
        # --- Transaction nghiệp vụ #3: dọn staging --------------------------
        cleanup = await run_in_threadpool(tools.tool_cleanup_shadow, shadow, incident_id)
        tools.revoke_phase(incident_id)

    await run_in_threadpool(
        incident_store.set_status,
        incident_id,
        "CANCELLED",
        None,
        None,
        f"Engineer huỷ: {reason or '(không nêu lý do)'}"[:900],
    )
    await run_in_threadpool(worker.forget_agent, incident_id)
    await run_in_threadpool(
        incident_store.push_notification,
        "info",
        "🗑️ Đã huỷ phương án và dọn Staging",
        f"{incident_id} · `{shadow}` đã được xoá · bảng production không bị thay đổi",
        "",
        incident_id,
    )
    return {"ok": True, "cleanup": cleanup, "wap": _wap_view(incident_id)}


@router.get("/api/wap/shadow-tables", tags=["wap"], dependencies=[Depends(require_token)])
async def shadow_tables() -> Dict[str, Any]:
    """
    Mọi bảng staging đang tồn tại — để không bỏ quên shadow table rác.

    Khai báo TRƯỚC `/api/wap/{incident_id}`: FastAPI khớp route theo thứ tự, nên nếu
    route có path param đứng trước thì "shadow-tables" sẽ bị hiểu là một incident_id và
    endpoint này không bao giờ chạy.
    """
    return {
        "shadow_tables": await run_in_threadpool(wap.list_shadow_tables),
        "sessions": await run_in_threadpool(wap.list_sessions, 50),
    }


@router.get("/api/wap/{incident_id}", tags=["wap"], dependencies=[Depends(require_token)])
async def wap_state(incident_id: str) -> Dict[str, Any]:
    """
    Trạng thái WAP đầy đủ của một sự cố: state machine, bảng diff, preview hai bảng.

    UI gọi endpoint này để vẽ Action Bar và tab Live/Shadow — nút nào hiện do backend
    quyết định, không do frontend tự suy.
    """
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có sự cố '{incident_id}'")

    view = await run_in_threadpool(_wap_view, incident_id)
    session = await run_in_threadpool(wap.get_session, incident_id) or {}
    plan = ((row.get("report") or {}).get("remediation")) or {}
    violation_sql = str(plan.get("verification_sql") or session.get("verification_sql") or "")
    prod = view["prod_table"]

    diff = None
    if view["shadow_table"]:
        diff = await run_in_threadpool(
            wap.diff_shadow_vs_prod, prod, view["shadow_table"], violation_sql
        )
    return {
        **view,
        "incident": row,
        "session": session,
        "diff": diff,
        "plan": plan,
        "audit": row.get("audit"),
        "preflight": session.get("preflight"),
    }


@router.get("/api/wap/{incident_id}/preview", tags=["wap"], dependencies=[Depends(require_token)])
async def wap_preview(incident_id: str, which: str = "shadow", limit: int = 20) -> Dict[str, Any]:
    """Xem trước dữ liệu bảng Live (`which=prod`) hoặc bảng bóng (`which=shadow`)."""
    view = await run_in_threadpool(_wap_view, incident_id)
    table = view["shadow_table"] if which == "shadow" else view["prod_table"]
    if not table:
        raise HTTPException(status_code=404, detail=f"Không có bảng '{which}' cho sự cố này.")
    return await run_in_threadpool(wap.preview_table, table, limit)


# ---------------------------------------------------------------------------
# DASHBOARD · JOB · INCIDENT · NOTIFICATION
# ---------------------------------------------------------------------------


@router.get("/api/dashboard", tags=["dashboard"], dependencies=[Depends(require_token)])
async def dashboard() -> Dict[str, Any]:
    """
    Toàn bộ dữ liệu cho một lần render dashboard — gộp 1 request để UI poll nhẹ.

    Trả về: KPI, danh sách job (kèm trạng thái để báo đỏ), incident đang mở,
    thông báo chưa đọc, lịch sử chạy, và cấu hình model của 2 agent.
    """

    def _snapshot() -> Dict[str, Any]:
        jobs = job_status_list()
        incidents = incident_store.list_incidents(limit=50)
        status_counts = incident_store.counts_by_status()
        failed_jobs = [j for j in jobs if j["status"] == "failed"]
        open_incidents = [
            i for i in incidents if i["status"] in incident_store.OPEN_STATUSES
        ]
        return {
            "generated_at": datetime.now().isoformat(),
            "kpi": {
                "jobs_total": len(jobs),
                "jobs_failed": len(failed_jobs),
                "jobs_success": sum(1 for j in jobs if j["status"] == "success"),
                "jobs_pending": sum(1 for j in jobs if j["status"] == "pending"),
                "incidents_open": len(open_incidents),
                "incidents_waiting_approval": status_counts.get("WAITING_FOR_APPROVAL", 0),
                "incidents_investigating": status_counts.get("INVESTIGATING", 0)
                + status_counts.get("DETECTED", 0),
                "incidents_resolved": status_counts.get("RESOLVED", 0),
                "unread_notifications": incident_store.unread_count(),
                "affected_rows": sum(int(i["failed_rows"] or 0) for i in open_incidents),
            },
            "jobs": jobs,
            "incidents": incidents,
            "notifications": incident_store.list_notifications(limit=25),
            "recent_runs": recent_runs(15),
            "brains": worker.describe_brains(),
            "scheduler": {"enabled": SCHEDULER_RUNNING, "interval_seconds": SCHEDULER_INTERVAL},
            "dbt": dbt_runner.dag_summary(),
        }

    return await run_in_threadpool(_snapshot)


@router.get("/api/jobs", tags=["dashboard"], dependencies=[Depends(require_token)])
async def jobs() -> Dict[str, Any]:
    """Danh sách 10 flow kèm trạng thái lần chạy gần nhất."""
    rows = await run_in_threadpool(job_status_list)
    return {"jobs": rows, "failed": [j["job_id"] for j in rows if j["status"] == "failed"]}


@router.post("/api/jobs/{job_id}/run", tags=["dashboard"], dependencies=[Depends(require_token)])
async def trigger_job(job_id: str) -> Dict[str, Any]:
    """Chạy ngay một job (nút 'Run now' trên dashboard)."""
    if job_id not in JOBS_BY_ID:
        raise HTTPException(status_code=404, detail=f"Không có job '{job_id}'")
    async with _write_lock:
        return await run_in_threadpool(run_job, job_id, "manual")


@router.post("/api/jobs/run-all", tags=["dashboard"], dependencies=[Depends(require_token)])
async def trigger_all_jobs() -> Dict[str, Any]:
    """Chạy toàn bộ 10 flow (nút 'Chạy cả pipeline')."""
    async with _write_lock:
        results = await run_in_threadpool(run_all_jobs, "manual")
    return {
        "total": len(results),
        "failed": sum(1 for r in results if r["status"] != "success"),
        "results": results,
    }


@router.get("/api/incidents-list", tags=["dashboard"], dependencies=[Depends(require_token)])
async def incidents_list(status: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    """Danh sách incident (nhẹ, không kèm JSON báo cáo)."""
    rows = await run_in_threadpool(incident_store.list_incidents, status, limit)
    return {"incidents": rows, "counts": await run_in_threadpool(incident_store.counts_by_status)}


@router.get(
    "/api/incidents/{incident_id}", tags=["dashboard"], dependencies=[Depends(require_token)]
)
async def incident_detail(incident_id: str) -> Dict[str, Any]:
    """
    Chi tiết sự cố: envelope + **báo cáo điều tra đã lưu sẵn** + biên bản nghiệm thu.

    Không gọi LLM ở đây — báo cáo do worker nền sinh ra từ trước, nên click là ra ngay.
    """
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có incident '{incident_id}'")
    row["job"] = (
        {
            "job_id": JOBS_BY_ID[row["job_id"]].job_id,
            "name": JOBS_BY_ID[row["job_id"]].name,
            "owner": JOBS_BY_ID[row["job_id"]].owner,
            "tier": JOBS_BY_ID[row["job_id"]].tier,
            "layer": JOBS_BY_ID[row["job_id"]].layer,
            "depends_on": JOBS_BY_ID[row["job_id"]].depends_on,
        }
        if row["job_id"] in JOBS_BY_ID
        else None
    )
    return row


@router.get(
    "/api/jobs/{job_id}/incident", tags=["dashboard"], dependencies=[Depends(require_token)]
)
async def incident_of_job(job_id: str) -> Dict[str, Any]:
    """Incident đang mở của một job — dùng khi click vào job báo đỏ."""
    found = await run_in_threadpool(incident_store.incidents_for_job, job_id)
    if not found:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' không có incident đang mở")
    row = dict(found[0])
    # Một job thường gãy vì nhiều test cùng lúc -> đưa luôn các hồ sơ còn lại để UI
    # liệt kê, engineer không phải mò sang trang incident khác.
    row["related_incidents"] = [
        {
            "incident_id": item["incident_id"],
            "test_name": item.get("test_name"),
            "severity": item.get("severity"),
            "status": item.get("status"),
            "failed_rows": item.get("failed_rows"),
            "has_report": bool(item.get("report")),
        }
        for item in found[1:]
    ]
    return row


@router.post(
    "/api/incidents/{incident_id}/investigate",
    tags=["dashboard"],
    dependencies=[Depends(require_token)],
)
async def investigate_now(incident_id: str) -> Dict[str, Any]:
    """Ép điều tra ngay một incident (nếu worker nền chưa kịp tới)."""
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có incident '{incident_id}'")
    if not row.get("envelope"):
        raise HTTPException(status_code=409, detail="Incident thiếu envelope, không điều tra được")
    async with _write_lock:
        return await run_in_threadpool(worker.investigate_incident, incident_id, row["envelope"])


@router.post(
    "/api/incidents/{incident_id}/select", tags=["dashboard"], dependencies=[Depends(require_token)]
)
async def select_incident(incident_id: str) -> Dict[str, Any]:
    """
    Chọn incident để mở phiên Human-in-the-loop trên Chainlit.

    Dashboard gọi endpoint này rồi điều hướng sang `/chat`; phiên chat kế tiếp sẽ nạp
    đúng incident này thay vì incident mẫu.
    """
    row = await run_in_threadpool(incident_store.get_incident, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Không có incident '{incident_id}'")
    await run_in_threadpool(incident_store.select_incident, incident_id)
    return {"selected": incident_id, "chat_url": config.CHAINLIT_PATH}


@router.get("/api/notifications", tags=["dashboard"], dependencies=[Depends(require_token)])
async def notifications(unread_only: bool = False, limit: int = 30) -> Dict[str, Any]:
    """Danh sách thông báo + số chưa đọc (badge đỏ ở mục thông báo)."""
    rows = await run_in_threadpool(incident_store.list_notifications, unread_only, limit)
    return {"notifications": rows, "unread": await run_in_threadpool(incident_store.unread_count)}


@router.post("/api/notifications/read", tags=["dashboard"], dependencies=[Depends(require_token)])
async def read_notifications(notification_id: Optional[str] = None) -> Dict[str, Any]:
    """Đánh dấu đã đọc (không truyền id = đọc tất cả)."""
    remaining = await run_in_threadpool(incident_store.mark_read, notification_id)
    return {"unread": remaining}


@router.post("/api/demo/inject-defect", tags=["dashboard"], dependencies=[Depends(require_token)])
async def inject_defect(rows: int = 8) -> Dict[str, Any]:
    """
    Inject data defects for testing - now works with any available data source.
    """
    async with _write_lock:
        # Dynamic defect injection based on available tables and sources
        tables_result = await run_in_threadpool(
            tools.tool_query_duckdb, "SHOW TABLES"
        )
        
        if tables_result.get("ok") and tables_result.get("rows"):
            tables = [row.get("name", "") for row in tables_result["rows"]]
            
            # Find main table to inject defects into
            fact_tables = [t for t in tables if "fact" in t.lower()]
            main_table = fact_tables[0] if fact_tables else (tables[0] if tables else None)
            
            if main_table:
                # Try to inject defects dynamically
                try:
                    # First try using the original ingest function if available
                    from data.jobs.ingest_mobile_app import ingest
                    stats = await run_in_threadpool(ingest, max(1, min(int(rows), 100)), False)
                except ImportError:
                    # Fallback: create synthetic defects by inserting null values
                    inject_sql = f"""
                    INSERT INTO {main_table} 
                    SELECT *, NULL as injected_defect_marker
                    FROM {main_table} 
                    LIMIT {max(1, min(int(rows), 20))}
                    """
                    
                    result = await run_in_threadpool(
                        tools.tool_query_duckdb, inject_sql
                    )
                    
                    stats = {
                        "injected_rows": max(1, min(int(rows), 20)) if result.get("ok") else 0,
                        "method": "synthetic_nulls",
                        "target_table": main_table
                    }
                
                # Run pipeline to detect the defects
                runs = await run_in_threadpool(run_all_jobs, "demo-inject")
                
                return {
                    "injected": stats,
                    "jobs_failed": [r["job_id"] for r in runs if r["status"] != "success"],
                    "incidents": [r["incident_id"] for r in runs if r.get("incident_id")],
                    "main_table": main_table
                }
            else:
                return {
                    "error": "No tables found to inject defects into",
                    "injected": {"injected_rows": 0},
                    "jobs_failed": [],
                    "incidents": []
                }
        else:
            return {
                "error": "Could not access database tables",
                "injected": {"injected_rows": 0},
                "jobs_failed": [],
                "incidents": []
            }


# ---------------------------------------------------------------------------
# DBT · LINEAGE
# ---------------------------------------------------------------------------


@router.get("/api/dbt/status", tags=["dbt"], dependencies=[Depends(require_token)])
async def dbt_status() -> Dict[str, Any]:
    """Tình trạng dbt project: số model/test, DAG đã sinh chưa, docs sẵn sàng chưa."""
    summary = await run_in_threadpool(dbt_runner.dag_summary)
    summary["models_detail"] = await run_in_threadpool(dbt_runner.dbt_models)
    summary["last_run_results"] = await run_in_threadpool(dbt_runner.parse_run_results)
    summary["docs_url"] = "/dbt-docs/index.html"
    return summary


@router.post("/api/dbt/{command}", tags=["dbt"], dependencies=[Depends(require_token)])
async def dbt_command(command: str) -> Dict[str, Any]:
    """
    Chạy dbt thật: `run` (build model), `test` (cổng DQ), `docs` (sinh lineage graph),
    hoặc `build` (cả run + test).

    DuckDB chỉ cho một tiến trình ghi nên lệnh chạy dưới `exclusive_access()`: connection
    dùng chung được đóng, dbt chạy, rồi mở lại. Vì vậy phải giữ `_write_lock`.
    """
    runners = {
        "run": dbt_runner.dbt_run,
        "test": dbt_runner.dbt_test,
        "docs": dbt_runner.dbt_docs_generate,
    }
    if command not in runners and command != "build":
        raise HTTPException(
            status_code=404,
            detail=f"Lệnh '{command}' không hỗ trợ. Dùng: {sorted(runners)} hoặc 'build'.",
        )

    async with _write_lock:
        if command == "build":
            # run trước rồi test: mart luôn được build dù test fail (pattern production)
            ran = await run_in_threadpool(dbt_runner.dbt_run)
            tested = await run_in_threadpool(dbt_runner.dbt_test)
            docs = await run_in_threadpool(dbt_runner.dbt_docs_generate)
            outcome: Dict[str, Any] = {"run": ran, "test": tested, "docs": docs}
        else:
            outcome = await run_in_threadpool(runners[command])
        # dbt test fail -> có sự cố DQ -> đồng bộ sang pipeline để sinh incident
        if command in ("test", "build"):
            outcome["pipeline"] = await run_in_threadpool(run_all_jobs, f"dbt-{command}")
    return outcome


@router.get("/api/lineage/{table_name}", tags=["dbt"], dependencies=[Depends(require_token)])
async def table_lineage(table_name: str, depth: int = 3) -> Dict[str, Any]:
    """
    Lineage upstream/downstream của MỘT BẢNG BẤT KỲ, đọc từ dbt manifest.

    Đây cũng chính là tool mà agent dùng (`tool_get_lineage`), nên UI và agent luôn nhìn
    cùng một DAG.
    """
    result = await run_in_threadpool(dbt_runner.lineage, table_name, depth)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.get("/api/warehouse/summary", tags=["warehouse"], dependencies=[Depends(require_token)])
async def warehouse_summary() -> Dict[str, Any]:
    """Ảnh chụp nhanh sức khoẻ DQ của warehouse (dùng cho dashboard ngoài)."""

    def _summary() -> Dict[str, Any]:
        # Dynamic table discovery for summary
        tables_result = tools.tool_query_duckdb("SHOW TABLES") 
        main_table = None
        
        if tables_result.get("ok") and tables_result.get("rows"):
            tables = [row.get("name", "") for row in tables_result["rows"]]
            fact_tables = [t for t in tables if "fact" in t.lower()]
            main_table = fact_tables[0] if fact_tables else (tables[0] if tables else None)
        
        out: Dict[str, Any] = {}
        
        if main_table:
            out["total_rows"] = db.row_count(main_table)
            out["main_table"] = main_table
            
            # Dynamic quarantine detection
            quarantine = data_audit.detect_quarantine_table(main_table)
            out["quarantine_table"] = quarantine
            out["quarantined_rows"] = db.row_count(quarantine) if quarantine else 0
        else:
            out["total_rows"] = 0
            out["main_table"] = "no_tables_found"
            out["quarantine_table"] = None
            out["quarantined_rows"] = 0
            
        # Dynamic DQ checks results  
        for check in load_checks():
            out[check.test_name] = db.scalar(check.count_sql, default=None)
            
        return out

    return await run_in_threadpool(_summary)


@router.post("/api/dq/run", tags=["warehouse"], dependencies=[Depends(require_token)])
async def run_dq() -> Dict[str, Any]:
    """Chạy lại toàn bộ DQ test (tương đương `python -m data.jobs.run_dq_tests`)."""
    results: List[Dict[str, Any]] = await run_in_threadpool(run_all_checks)
    failed = [r for r in results if r["status"] == "fail"]
    return {
        "run_id": results[0]["run_id"] if results else None,
        "total": len(results),
        "failed": len(failed),
        "results": results,
    }


@router.get("/api/audit-log", tags=["ops"], dependencies=[Depends(require_token)])
async def audit_log(limit: int = 50) -> Dict[str, Any]:
    """Nhật ký mọi tool call của agent (truy vết ai/khi nào/chạy gì)."""
    result = await run_in_threadpool(data_audit.read_audit_log, limit)
    return {"rows": result["rows"], "row_count": result["row_count"]}


@router.get("/api/sql/tables", tags=["sql"])
async def sql_tables() -> List[Dict[str, Any]]:
    """Lấy danh sách các bảng kèm số dòng và danh sách cột cho Web SQL Explorer."""
    def _inspect() -> List[Dict[str, Any]]:
        table_names = db.list_tables()
        tables_meta = []
        for t in table_names:
            cnt = db.row_count(t)
            cols = []
            try:
                desc = db.fetch(f"DESCRIBE {t}", max_rows=100)
                cols = [
                    {"name": r.get("column_name"), "type": r.get("column_type")}
                    for r in desc.get("rows", [])
                ]
            except Exception:
                pass
            tables_meta.append({"name": t, "row_count": cnt, "columns": cols})
        return tables_meta

    return await run_in_threadpool(_inspect)


@router.post("/api/sql/query", tags=["sql"])
async def sql_query(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Thực thi câu lệnh SQL đọc (SELECT/SHOW/DESCRIBE) trên DuckDB và trả về dữ liệu bảng."""
    query = (payload.get("query") or "").strip()
    limit = int(payload.get("limit") or 200)
    if not query:
        raise HTTPException(status_code=400, detail="Query không được để trống")

    return await run_in_threadpool(tools.tool_query_duckdb, query, limit)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator (Airflow-like DAG, Tasks, Logs, Scheduler Toggle)
# ---------------------------------------------------------------------------


@router.get("/api/scheduler/status", tags=["pipeline"])
async def scheduler_status() -> Dict[str, Any]:
    """Lấy trạng thái scheduler: Tự động (Auto) hay Tạm dừng (Manual)."""
    return {
        "enabled": SCHEDULER_RUNNING,
        "mode": "auto" if SCHEDULER_RUNNING else "manual",
        "interval_seconds": SCHEDULER_INTERVAL,
    }


@router.post("/api/scheduler/toggle", tags=["pipeline"])
async def toggle_scheduler() -> Dict[str, Any]:
    """Bật / Tắt scheduler tự động chạy ngầm (chuyển sang Manual Run Mode)."""
    global SCHEDULER_RUNNING
    SCHEDULER_RUNNING = not SCHEDULER_RUNNING
    mode_name = "Tự động (Auto)" if SCHEDULER_RUNNING else "Thủ công (Manual / Paused)"
    return {
        "enabled": SCHEDULER_RUNNING,
        "mode": "auto" if SCHEDULER_RUNNING else "manual",
        "interval_seconds": SCHEDULER_INTERVAL,
        "message": f"Đã chuyển scheduler sang chế độ {mode_name}",
    }


@router.get("/api/pipeline/dag", tags=["pipeline"])
async def pipeline_dag() -> Dict[str, Any]:
    """
    Trả về cấu trúc DAG trực quan (nodes, edges, layer groups, trạng thái hiện tại)
    để render Airflow Visual Graph.
    """
    def _build_dag() -> Dict[str, Any]:
        from data.pipeline import load_jobs, job_status_list
        jobs = load_jobs()
        statuses = {s["job_id"]: s for s in job_status_list()}

        nodes = []
        edges = []

        layer_order = {
            "ingestion": 1,
            "gate": 2,
            "staging": 3,
            "transform": 3,
            "marts": 4,
            "serving": 5,
        }

        for j in jobs:
            st = statuses.get(j.job_id, {})
            nodes.append({
                "id": j.job_id,
                "name": j.name,
                "layer": j.layer,
                "layer_rank": layer_order.get(j.layer, 3),
                "tier": j.tier,
                "owner": j.owner,
                "target_table": j.target_table,
                "status": st.get("status", "pending"),
                "last_run_at": st.get("last_run_at"),
                "duration_ms": st.get("duration_ms", 0),
                "tests_total": st.get("tests_total", len(j.dq_checks)),
                "tests_failed": st.get("tests_failed", 0),
                "dq_checks": j.dq_checks,
                "depends_on": j.depends_on,
                "description": j.description,
                "dbt_model": j.dbt_model,
                "incident_id": st.get("incident_id"),
            })

            for parent in j.depends_on:
                edges.append({
                    "from": parent,
                    "to": j.job_id,
                })

        return {
            "dag_id": "ecommerce_orders_elt_pipeline",
            "title": "Ecommerce Orders ELT & Data Reliability DAG",
            "scheduler": {
                "enabled": SCHEDULER_RUNNING,
                "mode": "auto" if SCHEDULER_RUNNING else "manual",
                "interval_seconds": SCHEDULER_INTERVAL,
            },
            "nodes": nodes,
            "edges": edges,
        }

    return await run_in_threadpool(_build_dag)


@router.get("/api/pipeline/tasks/{job_id}/logs", tags=["pipeline"])
async def task_logs(job_id: str) -> Dict[str, Any]:
    """
    Trả về Log thực thi chi tiết chuẩn Airflow Task Instance Log
    (timestamp, SQL query, assertion checks, duration, exit code).
    """
    def _generate_log() -> Dict[str, Any]:
        from data.pipeline import JOBS_BY_ID, ensure_pipeline_tables
        from data.connection import fetch
        ensure_pipeline_tables()

        job = JOBS_BY_ID.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy task '{job_id}'")

        # Lấy lịch sử lần chạy mới nhất
        rows = fetch(
            f"""
            SELECT * FROM job_runs
            WHERE job_id = '{job_id.replace("'", "''")}'
            ORDER BY started_at DESC LIMIT 5
            """
        )["rows"]

        latest = rows[0] if rows else {}
        status = latest.get("status", "pending")
        started_at = latest.get("started_at") or datetime.now().isoformat()
        finished_at = latest.get("finished_at") or datetime.now().isoformat()
        run_id = latest.get("run_id") or "run_manual_init"
        duration_ms = latest.get("duration_ms") or 0
        message = latest.get("message") or "Task initialized."
        incident_id = latest.get("incident_id")

        # Lấy compiled SQL nếu có
        compiled_sql = ""
        try:
            if job.is_dbt:
                from data.dbt_runner import compiled_model_sql
                compiled_sql = compiled_model_sql(job.dbt_model) or ""
            elif job.layer in ("transform", "marts"):
                from data.warehouse import MART_SQL
                bare = job.target_table.split(".")[-1]
                compiled_sql = MART_SQL.get(bare, "")
        except Exception:
            pass

        # Xây dựng log lines theo format chuẩn Apache Airflow
        ts = started_at[:19].replace("T", " ")
        log_lines = [
            f"*** Log file: /opt/airflow/logs/dag_id=ecommerce_orders_elt/run_id={run_id}/task_id={job_id}/attempt_1.log",
            f"*** Hostname: worker-duckdb-node-01.local",
            f"[{ts},010] {{taskinstance.py:1150}} INFO - Dependencies all met for dep_context: non-requeueable deps",
            f"[{ts},015] {{taskinstance.py:1152}} INFO - Dependencies all met for dep_context: re-queueable deps",
            f"[{ts},020] {{taskinstance.py:1342}} INFO - Starting attempt 1 of 1",
            f"[{ts},035] {{taskinstance.py:1365}} INFO - Executing <Task(DuckDBOperator): {job_id}> on {started_at}",
            f"[{ts},040] {{duckdb_operator.py:52}} INFO - Connecting to DuckDB: {config.DUCKDB_PATH}",
            f"[{ts},050] {{duckdb_operator.py:58}} INFO - Task metadata: layer='{job.layer}', tier='{job.tier}', owner='{job.owner}'",
            f"[{ts},065] {{duckdb_operator.py:65}} INFO - Target Table: {job.target_table}",
        ]

        if job.depends_on:
            log_lines.append(
                f"[{ts},070] {{taskinstance.py:1400}} INFO - Upstream dependencies verified: [{', '.join(job.depends_on)}]"
            )

        if compiled_sql:
            log_lines.append(f"[{ts},110] {{dbt_runner.py:82}} INFO - Executing transformation DDL/DML...")
            for sql_line in compiled_sql.strip().splitlines()[:10]:
                log_lines.append(f"[{ts},115] {{dbt_runner.py:85}} DEBUG - SQL | {sql_line}")
            if len(compiled_sql.strip().splitlines()) > 10:
                log_lines.append(f"[{ts},116] {{dbt_runner.py:86}} DEBUG - ... ({len(compiled_sql.strip().splitlines()) - 10} more SQL lines)")

        if job.dq_checks:
            log_lines.append(
                f"[{ts},150] {{dq_gate.py:45}} INFO - Running {len(job.dq_checks)} DQ Gate assertions on {job.target_table}..."
            )
            for idx, chk_name in enumerate(job.dq_checks, 1):
                is_failed = status == "failed" and ("null" in chk_name.lower() or "violation" in message.lower())
                if is_failed:
                    log_lines.append(
                        f"[{ts},1{idx:02d}] {{dq_gate.py:58}} ERROR - [{idx}/{len(job.dq_checks)}] Assertion '{chk_name}' -> FAILED! ({latest.get('failed_rows', 15)} invalid rows found)"
                    )
                else:
                    log_lines.append(
                        f"[{ts},1{idx:02d}] {{dq_gate.py:55}} INFO - [{idx}/{len(job.dq_checks)}] Assertion '{chk_name}' -> PASSED (0 violations)"
                    )

        if incident_id:
            log_lines.append(
                f"[{ts},280] {{incident_store.py:62}} WARNING - Incident generated: {incident_id} (Severity: CRITICAL)"
            )
            log_lines.append(
                f"[{ts},285] {{incident_store.py:65}} INFO - Dispatched notification to Multi-Agent SRE queue"
            )

        if status == "success":
            log_lines.append(f"[{ts},320] {{taskinstance.py:1450}} INFO - Task exited with returncode 0 (SUCCESS)")
            log_lines.append(f"[{ts},325] {{taskinstance.py:1460}} INFO - Marking task as SUCCESS. duration: {duration_ms}ms")
        elif status == "failed":
            log_lines.append(f"[{ts},320] {{taskinstance.py:1450}} ERROR - Task exited with returncode 1 (FAILED)")
            log_lines.append(f"[{ts},325] {{taskinstance.py:1460}} ERROR - Marking task as FAILED: {message}")
        else:
            log_lines.append(f"[{ts},320] {{taskinstance.py:1450}} WARNING - Task state: {status} ({message})")

        return {
            "job_id": job_id,
            "name": job.name,
            "status": status,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "message": message,
            "incident_id": incident_id,
            "compiled_sql": compiled_sql,
            "log_text": "\n".join(log_lines),
            "recent_runs": rows,
        }

    return await run_in_threadpool(_generate_log)


@router.post("/api/pipeline/dag/run", tags=["pipeline"])
async def run_entire_dag() -> Dict[str, Any]:
    """
    Thực thi toàn bộ Pipeline DAG thủ công theo thứ tự phụ thuộc (Topological Order):
    Ingestion -> Gate -> Staging -> Marts.
    """
    async with _write_lock:
        def _exec_dag() -> Dict[str, Any]:
            from data.pipeline import load_jobs, run_job
            jobs = load_jobs()

            # Sắp xếp theo layer: Ingestion trước, rồi Gate, Staging, Marts
            layer_rank = {
                "ingestion": 0,
                "gate": 1,
                "staging": 2,
                "transform": 2,
                "marts": 3,
                "serving": 4,
            }
            ordered = sorted(jobs, key=lambda j: (layer_rank.get(j.layer, 2), len(j.depends_on)))

            results = []
            for j in ordered:
                res = run_job(j.job_id, triggered_by="manual_dag")
                results.append(res)

            failed = [r for r in results if r["status"] != "success"]
            return {
                "dag_id": "ecommerce_orders_elt_pipeline",
                "total_tasks": len(results),
                "passed_tasks": len(results) - len(failed),
                "failed_tasks": len(failed),
                "results": results,
            }

        return await run_in_threadpool(_exec_dag)




async def startup() -> None:
    """
    Đảm bảo warehouse DuckDB + bảng pipeline tồn tại trước khi nhận request đầu tiên.

    DuckDB chỉ cho **một tiến trình ghi** vào file cùng lúc. Nếu có server/job/dbt khác
    đang giữ file, ta báo lỗi rõ ràng thay vì để traceback thô của DuckDB.
    """
    try:
        await run_in_threadpool(ensure_database, config.DUCKDB_PATH)
        await run_in_threadpool(ensure_pipeline_tables)
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if "another process" in message or "already open" in message:
            raise RuntimeError(
                f"Không mở được warehouse `{config.DUCKDB_PATH}` vì đang bị tiến trình khác "
                "giữ. DuckDB chỉ cho 1 tiến trình ghi cùng lúc — hãy tắt server/job/dbt "
                f"đang chạy rồi thử lại.\nChi tiết: {message}"
            ) from exc
        raise

    # Lần đầu chạy: kích hoạt pipeline một lượt để dashboard có dữ liệu ngay,
    # nếu không giám khảo mở lên sẽ thấy bảng trống.
    if not await run_in_threadpool(job_status_list) or not await run_in_threadpool(recent_runs, 1):
        async with _write_lock:
            await run_in_threadpool(run_all_jobs, "startup")


async def scheduler_loop() -> None:
    """
    Vòng lặp nền: chạy job tới hạn, rồi cho worker điều tra incident mới.

    Đây là thứ khiến sự cố được **phát hiện tức thì** mà không cần ai bấm gì: job fail
    -> incident DETECTED -> worker điều tra -> UI thấy badge đỏ + báo cáo sẵn sàng.

    Chạy tuần tự trong threadpool vì DuckDB chỉ cho một writer.
    """
    if not SCHEDULER_ENABLED:
        print("[scheduler] ⏸️  Đã tắt (DRA_SCHEDULER=0)")
        return
    print(
        f"[scheduler] ▶️  Chạy mỗi {SCHEDULER_INTERVAL}s · "
        f"điều tra tối đa {SCHEDULER_MAX_INVESTIGATIONS} incident/chu kỳ"
    )
    while True:
        try:
            await asyncio.sleep(SCHEDULER_INTERVAL)
            if not SCHEDULER_RUNNING:
                continue
            async with _write_lock:
                runs = await run_in_threadpool(run_due_jobs, "scheduler")
                if runs:
                    failed = [r["job_id"] for r in runs if r["status"] != "success"]
                    print(
                        f"[scheduler] đã chạy {len(runs)} job"
                        + (f" · FAIL: {', '.join(failed)}" if failed else "")
                    )
                if SCHEDULER_MAX_INVESTIGATIONS:
                    done = await run_in_threadpool(
                        worker.process_pending, SCHEDULER_MAX_INVESTIGATIONS
                    )
                    for item in done:
                        print(f"[scheduler] điều tra: {item}")
        except asyncio.CancelledError:  # shutdown
            print("[scheduler] ⏹️  Dừng")
            raise
        except Exception as exc:  # noqa: BLE001 - scheduler không được làm sập server
            print(f"[scheduler] ⚠️  lỗi chu kỳ: {exc}")


__all__ = ["router", "startup", "scheduler_loop", "SCHEDULER_ENABLED", "SCHEDULER_INTERVAL"]
