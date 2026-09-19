"""
web/backend/api.py — REST API (headless)
========================================

Dành cho hệ thống khác gọi vào: webhook từ dbt/Airflow, dashboard ngoài, cron, CI.
Không chứa logic UI. Mọi endpoint đều nằm dưới `/api` (trừ `/health`).

    GET  /health                  Health check cho AgentBase / K8s probe
    GET  /api/incidents/sample    Incident Envelope mẫu (số liệu thật từ DuckDB)
    POST /api/incidents           Nạp incident -> Agent 1 điều tra (?auto_approve=true để vá luôn)
    POST /api/audit               Agent 2 nghiệm thu độc lập
    GET  /api/warehouse/summary   Ảnh chụp sức khoẻ DQ của warehouse
    GET  /api/audit-log           Nhật ký mọi tool call của agent
    GET  /api/models              Cấu hình model của Maker/Checker (kiểm cross-model)
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool

import config
from ai import tools
from ai.agent import run_headless
from ai.auditor import run_audit_headless
from ai.llm import LLMSettings
from ai.schemas import AgentReport, IncidentInput
from data import audit as data_audit
from data import connection as db
from data.dq import DQ_CHECKS, run_all_checks
from data.incidents import build_sample_incident_payload
from data.warehouse import ensure_database
from web.backend.security import require_token

router = APIRouter()

#: Khoá tránh 2 request cùng ghi DuckDB một lúc
_write_lock = asyncio.Lock()


@router.get("/health", tags=["ops"])
async def health() -> Dict[str, Any]:
    """Health check: xác nhận DuckDB đọc được và trả về số dòng bảng fact."""

    def _probe() -> Dict[str, Any]:
        result = tools.tool_query_duckdb("SELECT COUNT(*) AS rows FROM fact_orders")
        return {
            "duckdb_ok": bool(result.get("ok")),
            "fact_orders_rows": (result.get("rows") or [{}])[0].get("rows"),
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


@router.get("/api/warehouse/summary", tags=["warehouse"], dependencies=[Depends(require_token)])
async def warehouse_summary() -> Dict[str, Any]:
    """Ảnh chụp nhanh sức khoẻ DQ của warehouse (dùng cho dashboard ngoài)."""

    def _summary() -> Dict[str, Any]:
        out: Dict[str, Any] = {"total_rows": db.row_count("fact_orders")}
        for check in DQ_CHECKS:
            out[check.test_name] = db.scalar(check.count_sql, default=None)
        quarantine = data_audit.detect_quarantine_table("fact_orders")
        out["quarantine_table"] = quarantine
        out["quarantined_rows"] = db.row_count(quarantine) if quarantine else 0
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


async def startup() -> None:
    """
    Đảm bảo warehouse DuckDB tồn tại trước khi nhận request đầu tiên.

    DuckDB chỉ cho **một tiến trình ghi** vào file cùng lúc. Nếu có server/job/dbt khác
    đang giữ file, ta báo lỗi rõ ràng thay vì để traceback thô của DuckDB.
    """
    try:
        await run_in_threadpool(ensure_database, config.DUCKDB_PATH)
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if "another process" in message or "already open" in message:
            raise RuntimeError(
                f"Không mở được warehouse `{config.DUCKDB_PATH}` vì đang bị tiến trình khác "
                "giữ. DuckDB chỉ cho 1 tiến trình ghi cùng lúc — hãy tắt server/job/dbt "
                f"đang chạy rồi thử lại.\nChi tiết: {message}"
            ) from exc
        raise


__all__ = ["router", "startup"]
