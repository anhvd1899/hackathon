"""
ai/worker.py — Worker nền điều tra sự cố
========================================

Cầu nối giữa scheduler (scope data) và Agent 1 (scope ai):

    job fail -> incident status=DETECTED  (data/pipeline.py ghi vào DB)
                        │
                        ▼  worker lấy ra, cho Agent 1 điều tra
              status=INVESTIGATING
                        │
                        ▼  ghi report_json vào DB
              status=WAITING_FOR_APPROVAL   <-- UI đọc tức thời, không cần gọi LLM

Vì sao phải có worker: giám khảo bấm vào job lỗi thì **báo cáo phải có sẵn**. Nếu điều
tra lúc click thì họ phải chờ vài phút và tốn token mỗi lần click.

Ràng buộc: DuckDB chỉ cho 1 tiến trình ghi, và mỗi lượt điều tra tốn token, nên worker
chạy **tuần tự, mỗi lần 1 incident**, có `max_per_cycle` để không bùng chi phí.
"""

from __future__ import annotations

import json
import threading
from typing import Any, Dict, List, Optional

from ai.agent import DataReliabilityAgent
from ai.llm import LLMSettings
from ai.schemas import AgentReport, IncidentInput
from data import incident_store

#: Chỉ cho một lượt điều tra chạy tại một thời điểm (DuckDB 1 writer + tiết kiệm token)
_investigate_lock = threading.Lock()

#: Cache agent theo incident để phiên chat sau đó dùng lại đúng ngữ cảnh đã điều tra
_agents: Dict[str, DataReliabilityAgent] = {}
_agents_lock = threading.Lock()


def get_agent(incident_id: str) -> Optional[DataReliabilityAgent]:
    """Lấy lại agent đã điều tra incident này (nếu còn trong process)."""
    with _agents_lock:
        return _agents.get(incident_id)


def remember_agent(incident_id: str, agent: DataReliabilityAgent) -> None:
    with _agents_lock:
        _agents[incident_id] = agent
        # Giữ tối đa 20 agent gần nhất để không phình bộ nhớ
        if len(_agents) > 20:
            for key in list(_agents)[:-20]:
                _agents.pop(key, None)


def forget_agent(incident_id: str) -> None:
    with _agents_lock:
        _agents.pop(incident_id, None)


def restore_agent(incident_id: str) -> Optional[DataReliabilityAgent]:
    """
    Lấy agent của một sự cố; nếu process đã restart thì **dựng lại từ DuckDB**.

    Đây là yêu cầu vận hành, không phải tiện nghi: sự cố ở trạng thái
    `WAITING_SHADOW_APPROVAL` có thể nằm chờ hàng giờ. Nếu server được deploy lại trong
    lúc đó và nút [Duyệt] chỉ hoạt động khi agent còn trong RAM thì engineer sẽ bấm vào
    một nút chết, và phải cho điều tra lại từ đầu — tốn token cho một việc đã làm xong.

    Bản dựng lại mất history hội thoại (nên phần diễn giải sẽ ngắn hơn), nhưng giữ đủ
    `incident` + `report` để chạy mọi hành động của WAP: đó là những gì các nút cần.
    """
    cached = get_agent(incident_id)
    if cached is not None:
        return cached

    row = incident_store.get_incident(incident_id)
    if not row or not row.get("envelope"):
        return None
    try:
        incident = IncidentInput(**row["envelope"])
    except Exception:  # noqa: BLE001
        return None

    agent = DataReliabilityAgent(incident=incident)
    if row.get("report"):
        try:
            agent.report = AgentReport.model_validate(row["report"])
            agent.status = agent.report.status
        except Exception:  # noqa: BLE001 - report cũ sai schema thì bỏ, vẫn dùng được agent
            agent.report = None
    agent.retry_count = int(row.get("retry_count") or 0)
    if agent.report is not None:
        agent.plan_version = agent.report.remediation.plan_version
    remember_agent(incident_id, agent)
    return agent


def investigate_incident(incident_id: str, envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cho Agent 1 điều tra một incident và LƯU báo cáo vào DB.

    Không raise: lỗi được ghi vào cột `error` và trạng thái quay về DETECTED để lần sau
    thử lại — worker nền không được phép làm sập server.
    """
    if not _investigate_lock.acquire(blocking=False):
        return {"ok": False, "skipped": "đang có lượt điều tra khác chạy"}

    try:
        incident_store.set_status(incident_id, "INVESTIGATING", error="")
        try:
            incident = IncidentInput(**envelope)
            agent = DataReliabilityAgent(incident=incident)
            report = agent.investigate()
        except Exception as exc:  # noqa: BLE001
            incident_store.set_status(incident_id, "DETECTED", error=str(exc)[:900])
            incident_store.push_notification(
                level="warning",
                title="⚠️ Điều tra thất bại",
                body=f"{incident_id}: {exc}"[:400],
                incident_id=incident_id,
            )
            return {"ok": False, "incident_id": incident_id, "error": str(exc)}

        remember_agent(incident_id, agent)
        plan = report.remediation
        incident_store.set_status(
            incident_id,
            "WAITING_SHADOW_APPROVAL",
            report_json=report.model_dump_json(),
            shadow_table=plan.shadow_table_name or "",
            retry_count=0,
            ready_for_production=False,
            error="",
        )
        incident_store.push_notification(
            level="info",
            title=(
                "🧪 Đã điều tra xong, chờ duyệt chạy thử trên Staging"
                if plan.preflight_passed
                else "⚠️ Đã điều tra xong nhưng script chưa qua preflight"
            ),
            body=(
                f"{incident_id} · {report.impact.severity.value} · "
                f"{report.impact.affected_row_count} dòng · "
                f"đề xuất {report.remediation.action_type.value}"
                + (
                    f" · bảng bóng `{plan.shadow_table_name}`"
                    if plan.preflight_passed
                    else f" · preflight lỗi: {plan.preflight_error[:120]}"
                )
            ),
            job_id=str(envelope.get("evidence_payload", {}).get("job_id", "")),
            incident_id=incident_id,
        )
        return {
            "ok": True,
            "incident_id": incident_id,
            "severity": report.impact.severity.value,
            "action_type": report.remediation.action_type.value,
            "tokens": agent.usage.total_tokens,
            "tool_calls": len(agent.tool_events),
        }
    finally:
        _investigate_lock.release()


def process_pending(max_per_cycle: int = 1) -> List[Dict[str, Any]]:
    """
    Lấy các incident đang ở trạng thái DETECTED và điều tra.

    `max_per_cycle=1` là mặc định có chủ ý: mỗi lượt điều tra tốn token và DuckDB chỉ
    cho một writer, nên xử lý từ từ theo độ ưu tiên severity còn hơn bùng chi phí.
    """
    results: List[Dict[str, Any]] = []
    for _ in range(max(1, max_per_cycle)):
        pending = incident_store.next_incident_to_investigate()
        if pending is None:
            break
        envelope = pending.get("envelope")
        if not envelope:
            incident_store.set_status(
                pending["incident_id"], "FAILED", error="Thiếu envelope, không điều tra được"
            )
            continue
        results.append(investigate_incident(str(pending["incident_id"]), envelope))
    return results


def pending_count() -> int:
    return int(incident_store.counts_by_status().get("DETECTED", 0))


def describe_brains() -> Dict[str, Any]:
    """Thông tin model của 2 agent (UI hiển thị ở dashboard)."""
    maker = LLMSettings.from_env()
    checker = LLMSettings.for_auditor(avoid_model=maker.model)
    return {
        "offline": not maker.api_key,
        "base_url": maker.base_url,
        "profile": maker.profile,
        "maker_model": maker.model,
        "checker_model": checker.model,
        "cross_model": bool(maker.api_key) and maker.model != checker.model,
        "maker_budget": maker.token_budget,
        "checker_budget": checker.token_budget,
    }


__all__ = [
    "investigate_incident",
    "process_pending",
    "pending_count",
    "get_agent",
    "remember_agent",
    "forget_agent",
    "restore_agent",
    "describe_brains",
]
