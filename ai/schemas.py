"""
schemas.py
==========
Data Contract của hệ 2 Agent (Pydantic v2).

  Agent 1 — Data SRE Agent (Maker)   : IncidentInput  -> AgentReport
  Agent 2 — Data Auditor  (Checker)  : AgentReport?   -> AuditReport
                                        (tự query DuckDB, KHÔNG tin Agent 1)

Nguyên tắc thiết kế:
- INPUT (`IncidentInput`) phải "mở" (generic envelope): mọi loại sự cố (DQ, pipeline fail,
  schema drift, thiếu tài nguyên hạ tầng...) đều nhận được mà KHÔNG bị lỗi validate.
  => `extra="allow"`, `evidence_payload` là Dict[str, Any] tự do.
- OUTPUT (`AgentReport`) phải "chặt" (strict contract) để Web UI render ổn định và
  để hệ thống downstream (ticketing, Slack, audit log) parse an toàn.
  => enum chuẩn hoá, có validator tự sửa các giá trị LLM trả về hơi lệch chuẩn.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# ---------------------------------------------------------------------------
# 1. ENUMS (chuẩn hoá vocabulary giữa Agent - UI - Audit log)
# ---------------------------------------------------------------------------


class IncidentType(str, Enum):
    """Phân loại sự cố. Chỉ mang tính gợi ý, input vẫn cho phép chuỗi tự do."""

    DATA_QUALITY = "DATA_QUALITY"
    PIPELINE_FAILURE = "PIPELINE_FAILURE"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    INFRA_RESOURCE = "INFRA_RESOURCE"
    UNKNOWN = "UNKNOWN"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ActionType(str, Enum):
    QUARANTINE_DATA = "QUARANTINE_DATA"
    BACKFILL = "BACKFILL"
    RERUN_PIPELINE = "RERUN_PIPELINE"
    SCALE_RESOURCE = "SCALE_RESOURCE"
    MANUAL_FIX = "MANUAL_FIX"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class IncidentStatus(str, Enum):
    """
    State machine của một sự cố theo mô hình Write–Audit–Publish.

    Luồng chuẩn (Two-Phase Human-in-the-loop):

        DETECTED
          -> INVESTIGATING            Agent 1 điều tra
          -> WAITING_SHADOW_APPROVAL  đã có plan qua preflight, chờ engineer duyệt BƯỚC 1
          -> STAGING_VERIFYING        đã chạy trên shadow table, Agent 2 đang nghiệm thu
          -> READY_FOR_PRODUCTION     Agent 2 PASS, chờ engineer duyệt BƯỚC 2
          -> PUBLISHED_RESOLVED       đã atomic swap sang bảng thật

    Nhánh thất bại:

          STAGING_VERIFYING -> AUDIT_FAILED_TRIAGE   Agent 2 bắt lỗi, engineer chọn 1 trong 3:
                                                     re-plan / sửa SQL tay / huỷ
          bất kỳ -> CANCELLED                        huỷ, shadow table đã được dọn

    Các trạng thái cũ (`WAITING_FOR_APPROVAL`, `EXECUTING`, `RESOLVED`, `FAILED`) được
    giữ lại: dữ liệu incident đã ghi trong DuckDB từ trước vẫn phải đọc được, và luồng
    CLI một bước vẫn dùng. Không xoá enum đang có dữ liệu ngoài đời là nguyên tắc.
    """

    # --- Luồng WAP (đang dùng) ---------------------------------------------
    INVESTIGATING = "INVESTIGATING"
    WAITING_SHADOW_APPROVAL = "WAITING_SHADOW_APPROVAL"
    STAGING_VERIFYING = "STAGING_VERIFYING"
    READY_FOR_PRODUCTION = "READY_FOR_PRODUCTION"
    AUDIT_FAILED_TRIAGE = "AUDIT_FAILED_TRIAGE"
    PUBLISHED_RESOLVED = "PUBLISHED_RESOLVED"
    CANCELLED = "CANCELLED"

    # --- Giữ tương thích ngược ---------------------------------------------
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    EXECUTING = "EXECUTING"
    RESOLVED = "RESOLVED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


#: Trạng thái mà engineer đang được chờ quyết định (UI phải hiện Action Bar).
AWAITING_HUMAN_STATUSES = frozenset(
    {
        IncidentStatus.WAITING_SHADOW_APPROVAL.value,
        IncidentStatus.READY_FOR_PRODUCTION.value,
        IncidentStatus.AUDIT_FAILED_TRIAGE.value,
        IncidentStatus.WAITING_FOR_APPROVAL.value,
    }
)

#: Trạng thái kết thúc — không còn hành động nào.
TERMINAL_STATUSES = frozenset(
    {
        IncidentStatus.PUBLISHED_RESOLVED.value,
        IncidentStatus.CANCELLED.value,
        IncidentStatus.RESOLVED.value,
        IncidentStatus.REJECTED.value,
    }
)


# Map severity -> icon để UI hiển thị nhanh
SEVERITY_ICON: Dict[str, str] = {
    "LOW": "🟢",
    "MEDIUM": "🟡",
    "HIGH": "🟠",
    "CRITICAL": "🔴",
}

STATUS_ICON: Dict[str, str] = {
    "DETECTED": "🚨",
    "INVESTIGATING": "🔍",
    "WAITING_SHADOW_APPROVAL": "🧪",
    "STAGING_VERIFYING": "🕵️‍♀️",
    "READY_FOR_PRODUCTION": "🚀",
    "AUDIT_FAILED_TRIAGE": "🛑",
    "PUBLISHED_RESOLVED": "🎉",
    "CANCELLED": "🗑️",
    # tương thích ngược
    "WAITING_FOR_APPROVAL": "⏸️",
    "EXECUTING": "⚙️",
    "RESOLVED": "✅",
    "REJECTED": "🚫",
    "FAILED": "❌",
}

#: Nhãn tiếng Việt cho từng trạng thái (UI và Chainlit dùng chung một nguồn).
STATUS_LABEL: Dict[str, str] = {
    "DETECTED": "Mới phát hiện",
    "INVESTIGATING": "Đang điều tra",
    "WAITING_SHADOW_APPROVAL": "Chờ duyệt chạy thử trên Staging",
    "STAGING_VERIFYING": "Đang nghiệm thu trên Staging",
    "READY_FOR_PRODUCTION": "Sẵn sàng Publish lên Production",
    "AUDIT_FAILED_TRIAGE": "Nghiệm thu thất bại — chờ engineer chọn hướng",
    "PUBLISHED_RESOLVED": "Đã publish lên Production",
    "CANCELLED": "Đã huỷ, staging đã dọn",
    "WAITING_FOR_APPROVAL": "Chờ phê duyệt",
    "EXECUTING": "Đang thực thi",
    "RESOLVED": "Đã xử lý",
    "REJECTED": "Bị từ chối",
    "FAILED": "Thất bại",
}


def _utcnow() -> datetime:
    """Timestamp UTC có timezone (tránh naive datetime khi ghi audit log)."""
    return datetime.now(timezone.utc)


def _coerce_enum(value: Any, enum_cls: type[Enum], default: Enum) -> Enum:
    """
    Chuẩn hoá giá trị LLM trả về thành enum.
    LLM hay trả 'high', 'Critical ', 'quarantine data' => tự map về đúng enum,
    không match được thì fallback về `default` (KHÔNG raise để UI không bị vỡ).
    """
    if isinstance(value, enum_cls):
        return value
    if value is None:
        return default
    token = str(value).strip().upper().replace(" ", "_").replace("-", "_")
    # Bỏ prefix kiểu "SEVERITY.HIGH" hoặc "ACTIONTYPE.BACKFILL"
    if "." in token:
        token = token.rsplit(".", 1)[-1]
    try:
        return enum_cls(token)
    except ValueError:
        # Thử match lỏng: chứa tên enum
        for member in enum_cls:
            if member.value in token or token in member.value:
                return member
        return default


# ---------------------------------------------------------------------------
# 2. INPUT CONTRACT — Generic Incident Envelope
# ---------------------------------------------------------------------------


class IncidentInput(BaseModel):
    """
    Envelope nhận sự cố từ bất kỳ nguồn nào: dbt test fail, Airflow callback,
    Great Expectations, alertmanager, log collector...

    Cố tình để lỏng:
    - `incident_type` là `str` (không bắt enum) -> nguồn lạ vẫn vào được.
    - `evidence_payload` là dict tự do -> chứa run_results.json, stacktrace,
      sample failed rows, metric hạ tầng... tuỳ nguồn.
    - `extra="allow"` -> field lạ được giữ lại thay vì bị reject.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    incident_id: str = Field(
        default_factory=lambda: f"INC-{_utcnow():%Y%m%d}-{uuid.uuid4().hex[:6].upper()}",
        description="Mã định danh sự cố, ví dụ INC-2026-DQ01.",
    )
    incident_type: str = Field(
        default=IncidentType.DATA_QUALITY.value,
        description="DATA_QUALITY | PIPELINE_FAILURE | SCHEMA_DRIFT | INFRA_RESOURCE | ...",
    )
    target_table: str = Field(
        default="unknown",
        description="Bảng/dataset xảy ra sự cố, ví dụ main.fact_orders.",
    )
    description: str = Field(
        default="",
        description="Mô tả tóm tắt từ test runner / pipeline / hệ thống monitoring.",
    )
    evidence_payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Bằng chứng thô dạng JSON: failed rule, cột lỗi, sample rows, log...",
    )
    source: str = Field(
        default="manual",
        description="Nguồn phát sinh alert: dbt | airflow | great_expectations | manual...",
    )
    detected_at: datetime = Field(
        default_factory=_utcnow,
        description="Thời điểm phát hiện sự cố.",
    )

    @field_validator("incident_type", mode="before")
    @classmethod
    def _normalize_incident_type(cls, v: Any) -> str:
        """Chuẩn hoá nhẹ (upper, thay space bằng _) nhưng không reject giá trị lạ."""
        if v is None or str(v).strip() == "":
            return IncidentType.UNKNOWN.value
        return str(v).strip().upper().replace(" ", "_").replace("-", "_")

    @field_validator("target_table", mode="before")
    @classmethod
    def _normalize_table(cls, v: Any) -> str:
        if v is None or str(v).strip() == "":
            return "unknown"
        # 'warehouse.fact_orders' -> giữ nguyên, chỉ trim
        return str(v).strip()

    @model_validator(mode="after")
    def _fill_description(self) -> "IncidentInput":
        """Nếu không có description, tự tổng hợp từ evidence để prompt không bị rỗng."""
        if not self.description.strip():
            self.description = (
                f"Sự cố {self.incident_type} trên bảng {self.target_table} "
                f"(không có mô tả chi tiết, xem evidence_payload)."
            )
        return self

    # -- Helpers cho prompt / UI -------------------------------------------------

    @property
    def bare_table_name(self) -> str:
        """Tên bảng không kèm schema/catalog: 'warehouse.main.fact_orders' -> 'fact_orders'."""
        return self.target_table.split(".")[-1]

    def to_prompt_block(self) -> str:
        """Serialize incident thành block text nhồi vào user message của LLM."""
        extras = {
            k: v
            for k, v in (self.model_extra or {}).items()
            if k not in {"incident_id", "target_table"}
        }
        payload = json.dumps(self.evidence_payload, ensure_ascii=False, indent=2, default=str)
        block = [
            "=== INCIDENT ENVELOPE ===",
            f"incident_id   : {self.incident_id}",
            f"incident_type : {self.incident_type}",
            f"target_table  : {self.target_table}",
            f"source        : {self.source}",
            f"detected_at   : {self.detected_at.isoformat()}",
            f"description   : {self.description}",
            "evidence_payload:",
            payload,
        ]
        if extras:
            block.append("extra_fields:")
            block.append(json.dumps(extras, ensure_ascii=False, indent=2, default=str))
        block.append("=== END INCIDENT ENVELOPE ===")
        return "\n".join(block)


# ---------------------------------------------------------------------------
# 3. OUTPUT CONTRACT — Structured Agent Report
# ---------------------------------------------------------------------------


class Diagnosis(BaseModel):
    """Kết quả Root Cause Analysis."""

    model_config = ConfigDict(extra="ignore")

    root_cause: str = Field(description="Nguyên nhân gốc rễ cụ thể, có số liệu chứng minh.")
    confidence_score: float = Field(
        default=0.6, ge=0.0, le=1.0, description="Độ tin cậy 0.0 - 1.0."
    )
    suspected_source: str = Field(
        default="UNKNOWN",
        description="Nguồn nghi vấn: Upstream API / ETL job / Source DB / Infra...",
    )
    evidence_summary: List[str] = Field(
        default_factory=list,
        description="Các bằng chứng rút ra từ query DuckDB (số liệu thật, không phỏng đoán).",
    )
    investigation_queries: List[str] = Field(
        default_factory=list,
        description="Các câu SQL Agent đã dùng để điều tra (phục vụ audit).",
    )

    @field_validator("confidence_score", mode="before")
    @classmethod
    def _clamp_confidence(cls, v: Any) -> float:
        """LLM hay trả '85%' hoặc 85 -> quy về thang 0..1."""
        if v is None:
            return 0.6
        if isinstance(v, str):
            v = v.strip().replace("%", "")
        try:
            score = float(v)
        except (TypeError, ValueError):
            return 0.6
        if score > 1.0:
            score = score / 100.0
        return max(0.0, min(1.0, score))

    @field_validator("evidence_summary", "investigation_queries", mode="before")
    @classmethod
    def _listify(cls, v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [v] if v.strip() else []
        return [str(x) for x in v]


class ImpactAssessment(BaseModel):
    """Blast radius: ảnh hưởng tới đâu, mức độ nào."""

    model_config = ConfigDict(extra="ignore")

    severity: Severity = Field(default=Severity.MEDIUM, description="LOW|MEDIUM|HIGH|CRITICAL.")
    affected_row_count: int = Field(default=0, ge=0, description="Số dòng dữ liệu vi phạm.")
    affected_downstream_tables: List[str] = Field(
        default_factory=list, description="Bảng hạ nguồn bị ảnh hưởng (theo lineage runbook)."
    )
    affected_dashboards: List[str] = Field(
        default_factory=list, description="Dashboard/report BI bị ảnh hưởng."
    )
    sla_breach: bool = Field(default=False, description="Có vi phạm SLA đã cam kết không.")
    business_impact: str = Field(
        default="", description="Diễn giải ảnh hưởng nghiệp vụ cho stakeholder."
    )

    @field_validator("severity", mode="before")
    @classmethod
    def _norm_severity(cls, v: Any) -> Severity:
        return _coerce_enum(v, Severity, Severity.MEDIUM)

    @field_validator("affected_row_count", mode="before")
    @classmethod
    def _norm_rows(cls, v: Any) -> int:
        if v is None:
            return 0
        if isinstance(v, str):
            digits = "".join(ch for ch in v if ch.isdigit())
            return int(digits) if digits else 0
        try:
            return max(0, int(v))
        except (TypeError, ValueError):
            return 0

    @field_validator("affected_downstream_tables", "affected_dashboards", mode="before")
    @classmethod
    def _listify(cls, v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [v] if v.strip() else []
        return [str(x) for x in v]

    @field_validator("sla_breach", mode="before")
    @classmethod
    def _norm_bool(cls, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        return str(v).strip().lower() in {"true", "yes", "1", "y", "có", "co"}


class RemediationPlan(BaseModel):
    """Kế hoạch khắc phục + lệnh thực thi thật (chạy trên DuckDB sink)."""

    model_config = ConfigDict(extra="ignore")

    action_type: ActionType = Field(
        default=ActionType.MANUAL_FIX,
        description="QUARANTINE_DATA|BACKFILL|RERUN_PIPELINE|SCALE_RESOURCE|MANUAL_FIX.",
    )
    summary: str = Field(default="", description="Giải thích hành động khắc phục cho người đọc.")
    executable_command: str = Field(
        default="",
        description="SQL DDL/DML (hoặc CLI) cụ thể để vá lỗi. Nhiều câu tách nhau bằng ';'.",
    )
    verification_sql: str = Field(
        default="",
        description="Câu SELECT COUNT(*) để verify sau khi vá; kỳ vọng trả về 0.",
    )
    rollback_hint: str = Field(
        default="", description="Cách rollback nếu remediation gây sự cố mới."
    )
    risk_level: RiskLevel = Field(default=RiskLevel.MEDIUM, description="LOW|MEDIUM|HIGH.")
    requires_human_approval: bool = Field(
        default=True, description="Luôn True trong kiến trúc HITL này."
    )

    # -- Write–Audit–Publish -------------------------------------------------
    # `executable_command` phía trên là script THÔ do LLM soạn (theo tên bảng thật).
    # Bốn field dưới đây là thứ hệ thống thực sự chạy, và chúng chỉ chạm vùng staging.
    target_production_table: str = Field(
        default="",
        description="Bảng production sẽ được vá, ví dụ 'stg_orders'. KHÔNG bị ghi ở phase Write.",
    )
    shadow_table_name: str = Field(
        default="",
        description="Bảng bóng nơi mọi lệnh vá thực sự chạy, ví dụ 'shadow_stg_orders'.",
    )
    shadow_execution_script: str = Field(
        default="",
        description=(
            "Script chạy ở phase Write: tạo shadow table từ bảng thật, đẩy dòng bẩn sang "
            "quarantine, rồi xoá dòng bẩn khỏi shadow. Chỉ được ghi vào shadow_* / quarantine_*."
        ),
    )
    publish_script: str = Field(
        default="",
        description=(
            "Script atomic swap để engineer xem trước ở bước 2. Việc chạy thật do "
            "`data/wap.atomic_publish()` đảm nhiệm vì cần kiểm schema và quản transaction."
        ),
    )
    preflight_passed: bool = Field(
        default=False,
        description=(
            "Script đã qua EXPLAIN + dry-run rollback chưa. Plan chưa preflight thì KHÔNG "
            "được trình cho engineer duyệt."
        ),
    )
    preflight_error: str = Field(
        default="",
        description="Thông báo lỗi của lần preflight gần nhất (rỗng nếu đã pass).",
    )
    plan_version: int = Field(
        default=1,
        description="1 = plan gốc, 2 = plan sau khi re-plan từ feedback của Agent 2.",
    )
    script_source: str = Field(
        default="",
        description=(
            "Ai viết script staging đang dùng: 'llm' (agent tự viết), 'llm_rewritten' "
            "(script agent viết theo bảng thật, hệ thống đổi sang shadow), 'system' "
            "(hệ thống tự dựng từ metadata dbt test), 'human' (engineer sửa tay). "
            "Ghi lại để truy vết khi hậu kiểm."
        ),
    )

    @field_validator("action_type", mode="before")
    @classmethod
    def _norm_action(cls, v: Any) -> ActionType:
        return _coerce_enum(v, ActionType, ActionType.MANUAL_FIX)

    @field_validator("risk_level", mode="before")
    @classmethod
    def _norm_risk(cls, v: Any) -> RiskLevel:
        return _coerce_enum(v, RiskLevel, RiskLevel.MEDIUM)

    @field_validator(
        "executable_command",
        "verification_sql",
        "shadow_execution_script",
        "publish_script",
        mode="before",
    )
    @classmethod
    def _clean_sql(cls, v: Any) -> str:
        """Bóc markdown fence nếu LLM trả ```sql ... ```."""
        if v is None:
            return ""
        text = str(v).strip()
        if text.startswith("```"):
            lines = [ln for ln in text.splitlines() if not ln.strip().startswith("```")]
            text = "\n".join(lines).strip()
        return text

    @property
    def statements(self) -> List[str]:
        """Tách executable_command thành từng câu lệnh SQL riêng."""
        return [s.strip() for s in self.executable_command.split(";") if s.strip()]

    @property
    def shadow_statements(self) -> List[str]:
        """Tách script staging thành từng câu lệnh riêng."""
        return [s.strip() for s in self.shadow_execution_script.split(";") if s.strip()]

    @property
    def is_stageable(self) -> bool:
        """Có đủ điều kiện để bấm [Duyệt chạy thử trên Staging] hay chưa."""
        return bool(
            self.shadow_execution_script.strip()
            and self.shadow_table_name.strip()
            and self.target_production_table.strip()
            and self.preflight_passed
        )


class AgentReport(BaseModel):
    """Báo cáo cuối cùng của Agent — đây chính là Output Contract cho Web UI."""

    model_config = ConfigDict(extra="ignore")

    incident_id: str = Field(default="UNKNOWN")
    target_table: str = Field(default="unknown")
    status: IncidentStatus = Field(default=IncidentStatus.WAITING_FOR_APPROVAL)
    diagnosis: Diagnosis
    impact: ImpactAssessment = Field(default_factory=lambda: ImpactAssessment())
    remediation: RemediationPlan = Field(default_factory=lambda: RemediationPlan())
    next_steps: List[str] = Field(default_factory=list, description="Việc cần làm tiếp / phòng ngừa.")
    agent_notes: str = Field(default="", description="Ghi chú thêm của Agent cho engineer.")
    generated_at: datetime = Field(default_factory=_utcnow)

    # Cho phép LLM đặt tên khác một chút: impact_assessment / remediation_plan
    @model_validator(mode="before")
    @classmethod
    def _accept_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        alias_map = {
            "impact_assessment": "impact",
            "remediation_plan": "remediation",
            "diagnostic": "diagnosis",
            "root_cause_analysis": "diagnosis",
        }
        for src, dst in alias_map.items():
            if src in data and dst not in data:
                data[dst] = data.pop(src)
        # Trường hợp LLM đặt diagnosis phẳng ở root
        if "diagnosis" not in data and "root_cause" in data:
            data["diagnosis"] = {
                "root_cause": data.get("root_cause", ""),
                "confidence_score": data.get("confidence_score", 0.6),
                "suspected_source": data.get("suspected_source", "UNKNOWN"),
                "evidence_summary": data.get("evidence_summary", []),
                "investigation_queries": data.get("investigation_queries", []),
            }
        return data

    @field_validator("status", mode="before")
    @classmethod
    def _norm_status(cls, v: Any) -> IncidentStatus:
        return _coerce_enum(v, IncidentStatus, IncidentStatus.WAITING_FOR_APPROVAL)

    @field_validator("next_steps", mode="before")
    @classmethod
    def _listify(cls, v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [v] if v.strip() else []
        return [str(x) for x in v]

    # -- Render cho Chainlit ----------------------------------------------------

    def to_markdown(self) -> str:
        """Render báo cáo dạng Markdown để hiển thị trên Chainlit."""
        sev = self.impact.severity.value
        sev_icon = SEVERITY_ICON.get(sev, "⚪")
        status_icon = STATUS_ICON.get(self.status.value, "•")
        conf_pct = f"{self.diagnosis.confidence_score * 100:.0f}%"

        def bullets(items: List[str], empty: str = "_(không có)_") -> str:
            return "\n".join(f"- {i}" for i in items) if items else empty

        parts = [
            f"## {status_icon} BÁO CÁO SỰ CỐ `{self.incident_id}`",
            "",
            f"| Trường | Giá trị |",
            f"| --- | --- |",
            f"| Bảng sự cố | `{self.target_table}` |",
            f"| Trạng thái | **{self.status.value}** |",
            f"| Mức độ | {sev_icon} **{sev}** |",
            f"| Số dòng vi phạm | **{self.impact.affected_row_count:,}** |",
            f"| Vi phạm SLA | {'⚠️ CÓ' if self.impact.sla_breach else 'Không'} |",
            f"| Độ tin cậy chẩn đoán | **{conf_pct}** |",
            "",
            "### 🔎 1. Chẩn đoán nguyên nhân gốc rễ",
            f"**Root cause:** {self.diagnosis.root_cause}",
            "",
            f"**Nguồn nghi vấn:** `{self.diagnosis.suspected_source}`",
            "",
            "**Bằng chứng thu được từ DuckDB:**",
            bullets(self.diagnosis.evidence_summary),
            "",
            "### 💥 2. Phạm vi ảnh hưởng (Blast Radius)",
            f"**Nghiệp vụ:** {self.impact.business_impact or '_(chưa đánh giá)_'}",
            "",
            "**Bảng hạ nguồn bị ảnh hưởng:**",
            bullets(self.impact.affected_downstream_tables),
            "",
            "**Dashboard bị ảnh hưởng:**",
            bullets(self.impact.affected_dashboards),
            "",
            "### 🛠️ 3. Kế hoạch khắc phục",
            f"**Hành động:** `{self.remediation.action_type.value}` "
            f"· **Rủi ro:** `{self.remediation.risk_level.value}`",
            "",
            self.remediation.summary or "_(chưa có mô tả)_",
            "",
        ]

        plan = self.remediation
        if plan.shadow_execution_script:
            # Kiến trúc WAP: engineer duyệt việc chạy trên bảng bóng, KHÔNG phải bảng thật.
            preflight_badge = (
                "✅ Preflight PASSED (đã EXPLAIN + dry-run, không còn Binder Error)"
                if plan.preflight_passed
                else f"❌ Preflight FAILED: {plan.preflight_error or 'chưa chạy preflight'}"
            )
            parts += [
                f"> 🛡️ **Zero Blast Radius.** Bảng production `{plan.target_production_table}` "
                f"sẽ **không bị chạm** ở bước này. Mọi lệnh vá chạy trên bảng bóng "
                f"`{plan.shadow_table_name}`.",
                "",
                f"**Kiểm tra trước khi trình anh:** {preflight_badge}",
                "",
                f"**Bước 1 — script chạy trên STAGING `{plan.shadow_table_name}` "
                "(chờ anh duyệt):**",
                "```sql",
                plan.shadow_execution_script,
                "```",
                "**Câu verify (em sẽ chạy trên bảng bóng):**",
                "```sql",
                plan.verification_sql or "-- (chưa có)",
                "```",
                f"**Bước 2 — chỉ mở sau khi Agent 2 nghiệm thu ĐẠT** (atomic swap sang "
                f"`{plan.target_production_table}`):",
                "```sql",
                plan.publish_script or "-- (sẽ sinh ở bước publish)",
                "```",
            ]
        else:
            parts += [
                "**Lệnh sẽ được thực thi trên DuckDB (chờ bạn duyệt):**",
                "```sql",
                plan.executable_command or "-- (chưa có lệnh)",
                "```",
                "**Câu lệnh verify sau khi vá:**",
                "```sql",
                plan.verification_sql or "-- (chưa có)",
                "```",
            ]

        if plan.plan_version > 1:
            parts += ["", f"> 🔁 Đây là **plan v{plan.plan_version}** (đã sửa theo phản hồi)."]
        if self.remediation.rollback_hint:
            parts += ["", f"**Rollback:** {self.remediation.rollback_hint}"]
        if self.next_steps:
            parts += ["", "### 📌 4. Việc cần làm tiếp / phòng ngừa", bullets(self.next_steps)]
        if self.agent_notes:
            parts += ["", f"> 🤖 **Ghi chú của Agent:** {self.agent_notes}"]

        parts += [
            "",
            "---",
            (
                "🧪 **Đang chờ anh duyệt BƯỚC 1 — chạy thử trên Staging.** Bảng thật chưa bị "
                "thay đổi gì cả, nên bấm duyệt ở đây là an toàn ạ. Anh vẫn có thể chat để "
                "chất vấn em (*“tại sao lại lỗi?”*, *“show thử 5 dòng dữ liệu lỗi”*) trước khi bấm."
                if self.remediation.shadow_execution_script
                else "⏸️ **Đang chờ phê duyệt (Human-in-the-loop).** Bạn có thể chat để chất vấn "
                "Agent (ví dụ: *“tại sao lại lỗi?”*, *“show thử 5 dòng dữ liệu lỗi”*) trước khi "
                "bấm duyệt."
            ),
        ]
        return "\n".join(parts)

    def to_json(self, indent: int = 2) -> str:
        return self.model_dump_json(indent=indent)


# ---------------------------------------------------------------------------
# 3b. AUDIT CONTRACT — Agent 2 (Data Auditor / Checker)
# ---------------------------------------------------------------------------


class AuditCheckItem(BaseModel):
    """
    Một hạng mục nghiệm thu độc lập.

    `query_executed` là câu SQL mà Agent 2 TỰ SINH và TỰ CHẠY trên DuckDB —
    không phải copy từ Agent 1. Đây là bằng chứng để engineer tái lập lại được.
    """

    model_config = ConfigDict(extra="ignore")

    check_name: str = Field(description="Tên hạng mục, ví dụ 'Check 1 - Cleanliness'.")
    query_executed: str = Field(default="", description="Câu SQL Agent 2 đã thực sự chạy.")
    expected_result: str = Field(default="", description="Kết quả kỳ vọng, ví dụ '0 dòng'.")
    actual_result: str = Field(default="", description="Kết quả thực tế đọc từ DuckDB.")
    passed: bool = Field(default=False, description="True nếu actual khớp expected.")

    # Các field mở rộng (tuỳ chọn) — giúp UI render đẹp, không bắt buộc với LLM
    category: str = Field(
        default="CUSTOM",
        description="CLEANLINESS | DATA_PRESERVATION | ROW_COUNT_INTEGRITY | "
        "DOWNSTREAM_CONSISTENCY | SCHEMA | CUSTOM",
    )
    severity: str = Field(
        default="BLOCKING",
        description="BLOCKING (fail là chặn) | WARNING (fail chỉ cảnh báo) | INFO.",
    )
    finding: str = Field(default="", description="Nhận xét/giải thích của Agent 2.")
    verified_by_engine: Optional[bool] = Field(
        default=None,
        description="Do hệ thống tự chạy lại bằng Python để đối chiếu, không phải LLM tự khai.",
    )

    @field_validator("passed", mode="before")
    @classmethod
    def _norm_passed(cls, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        return str(v).strip().lower() in {"true", "yes", "pass", "passed", "1", "ok", "có", "co"}

    @field_validator("category", "severity", mode="before")
    @classmethod
    def _upper(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip().upper().replace(" ", "_").replace("-", "_")

    @field_validator("query_executed", mode="before")
    @classmethod
    def _clean_sql(cls, v: Any) -> str:
        if v is None:
            return ""
        text = str(v).strip()
        if text.startswith("```"):
            lines = [ln for ln in text.splitlines() if not ln.strip().startswith("```")]
            text = "\n".join(lines).strip()
        return text

    @field_validator("expected_result", "actual_result", mode="before")
    @classmethod
    def _stringify(cls, v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False, default=str)
        return str(v)

    @property
    def is_blocking_failure(self) -> bool:
        return (not self.passed) and self.severity == "BLOCKING"

    @property
    def icon(self) -> str:
        if self.passed:
            return "✅"
        return "❌" if self.severity == "BLOCKING" else "⚠️"


class AuditReport(BaseModel):
    """
    Giấy nghiệm thu của Agent 2.

    Nguyên tắc chống "thông đồng": `verdict` KHÔNG được tin từ LLM.
    Validator luôn suy lại verdict từ danh sách `checks` — LLM khai
    AUDIT_PASSED mà còn check BLOCKING fail thì tự động bị hạ xuống AUDIT_FAILED.
    """

    model_config = ConfigDict(extra="ignore")

    audit_id: str = Field(
        default_factory=lambda: f"AUD-{_utcnow():%Y%m%d}-{uuid.uuid4().hex[:6].upper()}"
    )
    target_table: str = Field(default="unknown", description="Bảng chính được nghiệm thu.")
    quarantine_table: str = Field(
        default="",
        description="Bảng cách ly liên quan. Để rỗng/'N/A' nếu remediation không dùng quarantine.",
    )
    verdict: Literal["AUDIT_PASSED", "AUDIT_FAILED"] = Field(default="AUDIT_FAILED")
    checks: List[AuditCheckItem] = Field(default_factory=list)
    certification_summary: str = Field(
        default="", description="Kết luận nghiệm thu bằng ngôn ngữ cho engineer."
    )

    # Field mở rộng (tuỳ chọn)
    audited_incident_id: str = Field(default="")
    auditor_model: str = Field(
        default="",
        description="Model đã thực hiện nghiệm thu. Ghi lại để truy vết cross-model "
        "checking: biết rõ Checker soi bằng model nào, khác Maker hay không.",
    )
    auditor_notes: str = Field(default="")
    recommended_action: str = Field(
        default="",
        description="Việc engineer nên làm tiếp: ACCEPT | ROLLBACK | INVESTIGATE | ESCALATE.",
    )
    generated_at: datetime = Field(default_factory=_utcnow)

    # -- Write–Audit–Publish -------------------------------------------------
    shadow_table: str = Field(
        default="",
        description="Bảng bóng đã được nghiệm thu. Agent 2 soi shadow, KHÔNG soi bảng thật.",
    )
    is_ready_for_production: bool = Field(
        default=False,
        description=(
            "Cổng mở nút [Publish to Production]. Do validator tự suy từ `checks`, "
            "KHÔNG nhận giá trị từ LLM — nên LLM không thể tự mở cổng publish."
        ),
    )
    failed_details: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Hồ sơ lỗi máy đọc được, gửi ngược cho Agent 1 để re-plan: error_message, "
            "failed_checks, missing_columns, remaining_violations, root_cause_hint."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _accept_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        aliases = {
            "audit_checks": "checks",
            "check_items": "checks",
            "results": "checks",
            "summary": "certification_summary",
            "conclusion": "certification_summary",
            "table": "target_table",
            "incident_id": "audited_incident_id",
        }
        for src, dst in aliases.items():
            if src in data and dst not in data:
                data[dst] = data.pop(src)
        if isinstance(data.get("checks"), dict):
            # LLM đôi khi trả dict {check_name: {...}} thay vì list
            data["checks"] = [
                {**v, "check_name": v.get("check_name", k)} if isinstance(v, dict) else {"check_name": k}
                for k, v in data["checks"].items()
            ]
        return data

    @field_validator("verdict", mode="before")
    @classmethod
    def _norm_verdict(cls, v: Any) -> str:
        token = str(v or "").strip().upper().replace(" ", "_").replace("-", "_")
        if token in {"AUDIT_PASSED", "AUDIT_FAILED"}:
            return token
        if any(k in token for k in ("PASS", "OK", "CERTIFIED", "APPROVED", "SUCCESS")):
            return "AUDIT_PASSED"
        return "AUDIT_FAILED"

    @field_validator("quarantine_table", mode="before")
    @classmethod
    def _norm_quarantine(cls, v: Any) -> str:
        if v is None:
            return ""
        text = str(v).strip()
        return "" if text.upper() in {"N/A", "NONE", "NULL", "-"} else text

    @model_validator(mode="after")
    def _derive_verdict(self) -> "AuditReport":
        """
        Chốt lại verdict theo BẰNG CHỨNG, không theo lời LLM:
        - Không có check nào -> FAILED (không nghiệm thu khống được).
        - Còn bất kỳ check BLOCKING fail -> FAILED.
        """
        if not self.checks:
            self.verdict = "AUDIT_FAILED"
        elif any(c.is_blocking_failure for c in self.checks):
            self.verdict = "AUDIT_FAILED"
        else:
            self.verdict = "AUDIT_PASSED"

        # Cổng publish suy từ cùng một bằng chứng, không phải một cờ riêng mà LLM set
        # được. Hai nguồn sự thật cho cùng một quyết định là chỗ để lỗi lọt qua.
        self.is_ready_for_production = self.verdict == "AUDIT_PASSED"

        # Khi FAIL: tự đóng gói hồ sơ lỗi máy đọc được cho vòng re-plan.
        if not self.is_ready_for_production:
            self.failed_details = self._build_failed_details(self.failed_details)
        else:
            self.failed_details = None
        return self

    def _build_failed_details(
        self, existing: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Gói lý do thất bại thành dict để Agent 1 đọc được bằng máy.

        Quan trọng là phần `missing_columns`: lỗi kinh điển của vòng trước là câu SQL
        tham chiếu cột không tồn tại. Bóc sẵn tên cột từ thông báo Binder Error giúp
        Agent 1 biết phải `DESCRIBE` bảng nào thay vì đoán lại từ đầu.
        """
        blocking = [c for c in self.checks if c.is_blocking_failure]
        failed = [c for c in self.checks if not c.passed]
        messages = [
            f"{c.check_name}: kỳ vọng {c.expected_result or 'N/A'}, "
            f"thực tế {c.actual_result or 'N/A'}"
            for c in (blocking or failed)
        ]

        missing_columns: List[str] = []
        remaining_violations: Optional[int] = None
        for check in failed:
            blob = f"{check.actual_result} {check.finding}"
            for match in re.finditer(
                r'column\s+"?([A-Za-z_][A-Za-z_0-9]*)"?\s+(?:does not exist|not found)',
                blob,
                re.IGNORECASE,
            ):
                if match.group(1) not in missing_columns:
                    missing_columns.append(match.group(1))
            for match in re.finditer(
                r'Referenced column "([^"]+)" not found', blob, re.IGNORECASE
            ):
                if match.group(1) not in missing_columns:
                    missing_columns.append(match.group(1))
            if check.category == "CLEANLINESS" and remaining_violations is None:
                # LLM viết số dòng vi phạm theo nhiều kiểu: "63 dòng vi phạm",
                # "63 dong vi pham" (mất dấu), "63 rows violating". Bắt cả ba dạng —
                # con số này là thứ Agent 1 cần để biết đã sửa hết chưa.
                found = re.search(
                    r"(\d+)\s*(?:dòng|dong|row|rows|record|records)",
                    str(check.actual_result),
                    re.IGNORECASE,
                )
                if found:
                    remaining_violations = int(found.group(1))

        details: Dict[str, Any] = dict(existing or {})
        details.update(
            {
                "audit_id": self.audit_id,
                "verdict": self.verdict,
                "target_table": self.target_table,
                "shadow_table": self.shadow_table,
                "error_message": "; ".join(messages)[:1500] or "Không có hạng mục nào đạt.",
                "failed_checks": [
                    {
                        "check_name": c.check_name,
                        "category": c.category,
                        "severity": c.severity,
                        "expected": c.expected_result,
                        "actual": c.actual_result,
                        "query": c.query_executed,
                        "finding": c.finding,
                    }
                    for c in failed
                ],
                "blocking_count": len(blocking),
                "recommended_action": self.recommended_action or "INVESTIGATE",
            }
        )
        if missing_columns:
            details["missing_columns"] = missing_columns
            details["root_cause_hint"] = (
                f"SQL tham chiếu cột không tồn tại: {', '.join(missing_columns)}. "
                "Hãy DESCRIBE lại bảng liên quan trước khi viết script v2."
            )
        if remaining_violations is not None:
            details["remaining_violations"] = remaining_violations
        return details

    # -- Thống kê & render ------------------------------------------------------

    @property
    def passed_count(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def failed_checks(self) -> List[AuditCheckItem]:
        return [c for c in self.checks if not c.passed]

    @property
    def blocking_failures(self) -> List[AuditCheckItem]:
        return [c for c in self.checks if c.is_blocking_failure]

    def to_markdown(self) -> str:
        """Render giấy nghiệm thu cho Chainlit (giọng 'em', nhiều icon)."""
        ok = self.verdict == "AUDIT_PASSED"
        head_icon = "🎖️" if ok else "🚨"
        badge = "✅ **AUDIT PASSED**" if ok else "❌ **AUDIT FAILED**"

        parts = [
            f"## {head_icon} BIÊN BẢN NGHIỆM THU ĐỘC LẬP `{self.audit_id}`",
            "",
            "> 🕵️‍♀️ Em là **Data Auditor** — em kiểm tra độc lập, **không tin** báo cáo của "
            "anh SRE Agent ạ. Mọi con số dưới đây em tự query DuckDB để lấy 💪",
            "",
            "| Trường | Giá trị |",
            "| --- | --- |",
            f"| 🎯 Sự cố | `{self.audited_incident_id or 'N/A'}` |",
            f"| 🧠 Model nghiệm thu | `{self.auditor_model or 'N/A'}` |",
            f"| 🗂️ Bảng chính | `{self.target_table}` |",
            (
                f"| 🧪 Bảng em đã soi | `{self.shadow_table}` (staging — bảng thật chưa bị "
                "thay đổi) |"
                if self.shadow_table
                else f"| 🧪 Bảng em đã soi | `{self.target_table}` |"
            ),
            f"| 🧊 Bảng cách ly | `{self.quarantine_table or '(không dùng quarantine)'}` |",
            f"| 🧾 Kết luận | {badge} |",
            f"| 📊 Hạng mục đạt | **{self.passed_count}/{len(self.checks)}** |",
            "",
            "### 🔬 Chi tiết các hạng mục em đã kiểm",
        ]

        for idx, check in enumerate(self.checks, start=1):
            engine_note = ""
            if check.verified_by_engine is True:
                engine_note = " · 🤖 _đã đối chiếu lại bằng Python_"
            elif check.verified_by_engine is False:
                engine_note = " · ⚠️ _máy đối chiếu ra kết quả KHÁC_"
            parts += [
                "",
                f"**{check.icon} {idx}. {check.check_name}** "
                f"`[{check.category}]` `[{check.severity}]`{engine_note}",
                "",
                f"- 🎯 Kỳ vọng: `{check.expected_result or 'N/A'}`",
                f"- 📈 Thực tế: `{check.actual_result or 'N/A'}`",
            ]
            if check.finding:
                parts.append(f"- 💬 Nhận xét: {check.finding}")
            if check.query_executed:
                parts += ["", "```sql", check.query_executed, "```"]

        parts += ["", "### 📜 Kết luận nghiệm thu", self.certification_summary or "_(chưa có)_"]

        if self.blocking_failures:
            parts += [
                "",
                "### 🛑 Hạng mục chặn (phải xử lý ngay)",
                *[f"- ❌ **{c.check_name}**: {c.finding or c.actual_result}" for c in self.blocking_failures],
            ]
        if self.recommended_action:
            parts += ["", f"### 🧭 Em đề xuất: **{self.recommended_action}**"]
        if self.auditor_notes:
            parts += ["", f"> 📝 **Ghi chú của em:** {self.auditor_notes}"]

        if self.failed_details:
            hint = self.failed_details.get("root_cause_hint")
            missing = self.failed_details.get("missing_columns")
            parts += ["", "### 🔬 Hồ sơ lỗi em gửi lại cho anh SRE Agent"]
            if missing:
                parts.append(f"- 🧩 Cột không tồn tại: `{', '.join(missing)}`")
            if hint:
                parts.append(f"- 💡 {hint}")
            if self.failed_details.get("remaining_violations") is not None:
                parts.append(
                    f"- 🩸 Còn **{self.failed_details['remaining_violations']}** dòng vi phạm "
                    "trên bảng bóng"
                )

        parts += [
            "",
            "---",
            (
                (
                    "🎉 Bảng bóng đã sạch và không mất mát gì hết ạ — em bật cổng "
                    "**[🚀 Publish to Production]** cho anh rồi nhen! 💚"
                    if self.shadow_table
                    else "🎉 Dữ liệu đã sạch và không mất mát gì hết ạ, anh yên tâm ký nghiệm "
                    "thu nhen! 💚"
                )
                if ok
                else (
                    "😰 Em **chưa dám** cấp chứng nhận nên cổng publish vẫn đóng ạ. Bảng thật "
                    "vẫn nguyên vẹn 100%, anh chọn 1 trong 3: cho anh SRE Agent **re-plan**, "
                    "**sửa SQL tay**, hoặc **huỷ & dọn staging** nhé! 🙏"
                    if self.shadow_table
                    else "😰 Em **chưa dám** cấp chứng nhận đâu ạ. Anh xem mục chặn ở trên rồi "
                    "cân nhắc rollback theo `rollback_hint` của anh SRE Agent nhé! 🙏"
                )
            ),
        ]
        return "\n".join(parts)

    def to_json(self, indent: int = 2) -> str:
        return self.model_dump_json(indent=indent)


# ---------------------------------------------------------------------------
# 4. JSON template dùng để "ép" LLM trả đúng cấu trúc
# ---------------------------------------------------------------------------

REPORT_JSON_TEMPLATE = """{
  "incident_id": "string",
  "target_table": "string",
  "status": "WAITING_FOR_APPROVAL",
  "diagnosis": {
    "root_cause": "string - nguyên nhân gốc rễ, phải kèm số liệu thật lấy từ query",
    "confidence_score": 0.0,
    "suspected_source": "string - Upstream API | ETL job | Source DB | Infra",
    "evidence_summary": ["string - bằng chứng số liệu cụ thể"],
    "investigation_queries": ["string - SQL đã chạy"]
  },
  "impact": {
    "severity": "LOW | MEDIUM | HIGH | CRITICAL",
    "affected_row_count": 0,
    "affected_downstream_tables": ["string"],
    "affected_dashboards": ["string"],
    "sla_breach": false,
    "business_impact": "string"
  },
  "remediation": {
    "action_type": "QUARANTINE_DATA | BACKFILL | RERUN_PIPELINE | SCALE_RESOURCE | MANUAL_FIX",
    "summary": "string - giải thích cho engineer",
    "target_production_table": "string - bảng thật, ví dụ 'stg_orders' (KHÔNG ghi vào bảng này)",
    "shadow_table_name": "string - bảng bóng, PHẢI là 'shadow_' + tên bảng thật",
    "shadow_execution_script": "string - script CHỈ ghi vào shadow_* / quarantine_*: (1) CREATE OR REPLACE TABLE shadow_x AS SELECT * FROM x; (2) CREATE OR REPLACE TABLE quarantine_x AS SELECT *, 'REASON'::VARCHAR AS quarantine_reason, CURRENT_TIMESTAMP AS quarantined_at FROM x WHERE <dòng bẩn>; (3) DELETE FROM shadow_x WHERE <dòng bẩn>. TUYỆT ĐỐI KHÔNG rebuild bảng mart_* ở đây.",
    "executable_command": "string - để rỗng: kiến trúc WAP không cho ghi trực tiếp bảng thật",
    "verification_sql": "string - SELECT COUNT(*) ... kỳ vọng = 0, viết theo tên bảng thật, hệ thống tự đổi sang shadow",
    "rollback_hint": "string - ở WAP thì rollback = drop shadow table, bảng thật không bị chạm",
    "risk_level": "LOW | MEDIUM | HIGH",
    "requires_human_approval": true
  },
  "next_steps": ["string"],
  "agent_notes": "string"
}"""


AUDIT_JSON_TEMPLATE = """{
  "audited_incident_id": "string - mã sự cố đang nghiệm thu",
  "target_table": "string - bảng chính đã được vá",
  "quarantine_table": "string - bảng cách ly, để \\"\\" nếu remediation không dùng quarantine",
  "verdict": "AUDIT_PASSED | AUDIT_FAILED",
  "checks": [
    {
      "check_name": "string - ví dụ 'Check 1 - Cleanliness: không còn dòng vi phạm'",
      "category": "CLEANLINESS | DATA_PRESERVATION | ROW_COUNT_INTEGRITY | DOWNSTREAM_CONSISTENCY | SCHEMA | CUSTOM",
      "severity": "BLOCKING | WARNING | INFO",
      "query_executed": "string - SQL bạn ĐÃ THỰC SỰ chạy qua tool_query_duckdb",
      "expected_result": "string - ví dụ '0 dòng vi phạm'",
      "actual_result": "string - số liệu THẬT đọc từ kết quả tool",
      "passed": true,
      "finding": "string - nhận xét ngắn"
    }
  ],
  "certification_summary": "string - kết luận nghiệm thu cho engineer",
  "recommended_action": "ACCEPT | ROLLBACK | INVESTIGATE | ESCALATE",
  "auditor_notes": "string"
}"""


# ---------------------------------------------------------------------------
# 5. REQUEST CONTRACT — thao tác cứu hộ do engineer bấm trên UI
# ---------------------------------------------------------------------------


class ReplanRequest(BaseModel):
    """
    Engineer bấm `[🤖 Cho Agent 1 Re-plan]`.

    `retry_count` là chốt cứng của Bounded Reflection Loop: agent chỉ được sửa lại
    **một lần**. Không giới hạn thì một agent hiểu sai schema sẽ lặp vô hạn, đốt token
    và giữ sự cố ở trạng thái lửng lơ. Hết lượt thì bắt buộc chuyển cho người.
    """

    model_config = ConfigDict(extra="ignore")

    incident_id: str = Field(description="Mã sự cố cần lập lại kế hoạch.")
    feedback: str = Field(
        default="",
        description="Phản hồi cho Agent 1: thường là `failed_details` của Agent 2 dạng text.",
    )
    retry_count: int = Field(
        default=0, ge=0, description="Số lần đã re-plan trước đó. >= MAX_REPLAN thì từ chối."
    )
    failed_details: Optional[Dict[str, Any]] = Field(
        default=None, description="Hồ sơ lỗi máy đọc được từ AuditReport."
    )


class ManualOverrideRequest(BaseModel):
    """
    Engineer bấm `[✏️ Sửa SQL thủ công]` và nộp SQL tự viết.

    SQL này vẫn phải đi qua đúng các cổng như script của agent: guard phạm vi ghi
    (chỉ staging) và preflight. Người sửa tay cũng gõ sai cột như agent, và sự cố ở
    môi trường thật không phân biệt lỗi do ai gây ra.
    """

    model_config = ConfigDict(extra="ignore")

    incident_id: str = Field(description="Mã sự cố đang xử lý.")
    custom_sql: str = Field(description="Script SQL do engineer tự viết, chạy trên staging.")
    note: str = Field(default="", description="Ghi chú của engineer, vào audit log.")
    rerun_audit: bool = Field(
        default=True, description="Chạy lại Agent 2 nghiệm thu sau khi áp SQL sửa tay."
    )

    @field_validator("custom_sql", mode="before")
    @classmethod
    def _clean(cls, v: Any) -> str:
        text = str(v or "").strip()
        if text.startswith("```"):
            text = "\n".join(
                ln for ln in text.splitlines() if not ln.strip().startswith("```")
            ).strip()
        return text


#: Số lần re-plan tối đa cho một sự cố (Bounded Reflection Loop).
MAX_REPLAN_ATTEMPTS = 1


__all__ = [
    "IncidentType",
    "Severity",
    "ActionType",
    "RiskLevel",
    "IncidentStatus",
    "SEVERITY_ICON",
    "STATUS_ICON",
    "STATUS_LABEL",
    "AWAITING_HUMAN_STATUSES",
    "TERMINAL_STATUSES",
    "IncidentInput",
    "Diagnosis",
    "ImpactAssessment",
    "RemediationPlan",
    "AgentReport",
    "AuditCheckItem",
    "AuditReport",
    "ReplanRequest",
    "ManualOverrideRequest",
    "MAX_REPLAN_ATTEMPTS",
    "REPORT_JSON_TEMPLATE",
    "AUDIT_JSON_TEMPLATE",
]
