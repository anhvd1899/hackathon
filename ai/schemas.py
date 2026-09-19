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
    INVESTIGATING = "INVESTIGATING"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    EXECUTING = "EXECUTING"
    RESOLVED = "RESOLVED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


# Map severity -> icon để UI hiển thị nhanh
SEVERITY_ICON: Dict[str, str] = {
    "LOW": "🟢",
    "MEDIUM": "🟡",
    "HIGH": "🟠",
    "CRITICAL": "🔴",
}

STATUS_ICON: Dict[str, str] = {
    "INVESTIGATING": "🔍",
    "WAITING_FOR_APPROVAL": "⏸️",
    "EXECUTING": "⚙️",
    "RESOLVED": "✅",
    "REJECTED": "🚫",
    "FAILED": "❌",
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

    @field_validator("action_type", mode="before")
    @classmethod
    def _norm_action(cls, v: Any) -> ActionType:
        return _coerce_enum(v, ActionType, ActionType.MANUAL_FIX)

    @field_validator("risk_level", mode="before")
    @classmethod
    def _norm_risk(cls, v: Any) -> RiskLevel:
        return _coerce_enum(v, RiskLevel, RiskLevel.MEDIUM)

    @field_validator("executable_command", "verification_sql", mode="before")
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
            "**Lệnh sẽ được thực thi trên DuckDB (chờ bạn duyệt):**",
            "```sql",
            self.remediation.executable_command or "-- (chưa có lệnh)",
            "```",
            "**Câu lệnh verify sau khi vá:**",
            "```sql",
            self.remediation.verification_sql or "-- (chưa có)",
            "```",
        ]
        if self.remediation.rollback_hint:
            parts += ["", f"**Rollback:** {self.remediation.rollback_hint}"]
        if self.next_steps:
            parts += ["", "### 📌 4. Việc cần làm tiếp / phòng ngừa", bullets(self.next_steps)]
        if self.agent_notes:
            parts += ["", f"> 🤖 **Ghi chú của Agent:** {self.agent_notes}"]

        parts += [
            "",
            "---",
            "⏸️ **Đang chờ phê duyệt (Human-in-the-loop).** Bạn có thể chat để chất vấn Agent "
            "(ví dụ: *“tại sao lại lỗi?”*, *“show thử 5 dòng dữ liệu lỗi”*) trước khi bấm duyệt.",
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
        return self

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

        parts += [
            "",
            "---",
            (
                "🎉 Dữ liệu đã sạch và không mất mát gì hết ạ, anh yên tâm ký nghiệm thu nhen! 💚"
                if ok
                else "😰 Em **chưa dám** cấp chứng nhận đâu ạ. Anh xem mục chặn ở trên rồi "
                "cân nhắc rollback theo `rollback_hint` của anh SRE Agent nhé! 🙏"
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
    "executable_command": "string - SQL DuckDB hợp lệ, nhiều câu tách bằng ';'",
    "verification_sql": "string - SELECT COUNT(*) ... kỳ vọng = 0 sau khi vá",
    "rollback_hint": "string",
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


__all__ = [
    "IncidentType",
    "Severity",
    "ActionType",
    "RiskLevel",
    "IncidentStatus",
    "SEVERITY_ICON",
    "STATUS_ICON",
    "IncidentInput",
    "Diagnosis",
    "ImpactAssessment",
    "RemediationPlan",
    "AgentReport",
    "AuditCheckItem",
    "AuditReport",
    "REPORT_JSON_TEMPLATE",
    "AUDIT_JSON_TEMPLATE",
]
