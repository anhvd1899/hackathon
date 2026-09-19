"""
SCOPE: AI — Agent / LLM
=======================

Owner: AI Engineer.

Toàn bộ phần "não" của hệ thống. Triển khai NATIVE PYTHON + OpenAI SDK
(tool calling), **không dùng LangChain / LangGraph / CrewAI**.

    ai/
    ├── llm.py        LLMSettings (MaaS config), luân chuyển/failover model, ToolEvent
    ├── schemas.py    Data contract Pydantic v2 (IncidentInput, AgentReport, AuditReport)
    ├── tools.py      Tool schema + guard an toàn SQL + phân quyền tool theo vai
    ├── agent.py      Agent 1 — Data SRE Agent (MAKER): điều tra, đề xuất, vá dữ liệu
    ├── auditor.py    Agent 2 — Data Auditor (CHECKER): nghiệm thu độc lập
    ├── check_llm.py  Smoke test kết nối MaaS + tool calling cho mọi model sẽ dùng
    └── runbooks/     Tri thức nội bộ agent được đọc (SLA, lineage, playbook...)

QUY TẮC: package này được import `data/` (để chạy SQL) nhưng **KHÔNG được import `web/`**.
Nhờ vậy AI Engineer sửa prompt / thêm tool / đổi model mà không cần chạy UI.

Chạy thử nhanh:
    python -m ai.check_llm      # kiểm model + tool calling
    python -m ai.agent          # Agent 1 điều tra (in báo cáo ra console)
    python -m ai.auditor        # Agent 2 nghiệm thu
"""

from __future__ import annotations

from ai.agent import DataReliabilityAgent, run_headless  # noqa: F401
from ai.auditor import DataAuditorAgent, run_audit_headless  # noqa: F401
from ai.llm import LLMSettings, ToolEvent  # noqa: F401
from ai.schemas import AgentReport, AuditReport, IncidentInput, IncidentStatus  # noqa: F401

__all__ = [
    "DataReliabilityAgent",
    "DataAuditorAgent",
    "run_headless",
    "run_audit_headless",
    "LLMSettings",
    "ToolEvent",
    "IncidentInput",
    "AgentReport",
    "AuditReport",
    "IncidentStatus",
]
