"""
web/frontend/rendering.py — Hàm render cho UI
=============================================

Tách riêng phần "biến dữ liệu thành markdown/nút" khỏi phần điều phối luồng chat
(`ui.py`), để Web Engineer sửa cách trình bày mà không phải đọc logic HITL.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import chainlit as cl

import config
from ai import tools
from ai.llm import ToolEvent
from data import audit as data_audit
from data import connection as db

AUTHOR_SRE = "Data SRE Agent"
AUTHOR_AUDITOR = "Data Auditor"
AUTHOR_SYSTEM = "System"
AUTHOR_ALERT = "dbt alert"
AUTHOR_ENGINEER = "Engineer"

STEP_ICON: Dict[str, str] = {
    "tool_query_duckdb": "🦆",
    "tool_read_runbook": "📖",
    "tool_execute_remediation": "🛠️",
    "tool_verify_health": "🧪",
    "tool_get_incident_context": "🧭",
}


# ---------------------------------------------------------------------------
# 1. Bảng markdown
# ---------------------------------------------------------------------------


def markdown_table(rows: List[Dict[str, Any]], limit: int = 15) -> str:
    """Render list[dict] thành bảng markdown."""
    if not rows:
        return "_(không có dòng nào)_"
    visible = rows[:limit]
    cols = list(visible[0].keys())
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for row in visible:
        lines.append(
            "| " + " | ".join("NULL" if row.get(c) is None else str(row.get(c)) for c in cols) + " |"
        )
    if len(rows) > limit:
        lines.append(f"_… và {len(rows) - limit} dòng nữa_")
    return "\n".join(lines)


def format_tool_output(event: ToolEvent) -> str:
    """Render kết quả một tool call thành markdown gọn cho Step trên UI."""
    res = event.result
    if not res.get("ok"):
        return f"❌ {res.get('error', 'lỗi không rõ')}"

    if event.name == "tool_query_duckdb":
        rows = res.get("rows") or []
        return f"✅ {res.get('row_count', 0)} dòng trả về\n\n" + markdown_table(rows)

    if event.name == "tool_read_runbook":
        content = str(res.get("content", ""))
        return f"✅ Đã đọc runbook `{res.get('topic')}` ({len(content)} ký tự)"

    if event.name == "tool_execute_remediation":
        return f"✅ {res.get('message', '')} ({res.get('statements_executed', 0)} câu lệnh)"

    if event.name == "tool_verify_health":
        return f"{'✅' if res.get('healthy') else '⚠️'} {res.get('verdict', '')}"

    if event.name == "tool_get_incident_context":
        baseline = res.get("baseline_metrics") or {}
        lines = [
            f"✅ baseline: {'có' if res.get('baseline_available') else 'KHÔNG có'}"
            f" · quarantine phát hiện: `{res.get('detected_quarantine_table') or 'không có'}`",
            "",
        ]
        if baseline:
            lines += ["| metric (trước khi vá) | value |", "| --- | --- |"]
            lines += [f"| {k} | {v} |" for k, v in baseline.items()]
        executed = res.get("remediation_actually_executed") or []
        lines += ["", f"Số lệnh ghi/verify đã thực sự chạy theo audit log: **{len(executed)}**"]
        return "\n".join(lines)

    return "✅ " + json.dumps(res, ensure_ascii=False, default=str)[:500]


#: Chainlit chỉ phục vụ avatar khi tên khớp `^[a-zA-Z0-9_ .-]+$`
#: (xem `chainlit/server.py::get_avatar`) — tên step chứa emoji/SQL (`*`, `(`, `,`, `…`)
#: sẽ bị frontend GET `/avatars/<tên>` rồi nhận 400. Nên tên step phải ngắn + ASCII.
_AVATAR_SAFE_RE = re.compile(r"[^a-zA-Z0-9_ .\-]+")


def step_name(event: ToolEvent) -> str:
    """Tên ngắn, ASCII-safe cho `cl.Step` — SQL đầy đủ đã nằm trong `step.input`."""
    base = {
        "tool_query_duckdb": "DuckDB query",
        "tool_read_runbook": f"Read runbook {event.arguments.get('topic', '?')}",
        "tool_execute_remediation": "Execute remediation",
        "tool_verify_health": f"Verify {event.arguments.get('table_name', '?')}",
        "tool_get_incident_context": "Incident context",
    }.get(event.name, event.name)
    safe = _AVATAR_SAFE_RE.sub("", base).strip()
    return (safe[:80] or event.name)


async def render_tool_step(event: ToolEvent) -> None:
    """Hiển thị 1 tool call thành 1 Step có thể bấm mở xem chi tiết."""
    async with cl.Step(name=step_name(event), type="tool") as step:
        step.input = json.dumps(event.arguments, ensure_ascii=False, indent=2)[:2000]
        step.output = format_tool_output(event)


# ---------------------------------------------------------------------------
# 2. Nút hành động (Human-in-the-loop)
# ---------------------------------------------------------------------------


def publish_actions() -> List[cl.Action]:
    """
    **BƯỚC 2** — nút Publish, chỉ hiện sau khi Agent 2 nghiệm thu đạt.

    Tách thành hàm riêng (thay vì thêm nút vào `approval_actions`) để không có đường
    nào hiện nút này trước khi có chứng nhận.
    """
    return [
        cl.Action(
            name="publish_prod",
            payload={"decision": "publish"},
            label="🚀 PUBLISH TO PRODUCTION (Bảng Thật)",
            tooltip="Atomic swap bảng bóng thành bảng thật trong một transaction",
        ),
        cl.Action(
            name="cancel_shadow",
            payload={"decision": "cancel"},
            label="🛑 Huỷ & Xoá Staging",
            tooltip="Drop bảng bóng, bảng thật không bị thay đổi",
        ),
    ]


def triage_actions(can_replan: bool = True, retry_count: int = 0) -> List[cl.Action]:
    """
    Ba lựa chọn cứu hộ khi Agent 2 nghiệm thu KHÔNG ĐẠT.

    Không để engineer ở ngõ cụt: mỗi nhánh là một đường đi tiếp rõ ràng. Nút re-plan
    biến mất khi hết lượt (Bounded Reflection Loop) thay vì báo lỗi sau khi bấm.
    """
    actions: List[cl.Action] = []
    if can_replan:
        actions.append(
            cl.Action(
                name="replan_agent",
                payload={"decision": "replan"},
                label=f"🤖 Cho Agent 1 Re-plan ({retry_count}/1)",
                tooltip="Gửi mã lỗi của Agent 2 để Agent 1 soi DESCRIBE và viết script v2",
            )
        )
    actions.append(
        cl.Action(
            name="cancel_shadow",
            payload={"decision": "cancel"},
            label="🛑 Huỷ Bỏ & Xoá Staging",
            tooltip="Drop bảng bóng, đóng sự cố an toàn — bảng thật chưa từng bị chạm",
        )
    )
    return actions


def approval_actions() -> List[cl.Action]:
    """2 nút duyệt/từ chối cho Agent 1 — BƯỚC 1: chỉ chạy trên bảng bóng."""
    return [
        cl.Action(
            name="approve",
            payload={"decision": "approve"},
            label="🧪 Duyệt Chạy Thử Trên Staging",
            tooltip=(
                "Chạy script vá trên shadow_<table>. Bảng production KHÔNG bị chạm; "
                "Agent 2 sẽ nghiệm thu trên bảng bóng trước khi mở nút Publish."
            ),
        ),
        cl.Action(
            name="reject",
            payload={"decision": "reject"},
            label="❌ Từ chối",
            tooltip="Không thực thi, yêu cầu Agent 1 đề xuất phương án khác",
        ),
    ]


def audit_actions() -> List[cl.Action]:
    """Nút chạy nghiệm thu độc lập (Agent 2) — dùng được bất cứ lúc nào."""
    return [
        cl.Action(
            name="audit",
            payload={"decision": "audit"},
            label="🕵️‍♀️ Nghiệm thu độc lập (Agent 2)",
            tooltip="Agent 2 tự query DuckDB kiểm tra lại, không tin báo cáo của Agent 1",
        )
    ]


def recheck_decision_actions(allow_skip: bool = True) -> List[cl.Action]:
    """
    Điểm quyết định SAU KHI Agent 1 đã vá xong: engineer chọn có recheck hay không.

    Đây là human-in-the-loop thứ hai. Với lỗi đơn giản mà engineer đã biết rõ, bỏ qua
    recheck giúp tiết kiệm thời gian và token (Agent 2 tốn thêm ~30-60k token/lượt).

    `allow_skip=False` khi remediation THẤT BẠI — lúc đó chốt luôn là sai, nên chỉ cho
    recheck hoặc escalate.
    """
    actions = [
        cl.Action(
            name="audit",
            payload={"decision": "recheck"},
            label="🕵️‍♀️ Có, recheck độc lập đi",
            tooltip="Agent 2 tự viết SQL kiểm lại: dữ liệu còn bẩn không, có mất dòng nào không",
        )
    ]
    if allow_skip:
        actions.append(
            cl.Action(
                name="skip_audit",
                payload={"decision": "skip"},
                label="⚡ Không cần, chốt luôn",
                tooltip="Đóng incident ngay mà không chạy Agent 2 (dùng khi lỗi đơn giản, đã rõ)",
            )
        )
    return actions


# ---------------------------------------------------------------------------
# 3. Ảnh chụp warehouse
# ---------------------------------------------------------------------------


def warehouse_snapshot(target_table: str) -> str:
    """
    Bảng so sánh bảng chính vs bảng quarantine sau remediation.
    Tên bảng suy ra động (không hardcode) nên dùng được cho mọi sự cố.
    """
    bare = (target_table or "").split(".")[-1]
    quarantine = data_audit.detect_quarantine_table(target_table)
    targets = [t for t in (bare, quarantine) if t]
    if not targets:
        return ""
    lines = ["| bảng | số dòng |", "| --- | --- |"]
    for table in targets:
        icon = "🧊" if table == quarantine else "🗂️"
        lines.append(f"| {icon} `{table}` | {db.row_count(table)} |")
    return "\n".join(lines)


def brain_badge(model: str, offline: bool) -> str:
    return f"🟢 `{model}`" if not offline else "🟡 OFFLINE (mô phỏng)"


def welcome_message(
    maker_model: str,
    maker_offline: bool,
    checker_model: str,
    checker_offline: bool,
    checker_pool: Optional[List[str]] = None,
) -> str:
    """Tin nhắn mở đầu: ai làm gì, brain nào, quyền gì."""
    same_model = maker_model == checker_model
    if maker_offline:
        note = ""
    elif same_model:
        note = (
            "\n\n> ⚠️ Hai agent đang dùng **chung model** nên có cùng điểm mù. Set "
            "`DRA_AUDITOR_MODEL` hoặc `DRA_MODEL_POOL` trong `.env` để bật cross-model."
        )
    else:
        note = (
            "\n\n> 🔀 **Cross-model checking đang bật**: Checker soi bằng model khác Maker "
            "nên lỗi của model này khó lọt qua model kia."
        )
        if checker_pool and len(checker_pool) > 1:
            note += f" Pool luân chuyển: `{'`, `'.join(checker_pool)}`."

    return (
        "## 🛡️ Data Reliability Squad — Maker · Checker\n\n"
        "| Vai | Agent | Brain | Quyền trên DuckDB |\n"
        "| --- | --- | --- | --- |\n"
        f"| 👷‍♀️ **Maker** | Agent 1 — Data SRE Agent | {brain_badge(maker_model, maker_offline)} | "
        "đọc + **ghi** (chỉ sau khi anh duyệt) |\n"
        f"| 🕵️‍♀️ **Checker** | Agent 2 — Data Auditor | {brain_badge(checker_model, checker_offline)} | "
        "**chỉ đọc**, nghiệm thu độc lập |\n\n"
        f"- 🗄️ Warehouse: `{config.DUCKDB_PATH}` (DuckDB, vừa là Source vừa là Sink)\n"
        f"- 📖 Runbook khả dụng: `{'`, `'.join(tools.list_runbook_topics())}`"
        f"{note}\n\n"
        "📥 Đang nhận incident từ hàng đợi alert…"
    )


__all__ = [
    "AUTHOR_SRE",
    "AUTHOR_AUDITOR",
    "AUTHOR_SYSTEM",
    "AUTHOR_ALERT",
    "AUTHOR_ENGINEER",
    "markdown_table",
    "format_tool_output",
    "render_tool_step",
    "approval_actions",
    "publish_actions",
    "triage_actions",
    "audit_actions",
    "warehouse_snapshot",
    "brain_badge",
    "welcome_message",
]
