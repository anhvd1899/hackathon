"""
web/frontend/ui.py — Chainlit app (Human-in-the-loop)
=====================================================

Đây là **target của `mount_chainlit`** (xem `web/server.py`). File này chỉ điều phối
luồng hội thoại; phần render markdown/nút nằm ở `rendering.py`.

Luồng Maker–Checker trên UI:

    on_chat_start        nạp incident -> Agent 1 điều tra (stream từng tool call)
                         -> báo cáo + [✅ Duyệt] / [❌ Từ chối]
    action "approve"     Agent 1 vá + verify  ->  **Agent 2 tự động nghiệm thu độc lập**
    action "reject"      không thực thi gì, hỏi lý do -> Agent 1 re-plan
    action "audit"       chạy lại nghiệm thu bất cứ lúc nào
    on_message           engineer chất vấn; câu hỏi về nghiệm thu -> Agent 2, còn lại -> Agent 1
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import chainlit as cl
from fastapi.concurrency import run_in_threadpool

import config
from ai import tools
from ai.agent import DataReliabilityAgent
from ai.auditor import DataAuditorAgent
from ai.llm import LLMSettings, ToolEvent
from ai.schemas import AgentReport, AuditReport, IncidentInput, IncidentStatus
from data.incidents import build_sample_incident_payload
from data.warehouse import ensure_database
from web.frontend.rendering import (
    AUTHOR_ALERT,
    AUTHOR_AUDITOR,
    AUTHOR_ENGINEER,
    AUTHOR_SRE,
    AUTHOR_SYSTEM,
    approval_actions,
    audit_actions,
    render_tool_step,
    warehouse_snapshot,
    welcome_message,
)

# ---------------------------------------------------------------------------
# 1. Chạy hành động blocking của agent + stream tool call lên UI
# ---------------------------------------------------------------------------


async def run_agent_with_live_steps(agent: Any, blocking_call: Callable[[], Any]) -> Any:
    """
    Chạy một hành động blocking của agent trong threadpool, đồng thời stream các tool
    call lên UI ngay khi chúng xảy ra.

    Cơ chế: callback `on_tool_event` (chạy trong worker thread) đẩy event vào
    `asyncio.Queue` qua `loop.call_soon_threadsafe`; coroutine `_pump` (chạy trên event
    loop) mới là nơi gọi API Chainlit. Nhờ vậy không gọi Chainlit từ thread khác.
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    sentinel = object()

    def on_event(event: ToolEvent) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, event)

    async def _pump() -> None:
        while True:
            item = await queue.get()
            if item is sentinel:
                return
            try:
                await render_tool_step(item)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001 - lỗi render không được làm hỏng agent run
                pass

    previous_cb = agent.on_tool_event
    agent.on_tool_event = on_event
    pump_task = asyncio.create_task(_pump())
    try:
        return await cl.make_async(blocking_call)()
    finally:
        agent.on_tool_event = previous_cb
        queue.put_nowait(sentinel)
        await pump_task


# ---------------------------------------------------------------------------
# 2. Gửi báo cáo lên UI
# ---------------------------------------------------------------------------


async def send_agent_report(report: AgentReport, agent: DataReliabilityAgent) -> None:
    """Báo cáo của Agent 1 + JSON contract + 2 nút duyệt."""
    await cl.Message(
        content=report.to_markdown(),
        author=AUTHOR_SRE,
        actions=approval_actions(),
        elements=[
            cl.Text(
                name=f"{report.incident_id}-report.json",
                content=report.to_json(),
                display="side",
                language="json",
            )
        ],
    ).send()
    cl.user_session.set("report", report)
    cl.user_session.set("agent", agent)


async def send_audit_report(audit: AuditReport) -> None:
    """Biên bản nghiệm thu của Agent 2."""
    await cl.Message(
        content=audit.to_markdown(),
        author=AUTHOR_AUDITOR,
        elements=[
            cl.Text(
                name=f"{audit.audit_id}-audit.json",
                content=audit.to_json(),
                display="side",
                language="json",
            )
        ],
    ).send()
    cl.user_session.set("audit_report", audit)


# ---------------------------------------------------------------------------
# 3. Agent 2 — nghiệm thu độc lập
# ---------------------------------------------------------------------------


async def run_independent_audit(trigger: str = "auto") -> Optional[AuditReport]:
    """
    Chạy **Agent 2 (Data Auditor)** nghiệm thu độc lập.

    Agent 2 được khởi tạo MỚI, có `messages` riêng, chỉ được cấp tool đọc, và chạy
    bằng model khác Agent 1 (nếu cấu hình cross-model) — nó không thừa hưởng gì từ hội
    thoại của Agent 1 ngoài bản báo cáo (gắn nhãn "lời khai cần kiểm chứng").
    """
    incident: Optional[IncidentInput] = cl.user_session.get("incident")
    report: Optional[AgentReport] = cl.user_session.get("report")
    maker_agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    maker_model = maker_agent.settings.model if maker_agent else "?"

    auditor: Optional[DataAuditorAgent] = cl.user_session.get("auditor")
    if auditor is None:
        # Truyền model của Maker vào để Checker tự bốc model KHÁC trong pool
        auditor = DataAuditorAgent(settings=LLMSettings.for_auditor(avoid_model=maker_model))
        cl.user_session.set("auditor", auditor)

    cross_model = (not auditor.is_offline) and auditor.settings.model != maker_model
    if auditor.is_offline:
        brain = (
            "🟡 Brain của em: **OFFLINE** (chưa cấu hình `DRA_API_KEY`) — số liệu DuckDB "
            "vẫn là số liệu thật, lấy từ engine checks ạ"
        )
    else:
        brain = f"🧠 Brain của em: `{auditor.settings.model}`"
        brain += (
            f" — **khác model của anh SRE Agent** (`{maker_model}`) 🔀 nên em có góc nhìn "
            "độc lập, không bị mù cùng một chỗ với anh ấy ạ"
            if cross_model
            else f" (trùng model với anh SRE Agent `{maker_model}` ⚠️)"
        )

    intro = (
        "🕵️‍♀️ Em là **Data Auditor** (Agent 2) đây ạ! Em vào nghiệm thu **độc lập** phần "
        "anh SRE Agent vừa làm nhé.\n\n"
        f"{brain}\n\n"
        "🔒 Quyền của em: **chỉ đọc** DuckDB. Em **không dùng lại con số nào** trong báo "
        "cáo của anh ấy — em tự lấy baseline trước-khi-vá từ DuckDB rồi tự viết SQL kiểm "
        "3 việc: 🧼 dữ liệu còn bẩn không, 🧊 có xoá oan mất dòng nào không, "
        "🧮 tổng số dòng có bảo toàn không."
    )
    if trigger == "manual":
        intro += "\n\n_(Anh vừa bấm nút nghiệm thu lại — em chạy lại từ đầu ạ 🙆‍♀️)_"

    thinking = cl.Message(author=AUTHOR_AUDITOR, content=intro)
    await thinking.send()

    try:
        audit: AuditReport = await run_agent_with_live_steps(
            auditor, lambda: auditor.audit(incident=incident, remediation_report=report)
        )
    except Exception as exc:  # noqa: BLE001
        await cl.Message(
            author=AUTHOR_AUDITOR, content=f"😢 Em nghiệm thu bị lỗi giữa đường ạ: `{exc}`"
        ).send()
        return None

    await send_audit_report(audit)

    ok = audit.verdict == "AUDIT_PASSED"
    mismatch = any(c.verified_by_engine is False for c in audit.checks)
    tail = [
        f"### {'🎖️' if ok else '🛑'} Maker–Checker: "
        f"{'NGHIỆM THU ĐẠT' if ok else 'NGHIỆM THU KHÔNG ĐẠT'}",
        "",
        f"- 👷‍♀️ **Maker** (Agent 1) · `{maker_model}`: đã vá dữ liệu và tự verify.",
        f"- 🕵️‍♀️ **Checker** (Agent 2) · `{audit.auditor_model or auditor.settings.model}`: "
        f"tự chạy **{len(auditor.executed_queries())} câu SQL** độc lập, đạt "
        f"**{audit.passed_count}/{len(audit.checks)}** hạng mục.",
    ]
    if cross_model:
        tail.append(
            "- 🔀 **Cross-model checking**: hai vai chạy bằng hai model khác nhau, nên lỗi "
            "của model này khó lọt qua model kia."
        )
    if mismatch:
        tail.append(
            "- 🚨 **Có hạng mục LLM khai khác số liệu máy đo** → hệ thống đã lấy kết quả "
            "đo bằng Python và tự hạ verdict. Anh xem mục có dấu ⚠️ nhé."
        )
    if not ok:
        tail.append(
            "- 💡 Anh cân nhắc rollback theo `rollback_hint` trong báo cáo của Agent 1, "
            "hoặc escalate cho on-call theo `oncall_escalation`."
        )
    await cl.Message(author=AUTHOR_SYSTEM, content="\n".join(tail), actions=audit_actions()).send()
    return audit


# ---------------------------------------------------------------------------
# 4. CHAINLIT HANDLERS
# ---------------------------------------------------------------------------


@cl.on_chat_start
async def on_chat_start() -> None:
    """Khởi động phiên trực: nạp incident mẫu, Agent 1 điều tra, trình báo cáo + nút duyệt."""
    await run_in_threadpool(ensure_database, config.DUCKDB_PATH)

    agent = DataReliabilityAgent()
    checker_settings = LLMSettings.for_auditor(avoid_model=agent.settings.model)

    await cl.Message(
        author=AUTHOR_SYSTEM,
        content=welcome_message(
            maker_model=agent.settings.model,
            maker_offline=agent.is_offline,
            checker_model=checker_settings.model,
            checker_offline=not checker_settings.api_key,
            checker_pool=checker_settings.model_pool,
        ),
    ).send()

    # --- Nạp incident mẫu (mô phỏng webhook dbt test fail) ---
    payload = await run_in_threadpool(build_sample_incident_payload)
    incident = IncidentInput(**payload)

    await cl.Message(
        author=AUTHOR_ALERT,
        content=(
            f"### 🚨 Incident mới: `{incident.incident_id}`\n"
            f"- **Loại:** `{incident.incident_type}`\n"
            f"- **Bảng:** `{incident.target_table}`\n"
            f"- **Nguồn alert:** `{incident.source}`\n\n"
            f"{incident.description}"
        ),
        elements=[
            cl.Text(
                name="incident_payload.json",
                content=json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                display="side",
                language="json",
            )
        ],
    ).send()

    agent.load_incident(incident)
    cl.user_session.set("agent", agent)
    cl.user_session.set("incident", incident)
    cl.user_session.set("auditor", None)
    cl.user_session.set("audit_report", None)
    cl.user_session.set("awaiting_reject_reason", False)
    cl.user_session.set("resolved", False)

    thinking = cl.Message(
        author=AUTHOR_SRE, content="🔍 Em nhận ca rồi ạ, em đang điều tra trên DuckDB đây anh…"
    )
    await thinking.send()

    try:
        report: AgentReport = await run_agent_with_live_steps(agent, agent.investigate)
    except Exception as exc:  # noqa: BLE001
        thinking.content = (
            f"❌ Em gặp lỗi khi điều tra ạ: `{exc}`\n\n"
            "Anh kiểm tra lại `DRA_API_KEY` / `DRA_BASE_URL` / `DRA_MODEL` giúp em nhé "
            "(hoặc chạy `python -m ai.check_llm` để soi nhanh)."
        )
        await thinking.update()
        return

    thinking.content = (
        f"✅ Em điều tra xong rồi ạ — **{len(agent.tool_events)} lần gọi tool** "
        f"({len(agent.executed_queries())} câu SQL trên DuckDB) 📊"
    )
    await thinking.update()
    await send_agent_report(report, agent)


# Từ khoá để định tuyến câu hỏi sang Agent 2 thay vì Agent 1
_AUDITOR_KEYWORDS = (
    "audit", "auditor", "nghiệm thu", "nghiem thu", "kiểm toán", "kiem toan",
    "checker", "agent 2", "agent2", "biên bản", "bien ban", "chứng nhận",
    "chung nhan", "mất dữ liệu", "mat du lieu", "xoá oan", "xoa oan", "bảo toàn",
    "bao toan", "row count",
)


def route_to_auditor(question: str) -> bool:
    """Câu hỏi về nghiệm thu thì để Agent 2 trả lời, còn lại là việc của Agent 1."""
    low = question.lower()
    return any(keyword in low for keyword in _AUDITOR_KEYWORDS)


@cl.on_message
async def on_message(message: cl.Message) -> None:
    """
    Engineer chat tự do: "tại sao lại lỗi?", "show thử 5 dòng dữ liệu",
    "có mất dữ liệu không?"… Cả hai agent đều dùng tool DuckDB để trả lời bằng số
    liệu thật; Agent 2 vẫn chỉ được đọc.
    """
    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    if agent is None:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="Chưa có phiên điều tra nào ạ. Anh bấm **New Chat** để nạp incident mới nhé 🙏",
        ).send()
        return

    question = (message.content or "").strip()
    if not question:
        return

    # --- Engineer vừa bấm [Từ chối] -> tin nhắn này là lý do -> re-plan ---
    if cl.user_session.get("awaiting_reject_reason"):
        cl.user_session.set("awaiting_reject_reason", False)
        thinking = cl.Message(
            author=AUTHOR_SRE,
            content="🔁 Em ghi nhận rồi ạ. Em điều tra lại và lập phương án mới ngay…",
        )
        await thinking.send()
        try:
            await run_agent_with_live_steps(agent, lambda: agent.reject(question))
            new_report: AgentReport = await run_agent_with_live_steps(
                agent, lambda: agent.replan(question)
            )
        except Exception as exc:  # noqa: BLE001
            thinking.content = f"❌ Em chưa lập lại được kế hoạch ạ: `{exc}`"
            await thinking.update()
            return
        thinking.content = "✅ Em có phương án remediation mới rồi, anh xem lại giúp em nhé 🙆‍♀️"
        await thinking.update()
        await send_agent_report(new_report, agent)
        return

    # --- Định tuyến sang Agent 2 nếu là câu hỏi về nghiệm thu ---
    auditor: Optional[DataAuditorAgent] = cl.user_session.get("auditor")
    if auditor is not None and route_to_auditor(question):
        thinking = cl.Message(
            author=AUTHOR_AUDITOR, content="🔬 Em kiểm tra lại trên DuckDB rồi trả lời anh ngay…"
        )
        await thinking.send()
        try:
            answer = await run_agent_with_live_steps(auditor, lambda: auditor.ask(question))
        except Exception as exc:  # noqa: BLE001
            thinking.content = f"❌ Em bị lỗi khi tra cứu ạ: `{exc}`"
            await thinking.update()
            return
        thinking.content = answer or "_(Agent 2 không trả về nội dung)_"
        await thinking.update()
        return

    # --- Chất vấn Agent 1 ---
    thinking = cl.Message(author=AUTHOR_SRE, content="💭 Em đang tra cứu trên DuckDB ạ…")
    await thinking.send()
    try:
        answer = await run_agent_with_live_steps(agent, lambda: agent.ask(question))
    except Exception as exc:  # noqa: BLE001
        thinking.content = f"❌ Em bị lỗi khi xử lý câu hỏi ạ: `{exc}`"
        await thinking.update()
        return

    thinking.content = answer or "_(Agent không trả về nội dung)_"
    await thinking.update()

    if cl.user_session.get("report") is None:
        return
    if cl.user_session.get("resolved"):
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="Anh muốn em cho Agent 2 nghiệm thu lại lần nữa không ạ? 🕵️‍♀️",
            actions=audit_actions(),
        ).send()
    else:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="Anh duyệt để em thực thi remediation, hoặc hỏi em thêm gì cũng được ạ 🙆‍♀️",
            actions=approval_actions(),
        ).send()


@cl.action_callback("approve")
async def on_approve(action: cl.Action) -> None:
    """[✅ Duyệt] — Agent 1 vá + verify, rồi BÀN GIAO cho Agent 2 nghiệm thu độc lập."""
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    report: Optional[AgentReport] = cl.user_session.get("report")
    if agent is None or report is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy báo cáo để duyệt.").send()
        return
    if cl.user_session.get("resolved"):
        await cl.Message(
            author=AUTHOR_SYSTEM, content="Sự cố này đã được xử lý xong, không cần duyệt lại ạ."
        ).send()
        return

    await cl.Message(
        author=AUTHOR_ENGINEER,
        content=(
            f"✅ **APPROVED** kế hoạch `{report.remediation.action_type.value}` "
            f"cho `{report.incident_id}`."
        ),
    ).send()

    thinking = cl.Message(
        author=AUTHOR_SRE,
        content="⚙️ Em thực thi remediation trên DuckDB đây ạ (chạy trong transaction, "
        "lỗi là rollback toàn bộ)…",
    )
    await thinking.send()

    try:
        result: Dict[str, Any] = await run_agent_with_live_steps(agent, agent.approve)
    except Exception as exc:  # noqa: BLE001
        thinking.content = f"❌ Thực thi thất bại ạ: `{exc}`"
        await thinking.update()
        return

    execution = result.get("execution", {}) or {}
    verification = result.get("verification", {}) or {}
    ok = bool(result.get("ok"))

    thinking.content = (
        f"{'✅' if ok else '❌'} **{result.get('status')}** — "
        f"{execution.get('statements_executed', 0)} câu lệnh SQL đã chạy, "
        f"số dòng vi phạm còn lại: **{verification.get('violations', 'N/A')}**."
    )
    await thinking.update()

    await cl.Message(
        author=AUTHOR_SRE,
        content=result.get("summary") or "_(không có tổng kết)_",
        elements=[
            cl.Text(
                name="remediation_result.json",
                content=json.dumps(
                    {
                        "baseline_before": result.get("baseline"),
                        "execution": execution,
                        "verification": verification,
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ),
                display="side",
                language="json",
            )
        ],
    ).send()

    snapshot = await run_in_threadpool(warehouse_snapshot, report.target_table)
    if snapshot:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="### 📊 Trạng thái warehouse sau remediation\n" + snapshot,
        ).send()

    if ok:
        cl.user_session.set("resolved", True)
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content=(
                f"### ✅ Agent 1 báo `{report.incident_id}` → **RESOLVED**\n"
                "Nhưng theo nguyên tắc **Maker–Checker**, tự Agent 1 verify thì chưa đủ. "
                "Em chuyển ca sang **Agent 2 (Data Auditor)** nghiệm thu độc lập ngay đây ạ 👇"
            ),
        ).send()
    else:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content=(
                f"### ❌ Agent 1 báo `{report.incident_id}` → **FAILED**\n"
                "Remediation không đưa vi phạm về 0. Theo runbook `oncall_escalation`, "
                "Agent 1 KHÔNG tự thử lại lần 2. Em vẫn cho **Agent 2** vào soi để biết "
                "chính xác hiện trạng dữ liệu nhé 👇"
            ),
        ).send()

    await run_independent_audit(trigger="auto")


@cl.action_callback("reject")
async def on_reject(action: cl.Action) -> None:
    """[❌ Từ chối] — không thực thi gì, hỏi lý do rồi để Agent 1 re-plan."""
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    if agent is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy phiên điều tra.").send()
        return

    tools.lock_remediation()
    agent.status = IncidentStatus.REJECTED
    cl.user_session.set("awaiting_reject_reason", True)

    await cl.Message(
        author=AUTHOR_ENGINEER,
        content="❌ **REJECTED** — chưa thực thi bất kỳ thay đổi nào trên DuckDB.",
    ).send()
    await cl.Message(
        author=AUTHOR_SRE,
        content=(
            "🙇‍♀️ Dạ em hiểu rồi. Anh cho em biết **lý do từ chối** hoặc ràng buộc anh muốn nhé "
            "(ví dụ: *“không được xoá dòng nào, chỉ được đánh dấu”*, *“phải giữ nguyên doanh thu "
            "ngày 15/09”*, *“chờ team Mobile fix rồi backfill”*). Em sẽ điều tra lại và đề xuất "
            "phương án khác ngay ạ 💪"
        ),
    ).send()


@cl.action_callback("audit")
async def on_audit(action: cl.Action) -> None:
    """[🕵️‍♀️ Nghiệm thu độc lập] — chạy lại Agent 2 theo yêu cầu của engineer."""
    await action.remove()
    if cl.user_session.get("agent") is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Chưa có ca nào để nghiệm thu ạ 🙏").send()
        return
    await run_independent_audit(trigger="manual")
