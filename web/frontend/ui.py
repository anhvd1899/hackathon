"""
web/frontend/ui.py — Chainlit app (Human-in-the-loop)
=====================================================

Đây là **target của `mount_chainlit`** (xem `web/server.py`). File này chỉ điều phối
luồng hội thoại; phần render markdown/nút nằm ở `rendering.py`.

Luồng Maker–Checker trên UI:

    on_chat_start        nạp incident -> Agent 1 điều tra (stream từng tool call)
                         -> báo cáo + [✅ Duyệt] / [❌ Từ chối]
    action "approve"     Agent 1 vá + verify -> HỎI engineer: có recheck không?
    action "audit"       engineer chọn "có" -> Agent 2 nghiệm thu độc lập
                         (cũng dùng để chạy lại nghiệm thu bất cứ lúc nào)
    action "skip_audit"  engineer chọn "không" -> đóng incident luôn, ghi vết vào audit log
    action "reject"      không thực thi gì, hỏi lý do -> Agent 1 re-plan
    on_message           engineer chất vấn; câu hỏi về nghiệm thu -> Agent 2, còn lại -> Agent 1

Có HAI điểm human-in-the-loop:
    1. Trước khi GHI dữ liệu   -> approve / reject
    2. Sau khi ghi xong        -> recheck (Agent 2) / chốt luôn
Điểm thứ hai để engineer tự cân đối: lỗi đơn giản thì chốt cho nhanh, lỗi phức tạp thì
trả thêm thời gian + token để có biên bản nghiệm thu độc lập.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import chainlit as cl
from fastapi.concurrency import run_in_threadpool

import config
from ai import tools, worker
from ai.agent import DataReliabilityAgent
from ai.auditor import DataAuditorAgent, run_audit_headless
from ai.llm import LLMSettings, ToolEvent
from ai.schemas import (
    MAX_REPLAN_ATTEMPTS,
    AgentReport,
    AuditReport,
    IncidentInput,
    IncidentStatus,
)
from data import audit as data_audit
from data import incident_store
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
    publish_actions,
    recheck_decision_actions,
    render_tool_step,
    triage_actions,
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
    maker_usage = maker_agent.usage.describe() if maker_agent else "n/a"

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
        "",
        f"💰 **Chi phí token** · Maker: {maker_usage} · Checker: {auditor.usage.describe()}",
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


def _resolve_incident() -> tuple[Dict[str, Any], Optional[AgentReport], str]:
    """
    Chọn incident cho phiên chat này (chạy trong threadpool vì có truy vấn DuckDB).

    Trả về (envelope, báo cáo đã điều tra nếu có, ghi chú nguồn). Nếu worker nền đã điều
    tra xong thì **dùng lại báo cáo đó** — không điều tra lại, tiết kiệm token và cho
    engineer thấy đúng báo cáo mà họ vừa xem trên dashboard.
    """
    incident_id = incident_store.selected_incident_id()
    row = incident_store.get_incident(incident_id) if incident_id else None

    if row is None:
        waiting = incident_store.list_incidents(status="WAITING_FOR_APPROVAL", limit=1)
        if waiting:
            row = incident_store.get_incident(str(waiting[0]["incident_id"]))
        else:
            detected = incident_store.list_incidents(status="DETECTED", limit=1)
            if detected:
                row = incident_store.get_incident(str(detected[0]["incident_id"]))

    if row is None or not row.get("envelope"):
        return build_sample_incident_payload(), None, "incident mẫu (không có sự cố nào đang mở)"

    incident_store.clear_selection()
    report: Optional[AgentReport] = None
    if row.get("report"):
        try:
            report = AgentReport.model_validate(row["report"])
        except Exception:  # noqa: BLE001 - báo cáo cũ lỗi thì điều tra lại
            report = None
    note = (
        f"sự cố `{row['incident_id']}` từ job `{row['job_id']}`"
        + (" · dùng lại báo cáo worker nền đã điều tra" if report else " · chưa có báo cáo")
    )
    return row["envelope"], report, note


@cl.on_chat_start
async def on_chat_start() -> None:
    """
    Khởi động phiên trực: nạp incident đang cần xử lý (từ dashboard hoặc hàng đợi),
    trình báo cáo + nút duyệt.
    """
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

    # --- Nạp incident cần xử lý ------------------------------------------
    # Ưu tiên incident mà engineer vừa bấm "Mở phiên xử lý" trên dashboard, sau đó tới
    # incident đang chờ duyệt cũ nhất, cuối cùng mới fallback về incident mẫu.
    payload, preloaded_report, source_note = await run_in_threadpool(_resolve_incident)
    incident = IncidentInput(**payload)

    await cl.Message(
        author=AUTHOR_ALERT,
        content=(
            f"### 🚨 Incident mới: `{incident.incident_id}`\n"
            f"- **Loại:** `{incident.incident_type}`\n"
            f"- **Bảng:** `{incident.target_table}`\n"
            f"- **Nguồn alert:** `{incident.source}`\n"
            f"- **Nạp từ:** {source_note}\n\n"
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
    cl.user_session.set("awaiting_recheck_decision", False)
    cl.user_session.set("audit_skipped", False)
    cl.user_session.set("resolved", False)

    # --- Nếu worker nền đã điều tra rồi thì DÙNG LẠI, không điều tra lần hai -----
    cached = worker.get_agent(incident.incident_id)
    if cached is not None and cached.report is not None:
        agent = cached
        agent.on_tool_event = None
        cl.user_session.set("agent", agent)
        await cl.Message(
            author=AUTHOR_SRE,
            content=(
                "✅ Em đã điều tra ca này ở luồng nền rồi ạ, em không chạy lại để khỏi tốn "
                f"token 💰\n\n- **{len(agent.tool_events)} lần gọi tool** "
                f"({len(agent.executed_queries())} câu SQL trên DuckDB)\n"
                f"- Chi phí: {agent.usage.describe()}"
            ),
        ).send()
        await send_agent_report(agent.report, agent)
        return

    if preloaded_report is not None:
        # Có báo cáo trong DB nhưng agent gốc không còn trong process (server restart)
        agent.report = preloaded_report
        agent.status = preloaded_report.status
        cl.user_session.set("agent", agent)
        await cl.Message(
            author=AUTHOR_SRE,
            content=(
                "✅ Em lấy lại báo cáo đã điều tra từ kho sự cố ạ (worker nền làm trước đó). "
                "Anh xem và quyết định duyệt hay không nhé 🙆‍♀️"
            ),
        ).send()
        await send_agent_report(preloaded_report, agent)
        return

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
        f"({len(agent.executed_queries())} câu SQL trên DuckDB) 📊\n\n"
        f"💰 Chi phí: {agent.usage.describe()} · {agent.settings.describe_budget()}"
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
    if cl.user_session.get("awaiting_recheck_decision"):
        # Agent 1 đã vá xong nhưng engineer chưa chọn recheck hay chốt luôn
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="Anh chốt giúp em: có cần **recheck độc lập** không ạ? 🤔",
            actions=recheck_decision_actions(allow_skip=bool(cl.user_session.get("resolved"))),
        ).send()
    elif cl.user_session.get("resolved"):
        nudge = (
            "Ca này **chưa qua nghiệm thu độc lập** — anh muốn em cho Agent 2 soi lại không ạ? 🕵️‍♀️"
            if cl.user_session.get("audit_skipped")
            else "Anh muốn em cho Agent 2 nghiệm thu lại lần nữa không ạ? 🕵️‍♀️"
        )
        await cl.Message(author=AUTHOR_SYSTEM, content=nudge, actions=audit_actions()).send()
    else:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="Anh duyệt để em thực thi remediation, hoặc hỏi em thêm gì cũng được ạ 🙆‍♀️",
            actions=approval_actions(),
        ).send()


@cl.action_callback("approve")
async def on_approve(action: cl.Action) -> None:
    """
    [🧪 Duyệt chạy thử trên Staging] — **BƯỚC 1** của Two-Phase Human-in-the-loop.

    Chạy script vá trên `shadow_<table>` rồi cho Agent 2 nghiệm thu ngay trên bảng bóng
    đó. Bảng production không bị chạm ở bước này, nên đây là nút an toàn: sai thì chỉ
    cần xoá bảng bóng.

    Nút Publish (bước 2) chỉ xuất hiện khi Agent 2 cấp chứng nhận.
    """
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    report: Optional[AgentReport] = cl.user_session.get("report")
    if agent is None or report is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy báo cáo để duyệt.").send()
        return
    if cl.user_session.get("published"):
        await cl.Message(
            author=AUTHOR_SYSTEM, content="Sự cố này đã publish xong, không cần duyệt lại ạ."
        ).send()
        return

    plan = report.remediation
    await cl.Message(
        author=AUTHOR_ENGINEER,
        content=(
            f"🧪 **APPROVED BƯỚC 1** — chạy thử `{plan.action_type.value}` cho "
            f"`{report.incident_id}` trên bảng bóng `{plan.shadow_table_name}`."
        ),
    ).send()

    thinking = cl.Message(
        author=AUTHOR_SRE,
        content=(
            f"⚙️ Em dựng bảng bóng `{plan.shadow_table_name}` và chạy script vá **trên đó** "
            f"ạ. Bảng thật `{plan.target_production_table}` em không chạm vào đâu 🛡️"
        ),
    )
    await thinking.send()

    try:
        result: Dict[str, Any] = await run_agent_with_live_steps(
            agent, lambda: agent.approve_shadow(narrate=False)
        )
    except Exception as exc:  # noqa: BLE001
        thinking.content = f"❌ Chạy trên staging thất bại ạ: `{exc}`"
        await thinking.update()
        return

    execution = result.get("execution", {}) or {}
    staged = bool(result.get("ok"))
    incident_store.set_status(
        report.incident_id,
        "STAGING_VERIFYING" if staged else "AUDIT_FAILED_TRIAGE",
        shadow_table=result.get("shadow_table") or plan.shadow_table_name,
        error=None if staged else str(result.get("error"))[:900],
    )

    if not staged:
        error_detail = str(result.get("error"))[:300]
        thinking.content = (
            f"❌ Không chạy được trên staging ạ: `{error_detail}`\n\n"
            f"🛡️ Nhưng bảng thật `{plan.target_production_table}` **vẫn nguyên vẹn 100%** nên "
            "mình chưa mất gì cả."
        )
        await thinking.update()
        cl.user_session.set("triage", True)

        # Thông báo tại chỗ, không xui người dùng đi tìm dashboard
        fail_text = (
            f"### 🚨 REMEDIATION FAILED — Staging (Data Safe)\n\n"
            f"**Nguyên nhân:** `{error_detail}`\n\n"
            f"🛡️ Bảng production chưa bị thay đổi (Data Safe 100%).\n"
            f"Vui lòng chọn hướng xử lý cứu hộ ngay bên dưới:"
        )

        # Gắn 3 nút hành động trực tiếp vào khung chat Chainlit
        actions = [
            cl.Action(
                name="replan_agent",
                value=report.incident_id,
                payload={"decision": "replan"},
                label=f"🤖 Cho Agent 1 Re-plan ({agent.retry_count}/{MAX_REPLAN_ATTEMPTS})",
            ),
            cl.Action(
                name="action_manual_sql",
                value=report.incident_id,
                payload={"decision": "manual_sql"},
                label="✏️ Sửa SQL Thủ Công",
            ),
            cl.Action(
                name="cancel_shadow",
                value=report.incident_id,
                payload={"decision": "cancel"},
                label="🛑 Huỷ Bỏ & Dọn Staging",
            ),
        ]
        await cl.Message(author=AUTHOR_SYSTEM, content=fail_text, actions=actions).send()
        return

    thinking.content = (
        f"✅ Đã chạy **{execution.get('statements_executed', 0)}** câu lệnh trên bảng bóng "
        f"`{execution.get('shadow_table')}`:\n"
        f"- 🧪 Bảng bóng: **{execution.get('rows_shadow')}** dòng · "
        f"**{execution.get('violations_after')}** vi phạm\n"
        f"- 🗂️ Bảng thật: **{execution.get('rows_prod_before')}** dòng · "
        f"**{execution.get('violations_before')}** vi phạm (chưa bị chạm — đúng thiết kế)"
    )
    await thinking.update()

    # ---- Agent 2 nghiệm thu NGAY trên bảng bóng --------------------------
    # Ở kiến trúc WAP, nghiệm thu không còn là lựa chọn tuỳ ý: nó là **cổng** mở nút
    # Publish. Bỏ qua nghiệm thu thì không có đường nào lên production cả.
    auditor_msg = cl.Message(
        author=AUTHOR_AUDITOR,
        content=f"🕵️‍♀️ Em soi bảng bóng `{execution.get('shadow_table')}` đây ạ…",
    )
    await auditor_msg.send()

    audit_payload = await run_in_threadpool(
        run_audit_headless, agent.incident, report, execution.get("shadow_table") or ""
    )
    ready = bool(audit_payload.get("is_ready_for_production"))
    audit_report = audit_payload.get("audit_report") or {}

    incident_store.set_status(
        report.incident_id,
        "READY_FOR_PRODUCTION" if ready else "AUDIT_FAILED_TRIAGE",
        audit_json=json.dumps(audit_report, ensure_ascii=False, default=str),
        ready_for_production=ready,
        error="" if ready else str(
            (audit_payload.get("failed_details") or {}).get("error_message") or ""
        )[:900],
    )

    auditor_msg.content = (
        f"{'🎖️' if ready else '🛑'} **{audit_report.get('verdict', 'AUDIT_FAILED')}** — "
        f"{sum(1 for c in audit_report.get('checks', []) if c.get('passed'))}"
        f"/{len(audit_report.get('checks', []))} hạng mục đạt trên bảng bóng."
    )
    await auditor_msg.update()

    diff = audit_payload.get("shadow_diff") or {}
    await cl.Message(
        author=AUTHOR_AUDITOR,
        content=(
            "### 🔬 Đối chiếu trước khi Publish\n\n"
            "| Chỉ số | Bảng thật (hiện tại) | Bảng bóng (sau vá) |\n"
            "| --- | --- | --- |\n"
            f"| Số dòng vi phạm | {diff.get('violations_prod')} | "
            f"**{diff.get('violations_shadow')}** |\n"
            f"| Tổng số dòng | {diff.get('rows_prod')} | {diff.get('rows_shadow')} |\n"
            f"| Dòng đã cách ly | — | {diff.get('rows_quarantine', diff.get('rows_removed'))} |\n"
            f"| Schema khớp | — | {'✅ khớp' if diff.get('schema_match') else '❌ lệch'} |\n"
        ),
        elements=[
            cl.Text(
                name="audit_on_shadow.json",
                content=json.dumps(audit_payload, ensure_ascii=False, indent=2, default=str),
                display="side",
                language="json",
            )
        ],
    ).send()

    if ready:
        cl.user_session.set("ready_for_production", True)
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content=(
                f"### 🚀 SẴN SÀNG PUBLISH — `{report.incident_id}`\n"
                f"Agent 2 đã nghiệm thu **ĐẠT** trên bảng bóng. Bảng thật "
                f"`{plan.target_production_table}` hiện vẫn còn "
                f"**{diff.get('violations_prod')}** dòng vi phạm.\n\n"
                "Bấm nút dưới để **atomic swap** bảng bóng thành bảng thật (một transaction, "
                "không có cửa sổ nào consumer đọc được bảng nửa vời) 👇"
            ),
            actions=publish_actions(),
        ).send()
    else:
        cl.user_session.set("triage", True)
        failed = audit_payload.get("failed_details") or {}
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content=(
                f"### 🛑 AUDIT FAILED — cổng Publish vẫn đóng\n"
                f"`{str(failed.get('error_message'))[:400]}`\n\n"
                f"🛡️ Bảng production **nguyên vẹn 100%** — mọi lệnh vá chỉ chạy trên bảng bóng.\n\n"
                "Anh chọn một trong ba hướng cứu hộ 👇"
            ),
            actions=triage_actions(
                can_replan=agent.retry_count < MAX_REPLAN_ATTEMPTS,
                retry_count=agent.retry_count,
            ),
        ).send()


@cl.action_callback("publish_prod")
async def on_publish_prod(action: cl.Action) -> None:
    """[🚀 Publish to Production] — **BƯỚC 2**: atomic swap sang bảng thật."""
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    report: Optional[AgentReport] = cl.user_session.get("report")
    if agent is None or report is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy phiên xử lý.").send()
        return
    if not cl.user_session.get("ready_for_production"):
        # Cửa này đóng cả ở đây, không chỉ ở chỗ ẩn nút: nút có thể còn sót trên màn hình
        # cũ sau khi trạng thái đã đổi.
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="⛔ Chưa có chứng nhận của Agent 2 nên em không publish được ạ.",
        ).send()
        return

    plan = report.remediation
    await cl.Message(
        author=AUTHOR_ENGINEER,
        content=f"🚀 **PUBLISH** `{plan.shadow_table_name}` → `{plan.target_production_table}`.",
    ).send()

    thinking = cl.Message(author=AUTHOR_SRE, content="⚙️ Em tráo bảng trong một transaction ạ…")
    await thinking.send()

    tools.grant_phase(report.incident_id, phase=tools.PHASE_PUBLISH)
    try:
        result = await run_in_threadpool(
            tools.tool_atomic_publish_to_prod,
            plan.shadow_table_name,
            plan.target_production_table,
            report.incident_id,
            False,
        )
    finally:
        tools.revoke_phase(report.incident_id, phase=tools.PHASE_PUBLISH)

    if not result.get("ok"):
        thinking.content = f"❌ Publish thất bại, đã ROLLBACK ạ: `{result.get('error')}`"
        await thinking.update()
        incident_store.set_status(
            report.incident_id, "AUDIT_FAILED_TRIAGE", error=str(result.get("error"))[:900]
        )
        return

    thinking.content = (
        f"🎉 Đã publish xong ạ! `{result.get('prod_table')}`: "
        f"**{result.get('rows_prod_before')}** → **{result.get('rows_prod_after')}** dòng."
    )
    await thinking.update()

    cl.user_session.set("published", True)
    incident_store.set_status(
        report.incident_id, "PUBLISHED_RESOLVED", ready_for_production=True
    )

    snapshot = await run_in_threadpool(warehouse_snapshot, plan.target_production_table)
    if snapshot:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="### 📊 Trạng thái warehouse sau Publish\n" + snapshot,
        ).send()

    await cl.Message(
        author=AUTHOR_SRE,
        content=(
            f"### ✅ `{report.incident_id}` → **PUBLISHED_RESOLVED**\n"
            f"- 🗂️ Bảng thật đã nhận dữ liệu sạch từ bảng bóng\n"
            f"- 🧊 Dữ liệu bẩn vẫn nằm trong `quarantine_*` để backfill sau\n"
            f"- 💰 Chi phí phiên này: {agent.usage.describe()}\n\n"
            "**Việc cần làm tiếp:** dựng lại các mart hạ nguồn bằng "
            "`dbt run --select <model>` — em cố tình không tự viết SQL rebuild vì để dbt "
            "biên dịch theo lineage thật thì không bị sai tên cột ạ 💚"
        ),
    ).send()


@cl.action_callback("cancel_shadow")
@cl.action_callback("action_cancel_shadow")
async def on_cancel_shadow(action: cl.Action) -> None:
    """[🛑 Huỷ bỏ & Xoá Staging] — dọn bảng bóng, đóng sự cố ở CANCELLED."""
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    report: Optional[AgentReport] = cl.user_session.get("report")
    if agent is None or report is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy phiên xử lý.").send()
        return

    result = await run_in_threadpool(agent.cancel_shadow, "engineer huỷ trên Chainlit")
    incident_store.set_status(report.incident_id, "CANCELLED", error="Engineer huỷ phương án")
    cl.user_session.set("triage", False)
    await cl.Message(
        author=AUTHOR_SRE,
        content=result.get("summary") or "🗑️ Đã dọn bảng bóng ạ.",
    ).send()


@cl.action_callback("action_manual_sql")
async def on_action_manual_sql(action: cl.Action) -> None:
    """[✏️ Sửa SQL Thủ Công] — hướng dẫn engineer nhập SQL hoặc mở SQL Studio."""
    await action.remove()
    await cl.Message(
        author=AUTHOR_SYSTEM,
        content=(
            "✏️ **Sửa SQL thủ công**:\n"
            "- Anh có thể nhập trực tiếp câu lệnh SQL vào khung chat này (bắt đầu bằng `SQL:` hoặc paste code block SQL).\n"
            "- Hoặc chuyển sang tab **Remediation Matrix** / **SQL Studio** trên Dashboard để chỉnh sửa và chạy trực tiếp trên Staging."
        ),
    ).send()


@cl.action_callback("replan_agent")
@cl.action_callback("action_replan")
async def on_replan_agent(action: cl.Action) -> None:
    """[🤖 Cho Agent 1 Re-plan] — Bounded Reflection Loop, tối đa 1 lượt."""
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    report: Optional[AgentReport] = cl.user_session.get("report")
    if agent is None or report is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy phiên xử lý.").send()
        return

    row = await run_in_threadpool(incident_store.get_incident, report.incident_id)
    failed_details = ((row or {}).get("audit") or {}).get("failed_details") or {
        "error_message": str((row or {}).get("error") or "")
    }

    thinking = cl.Message(
        author=AUTHOR_SRE,
        content="🔁 Em đọc lại lỗi của chị Auditor, soi `DESCRIBE` bảng rồi viết script v2 ạ…",
    )
    await thinking.send()

    result = await run_agent_with_live_steps(
        agent, lambda: agent.replan_with_feedback(failed_details, agent.retry_count)
    )
    thinking.content = result.get("summary") or "_(không có tổng kết)_"
    await thinking.update()

    if not result.get("allowed"):
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content=(
                "⛔ Hết lượt re-plan. Anh sửa SQL tay ở **SQL Studio** trên dashboard (`/`), "
                "hoặc escalate cho on-call theo runbook nhé."
            ),
        ).send()
        return

    new_report = AgentReport.model_validate(result.get("report") or {})
    cl.user_session.set("report", new_report)
    incident_store.set_status(
        report.incident_id,
        result.get("status") or "WAITING_SHADOW_APPROVAL",
        report_json=new_report.model_dump_json(),
        retry_count=result.get("retry_count"),
        ready_for_production=False,
    )
    await cl.Message(author=AUTHOR_SRE, content=new_report.to_markdown()).send()
    if new_report.remediation.is_stageable:
        await cl.Message(
            author=AUTHOR_SYSTEM,
            content="Anh xem plan v2 rồi duyệt chạy thử lại giúp em nhé 🙆‍♀️",
            actions=approval_actions(),
        ).send()


@cl.action_callback("reject")
async def on_reject(action: cl.Action) -> None:
    """[❌ Từ chối] — không thực thi gì, hỏi lý do rồi để Agent 1 re-plan."""
    await action.remove()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    if agent is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Không tìm thấy phiên điều tra.").send()
        return

    report: Optional[AgentReport] = cl.user_session.get("report")
    if report is not None:
        tools.lock_remediation(report.incident_id)
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
    """
    [🕵️‍♀️ Recheck / Nghiệm thu độc lập] — engineer CHỌN cho Agent 2 vào kiểm.

    Dùng cho cả 2 tình huống:
      - ngay sau khi Agent 1 vá xong (trả lời "có recheck"),
      - hoặc chạy lại bất cứ lúc nào sau đó.
    """
    await action.remove()
    if cl.user_session.get("agent") is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Chưa có ca nào để nghiệm thu ạ 🙏").send()
        return

    first_time = bool(cl.user_session.get("awaiting_recheck_decision"))
    cl.user_session.set("awaiting_recheck_decision", False)
    cl.user_session.set("audit_skipped", False)

    report: Optional[AgentReport] = cl.user_session.get("report")
    await run_in_threadpool(
        data_audit.log_tool_call,
        "human_decision",
        "REQUEST_RECHECK",
        f"incident={report.incident_id if report else '?'}",
        "ok",
        "Engineer yêu cầu Agent 2 nghiệm thu độc lập",
    )
    await cl.Message(
        author=AUTHOR_ENGINEER,
        content="🕵️‍♀️ **YÊU CẦU RECHECK** — cho Agent 2 nghiệm thu độc lập.",
    ).send()
    await run_independent_audit(trigger="auto" if first_time else "manual")


@cl.action_callback("skip_audit")
async def on_skip_audit(action: cl.Action) -> None:
    """
    [⚡ Không cần, chốt luôn] — đóng incident mà KHÔNG chạy Agent 2.

    Dùng khi engineer đã biết rõ lỗi đơn giản, không cần tốn thêm thời gian/token cho
    vòng nghiệm thu độc lập. Quyết định này được ghi vào `agent_audit_log` để sau này
    truy được: ca nào đã bỏ qua Checker và ai bỏ qua.
    """
    await action.remove()

    report: Optional[AgentReport] = cl.user_session.get("report")
    if report is None:
        await cl.Message(author=AUTHOR_SYSTEM, content="Chưa có ca nào để chốt ạ 🙏").send()
        return

    cl.user_session.set("awaiting_recheck_decision", False)
    cl.user_session.set("audit_skipped", True)
    cl.user_session.set("resolved", True)

    await run_in_threadpool(
        data_audit.log_tool_call,
        "human_decision",
        "SKIP_RECHECK",
        f"incident={report.incident_id}",
        "ok",
        "Engineer chốt luôn, bỏ qua nghiệm thu độc lập của Agent 2",
    )

    await cl.Message(
        author=AUTHOR_ENGINEER,
        content="⚡ **CHỐT LUÔN** — bỏ qua bước recheck của Agent 2.",
    ).send()

    agent: Optional[DataReliabilityAgent] = cl.user_session.get("agent")
    usage = agent.usage.describe() if agent else "n/a"
    snapshot = await run_in_threadpool(warehouse_snapshot, report.target_table)

    await cl.Message(
        author=AUTHOR_SYSTEM,
        content=(
            f"### ✅ Incident `{report.incident_id}` → **RESOLVED** (không recheck)\n"
            f"- 🛠️ Hành động: `{report.remediation.action_type.value}`\n"
            f"- 🧾 Agent 1 tự verify: vi phạm về **0**\n"
            f"- 💰 Tổng chi phí: {usage} _(tiết kiệm được cả lượt Agent 2)_\n"
            + (f"\n{snapshot}\n" if snapshot else "")
            + "\n⚠️ **Lưu ý cho hồ sơ:** ca này **chưa qua nghiệm thu độc lập**, nên bằng "
            "chứng duy nhất là lời tự verify của Agent 1. Quyết định bỏ qua đã được ghi vào "
            "`agent_audit_log` (`human_decision` / `SKIP_RECHECK`) để audit sau này truy được.\n\n"
            "Đổi ý thì anh bấm nút dưới đây, em cho Agent 2 vào soi lại bất cứ lúc nào ạ 🙆‍♀️"
        ),
        actions=audit_actions(),
    ).send()
