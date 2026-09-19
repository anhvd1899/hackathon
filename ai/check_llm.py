"""
check_llm.py
============
Smoke test kết nối MaaS (GreenNode / VNG Cloud) TRƯỚC khi demo.

Kiểm tra theo thứ tự rủi ro giảm dần:
  1. Cấu hình đã đọc được chưa (api_key / base_url).
  2. Endpoint có sống không + model id có đúng không (`/models`).
  3. Với **TỪNG model** sẽ được dùng (Maker, Checker, và mọi model trong pool):
     a. chat completion cơ bản;
     b. **Function/Tool Calling** — điều kiện SỐNG CÒN: cả 2 agent đều dựa 100% vào
        tool calling để query DuckDB. Model không hỗ trợ thì agent sẽ "bịa" số liệu.
     c. vòng lặp đầy đủ tool -> kết quả thật -> model kết luận;
     d. `response_format={"type":"json_object"}` (nice-to-have, thiếu thì có fallback).
  4. Cảnh báo nếu Maker và Checker đang dùng CHUNG model (mất tác dụng cross-model).

Cách dùng:
    python -m ai.check_llm                      # kiểm Maker + Checker + pool
    python -m ai.check_llm --model qwen/qwen3.6-flash   # chỉ kiểm 1 model
    python -m ai.check_llm --list-models
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple

from ai import tools  # noqa: F401  - nạp .env + khởi tạo đường dẫn DuckDB
from ai.llm import LLMSettings

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"

results: List[Tuple[str, str, str]] = []


def record(name: str, status: str, detail: str = "") -> None:
    results.append((name, status, detail))
    print(f"{status}  {name}" + (f"\n       └─ {detail}" if detail else ""))


def mask(secret: Optional[str]) -> str:
    if not secret:
        return "(trống)"
    if len(secret) <= 10:
        return secret[:2] + "*" * (len(secret) - 2)
    return f"{secret[:6]}…{secret[-4:]} (dài {len(secret)} ký tự)"


def probe_model(client: Any, model: str, label: str) -> Dict[str, bool]:
    """Chạy bộ kiểm tra cho một model cụ thể. Trả về dict các mục đạt/không."""
    print()
    print(f"┌─ Kiểm model: {model}   [{label}]")
    outcome = {"chat": False, "tools": False, "loop": False, "json": False}

    # --- a. chat completion ------------------------------------------------
    try:
        ping = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Trả lời cực ngắn."},
                {"role": "user", "content": "Nói đúng một từ: OK"},
            ],
            max_tokens=16,
            temperature=0,
        )
        content = (ping.choices[0].message.content or "").strip()
        usage = getattr(ping, "usage", None)
        record(
            f"[{model}] chat completion",
            PASS,
            f"trả về {content[:50]!r}" + (f" | tokens={usage.total_tokens}" if usage else ""),
        )
        outcome["chat"] = True
    except Exception as exc:  # noqa: BLE001
        record(
            f"[{model}] chat completion",
            FAIL,
            f"{type(exc).__name__}: {str(exc)[:240]}\n"
            "       Thường do: sai base_url (thiếu /v1), sai model id, key hết hạn/quota.",
        )
        return outcome

    # --- b + c. tool calling & vòng lặp đầy đủ -----------------------------
    try:
        probe = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Bạn là SRE dữ liệu. Bạn KHÔNG được đoán số liệu. "
                        "Muốn biết số liệu thì phải gọi tool tool_query_duckdb."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        "Bảng fact_orders có bao nhiêu dòng customer_id bị NULL? "
                        "Hãy dùng tool để truy vấn."
                    ),
                },
            ],
            tools=tools.TOOLS_SCHEMA,
            tool_choice="auto",
            max_tokens=512,
            temperature=0,
        )
        message = probe.choices[0].message
        tool_calls = getattr(message, "tool_calls", None)
        if not tool_calls:
            record(
                f"[{model}] tool/function calling",
                FAIL,
                "Model KHÔNG gọi tool (chỉ trả text: "
                f"{' '.join((message.content or '').split())[:100]!r}).\n"
                "       Model này KHÔNG dùng được cho cả Agent 1 và Agent 2.",
            )
            return outcome

        first = tool_calls[0]
        args_preview = " ".join(str(first.function.arguments or "").split())[:110]
        record(
            f"[{model}] tool/function calling",
            PASS,
            f"gọi `{first.function.name}` với args: {args_preview}",
        )
        outcome["tools"] = True

        # Chạy thật tool đó trên DuckDB rồi đưa kết quả về cho model
        result = tools.execute_tool(first.function.name, first.function.arguments)
        followup = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Trả lời ngắn, dựa đúng số liệu từ tool."},
                {"role": "user", "content": "fact_orders có bao nhiêu dòng customer_id NULL?"},
                {
                    "role": "assistant",
                    "content": message.content,
                    "tool_calls": [
                        {
                            "id": first.id,
                            "type": "function",
                            "function": {
                                "name": first.function.name,
                                "arguments": first.function.arguments or "{}",
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": first.id,
                    "name": first.function.name,
                    "content": json.dumps(result, ensure_ascii=False, default=str)[:4000],
                },
            ],
            max_tokens=256,
            temperature=0,
        )
        answer = " ".join((followup.choices[0].message.content or "").split())
        record(
            f"[{model}] vòng lặp tool → kết quả → kết luận",
            PASS if answer else WARN,
            f"model kết luận: {answer[:160]!r}",
        )
        outcome["loop"] = bool(answer)
    except Exception as exc:  # noqa: BLE001
        record(
            f"[{model}] tool/function calling",
            FAIL,
            f"{type(exc).__name__}: {str(exc)[:240]}\n"
            "       Endpoint có thể không nhận tham số `tools` -> model này không dùng được.",
        )
        return outcome

    # --- d. JSON mode ------------------------------------------------------
    try:
        js = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": 'Trả về đúng JSON: {"severity": "HIGH", "rows": 15}'}
            ],
            response_format={"type": "json_object"},
            max_tokens=128,
            temperature=0,
        )
        raw = (js.choices[0].message.content or "").strip()
        json.loads(raw)
        record(f"[{model}] JSON mode (response_format)", PASS, f"JSON hợp lệ: {raw[:60]}")
        outcome["json"] = True
    except Exception as exc:  # noqa: BLE001
        record(
            f"[{model}] JSON mode (response_format)",
            WARN,
            f"không hỗ trợ ({type(exc).__name__}: {str(exc)[:100]}). "
            "Không sao — agent tự fallback bóc JSON từ text.",
        )
    return outcome


def main() -> int:
    parser = argparse.ArgumentParser(description="Kiểm tra kết nối MaaS cho Data Reliability Squad")
    parser.add_argument("--api-key", default=None, help="Ghi đè DRA_API_KEY")
    parser.add_argument("--base-url", default=None, help="Ghi đè DRA_BASE_URL")
    parser.add_argument("--model", default=None, help="Chỉ kiểm duy nhất model này")
    parser.add_argument("--list-models", action="store_true", help="In toàn bộ model endpoint trả về")
    args = parser.parse_args()

    maker = LLMSettings.from_env()
    if args.api_key:
        maker.api_key = args.api_key
    if args.base_url:
        maker.base_url = args.base_url
    if args.model:
        maker.model = args.model
        maker.model_pool = [args.model]
    checker = LLMSettings.for_auditor(avoid_model=maker.model)
    if args.model:
        checker.model = args.model
        checker.model_pool = [args.model]

    print("=" * 78)
    print("KIỂM TRA KẾT NỐI MaaS — Data Reliability Squad (Maker · Checker)")
    print("=" * 78)
    print(f"  base_url          : {maker.base_url}")
    print(f"  api_key           : {mask(maker.api_key)}")
    print(f"  model Maker  (A1) : {maker.model}")
    print(f"  model Checker(A2) : {checker.model}")
    print(f"  model pool        : {', '.join(maker.model_pool) or '(không dùng pool)'}")
    print(f"  timeout           : {maker.request_timeout}s")
    print("-" * 78)

    # ---- 1. Cấu hình -------------------------------------------------------
    if not maker.api_key:
        record(
            "1. Cấu hình API key",
            FAIL,
            "Chưa có DRA_API_KEY. Tạo file .env (copy từ .env.example) rồi điền "
            "DRA_API_KEY / DRA_BASE_URL / DRA_MODEL. Không có key thì app chạy OFFLINE.",
        )
        return 1
    record("1. Cấu hình API key", PASS, f"đọc được key {mask(maker.api_key)}")

    try:
        from openai import OpenAI
    except ImportError:
        record("1b. Package openai", FAIL, "pip install -r requirements.txt")
        return 1

    client = OpenAI(
        api_key=maker.api_key,
        base_url=maker.base_url,
        timeout=maker.request_timeout,
        max_retries=1,
    )

    # ---- 2. Liệt kê model --------------------------------------------------
    # Tập model cần kiểm: Maker + Checker + toàn bộ pool (vì failover có thể nhảy vào)
    to_check: List[str] = []
    for model in [maker.model, checker.model, *maker.model_pool, *checker.model_pool]:
        if model and model not in to_check:
            to_check.append(model)

    available: List[str] = []
    try:
        available = [m.id for m in client.models.list().data]
        if args.list_models:
            for mid in available:
                print(f"       • {mid}")
        missing = [m for m in to_check if m not in available]
        if not available:
            record("2. Danh sách model", WARN, "endpoint trả về danh sách rỗng")
        elif missing:
            record(
                "2. Danh sách model",
                FAIL,
                f"model không tồn tại: {missing}. Model khả dụng: " + ", ".join(available[:10]),
            )
        else:
            record(
                "2. Danh sách model",
                PASS,
                f"{len(available)} model khả dụng, {len(to_check)} model sẽ dùng đều tồn tại",
            )
    except Exception as exc:  # noqa: BLE001
        record(
            "2. Danh sách model",
            WARN,
            f"không gọi được /models ({type(exc).__name__}: {str(exc)[:120]}). "
            "Nhiều MaaS chặn endpoint này — bỏ qua, các bước sau vẫn kết luận được.",
        )

    # ---- 3. Kiểm từng model ------------------------------------------------
    roles: Dict[str, List[str]] = {}
    for model in to_check:
        tags = []
        if model == maker.model:
            tags.append("Maker/Agent 1")
        if model == checker.model:
            tags.append("Checker/Agent 2")
        if not tags:
            tags.append("pool/failover")
        roles[model] = tags

    outcomes: Dict[str, Dict[str, bool]] = {}
    for model in to_check:
        outcomes[model] = probe_model(client, model, " + ".join(roles[model]))

    # ---- 4. Cross-model -----------------------------------------------------
    print()
    if maker.model == checker.model:
        record(
            "4. Cross-model checking",
            WARN,
            "Maker và Checker đang dùng CHUNG model -> hai agent có cùng điểm mù, "
            "Checker dễ đồng thuận với Maker. Set DRA_AUDITOR_MODEL hoặc DRA_MODEL_POOL "
            "trong .env để bật cross-model.",
        )
    else:
        record(
            "4. Cross-model checking",
            PASS,
            f"Maker `{maker.model}` vs Checker `{checker.model}` — hai model khác nhau ✅",
        )

    # ---- Kết luận ---------------------------------------------------------
    print("-" * 78)
    unusable = [m for m, o in outcomes.items() if not (o["chat"] and o["tools"])]
    if unusable:
        print("KẾT LUẬN: ❌ Có model KHÔNG dùng được (thiếu chat hoặc tool calling):")
        for model in unusable:
            print(f"  • {model}  [{' + '.join(roles[model])}]")
        print("  Hãy bỏ model đó khỏi .env, rồi chạy lại check_llm.py.")
        print("=" * 78)
        return 1

    print("KẾT LUẬN: ✅ Tất cả model đều dùng được (chat + tool calling).")
    no_json = [m for m, o in outcomes.items() if not o["json"]]
    if no_json:
        print(f"  (JSON mode không hỗ trợ ở: {', '.join(no_json)} — agent có fallback, vẫn chạy)")
    print("  Chạy Agent 1 CLI    : python -m ai.agent")
    print("  Chạy Agent 2 CLI    : python -m ai.auditor")
    print("  Chạy UI + API       : uvicorn app:app --port 8000  ->  http://localhost:8000/chat")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
