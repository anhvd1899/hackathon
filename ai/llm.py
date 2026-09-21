"""
ai/llm.py — Hạ tầng LLM dùng chung cho cả 2 agent
=================================================

Chứa những gì Agent 1 (Maker) và Agent 2 (Checker) đều cần:

  - `LLMSettings`      : cấu hình kết nối MaaS (OpenAI-compatible) + **luân chuyển model**.
  - `ToolEvent`        : một lần gọi tool, dùng để stream lên UI và ghi audit.
  - Mock primitives    : giả lập object response của OpenAI SDK cho chế độ OFFLINE.
  - `extract_json`     : bóc JSON từ output LLM (kể cả khi bị bọc markdown fence).

Không chứa prompt, không chứa vòng lặp agent — những thứ đó thuộc `agent.py` / `auditor.py`.
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Mặc định trỏ vào Google AI Studio (Gemini) qua endpoint **OpenAI-compatible**.
#
# Nhờ endpoint này mà toàn bộ code agent (OpenAI SDK + tool calling) dùng được với
# Gemini mà không phải viết lại: chỉ đổi `DRA_BASE_URL` + `DRA_MODEL` trong .env.
# Đổi nhà cung cấp khác (GreenNode/VNG, OpenAI, Groq…) cũng chỉ là đổi 2 biến đó.
DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

# Để rỗng có chủ ý: tên model của Google thay đổi theo thời điểm, hardcode một tên
# cũ sẽ gây lỗi 404 khó hiểu. Hãy chạy `python -m ai.check_llm --auto-pick` để hệ
# thống tự liệt kê model tài khoản bạn có và chọn cái rẻ nhất chạy được.
DEFAULT_MODEL = ""

# ---------------------------------------------------------------------------
# 1. Luân chuyển model (cross-model rotation)
# ---------------------------------------------------------------------------
#
# Con trỏ round-robin dùng chung cho cả process, tách theo "role" (maker/auditor)
# để hai vai không vô tình bốc trùng model của nhau.

_pool_lock = threading.Lock()
_pool_cursor: Dict[str, int] = {}

# Lỗi tạm thời / lỗi thuộc về model -> nên đổi sang model khác trong pool.
# Lỗi cấu hình (sai key, 401/403) thì đổi model cũng vô ích nên KHÔNG failover.
_FAILOVER_HINTS = (
    "rate limit", "rate_limit", "429", "too many requests", "quota",
    "500", "502", "503", "504", "overloaded", "capacity", "unavailable",
    "timeout", "timed out", "model not found", "does not exist", "no such model",
    "context length", "context_length", "maximum context",
)


#: Xếp hạng "độ rẻ token" theo từ khoá trong tên model. Điểm càng thấp càng rẻ.
#: Dùng để `check_llm --auto-pick` thử model rẻ trước, không phụ thuộc việc hardcode
#: một tên model cụ thể (tên model của các nhà cung cấp thay đổi liên tục).
CHEAPNESS_HINTS: List[tuple[str, int]] = [
    ("flash-lite", 0),
    ("lite", 1),
    ("mini", 1),
    ("flash", 2),
    ("small", 3),
    ("8b", 3),
    ("pro", 8),
    ("ultra", 9),
    ("thinking", 9),
]


def model_cost_rank(model_id: str) -> int:
    """
    Điểm ước lượng độ tốn token của một model (thấp = rẻ).

    Chỉ dựa vào tên nên là heuristic, nhưng đủ để ưu tiên thử `*-flash-lite` trước
    `*-pro`. Model không khớp từ khoá nào được cho điểm trung bình.

    So khớp theo *segment* (cắt tên theo `-`, `_`, `.`, `/`, khoảng trắng) chứ không
    phải substring, vì substring gây dương tính giả kinh điển: chữ "ge-MINI" chứa
    "mini" nên `gemini-2.5-pro` từng bị tính là model rẻ. Riêng từ khoá có dấu `-`
    (vd "flash-lite") vẫn so khớp trên cả tên vì nó trải qua nhiều segment.
    """
    name = (model_id or "").lower()
    segments = {seg for seg in re.split(r"[^a-z0-9]+", name) if seg}
    scores: List[int] = []
    for keyword, score in CHEAPNESS_HINTS:
        hit = keyword in name if "-" in keyword else keyword in segments
        if hit:
            scores.append(score)
    return min(scores) if scores else 5


#: Sentinel do Google cung cấp để bỏ qua kiểm tra thought_signature khi client không
#: có signature để gửi lại (ví dụ lịch sử dựng lại từ DB, hoặc mock/offline).
GEMINI_SIGNATURE_BYPASS = "skip_thought_signature_validator"


def is_google_endpoint(base_url: str) -> bool:
    """base_url có phải endpoint OpenAI-compatible của Google AI Studio."""
    return "generativelanguage.googleapis.com" in (base_url or "")


def _tool_call_extra_content(tool_call: Any) -> Optional[Dict[str, Any]]:
    """
    Bóc field phi chuẩn `extra_content` của một tool_call.

    Gemini gắn chữ ký phần suy luận vào
    `choices[0].message.tool_calls[N].extra_content.google.thought_signature`.
    OpenAI SDK cho phép field lạ nên nó nằm ở `model_extra`, không phải attribute.
    """
    extra = getattr(tool_call, "extra_content", None)
    if extra is None:
        model_extra = getattr(tool_call, "model_extra", None) or {}
        extra = model_extra.get("extra_content")
    if extra is None and isinstance(tool_call, dict):
        extra = tool_call.get("extra_content")
    if hasattr(extra, "model_dump"):
        try:
            extra = extra.model_dump(exclude_none=True)
        except Exception:  # noqa: BLE001
            extra = None
    return extra if isinstance(extra, dict) and extra else None


def assistant_message_to_dict(msg: Any, gemini: bool = False) -> Dict[str, Any]:
    """
    Chuyển message object của SDK (hoặc mock) về dict để append vào history.

    **Bắt buộc giữ `extra_content` của từng tool_call.** Gemini ký phần "thinking" rồi
    đính chữ ký vào tool_call; lượt sau replay lịch sử mà thiếu chữ ký thì API trả
    400 INVALID_ARGUMENT "Function call is missing a thought_signature". Client OpenAI
    chuẩn vứt field lạ này đi, nên phải tự bê nguyên sang.

    Khi không có chữ ký (mock offline, lịch sử dựng lại từ DB) thì điền sentinel
    `skip_thought_signature_validator` — đường thoát do Google cung cấp — để request
    vẫn hợp lệ. Chỉ áp dụng cho endpoint Google; provider khác không nhận field này.
    """
    out: Dict[str, Any] = {"role": "assistant", "content": getattr(msg, "content", None)}
    tool_calls = getattr(msg, "tool_calls", None)
    if not tool_calls:
        return out

    entries: List[Dict[str, Any]] = []
    for tool_call in tool_calls:
        entry: Dict[str, Any] = {
            "id": tool_call.id,
            "type": "function",
            "function": {
                "name": tool_call.function.name,
                "arguments": tool_call.function.arguments or "{}",
            },
        }
        extra = _tool_call_extra_content(tool_call)
        if extra:
            entry["extra_content"] = extra
        elif gemini:
            entry["extra_content"] = {
                "google": {"thought_signature": GEMINI_SIGNATURE_BYPASS}
            }
        entries.append(entry)
    out["tool_calls"] = entries
    return out


def normalize_model_id(model_id: str) -> str:
    """
    Bỏ tiền tố 'models/' mà Gemini trả về ở endpoint `/models`.

    Endpoint OpenAI-compatible của Google liệt kê id dạng `models/gemini-...` nhưng
    khi gọi chat.completions thì dùng tên trần.
    """
    text = (model_id or "").strip()
    return text[len("models/"):] if text.startswith("models/") else text


def parse_model_pool(raw: Optional[str]) -> List[str]:
    """Đọc danh sách model từ chuỗi env, phân tách bằng ',' hoặc ';'."""
    if not raw:
        return []
    seen: set[str] = set()
    pool: List[str] = []
    for part in re.split(r"[,;]", raw):
        item = part.strip()
        if item and item not in seen:
            seen.add(item)
            pool.append(item)
    return pool


def pick_from_pool(pool: List[str], role: str, avoid: Optional[str] = None) -> Optional[str]:
    """
    Bốc model kế tiếp trong pool theo round-robin.

    `avoid`: model muốn tránh (thường là model của vai còn lại). Nếu pool còn lựa chọn
    khác thì sẽ bỏ qua `avoid` — đây là cốt lõi của cross-model checking: Checker không
    nên dùng chung model với Maker vì hai bên sẽ có cùng điểm mù.
    """
    if not pool:
        return None
    candidates = [m for m in pool if m != avoid] or list(pool)
    with _pool_lock:
        index = _pool_cursor.get(role, 0)
        _pool_cursor[role] = index + 1
    return candidates[index % len(candidates)]


def reset_pool_rotation() -> None:
    """Reset con trỏ round-robin (dùng trong test cho kết quả xác định)."""
    with _pool_lock:
        _pool_cursor.clear()


# ---------------------------------------------------------------------------
# 2. LLMSettings
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 1b. PROFILE TIẾT KIỆM TOKEN
# ---------------------------------------------------------------------------
#
# Chi phí token của một agent loop tăng theo BÌNH PHƯƠNG số vòng lặp: mỗi vòng phải
# gửi lại toàn bộ history (gồm cả output tool của các vòng trước). Vì vậy 4 núm xoay
# hiệu quả nhất là:
#   1. max_iterations       — số vòng gọi LLM tối đa
#   2. max_history_messages — cửa sổ history trước khi nén phần cũ
#   3. max_tool_chars       — cắt output tool (runbook 4KB/lần là rất tốn)
#   4. max_tokens           — độ dài output mỗi lần gọi
#
# `token_budget` là chốt an toàn cuối: vượt ngưỡng thì agent bị buộc kết luận ngay.

# BÀI HỌC TỪ ĐO THỰC TẾ (glm-5.2 trên MaaS VNG, cùng 1 incident):
#
#   max_tokens=1200, tool_chars=1500, không catalog  ->  105k token, 836s, THẤT BẠI
#   max_tokens=2048, tool_chars=4000, có catalog     ->  119k token, 357s, thành công
#
# Nghịch lý: siết `max_tokens` lại TỐN HƠN. Vì glm-5.2 là **reasoning model**, thinking
# token ăn hết hạn mức trước khi nó kịp phát ra tool call / câu trả lời -> output bị cắt
# -> agent phải lặp lại (18 tool call thay vì 11) và JSON báo cáo bị đứt giữa dòng.
# Bỏ catalog cũng phản tác dụng: agent phải tự chạy thêm SHOW TABLES/DESCRIBE.
#
# => Tiết kiệm phải đến từ **ít vòng lặp** + **history nhỏ**, KHÔNG phải từ việc siết
#    output. Các profile dưới đây được tinh chỉnh theo đúng kết luận đó.

PROFILES: Dict[str, Dict[str, Any]] = {
    # Rẻ nhất mà vẫn xong việc: cắt mạnh số vòng lặp, giữ output đủ rộng cho model
    # reasoning, giữ catalog để agent không phải đi khám phá schema.
    "thrifty": {
        "max_iterations": 5,
        "max_tokens": 2500,
        "max_history_messages": 14,
        "max_tool_chars": 3000,
        "max_result_rows": 15,
        "token_budget": 80_000,
        "include_catalog": True,
    },
    # Mặc định: cân bằng giữa chất lượng điều tra và chi phí
    "balanced": {
        "max_iterations": 8,
        "max_tokens": 3000,
        "max_history_messages": 20,
        "max_tool_chars": 4000,
        "max_result_rows": 25,
        "token_budget": 150_000,
        "include_catalog": True,
    },
    # Dành cho lúc demo: cho agent điều tra sâu, báo cáo dày
    "thorough": {
        "max_iterations": 14,
        "max_tokens": 4096,
        "max_history_messages": 60,
        "max_tool_chars": 12000,
        "max_result_rows": 50,
        "token_budget": 0,  # 0 = không giới hạn
        "include_catalog": True,
    },
}

DEFAULT_PROFILE = "balanced"


def resolve_profile(name: Optional[str]) -> tuple[str, Dict[str, Any]]:
    """Trả về (tên profile, tham số). Tên lạ thì fallback về profile mặc định."""
    key = (name or DEFAULT_PROFILE).strip().lower()
    if key not in PROFILES:
        key = DEFAULT_PROFILE
    return key, dict(PROFILES[key])


# ---------------------------------------------------------------------------
# 1c. Đếm token
# ---------------------------------------------------------------------------


@dataclass
class TokenUsage:
    """Cộng dồn token của một agent qua nhiều lần gọi LLM."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    calls: int = 0

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def add(self, response: Any) -> None:
        """Cộng usage từ response của OpenAI SDK (bỏ qua nếu endpoint không trả usage)."""
        self.calls += 1
        usage = getattr(response, "usage", None)
        if usage is None:
            return
        self.prompt_tokens += int(getattr(usage, "prompt_tokens", 0) or 0)
        self.completion_tokens += int(getattr(usage, "completion_tokens", 0) or 0)

    def reset(self) -> None:
        self.prompt_tokens = self.completion_tokens = self.calls = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "calls": self.calls,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }

    def describe(self) -> str:
        return (
            f"{self.calls} lần gọi LLM · {self.total_tokens:,} tokens "
            f"(in {self.prompt_tokens:,} / out {self.completion_tokens:,})"
        )


@dataclass
class LLMSettings:
    """Cấu hình kết nối MaaS + ngân sách token (đọc từ biến môi trường)."""

    api_key: Optional[str] = None
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    temperature: float = 0.1
    max_tokens: int = 2048
    max_iterations: int = 9
    request_timeout: float = 120.0
    #: Danh sách model để luân chuyển / failover. Rỗng = chỉ dùng `model`.
    model_pool: List[str] = field(default_factory=list)
    #: "maker" (Agent 1) hoặc "auditor" (Agent 2) — tách con trỏ round-robin
    role: str = "maker"

    # --- Ngân sách token ---
    profile: str = DEFAULT_PROFILE
    #: Số message giữ lại trước khi nén phần giữa của history
    max_history_messages: int = 22
    #: Cắt output mỗi tool ở bao nhiêu ký tự (runbook/query lớn rất tốn token)
    max_tool_chars: int = 4000
    #: Số dòng tối đa agent nên lấy mỗi query (đưa vào prompt + clamp khi gọi tool)
    max_result_rows: int = 25
    #: Tổng token tối đa cho một lượt agent. 0 = không giới hạn.
    token_budget: int = 150_000
    #: Có nhồi catalog schema vào system prompt không (tốn ~500-1500 token)
    include_catalog: bool = True

    @classmethod
    def from_env(cls) -> "LLMSettings":
        """Cấu hình cho Agent 1 (Maker)."""
        api_key = (
            os.getenv("DRA_API_KEY")
            or os.getenv("GREENNODE_API_KEY")
            or os.getenv("MAAS_API_KEY")
            or os.getenv("OPENAI_API_KEY")
        )
        model = os.getenv("DRA_MODEL", DEFAULT_MODEL)
        pool = parse_model_pool(os.getenv("DRA_MODEL_POOL"))
        profile_name, budget = resolve_profile(os.getenv("DRA_PROFILE"))

        def _num(env_name: str, key: str, caster: Any) -> Any:
            """Biến môi trường cụ thể LUÔN thắng giá trị của profile."""
            raw = os.getenv(env_name)
            if raw is None or raw.strip() == "":
                return budget[key]
            try:
                return caster(raw)
            except ValueError:
                return budget[key]

        settings = cls(
            api_key=api_key or None,
            base_url=os.getenv("DRA_BASE_URL") or os.getenv("OPENAI_BASE_URL") or DEFAULT_BASE_URL,
            model=model,
            temperature=float(os.getenv("DRA_TEMPERATURE", "0.1")),
            request_timeout=float(os.getenv("DRA_TIMEOUT", "120")),
            model_pool=pool,
            role="maker",
            profile=profile_name,
            max_tokens=_num("DRA_MAX_TOKENS", "max_tokens", int),
            max_iterations=_num("DRA_MAX_ITERATIONS", "max_iterations", int),
            max_history_messages=_num("DRA_MAX_HISTORY", "max_history_messages", int),
            max_tool_chars=_num("DRA_MAX_TOOL_CHARS", "max_tool_chars", int),
            max_result_rows=_num("DRA_MAX_RESULT_ROWS", "max_result_rows", int),
            token_budget=_num("DRA_TOKEN_BUDGET", "token_budget", int),
            include_catalog=str(
                os.getenv("DRA_INCLUDE_CATALOG", str(budget["include_catalog"]))
            ).strip().lower() in {"1", "true", "yes", "y"},
        )
        # Có pool nhưng không chỉ định DRA_MODEL -> lấy model đầu pool
        if pool and not os.getenv("DRA_MODEL"):
            settings.model = pool[0]
        # Luôn đảm bảo model đang dùng có mặt trong pool để failover xoay vòng được
        if pool and settings.model not in pool:
            settings.model_pool = [settings.model] + pool
        # Bẫy cấu hình hay gặp: base_url trỏ về chính server của app (localhost:8000)
        # thay vì endpoint MaaS. Khi đó OpenAI SDK sẽ GET /v1/models (và /chat/completions)
        # lên FastAPI của mình -> 404, agent tưởng LLM chết. Cảnh báo ASCII-only để
        # không vỡ console Windows (cp1252).
        _host = (settings.base_url or "").lower()
        if "localhost" in _host or "127.0.0.1" in _host:
            print(
                "[ai] WARNING: DRA_BASE_URL tro ve localhost "
                f"({settings.base_url}) - day la server cua app, khong phai MaaS. "
                "DRA_BASE_URL phai la endpoint OpenAI-compatible "
                "(vi du https://maas.api.greennode.ai/v1)."
            )
        return settings

    @classmethod
    def for_auditor(cls, avoid_model: Optional[str] = None) -> "LLMSettings":
        """
        Cấu hình cho Agent 2 (Checker) — **dùng chung API key / base_url với Agent 1**,
        nhưng model thì tách ra được.

        Thứ tự ưu tiên chọn model:
          1. `DRA_AUDITOR_MODEL` — chỉ định cứng.
          2. `DRA_AUDITOR_MODEL_POOL` (hoặc `DRA_MODEL_POOL`) — luân chuyển round-robin,
             tự động **tránh** model mà Maker đang dùng.
          3. `DRA_MODEL` — fallback: dùng chung model với Maker.

        Vì sao nên khác model: hai model giống nhau có cùng điểm mù, Checker sẽ có xu
        hướng đồng thuận với Maker (correlated failure) thay vì bắt lỗi.
        """
        settings = cls.from_env()
        settings.role = "auditor"
        maker_model = avoid_model or os.getenv("DRA_MODEL") or settings.model

        pool = parse_model_pool(
            os.getenv("DRA_AUDITOR_MODEL_POOL") or os.getenv("DRA_MODEL_POOL")
        )
        settings.model_pool = pool

        forced = os.getenv("DRA_AUDITOR_MODEL", "").strip()
        if forced:
            settings.model = forced
            if pool and forced not in pool:
                settings.model_pool = [forced] + pool
        elif pool:
            settings.model = (
                pick_from_pool(pool, role="auditor", avoid=maker_model) or settings.model
            )

        # Checker chỉ nghiệm thu (ít bước hơn điều tra) nên mặc định rẻ hơn Maker:
        # 2/3 số vòng lặp và 2/3 ngân sách token.
        settings.max_iterations = max(4, int(settings.max_iterations * 2 / 3))
        if settings.token_budget:
            settings.token_budget = int(settings.token_budget * 2 / 3)

        for env_name, attr, caster in (
            ("DRA_AUDITOR_TEMPERATURE", "temperature", float),
            ("DRA_AUDITOR_MAX_TOKENS", "max_tokens", int),
            ("DRA_AUDITOR_MAX_ITERATIONS", "max_iterations", int),
            ("DRA_AUDITOR_TOKEN_BUDGET", "token_budget", int),
        ):
            raw = os.getenv(env_name)
            if raw and raw.strip():
                try:
                    setattr(settings, attr, caster(raw))
                except ValueError:
                    pass
        return settings

    # -- luân chuyển / failover -------------------------------------------

    @property
    def alternatives(self) -> List[str]:
        """Các model khác trong pool có thể chuyển sang."""
        return [m for m in self.model_pool if m != self.model]

    def failover(self, exc: Exception) -> Optional[str]:
        """
        Đổi sang model kế tiếp trong pool khi gặp lỗi tạm thời / lỗi thuộc về model.

        Trả về tên model mới nếu đã đổi, None nếu không nên đổi (ví dụ lỗi 401 sai key
        thì đổi model cũng vô nghĩa) hoặc pool không còn lựa chọn nào khác.
        """
        alternatives = self.alternatives
        if not alternatives:
            return None
        if not any(hint in str(exc).lower() for hint in _FAILOVER_HINTS):
            return None
        failed = self.model
        self.model = alternatives[0]
        # Đẩy model vừa lỗi xuống cuối pool để lần sau không bốc lại ngay
        rest = [m for m in self.model_pool if m not in (self.model, failed)]
        self.model_pool = [self.model] + rest + [failed]
        return self.model

    def describe(self) -> str:
        """Mô tả ngắn để log / hiển thị UI."""
        extra = f" (pool: {', '.join(self.model_pool)})" if len(self.model_pool) > 1 else ""
        return f"{self.model}{extra}"

    def describe_budget(self) -> str:
        """Mô tả ngân sách token đang áp dụng."""
        cap = f"{self.token_budget:,} tokens" if self.token_budget else "không giới hạn"
        return (
            f"profile=`{self.profile}` · tối đa {self.max_iterations} vòng · "
            f"{self.max_tokens} token/lần · ngân sách {cap}"
        )


# ---------------------------------------------------------------------------
# 3. ToolEvent
# ---------------------------------------------------------------------------


@dataclass
class ToolEvent:
    """Một lần agent gọi tool — dùng để stream lên UI và ghi audit."""

    name: str
    arguments: Dict[str, Any] = field(default_factory=dict)
    result: Dict[str, Any] = field(default_factory=dict)
    ok: bool = True
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def short_label(self) -> str:
        if self.name == "tool_query_duckdb":
            query = " ".join(str(self.arguments.get("query", "")).split())
            return f"SQL: {query[:110]}{'…' if len(query) > 110 else ''}"
        if self.name == "tool_read_runbook":
            return f"Đọc runbook: {self.arguments.get('topic', '?')}"
        if self.name == "tool_execute_remediation":
            return "Thực thi remediation trên DuckDB"
        if self.name == "tool_verify_health":
            return f"Verify lại bảng {self.arguments.get('table_name', '?')}"
        if self.name == "tool_get_incident_context":
            return "Lấy bằng chứng khách quan (baseline + lệnh đã chạy)"
        return self.name


# ---------------------------------------------------------------------------
# 4. Mock primitives cho chế độ OFFLINE
# ---------------------------------------------------------------------------
#
# Giả lập đúng shape object mà OpenAI SDK trả về, để vòng lặp agent không cần biết
# mình đang nói chuyện với MaaS thật hay với bộ não mô phỏng.


class MockFunction:
    def __init__(self, name: str, arguments: Dict[str, Any]) -> None:
        self.name = name
        self.arguments = json.dumps(arguments, ensure_ascii=False)


class MockToolCall:
    def __init__(self, name: str, arguments: Dict[str, Any]) -> None:
        self.id = f"call_{uuid.uuid4().hex[:12]}"
        self.type = "function"
        self.function = MockFunction(name, arguments)


class MockMessage:
    def __init__(
        self, content: Optional[str] = None, tool_calls: Optional[List[Any]] = None
    ) -> None:
        self.role = "assistant"
        self.content = content
        self.tool_calls = tool_calls


class MockChoice:
    def __init__(self, message: MockMessage) -> None:
        self.message = message
        self.finish_reason = "tool_calls" if message.tool_calls else "stop"


class MockResponse:
    def __init__(self, message: MockMessage) -> None:
        self.choices = [MockChoice(message)]
        self.usage = None


# ---------------------------------------------------------------------------
# 5. Bóc JSON từ output LLM
# ---------------------------------------------------------------------------


def extract_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Bóc object JSON đầu tiên trong text (LLM hay bọc trong ```json ... ``` hoặc thêm
    lời dẫn). Trả None nếu không parse được.
    """
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned)
        cleaned = re.sub(r"```\s*$", "", cleaned).strip()

    try:
        data = json.loads(cleaned)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass

    # Quét cân bằng ngoặc để lấy object JSON đầu tiên
    start = cleaned.find("{")
    while start != -1:
        depth = 0
        in_str = False
        escape = False
        for idx in range(start, len(cleaned)):
            char = cleaned[idx]
            if in_str:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_str = False
                continue
            if char == '"':
                in_str = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    try:
                        data = json.loads(cleaned[start : idx + 1])
                        if isinstance(data, dict):
                            return data
                    except json.JSONDecodeError:
                        break
        start = cleaned.find("{", start + 1)
    return None


__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_MODEL",
    "DEFAULT_PROFILE",
    "PROFILES",
    "resolve_profile",
    "TokenUsage",
    "LLMSettings",
    "ToolEvent",
    "parse_model_pool",
    "pick_from_pool",
    "reset_pool_rotation",
    "model_cost_rank",
    "normalize_model_id",
    "assistant_message_to_dict",
    "is_google_endpoint",
    "GEMINI_SIGNATURE_BYPASS",
    "CHEAPNESS_HINTS",
    "MockFunction",
    "MockToolCall",
    "MockMessage",
    "MockChoice",
    "MockResponse",
    "extract_json",
]
