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

DEFAULT_BASE_URL = "https://maas.api.greennode.ai/v1"
DEFAULT_MODEL = "Llama-3.3-70B-Instruct"

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


@dataclass
class LLMSettings:
    """Cấu hình kết nối MaaS (đọc từ biến môi trường)."""

    api_key: Optional[str] = None
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    temperature: float = 0.1
    max_tokens: int = 2048
    max_iterations: int = 12
    request_timeout: float = 120.0
    #: Danh sách model để luân chuyển / failover. Rỗng = chỉ dùng `model`.
    model_pool: List[str] = field(default_factory=list)
    #: "maker" (Agent 1) hoặc "auditor" (Agent 2) — tách con trỏ round-robin
    role: str = "maker"

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
        settings = cls(
            api_key=api_key or None,
            base_url=os.getenv("DRA_BASE_URL") or os.getenv("OPENAI_BASE_URL") or DEFAULT_BASE_URL,
            model=model,
            temperature=float(os.getenv("DRA_TEMPERATURE", "0.1")),
            max_tokens=int(os.getenv("DRA_MAX_TOKENS", "2048")),
            max_iterations=int(os.getenv("DRA_MAX_ITERATIONS", "12")),
            request_timeout=float(os.getenv("DRA_TIMEOUT", "120")),
            model_pool=pool,
            role="maker",
        )
        # Có pool nhưng không chỉ định DRA_MODEL -> lấy model đầu pool
        if pool and not os.getenv("DRA_MODEL"):
            settings.model = pool[0]
        # Luôn đảm bảo model đang dùng có mặt trong pool để failover xoay vòng được
        if pool and settings.model not in pool:
            settings.model_pool = [settings.model] + pool
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

        for env_name, attr, caster in (
            ("DRA_AUDITOR_TEMPERATURE", "temperature", float),
            ("DRA_AUDITOR_MAX_TOKENS", "max_tokens", int),
            ("DRA_AUDITOR_MAX_ITERATIONS", "max_iterations", int),
        ):
            raw = os.getenv(env_name)
            if raw:
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
    "LLMSettings",
    "ToolEvent",
    "parse_model_pool",
    "pick_from_pool",
    "reset_pool_rotation",
    "MockFunction",
    "MockToolCall",
    "MockMessage",
    "MockChoice",
    "MockResponse",
    "extract_json",
]
