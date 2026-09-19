"""
config.py — Cấu hình dùng chung cho cả 3 scope
==============================================

Đây là module "lá" (leaf): KHÔNG import gì từ `data/`, `ai/`, `web/` — nhờ vậy cả ba
scope đều import được mà không tạo phụ thuộc vòng.

Quy tắc phụ thuộc của project (rất quan trọng khi chia việc):

    config.py  (đường dẫn, biến môi trường)
       ▲   ▲   ▲
       │   │   └──────────────┐
    data/  ai/ ───────────►   web/
      ▲     │
      └─────┘
    data  : KHÔNG import ai/ hay web/
    ai    : được import data/ (để chạy SQL), KHÔNG import web/
    web   : được import ai/ và data/

Ai sửa file nào thì chỉ cần chạy lại scope của mình, không sợ vỡ scope khác.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# 1. Nạp biến môi trường từ .env (không cần thêm dependency)
# ---------------------------------------------------------------------------


def load_env_file(path: str | os.PathLike[str] | None = None) -> Path | None:
    """
    Nạp `.env` vào os.environ. Biến đã có sẵn trong môi trường (Docker, AgentBase,
    CI) LUÔN được ưu tiên — file .env chỉ bù những gì còn thiếu.

    Trả về đường dẫn file đã nạp, hoặc None nếu không có file nào.
    """
    candidate = Path(path or os.getenv("DRA_ENV_FILE") or (PROJECT_ROOT / ".env"))
    if not candidate.is_file():
        return None
    try:
        for raw in candidate.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError:
        return None
    return candidate


ENV_FILE = load_env_file()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# 2. Scope DATA — warehouse
# ---------------------------------------------------------------------------

#: File DuckDB. Vừa là Source (chứa data lỗi) vừa là Sink (nơi ghi data sạch).
DUCKDB_PATH: str = os.getenv("DRA_DUCKDB_PATH") or str(
    PROJECT_ROOT / "data" / "warehouse.duckdb"
)

#: Thư mục dbt project (scope data).
DBT_PROJECT_DIR: Path = Path(os.getenv("DRA_DBT_DIR") or (PROJECT_ROOT / "data" / "dbt"))

#: Số dòng tối đa một tool được trả về cho LLM (chống vỡ context window).
MAX_RESULT_ROWS: int = _env_int("DRA_MAX_RESULT_ROWS", 50)


# ---------------------------------------------------------------------------
# 3. Scope AI — runbook
# ---------------------------------------------------------------------------

#: Thư mục runbook nội bộ mà agent được đọc (SLA, lineage, playbook...).
RUNBOOK_DIR: Path = Path(os.getenv("DRA_RUNBOOK_DIR") or (PROJECT_ROOT / "ai" / "runbooks"))


# ---------------------------------------------------------------------------
# 4. Scope WEB — server
# ---------------------------------------------------------------------------

APP_TITLE: str = "Data Reliability Squad (Maker · Checker)"
APP_VERSION: str = "2.0.0"

#: Đường dẫn mount UI Chainlit trong FastAPI.
CHAINLIT_PATH: str = os.getenv("DRA_CHAINLIT_PATH", "/chat")

#: File Chainlit app (target của mount_chainlit) — thuộc scope web/frontend.
CHAINLIT_TARGET: Path = PROJECT_ROOT / "web" / "frontend" / "ui.py"

HOST: str = os.getenv("DRA_HOST", "0.0.0.0")
PORT: int = _env_int("PORT", _env_int("DRA_PORT", 8000))

#: Token bảo vệ REST API /api/*. Để trống = KHÔNG xác thực (chỉ nên dùng khi demo local).
API_TOKEN: str = os.getenv("DRA_API_TOKEN", "").strip()


__all__ = [
    "PROJECT_ROOT",
    "ENV_FILE",
    "load_env_file",
    "DUCKDB_PATH",
    "DBT_PROJECT_DIR",
    "MAX_RESULT_ROWS",
    "RUNBOOK_DIR",
    "APP_TITLE",
    "APP_VERSION",
    "CHAINLIT_PATH",
    "CHAINLIT_TARGET",
    "HOST",
    "PORT",
    "API_TOKEN",
]
