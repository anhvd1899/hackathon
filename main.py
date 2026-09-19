"""
main.py — Entrypoint duy nhất của ứng dụng
==========================================

    uvicorn main:app --host 0.0.0.0 --port 8000
    python main.py                      # tương đương, đọc HOST/PORT từ .env

Sau khi chạy:
    http://localhost:8000/chat      UI Human-in-the-loop (Chainlit)
    http://localhost:8000/docs      Swagger của REST API
    http://localhost:8000/health    Health check

Các entrypoint khác theo scope:
    python -m data.jobs.seed_warehouse --force    # scope DATA: tạo lại warehouse
    python -m data.jobs.run_dq_tests              # scope DATA: chạy DQ test
    python -m ai.check_llm                        # scope AI: kiểm model + tool calling
    python -m ai.agent                            # scope AI: Agent 1 (CLI)
    python -m ai.auditor                          # scope AI: Agent 2 (CLI)
"""

from __future__ import annotations

import config
from web.server import app

__all__ = ["app"]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=config.HOST, port=config.PORT, reload=False)
