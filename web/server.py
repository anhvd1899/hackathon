"""
web/server.py — FastAPI app + mount Chainlit
============================================

Gộp REST API (scope backend) và UI Chainlit (scope frontend) vào **cùng một process,
cùng một port**:

    FastAPI app ──┬── /health, /api/*   -> web/backend/api.py
                  └── /chat             -> web/frontend/ui.py (mount_chainlit)
    GET / -> redirect sang /chat

Chạy:  uvicorn main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

import config
from web.backend.api import router, startup
from web.backend.security import warn_if_open


def create_app() -> FastAPI:
    """Tạo FastAPI app và mount Chainlit vào đường dẫn `config.CHAINLIT_PATH`."""
    app = FastAPI(
        title=config.APP_TITLE,
        version=config.APP_VERSION,
        description=(
            "Hệ 2 agent theo mô hình Maker–Checker: Agent 1 (Data SRE) điều tra & vá dữ liệu "
            "trên DuckDB sau khi engineer duyệt; Agent 2 (Data Auditor) nghiệm thu độc lập. "
            f"UI Human-in-the-loop tại {config.CHAINLIT_PATH}."
        ),
    )

    app.include_router(router)

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:  # pragma: no cover - chỉ redirect
        return RedirectResponse(url=config.CHAINLIT_PATH)

    @app.on_event("startup")
    async def _on_startup() -> None:
        await startup()
        warn_if_open()

    # Mount Chainlit SAU khi đã khai báo route để không bị nuốt path.
    # Import trễ vì chainlit sẽ load `ui.py` như một module riêng.
    from chainlit.utils import mount_chainlit

    mount_chainlit(app=app, target=str(config.CHAINLIT_TARGET), path=config.CHAINLIT_PATH)
    return app


app = create_app()

__all__ = ["app", "create_app"]
