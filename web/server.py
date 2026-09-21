"""
web/server.py — FastAPI app + dashboard + mount Chainlit
========================================================

Một process, một port, ba mặt tiền:

    /            Dashboard (HTML/CSS/JS tĩnh)  -> web/frontend/static/
    /api/*       REST API                      -> web/backend/api.py
    /chat        UI Human-in-the-loop          -> web/frontend/ui.py (Chainlit)

Ngoài ra khởi động một **scheduler nền** để job chạy theo lịch và sự cố được phát hiện
mà không cần ai bấm gì (xem `web/backend/api.py::scheduler_loop`).

Chạy:  uvicorn main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

import config
from web.backend.api import router, scheduler_loop, startup
from web.backend.security import warn_if_open

STATIC_DIR = Path(__file__).resolve().parent / "frontend" / "static"

#: Thư mục artifact của dbt (index.html + manifest.json + catalog.json)
DBT_TARGET_DIR = config.DBT_PROJECT_DIR / "target"

#: Task của scheduler nền, giữ tham chiếu để shutdown gọn gàng
_scheduler_task: Optional[asyncio.Task] = None


def create_app() -> FastAPI:
    """Tạo FastAPI app: REST + dashboard tĩnh + mount Chainlit."""
    app = FastAPI(
        title=config.APP_TITLE,
        version=config.APP_VERSION,
        description=(
            "Hệ 2 agent theo mô hình Maker–Checker trên 10 data flow: scheduler phát hiện "
            "sự cố DQ, Agent 1 (Data SRE) điều tra nền và đề xuất script vá, engineer duyệt "
            "trên UI, Agent 2 (Data Auditor) nghiệm thu độc lập. "
            f"Dashboard tại /, UI Human-in-the-loop tại {config.CHAINLIT_PATH}."
        ),
    )

    app.include_router(router)

    # --- dbt docs (Lineage Graph DAG) -------------------------------------
    # dbt sinh `target/index.html` là một SPA tự nạp manifest.json + catalog.json nằm
    # cùng thư mục, nên chỉ cần mount cả target/ là giao diện lineage chạy nguyên bản —
    # kể cả DAG có hàng trăm node. Không dùng /docs vì đó là Swagger của FastAPI.
    if DBT_TARGET_DIR.is_dir():
        app.mount("/dbt-docs", StaticFiles(directory=str(DBT_TARGET_DIR)), name="dbt-docs")

    @app.get("/lineage", include_in_schema=False)
    async def lineage_page() -> FileResponse:
        """Trang bọc iframe dbt docs (để có thanh điều hướng của app)."""
        return FileResponse(STATIC_DIR / "lineage.html")

    @app.get("/query", include_in_schema=False)
    async def query_page() -> FileResponse:
        """Trang Web SQL Client / DBeaver-style Explorer."""
        return FileResponse(STATIC_DIR / "query.html")

    @app.get("/pipeline", include_in_schema=False)
    @app.get("/dag", include_in_schema=False)
    async def pipeline_page() -> FileResponse:
        """Trang Airflow-style Pipeline Orchestrator & DAG Inspector."""
        return FileResponse(STATIC_DIR / "pipeline.html")

    # --- Dashboard tĩnh ---------------------------------------------------
    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

        @app.get("/", include_in_schema=False)
        async def dashboard_page() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

        # Cho phép nạp ./styles.css và ./app.js theo đường dẫn tương đối từ "/"
        @app.get("/styles.css", include_in_schema=False)
        async def styles() -> FileResponse:
            return FileResponse(STATIC_DIR / "styles.css", media_type="text/css")

        @app.get("/app.js", include_in_schema=False)
        async def script() -> FileResponse:
            return FileResponse(STATIC_DIR / "app.js", media_type="application/javascript")

    else:  # pragma: no cover - chỉ xảy ra nếu thiếu thư mục static

        @app.get("/", include_in_schema=False)
        async def fallback() -> RedirectResponse:
            return RedirectResponse(url=config.CHAINLIT_PATH)

    @app.on_event("startup")
    async def _on_startup() -> None:
        global _scheduler_task
        await startup()
        warn_if_open()
        _scheduler_task = asyncio.create_task(scheduler_loop())

    @app.on_event("shutdown")
    async def _on_shutdown() -> None:
        if _scheduler_task is not None and not _scheduler_task.done():
            _scheduler_task.cancel()

    # Mount Chainlit SAU khi khai báo route để không bị nuốt path.
    # Import trễ vì chainlit sẽ load `ui.py` như một module riêng.
    from chainlit.utils import mount_chainlit

    mount_chainlit(app=app, target=str(config.CHAINLIT_TARGET), path=config.CHAINLIT_PATH)
    return app


app = create_app()

__all__ = ["app", "create_app"]
