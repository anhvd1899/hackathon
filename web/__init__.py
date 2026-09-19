"""
SCOPE: WEB — Backend + Frontend
===============================

Owner: Web / Fullstack Engineer.

    web/
    ├── server.py            FastAPI app + mount Chainlit (1 process, 1 port)
    ├── backend/
    │   ├── api.py           REST API /health, /api/* (headless, cho hệ thống khác gọi)
    │   └── security.py      Bearer token cho /api/*
    └── frontend/
        ├── ui.py            Chainlit app: Human-in-the-loop (nút Duyệt/Từ chối/Nghiệm thu)
        ├── rendering.py     Hàm render markdown cho tool step, bảng, nút
        └── chainlit.md      Trang welcome của Chainlit

Kiến trúc mount:

    FastAPI app ──┬── /health, /api/*   (REST)
                  └── /chat             (Chainlit UI)
    GET / -> redirect sang /chat

QUY TẮC: package này được import `ai/` và `data/`, nhưng `ai/` và `data/` **không bao giờ
import `web/`**. Nhờ vậy Web Engineer đổi UI/API mà không ảnh hưởng agent hay pipeline.

Chạy:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""
