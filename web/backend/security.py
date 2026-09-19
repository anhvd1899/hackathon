"""
web/backend/security.py — Bảo vệ REST API
=========================================

Xác thực tối thiểu bằng Bearer token cho các endpoint `/api/*`.

LƯU Ý BẢO MẬT: nếu `DRA_API_TOKEN` không được set thì `/api/*` là **KHÔNG có xác thực**.
Khi deploy lên môi trường dùng chung hãy set token (hoặc đặt sau API Gateway), vì
`POST /api/incidents?auto_approve=true` có thể khiến agent GHI dữ liệu mà không qua
người duyệt.
"""

from __future__ import annotations

from fastapi import HTTPException, Request, status

import config


async def require_token(request: Request) -> None:
    """Dependency: chặn request nếu thiếu/sai `Authorization: Bearer <DRA_API_TOKEN>`."""
    if not config.API_TOKEN:
        return
    header = request.headers.get("authorization", "")
    provided = header[7:].strip() if header.lower().startswith("bearer ") else ""
    if provided != config.API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Thiếu hoặc sai Bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def warn_if_open() -> None:
    """In cảnh báo lúc startup nếu API đang mở."""
    if not config.API_TOKEN:
        print(
            "[web] ⚠️  DRA_API_TOKEN chưa được set -> REST API /api/* đang MỞ (không auth). "
            "Hãy set token khi deploy môi trường dùng chung."
        )


__all__ = ["require_token", "warn_if_open"]
