"""
data/connection.py — Kết nối DuckDB dùng chung
==============================================

DuckDB là in-process OLAP: mỗi tiến trình chỉ nên mở **một** connection tới file, và
connection đó không thread-safe. Module này giữ một connection singleton kèm `RLock`
để mọi lời gọi (từ agent chạy trong threadpool, từ FastAPI, từ job CLI) đều được
tuần tự hoá an toàn.

Lưu ý quan trọng đã gặp trong thực tế: **không được** mở thêm `duckdb.connect(path,
read_only=True)` ở nơi khác trong cùng process — DuckDB sẽ báo
"Can't open a connection to same database file with a different configuration".
Vì vậy mọi thành phần phải đi qua `get_connection()`.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

import duckdb

import config

#: Lock dùng chung cho toàn bộ truy cập DuckDB trong process.
CONN_LOCK = threading.RLock()

_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Trả về connection dùng chung; tự bootstrap warehouse nếu file chưa tồn tại."""
    global _conn
    with CONN_LOCK:
        if _conn is None:
            # import trễ để tránh phụ thuộc vòng warehouse <-> connection
            from data.warehouse import ensure_database

            ensure_database(config.DUCKDB_PATH)
            _conn = duckdb.connect(config.DUCKDB_PATH)
        return _conn


def close_connection() -> None:
    """Đóng connection (dùng khi shutdown app, hoặc trước khi tạo lại file DB)."""
    global _conn
    with CONN_LOCK:
        if _conn is not None:
            _conn.close()
            _conn = None


# ---------------------------------------------------------------------------
# Chuyển kiểu DuckDB -> kiểu JSON-safe
# ---------------------------------------------------------------------------


def to_jsonable(value: Any) -> Any:
    """Decimal/date/timestamp/bytes -> kiểu mà `json.dumps` hiểu được."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value)


# ---------------------------------------------------------------------------
# Đọc dữ liệu
# ---------------------------------------------------------------------------


def fetch(query: str, max_rows: int = config.MAX_RESULT_ROWS) -> Dict[str, Any]:
    """
    Chạy một câu SELECT và trả về dict:
        {columns, rows, row_count, returned_rows, truncated}

    `max_rows` chỉ giới hạn số dòng TRẢ VỀ; `row_count` vẫn là tổng số dòng thật.
    """
    con = get_connection()
    with CONN_LOCK:
        cur = con.execute(query)
        columns = [d[0] for d in cur.description] if cur.description else []
        raw_rows = cur.fetchall()

    rows = [
        {col: to_jsonable(val) for col, val in zip(columns, row)} for row in raw_rows[:max_rows]
    ]
    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(raw_rows),
        "returned_rows": len(rows),
        "truncated": len(raw_rows) > max_rows,
    }


def scalar(query: str, default: Any = None) -> Any:
    """Lấy giá trị đầu tiên của dòng đầu tiên (tiện cho COUNT(*))."""
    try:
        result = fetch(query, max_rows=1)
    except duckdb.Error:
        return default
    if not result["rows"]:
        return default
    return list(result["rows"][0].values())[0]


def list_tables() -> List[str]:
    """Danh sách bảng hiện có trong warehouse."""
    try:
        return [r["name"] for r in fetch("SHOW TABLES", max_rows=500)["rows"]]
    except (duckdb.Error, KeyError):
        return []


def row_count(table: str) -> Optional[int]:
    """Đếm số dòng của bảng; trả None nếu bảng không tồn tại/không đếm được."""
    value = scalar(f"SELECT COUNT(*) AS c FROM {table}")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def table_exists(table: str) -> bool:
    return table.lower() in {t.lower() for t in list_tables()}


# ---------------------------------------------------------------------------
# Ghi dữ liệu
# ---------------------------------------------------------------------------


def execute_script(statements: Iterable[str]) -> List[Dict[str, Any]]:
    """
    Chạy nhiều câu lệnh ghi trong MỘT transaction.

    Lỗi ở bất kỳ câu nào -> ROLLBACK toàn bộ rồi raise `duckdb.Error`.
    Các lệnh BEGIN/COMMIT/ROLLBACK do người gọi (hoặc LLM) tự thêm sẽ bị bỏ qua
    vì transaction đã do hàm này quản lý.
    """
    con = get_connection()
    executed: List[Dict[str, Any]] = []
    with CONN_LOCK:
        try:
            con.execute("BEGIN TRANSACTION")
            for statement in statements:
                stmt = statement.strip()
                if not stmt:
                    continue
                head = stmt.split(None, 1)[0].lower()
                if head in {"begin", "commit", "rollback"}:
                    executed.append({"statement": stmt, "status": "skipped (tự quản transaction)"})
                    continue
                cur = con.execute(stmt)
                info: Dict[str, Any] = {"statement": stmt, "status": "ok"}
                if cur.description:
                    fetched = cur.fetchall()
                    if fetched and len(fetched[0]) == 1:
                        info["result"] = to_jsonable(fetched[0][0])
                executed.append(info)
            con.execute("COMMIT")
        except duckdb.Error:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            raise
    return executed


__all__ = [
    "CONN_LOCK",
    "get_connection",
    "close_connection",
    "to_jsonable",
    "fetch",
    "scalar",
    "list_tables",
    "row_count",
    "table_exists",
    "execute_script",
]
