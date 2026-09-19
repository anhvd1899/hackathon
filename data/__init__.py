"""
SCOPE: DATA — Data Engineering
==============================

Owner: Data Engineer / Analytics Engineer.

Chứa mọi thứ liên quan tới dữ liệu và pipeline, **không chứa logic AI, không chứa web**:

    data/
    ├── connection.py    Kết nối DuckDB dùng chung + chạy SQL an toàn (transaction, lock)
    ├── warehouse.py     DDL + sinh dữ liệu demo + cấy lỗi DQ (bootstrap warehouse)
    ├── audit.py         agent_audit_log + dq_baseline_snapshot (mốc trước/sau khi vá)
    ├── incidents.py     Đóng gói Incident Envelope từ kết quả DQ test
    ├── dq.py            Bộ DQ test (chạy được không cần dbt) + ghi dq_test_results
    ├── jobs/            Các data flow job chạy bằng CLI
    │   ├── seed_warehouse.py     tạo lại warehouse + cấy lỗi
    │   ├── ingest_mobile_app.py  mô phỏng job ingestion upstream gây ra sự cố
    │   ├── run_dq_tests.py       chạy DQ test (dbt nếu có, không thì SQL thuần)
    │   └── rebuild_marts.py      build lại mart hạ nguồn
    └── dbt/             dbt project (dbt-duckdb): sources, tests, mart models

QUY TẮC: package này **KHÔNG được import `ai/` hoặc `web/`**. Nhờ vậy Data Engineer
có thể đổi schema / thêm job / thêm dbt test mà không cần biết gì về agent hay UI.

Giao diện mà scope khác được dùng (public API):
    - connection: get_connection, fetch, execute_script, list_tables, row_count, to_jsonable
    - warehouse : ensure_database, bootstrap
    - audit     : log_tool_call, capture_baseline, read_incident_context, detect_quarantine_table
    - incidents : build_sample_incident_payload
    - dq        : run_all_checks, DQ_CHECKS
"""

from __future__ import annotations

from data.audit import (  # noqa: F401
    capture_baseline,
    detect_quarantine_table,
    log_tool_call,
    read_incident_context,
)
from data.connection import (  # noqa: F401
    close_connection,
    execute_script,
    fetch,
    get_connection,
    list_tables,
    row_count,
    to_jsonable,
)
from data.incidents import build_sample_incident_payload  # noqa: F401
from data.warehouse import bootstrap, ensure_database  # noqa: F401

__all__ = [
    "get_connection",
    "close_connection",
    "fetch",
    "execute_script",
    "list_tables",
    "row_count",
    "to_jsonable",
    "bootstrap",
    "ensure_database",
    "log_tool_call",
    "capture_baseline",
    "read_incident_context",
    "detect_quarantine_table",
    "build_sample_incident_payload",
]
