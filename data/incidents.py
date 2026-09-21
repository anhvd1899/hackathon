"""
data/incidents.py — Đóng gói Incident Envelope
==============================================

Biến kết quả DQ test (bảng `dq_test_results`) + log pipeline thành **Incident Envelope**
dạng dict để scope AI nhận vào.

Chủ ý trả về `dict` chứ không phải Pydantic model: scope `data/` không được phụ thuộc
`ai/schemas.py`. Bên AI sẽ tự validate bằng `IncidentInput(**payload)`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import config
from data.connection import fetch, scalar
from data.dq import CHECKS_BY_NAME, latest_run_id
from data.warehouse import BROKEN_SOURCE, BROKEN_VERSION, ensure_database

#: Log ingestion mô phỏng — bằng chứng "mềm" giúp agent khoanh vùng nguyên nhân
PIPELINE_LOG_TAIL: List[str] = [
    "02:29:58 [INFO ] ingest_mobile_app_v3: pulling batch window 2026-09-15",
    "02:30:01 [WARN ] ingest_mobile_app_v3: field 'user_ref' missing in 15 payloads (sdk 3.4.1)",
    "02:30:02 [INFO ] ingest_mobile_app_v3: loaded 15 rows into staging",
    "02:30:44 [ERROR] dbt: FAIL 15 not_null_fact_orders_customer_id",
    "02:30:44 [ERROR] dbt: Done. PASS=32 WARN=0 ERROR=4 SKIP=7 TOTAL=43",
]


def build_incident_payload(
    test_name: str = "source_not_null_warehouse_fact_orders_customer_id",
    incident_id: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Đóng gói Incident Envelope cho MỘT DQ test đang fail.

    Mọi số liệu đều query trực tiếp từ DuckDB nên luôn khớp thực tế — agent không
    thể "được mớm" số sai từ đầu vào.
    """
    ensure_database(db_path or config.DUCKDB_PATH)

    check = CHECKS_BY_NAME.get(test_name)
    if check is None:
        raise ValueError(
            f"Không có DQ test '{test_name}'. Các test khả dụng: {sorted(CHECKS_BY_NAME)}"
        )

    failures = int(scalar(check.count_sql, default=0) or 0)
    # Dùng bảng mục tiêu động thay vì hardcode fact_orders
    target_table = check.model or "fact_orders"
    total = int(scalar(f"SELECT COUNT(*) AS c FROM {target_table}", default=0) or 0)
    run_id = latest_run_id() or "dq-run-unknown"

    sample = fetch(
        f"SELECT * FROM {check.model} WHERE {check.column_name} IS NULL LIMIT 3"
        if check.test_type == "not_null"
        else f"SELECT * FROM {check.model} LIMIT 3",
        max_rows=3,
    )["rows"]

    other_failed = fetch(
        "SELECT test_name, column_name, failures FROM dq_test_results "
        f"WHERE status = 'fail' AND test_name <> '{test_name}' AND run_id = '{run_id}' "
        "ORDER BY failures DESC",
        max_rows=20,
    )["rows"]

    return {
        "incident_id": incident_id or "INC-2026-DQ01",
        "incident_type": "DATA_QUALITY",
        "target_table": f"main.{check.model}",
        "source": "dbt",
        "description": (
            f"dbt test FAILED: test `{check.test_name}` phát hiện {failures} dòng vi phạm "
            f"trên tổng {total} dòng của bảng {check.model}. "
            "Job dbt build chạy lúc 02:30 ngày 2026-09-15 bị dừng, các mart hạ nguồn "
            "(mart_daily_revenue, mart_customer_ltv) đang giữ dữ liệu không nhất quán."
        ),
        "evidence_payload": {
            "dbt_run_id": run_id,
            "failed_test": check.test_name,
            "test_type": check.test_type,
            "model": check.model,
            "column": check.column_name,
            "accepted_values": check.accepted_values,
            "failures": failures,
            "total_rows_scanned": total,
            "failure_rate_pct": round(failures * 100.0 / total, 3) if total else 0.0,
            "compiled_sql": check.count_sql,
            "rule_description": check.description,
            "sample_failed_rows": sample,
            "other_failed_tests": [
                {
                    "test_name": row.get("test_name"),
                    "column": row.get("column_name"),
                    "failures": row.get("failures"),
                }
                for row in other_failed
            ],
            "upstream_hint": {
                "source_system": BROKEN_SOURCE,
                "source_version": BROKEN_VERSION,
            },
            "pipeline_log_tail": PIPELINE_LOG_TAIL,
        },
    }


def build_dynamic_sample_incident_payload(db_path: Optional[str] = None) -> Dict[str, Any]:
    """Generate dynamic incident sample based on current failed tests."""
    ensure_database(db_path or config.DUCKDB_PATH)
    
    # Find any failing test to create a realistic sample
    failed_tests_result = fetch(
        "SELECT test_name, column_name, failures, model FROM dq_test_results "
        "WHERE status = 'fail' ORDER BY failures DESC LIMIT 1",
        max_rows=1
    )
    
    if failed_tests_result["rows"]:
        # Use real failing test
        failed_test = failed_tests_result["rows"][0]
        test_name = failed_test["test_name"]
        
        return build_incident_payload(
            test_name=test_name,
            db_path=db_path
        )
    else:
        # No real failures, create sample with available data
        # Find the largest table as a good example
        tables_result = fetch("SHOW TABLES", max_rows=50)
        
        if tables_result["rows"]:
            tables = [row["name"] for row in tables_result["rows"]]
            
            # Prefer fact tables
            fact_tables = [t for t in tables if "fact" in t.lower()]
            main_table = fact_tables[0] if fact_tables else tables[0]
            
            # Generate a synthetic incident for demonstration
            return {
                "incident_id": "INC-DEMO-001",
                "incident_type": "DATA_QUALITY", 
                "target_table": f"main.{main_table}",
                "source": "dynamic_discovery",
                "description": (
                    f"Dynamic incident sample for table {main_table}. "
                    f"This is a demonstration of the universal incident handling system."
                ),
                "evidence_payload": {
                    "dbt_run_id": "demo-run",
                    "failed_test": f"sample_test_{main_table}",
                    "test_type": "custom",
                    "model": main_table,
                    "column": "dynamic_column",
                    "accepted_values": None,
                    "failures": 0,
                    "total_rows_scanned": 0,
                    "failure_rate_pct": 0.0,
                    "compiled_sql": f"SELECT COUNT(*) FROM {main_table}",
                    "rule_description": "Dynamic sample incident for demonstration",
                    "sample_failed_rows": [],
                    "other_failed_tests": [],
                    "upstream_hint": {
                        "source_system": "dynamic_system",
                        "source_version": "dynamic_version",
                    },
                    "pipeline_log_tail": [
                        f"[INFO] Dynamic discovery found table: {main_table}",
                        "[INFO] Generating sample incident for demonstration",
                        "[INFO] Universal system ready for any data quality issue"
                    ],
                }
            }
        else:
            # Fallback to basic structure
            return {
                "incident_id": "INC-EMPTY-001", 
                "incident_type": "DATA_QUALITY",
                "target_table": "main.no_tables",
                "source": "empty_database",
                "description": "No tables found in database - empty database scenario",
                "evidence_payload": {
                    "dbt_run_id": "empty-run",
                    "failed_test": "no_test",
                    "test_type": "empty",
                    "model": "no_tables",
                    "column": "no_column", 
                    "accepted_values": None,
                    "failures": 0,
                    "total_rows_scanned": 0,
                    "failure_rate_pct": 0.0,
                    "compiled_sql": "SELECT 0",
                    "rule_description": "Empty database - no data to analyze",
                    "sample_failed_rows": [],
                    "other_failed_tests": [],
                    "upstream_hint": {
                        "source_system": "empty",
                        "source_version": "none",
                    },
                    "pipeline_log_tail": [
                        "[WARN] No tables found in database",
                        "[INFO] System ready to handle incidents when data is available"
                    ],
                }
            }


def build_sample_incident_payload(db_path: Optional[str] = None) -> Dict[str, Any]:
    """Enhanced sample incident that adapts to current database state."""
    # Try dynamic approach first, fallback to hardcoded if needed
    try:
        return build_dynamic_sample_incident_payload(db_path)
    except Exception:
        # Fallback to original hardcoded approach
        return build_incident_payload(db_path=db_path)


__all__ = ["build_incident_payload", "build_sample_incident_payload", "build_dynamic_sample_incident_payload", "PIPELINE_LOG_TAIL"]
