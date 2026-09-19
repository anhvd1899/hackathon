"""
data/dq.py — Bộ Data Quality test
=================================

Định nghĩa các DQ test ở MỘT chỗ duy nhất, dùng chung cho:
  - `data/dbt/models/schema.yml` (khi chạy dbt thật)
  - `data/jobs/run_dq_tests.py`  (khi chạy không cần dbt)
  - `data/incidents.py`          (đóng gói kết quả fail thành Incident Envelope)

Mỗi check có `count_sql` trả về đúng một số: **số dòng vi phạm** (kỳ vọng = 0).
Đây cũng chính là câu SQL mà Agent 2 dùng lại để nghiệm thu độc lập, nên nó phải là
định nghĩa "nguồn sự thật" của rule — không để agent tự bịa.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from data.connection import CONN_LOCK, get_connection, scalar


@dataclass(frozen=True)
class DQCheck:
    """Một DQ test trên warehouse."""

    test_name: str
    model: str
    column_name: str
    test_type: str  # not_null | unique | accepted_values | positive_value
    count_sql: str
    description: str = ""
    accepted_values: Optional[List[str]] = None

    def run(self) -> int:
        """Trả về số dòng vi phạm."""
        value = scalar(self.count_sql, default=0)
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0


DQ_CHECKS: List[DQCheck] = [
    DQCheck(
        test_name="not_null_fact_orders_customer_id",
        model="fact_orders",
        column_name="customer_id",
        test_type="not_null",
        count_sql="SELECT COUNT(*) AS violations FROM fact_orders WHERE customer_id IS NULL",
        description="Mọi đơn hàng phải quy được về một khách hàng.",
    ),
    DQCheck(
        test_name="accepted_values_fact_orders_order_status",
        model="fact_orders",
        column_name="order_status",
        test_type="accepted_values",
        count_sql=(
            "SELECT COUNT(*) AS violations FROM fact_orders WHERE order_status NOT IN "
            "('COMPLETED','PENDING','CANCELLED','REFUNDED')"
        ),
        description="order_status phải nằm trong tập giá trị đã chuẩn hoá.",
        accepted_values=["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"],
    ),
    DQCheck(
        test_name="unique_fact_orders_order_id",
        model="fact_orders",
        column_name="order_id",
        test_type="unique",
        count_sql=(
            "SELECT COUNT(*) AS violations FROM (SELECT order_id FROM fact_orders "
            "GROUP BY order_id HAVING COUNT(*) > 1)"
        ),
        description="order_id là khoá chính, không được trùng.",
    ),
    DQCheck(
        test_name="positive_value_fact_orders_total_amount",
        model="fact_orders",
        column_name="total_amount",
        test_type="positive_value",
        count_sql="SELECT COUNT(*) AS violations FROM fact_orders WHERE total_amount < 0",
        description="Doanh thu không được âm (refund phải ghi ở cột riêng).",
    ),
    DQCheck(
        test_name="valid_email_fact_orders_customer_email",
        model="fact_orders",
        column_name="customer_email",
        test_type="not_null",
        count_sql=(
            "SELECT COUNT(*) AS violations FROM fact_orders "
            "WHERE customer_email IS NOT NULL AND customer_email NOT LIKE '%@%'"
        ),
        description="Email phải đúng định dạng cơ bản (có '@').",
    ),
    DQCheck(
        test_name="not_null_fact_orders_order_date",
        model="fact_orders",
        column_name="order_date",
        test_type="not_null",
        count_sql="SELECT COUNT(*) AS violations FROM fact_orders WHERE order_date IS NULL",
        description="Mọi đơn phải có ngày đặt.",
    ),
]

CHECKS_BY_NAME: Dict[str, DQCheck] = {c.test_name: c for c in DQ_CHECKS}


def run_all_checks(
    run_id: Optional[str] = None,
    executed_at: Optional[datetime] = None,
    persist: bool = True,
) -> List[Dict[str, Any]]:
    """
    Chạy toàn bộ DQ check và (mặc định) ghi kết quả vào bảng `dq_test_results`.

    Đây là bản "mock dbt test" — cùng shape dữ liệu với `run_results.json` của dbt
    nên phần downstream (incident envelope, agent) không cần biết test chạy bằng gì.
    """
    stamp = executed_at or datetime.now()
    rid = run_id or f"dq-run-{stamp:%Y%m%d-%H%M%S}"

    results: List[Dict[str, Any]] = []
    for check in DQ_CHECKS:
        failures = check.run()
        results.append(
            {
                "run_id": rid,
                "test_name": check.test_name,
                "model": check.model,
                "column_name": check.column_name,
                "status": "fail" if failures else "pass",
                "failures": failures,
                "executed_at": stamp,
                "message": (
                    f"Got {failures} results, configured to fail if != 0" if failures else "OK"
                ),
            }
        )

    if persist:
        con = get_connection()
        with CONN_LOCK:
            con.execute("DELETE FROM dq_test_results WHERE run_id = ?", [rid])
            con.executemany(
                "INSERT INTO dq_test_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        r["run_id"], r["test_name"], r["model"], r["column_name"],
                        r["status"], r["failures"], r["executed_at"], r["message"],
                    )
                    for r in results
                ],
            )
    return results


def latest_run_id() -> Optional[str]:
    """run_id của lần chạy DQ test gần nhất."""
    value = scalar(
        "SELECT run_id FROM dq_test_results ORDER BY executed_at DESC, run_id DESC LIMIT 1"
    )
    return str(value) if value else None


__all__ = ["DQCheck", "DQ_CHECKS", "CHECKS_BY_NAME", "run_all_checks", "latest_run_id"]
