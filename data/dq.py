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

from data.connection import CONN_LOCK, fetch, get_connection, scalar


@dataclass(frozen=True)
class DQCheck:
    """
    Một DQ test trên warehouse.

    Có 2 nguồn:
      - `source="dbt"`      : sinh từ manifest dbt (mặc định, mở rộng vô hạn)
      - `source="builtin"`  : bộ dự phòng viết tay trong file này
    """

    test_name: str
    model: str
    column_name: str
    test_type: str  # not_null | unique | accepted_values | singular | ...
    count_sql: str
    description: str = ""
    accepted_values: Optional[List[str]] = None
    source: str = "builtin"
    severity: str = "error"

    def run(self) -> int:
        """
        Trả về số dòng vi phạm (0 = đạt).

        **Cố tình KHÔNG nuốt lỗi SQL.** Nếu dùng `scalar()` (có default) thì một check
        bị lỗi — ví dụ bảng staging chưa được dbt build — sẽ trả 0 và bị báo là PASS,
        tức là hệ thống nói "dữ liệu sạch" trong khi thực tế nó không kiểm được gì.
        Ném lỗi lên để caller đánh dấu trạng thái `error` thay vì `pass`.
        """
        if not self.count_sql.strip():
            raise ValueError(f"Check '{self.test_name}' không có count_sql")
        result = fetch(self.count_sql.rstrip().rstrip(";"), max_rows=1)
        if not result["rows"]:
            raise ValueError(f"Check '{self.test_name}' không trả về dòng nào")
        value = list(result["rows"][0].values())[0]
        try:
            return int(value or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Check '{self.test_name}' trả về giá trị không phải số: {value!r}"
            ) from exc


#: Bộ check DỰ PHÒNG, chỉ dùng khi chưa có manifest dbt (chưa cài dbt / chưa chạy dbt).
#: Nguồn sự thật chính là các test trong `data/dbt/` — xem `load_checks()` bên dưới.
FALLBACK_CHECKS: List[DQCheck] = [
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

# ---------------------------------------------------------------------------
# Nguồn sự thật: test dbt (manifest) — fallback về FALLBACK_CHECKS
# ---------------------------------------------------------------------------


def load_checks(prefer_dbt: bool = True) -> List[DQCheck]:
    """
    Trả về bộ DQ check đang hiệu lực.

    Ưu tiên **test dbt đọc từ manifest**: mỗi test dbt (generic hay singular) được
    chuyển thành một `DQCheck` với `count_sql` bọc quanh SQL mà chính dbt đã compile.
    Nhờ vậy thêm test vào `data/dbt/models/*/schema.yml` là hệ thống tự biết — không
    phải sửa dòng Python nào, và không bị fix cứng vào use case demo.

    Chỉ khi chưa có manifest (chưa cài dbt-duckdb, hoặc chưa `dbt docs generate`) thì
    mới dùng `FALLBACK_CHECKS`.
    """
    if prefer_dbt:
        try:
            from data.dbt_runner import dbt_tests

            converted = [
                DQCheck(
                    test_name=test.name,
                    model=test.model or "fact_orders",
                    column_name=test.column_name,
                    test_type=test.test_type,
                    count_sql=test.count_sql,
                    description=test.description or f"dbt test `{test.name}`",
                    accepted_values=None,
                    source="dbt",
                    severity=test.severity,
                )
                for test in dbt_tests()
                if test.count_sql
            ]
            if converted:
                return converted
        except Exception:  # noqa: BLE001 - thiếu dbt thì rơi về fallback
            pass
    return list(FALLBACK_CHECKS)


def checks_by_name(prefer_dbt: bool = True) -> Dict[str, DQCheck]:
    return {check.test_name: check for check in load_checks(prefer_dbt)}


class _LazyCheckList(list):
    """
    Cho phép `DQ_CHECKS` / `CHECKS_BY_NAME` vẫn dùng như trước (code cũ không phải sửa)
    nhưng luôn đọc lại từ manifest khi được truy cập.
    """

    def _refresh(self) -> None:
        self[:] = load_checks()

    def __iter__(self):  # type: ignore[override]
        self._refresh()
        return list.__iter__(self)

    def __len__(self) -> int:  # type: ignore[override]
        self._refresh()
        return list.__len__(self)

    def __getitem__(self, index):  # type: ignore[override]
        self._refresh()
        return list.__getitem__(self, index)


class _LazyCheckMap(dict):
    """Bản dict tương ứng, tự nạp lại từ manifest mỗi lần tra cứu."""

    def _refresh(self) -> None:
        self.clear()
        self.update(checks_by_name())

    def get(self, key, default=None):  # type: ignore[override]
        self._refresh()
        return dict.get(self, key, default)

    def __getitem__(self, key):  # type: ignore[override]
        self._refresh()
        return dict.__getitem__(self, key)

    def __contains__(self, key) -> bool:  # type: ignore[override]
        self._refresh()
        return dict.__contains__(self, key)

    def __iter__(self):  # type: ignore[override]
        self._refresh()
        return dict.__iter__(self)

    def __len__(self) -> int:  # type: ignore[override]
        self._refresh()
        return dict.__len__(self)

    def keys(self):  # type: ignore[override]
        self._refresh()
        return dict.keys(self)

    def values(self):  # type: ignore[override]
        self._refresh()
        return dict.values(self)


DQ_CHECKS: List[DQCheck] = _LazyCheckList(FALLBACK_CHECKS)
CHECKS_BY_NAME: Dict[str, DQCheck] = _LazyCheckMap()


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
    for check in load_checks():
        try:
            failures = check.run()
            status = "fail" if failures else "pass"
            message = f"Got {failures} results, configured to fail if != 0" if failures else "OK"
        except Exception as exc:  # noqa: BLE001
            # Không kiểm được KHÁC với kiểm xong và sạch -> phải là 'error', không phải 'pass'
            failures = 0
            status = "error"
            message = f"Không chạy được check: {exc}"[:400]
        results.append(
            {
                "run_id": rid,
                "test_name": check.test_name,
                "model": check.model,
                "column_name": check.column_name,
                "status": status,
                "failures": failures,
                "executed_at": stamp,
                "message": message,
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


__all__ = [
    "DQCheck",
    "FALLBACK_CHECKS",
    "DQ_CHECKS",
    "CHECKS_BY_NAME",
    "load_checks",
    "checks_by_name",
    "run_all_checks",
    "latest_run_id",
]
