"""
data/warehouse.py — Khởi tạo DuckDB warehouse + cấy lỗi DQ để demo
==================================================================

DuckDB đóng 2 vai trong kiến trúc:
  - SOURCE : chứa dữ liệu bẩn để agent tự query điều tra (RCA).
  - SINK   : nơi agent ghi dữ liệu sạch / quarantine sau khi engineer duyệt.

Dữ liệu demo sinh DETERMINISTIC (`random.seed(42)`) để mỗi lần demo ra đúng cùng con
số -> dễ kiểm chứng kết luận của agent.

Kịch bản lỗi được cấy chủ động:
  1. 15 dòng `customer_id IS NULL` — tất cả đến từ `source_system='mobile_app_v3'`,
     `source_version='3.4.1'`, dồn trong đúng 1 batch ingest ngày cuối.
     => Agent phải phát hiện được PATTERN này chứ không phải lỗi rải rác random.
  2.  6 dòng `order_status` sai định dạng ('completed', ' Completed ', 'COMPLETE'...).
  3.  3 dòng `total_amount` âm (refund ghi sai dấu).
  4.  3 dòng `customer_email` sai format (thiếu '@').
  5.  2 dòng `order_id` bị trùng (vi phạm unique key).

CLI:  python -m data.jobs.seed_warehouse --force
"""

from __future__ import annotations

import os
import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import duckdb

import config

# ---------------------------------------------------------------------------
# Tham số sinh dữ liệu
# ---------------------------------------------------------------------------

SEED = 42
N_ROWS = 1000
N_CUSTOMERS = 400

N_NULL_CUSTOMER = 15
N_BAD_STATUS = 6
N_NEGATIVE_AMOUNT = 3
N_BAD_EMAIL = 3
N_DUPLICATE_ID = 2

#: Batch ingest cuối cùng — nơi tập trung lỗi NULL customer_id
BATCH_DATE = date(2026, 9, 15)
BATCH_TS = datetime(2026, 9, 15, 2, 15, 0)
BROKEN_SOURCE = "mobile_app_v3"
BROKEN_VERSION = "3.4.1"
HEALTHY_VERSION = "3.4.0"

VALID_ORDER_STATUSES = ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]

PRODUCT_SKUS = [
    "SKU-LAPTOP-001", "SKU-PHONE-002", "SKU-TABLET-003", "SKU-AUDIO-004",
    "SKU-CAMERA-005", "SKU-WATCH-006", "SKU-MONITOR-007", "SKU-KEYB-008",
]
PAYMENT_METHODS = ["CREDIT_CARD", "MOMO", "ZALOPAY", "COD", "BANK_TRANSFER"]
SOURCE_SYSTEMS = ["erp_core", "web_checkout", BROKEN_SOURCE, "partner_api"]
SOURCE_WEIGHTS = [0.40, 0.30, 0.20, 0.10]
REGIONS = ["HCM", "HN", "DN", "CT", "HP"]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS: List[str] = [
    """
    CREATE OR REPLACE TABLE dim_customers (
        customer_id      VARCHAR PRIMARY KEY,
        customer_name    VARCHAR,
        segment          VARCHAR,
        region           VARCHAR,
        signup_date      DATE
    );
    """,
    """
    CREATE OR REPLACE TABLE fact_orders (
        order_id         BIGINT,
        order_date       DATE,
        customer_id      VARCHAR,
        customer_email   VARCHAR,
        product_sku      VARCHAR,
        quantity         INTEGER,
        unit_price       DECIMAL(14,2),
        total_amount     DECIMAL(14,2),
        currency         VARCHAR,
        order_status     VARCHAR,
        payment_method   VARCHAR,
        source_system    VARCHAR,
        source_version   VARCHAR,
        ingested_at      TIMESTAMP
    );
    """,
    """
    CREATE OR REPLACE TABLE dq_test_results (
        run_id           VARCHAR,
        test_name        VARCHAR,
        model            VARCHAR,
        column_name      VARCHAR,
        status           VARCHAR,
        failures         BIGINT,
        executed_at      TIMESTAMP,
        message          VARCHAR
    );
    """,
    # Nhật ký mọi tool call của agent (truy vết ai/khi nào/chạy gì)
    """
    CREATE TABLE IF NOT EXISTS agent_audit_log (
        log_id           BIGINT,
        logged_at        TIMESTAMP,
        incident_id      VARCHAR,
        tool_name        VARCHAR,
        action           VARCHAR,
        payload          VARCHAR,
        status           VARCHAR,
        detail           VARCHAR
    );
    """,
    # Ảnh chụp trạng thái TRƯỚC khi vá dữ liệu — do tầng code ghi (không qua LLM),
    # giúp Agent 2 có mốc đáng tin để kiểm "có xoá mất dữ liệu oan không".
    # Thiết kế long/narrow (metric-value) nên dùng được cho mọi loại sự cố.
    """
    CREATE TABLE IF NOT EXISTS dq_baseline_snapshot (
        snapshot_id      BIGINT,
        captured_at      TIMESTAMP,
        incident_id      VARCHAR,
        target_table     VARCHAR,
        metric           VARCHAR,
        value            BIGINT,
        detail           VARCHAR
    );
    """,
]

#: SQL build lại 2 mart hạ nguồn — dùng cho cả bootstrap và job rebuild_marts
MART_SQL: Dict[str, str] = {
    "mart_daily_revenue": """
        CREATE OR REPLACE TABLE mart_daily_revenue AS
        SELECT
            order_date,
            COUNT(*)                                             AS order_count,
            COUNT(DISTINCT customer_id)                          AS unique_customers,
            SUM(total_amount)                                    AS gross_revenue,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)  AS orphan_orders
        FROM fact_orders
        GROUP BY order_date
        ORDER BY order_date
    """,
    "mart_customer_ltv": """
        CREATE OR REPLACE TABLE mart_customer_ltv AS
        SELECT
            f.customer_id,
            c.segment,
            c.region,
            COUNT(*)            AS lifetime_orders,
            SUM(f.total_amount) AS lifetime_value,
            MAX(f.order_date)   AS last_order_date
        FROM fact_orders f
        LEFT JOIN dim_customers c USING (customer_id)
        GROUP BY f.customer_id, c.segment, c.region
    """,
}


# ---------------------------------------------------------------------------
# Sinh dữ liệu
# ---------------------------------------------------------------------------


def _gen_customers(rng: random.Random) -> List[Tuple[Any, ...]]:
    """Bảng dimension khách hàng (dữ liệu sạch)."""
    segments = ["ENTERPRISE", "SMB", "RETAIL", "VIP"]
    return [
        (
            f"CUST-{i:05d}",
            f"Khach Hang {i:05d}",
            rng.choice(segments),
            rng.choice(REGIONS),
            date(2024, 1, 1) + timedelta(days=rng.randint(0, 700)),
        )
        for i in range(1, N_CUSTOMERS + 1)
    ]


def _gen_orders(rng: random.Random) -> List[List[Any]]:
    """Sinh N_ROWS đơn hàng sạch (sẽ mutate để cấy lỗi sau)."""
    rows: List[List[Any]] = []
    start_date = BATCH_DATE - timedelta(days=29)

    for idx in range(N_ROWS):
        order_date = start_date + timedelta(days=rng.randint(0, 29))
        cust_no = rng.randint(1, N_CUSTOMERS)
        qty = rng.randint(1, 5)
        unit_price = Decimal(str(rng.choice([199000, 349000, 599000, 1290000, 2490000, 5990000])))
        source = rng.choices(SOURCE_SYSTEMS, weights=SOURCE_WEIGHTS)[0]

        rows.append(
            [
                100_001 + idx,                                     # order_id
                order_date,                                        # order_date
                f"CUST-{cust_no:05d}",                             # customer_id
                f"khachhang{cust_no:05d}@example.com",             # customer_email
                rng.choice(PRODUCT_SKUS),                          # product_sku
                qty,                                               # quantity
                unit_price,                                        # unit_price
                (unit_price * qty).quantize(Decimal("0.01")),      # total_amount
                "VND",                                             # currency
                rng.choices(VALID_ORDER_STATUSES, weights=[0.70, 0.15, 0.10, 0.05])[0],
                rng.choice(PAYMENT_METHODS),                       # payment_method
                source,                                            # source_system
                HEALTHY_VERSION if source == BROKEN_SOURCE else "1.0.0",
                datetime.combine(order_date, datetime.min.time())
                + timedelta(hours=rng.randint(1, 23), minutes=rng.randint(0, 59)),
            ]
        )
    return rows


def _inject_defects(rows: List[List[Any]], rng: random.Random) -> Dict[str, Any]:
    """Cấy lỗi vào dataset. Trả về metadata mô tả các lỗi đã cấy."""
    C_ORDER_ID, C_ORDER_DATE, C_CUSTOMER_ID, C_EMAIL = 0, 1, 2, 3
    C_TOTAL, C_STATUS, C_SOURCE, C_VERSION, C_INGESTED = 7, 9, 11, 12, 13

    used: set[int] = set()

    def pick(n: int) -> List[int]:
        chosen: List[int] = []
        while len(chosen) < n:
            i = rng.randrange(N_ROWS)
            if i not in used:
                used.add(i)
                chosen.append(i)
        return sorted(chosen)

    # --- Lỗi 1: NULL customer_id, dồn vào 1 batch của mobile_app_v3 v3.4.1 ----
    null_order_ids: List[int] = []
    for k, i in enumerate(pick(N_NULL_CUSTOMER)):
        rows[i][C_CUSTOMER_ID] = None   # <-- vi phạm not_null
        rows[i][C_EMAIL] = None
        rows[i][C_ORDER_DATE] = BATCH_DATE
        rows[i][C_SOURCE] = BROKEN_SOURCE
        rows[i][C_VERSION] = BROKEN_VERSION
        rows[i][C_INGESTED] = BATCH_TS + timedelta(seconds=k * 7)
        null_order_ids.append(rows[i][C_ORDER_ID])

    # --- Lỗi 2: order_status sai định dạng -----------------------------------
    bad_status_values = ["completed", " Completed ", "COMPLETE", "compleTED", "Pending", "cancelled"]
    bad_status_order_ids: List[int] = []
    for i, bad in zip(pick(N_BAD_STATUS), bad_status_values):
        rows[i][C_STATUS] = bad
        bad_status_order_ids.append(rows[i][C_ORDER_ID])

    # --- Lỗi 3: total_amount âm ---------------------------------------------
    neg_order_ids: List[int] = []
    for i in pick(N_NEGATIVE_AMOUNT):
        rows[i][C_TOTAL] = -abs(rows[i][C_TOTAL])
        rows[i][C_STATUS] = "REFUNDED"
        neg_order_ids.append(rows[i][C_ORDER_ID])

    # --- Lỗi 4: email sai format -------------------------------------------
    bad_emails = ["nguyenvana#example.com", "tranthib.example.com", "  "]
    bad_email_order_ids: List[int] = []
    for i, bad in zip(pick(N_BAD_EMAIL), bad_emails):
        rows[i][C_EMAIL] = bad
        bad_email_order_ids.append(rows[i][C_ORDER_ID])

    # --- Lỗi 5: duplicate order_id ------------------------------------------
    dup_order_ids: List[int] = []
    for i in pick(N_DUPLICATE_ID):
        rows[i][C_ORDER_ID] = 100_001
        dup_order_ids.append(100_001)

    return {
        "null_customer_id": {
            "count": N_NULL_CUSTOMER,
            "sample_order_ids": null_order_ids[:5],
            "source_system": BROKEN_SOURCE,
            "source_version": BROKEN_VERSION,
            "batch_date": BATCH_DATE.isoformat(),
        },
        "invalid_order_status": {
            "count": N_BAD_STATUS,
            "sample_order_ids": bad_status_order_ids[:5],
        },
        "negative_total_amount": {"count": N_NEGATIVE_AMOUNT, "sample_order_ids": neg_order_ids},
        "invalid_email_format": {"count": N_BAD_EMAIL, "sample_order_ids": bad_email_order_ids},
        "duplicate_order_id": {"count": N_DUPLICATE_ID, "sample_order_ids": dup_order_ids},
    }


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def bootstrap(db_path: str | None = None, force: bool = False) -> Dict[str, Any]:
    """
    Tạo (hoặc tạo lại) warehouse DuckDB.

    Args:
        db_path: đường dẫn file .duckdb (mặc định `config.DUCKDB_PATH`).
        force:   True -> xoá file cũ và sinh lại từ đầu.

    Returns:
        dict thống kê + metadata các lỗi đã cấy.
    """
    path = db_path or config.DUCKDB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)

    if force and os.path.exists(path):
        # Đóng connection dùng chung trước khi xoá file, tránh giữ handle mồ côi
        from data.connection import close_connection

        close_connection()
        os.remove(path)

    rng = random.Random(SEED)
    customers = _gen_customers(rng)
    orders = _gen_orders(rng)
    defects = _inject_defects(orders, rng)

    con = duckdb.connect(path)
    try:
        for ddl in DDL_STATEMENTS:
            con.execute(ddl)

        con.executemany("INSERT INTO dim_customers VALUES (?, ?, ?, ?, ?)", customers)
        con.executemany(
            "INSERT INTO fact_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(r) for r in orders],
        )
        for sql in MART_SQL.values():
            con.execute(sql)

        stats = {
            "db_path": os.path.abspath(path),
            "fact_orders_rows": con.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0],
            "dim_customers_rows": con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0],
            "null_customer_id_rows": con.execute(
                "SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL"
            ).fetchone()[0],
            "invalid_status_rows": con.execute(
                "SELECT COUNT(*) FROM fact_orders WHERE order_status NOT IN "
                "('COMPLETED','PENDING','CANCELLED','REFUNDED')"
            ).fetchone()[0],
            "negative_amount_rows": con.execute(
                "SELECT COUNT(*) FROM fact_orders WHERE total_amount < 0"
            ).fetchone()[0],
            "invalid_email_rows": con.execute(
                "SELECT COUNT(*) FROM fact_orders "
                "WHERE customer_email IS NOT NULL AND customer_email NOT LIKE '%@%'"
            ).fetchone()[0],
            "duplicate_order_ids": con.execute(
                "SELECT COUNT(*) FROM (SELECT order_id FROM fact_orders "
                "GROUP BY order_id HAVING COUNT(*) > 1)"
            ).fetchone()[0],
            "defects": defects,
        }
    finally:
        con.close()

    # Ghi kết quả DQ test ngay sau khi seed để incident payload có dữ kiện gốc
    from data.dq import run_all_checks

    run_all_checks(run_id="dbt-run-20260915-0230", executed_at=datetime(2026, 9, 15, 2, 30, 0))
    return stats


def ensure_database(db_path: str | None = None) -> str:
    """
    Đảm bảo database tồn tại và có bảng `fact_orders`.
    Được gọi lúc khởi động app để lần chạy đầu tiên không bị lỗi.
    """
    path = db_path or config.DUCKDB_PATH
    need_bootstrap = not os.path.exists(path)
    if not need_bootstrap:
        con = duckdb.connect(path)
        try:
            tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
            need_bootstrap = "fact_orders" not in tables
        except duckdb.Error:
            need_bootstrap = True
        finally:
            con.close()
    if need_bootstrap:
        bootstrap(path, force=False)
    return path


__all__ = [
    "SEED",
    "N_ROWS",
    "BATCH_DATE",
    "BROKEN_SOURCE",
    "BROKEN_VERSION",
    "VALID_ORDER_STATUSES",
    "DDL_STATEMENTS",
    "MART_SQL",
    "bootstrap",
    "ensure_database",
]
