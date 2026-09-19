"""
Job: ingest_mobile_app — mô phỏng job ingestion upstream (thủ phạm gây sự cố DQ)
================================================================================

Job này đại diện cho `ingest_mobile_app_v3` trong lineage: nạp đơn hàng từ mobile SDK
vào `fact_orders`. Sau khi SDK nâng lên 3.4.1, field định danh khách hàng đổi tên
`user_ref` → `customer_ref` nhưng job vẫn map theo tên cũ, nên ghi NULL vào `customer_id`.

Dùng để **cấy lại lỗi** cho lần demo tiếp theo mà không phải seed lại cả warehouse
(nhanh hơn `seed_warehouse --force` và giữ nguyên audit log).

    python -m data.jobs.ingest_mobile_app                 # nạp 15 dòng lỗi (sdk 3.4.1)
    python -m data.jobs.ingest_mobile_app --rows 5
    python -m data.jobs.ingest_mobile_app --fixed         # nạp bản ĐÃ FIX (có customer_id)
"""

from __future__ import annotations

import argparse
import random
from datetime import timedelta
from decimal import Decimal
from typing import Any, Dict, List

from data.connection import CONN_LOCK, get_connection, row_count, scalar
from data.warehouse import (
    BATCH_DATE,
    BATCH_TS,
    BROKEN_SOURCE,
    BROKEN_VERSION,
    HEALTHY_VERSION,
    N_CUSTOMERS,
    PAYMENT_METHODS,
    PRODUCT_SKUS,
)


def ingest(rows: int = 15, fixed: bool = False, seed: int = 4321) -> Dict[str, Any]:
    """
    Nạp thêm `rows` đơn hàng từ mobile_app_v3 vào fact_orders.

    Args:
        rows:  số dòng nạp.
        fixed: True  -> mapping đã được sửa, `customer_id` hợp lệ (dùng để demo backfill).
               False -> mô phỏng bug SDK 3.4.1, ghi NULL vào `customer_id`.
    """
    rng = random.Random(seed)
    con = get_connection()
    max_order_id = int(scalar("SELECT COALESCE(MAX(order_id), 100000) FROM fact_orders") or 100000)

    payload: List[tuple[Any, ...]] = []
    for offset in range(rows):
        cust_no = rng.randint(1, N_CUSTOMERS)
        qty = rng.randint(1, 5)
        unit_price = Decimal(str(rng.choice([199000, 349000, 599000, 1290000, 2490000])))
        payload.append(
            (
                max_order_id + 1 + offset,
                BATCH_DATE,
                f"CUST-{cust_no:05d}" if fixed else None,      # <-- bug: mapping sai -> NULL
                f"khachhang{cust_no:05d}@example.com" if fixed else None,
                rng.choice(PRODUCT_SKUS),
                qty,
                unit_price,
                (unit_price * qty).quantize(Decimal("0.01")),
                "VND",
                "COMPLETED",
                rng.choice(PAYMENT_METHODS),
                BROKEN_SOURCE,
                HEALTHY_VERSION if fixed else BROKEN_VERSION,
                BATCH_TS + timedelta(seconds=offset * 7),
            )
        )

    before_total = row_count("fact_orders")
    before_nulls = int(scalar("SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL") or 0)
    with CONN_LOCK:
        con.executemany(
            "INSERT INTO fact_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )
    after_total = row_count("fact_orders")
    after_nulls = int(scalar("SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL") or 0)

    return {
        "rows_ingested": len(payload),
        "sdk_version": HEALTHY_VERSION if fixed else BROKEN_VERSION,
        "mapping_fixed": fixed,
        "fact_orders_rows": {"before": before_total, "after": after_total},
        "null_customer_id": {"before": before_nulls, "after": after_nulls},
        "order_id_range": [payload[0][0], payload[-1][0]] if payload else [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Mô phỏng job ingestion mobile_app_v3")
    parser.add_argument("--rows", type=int, default=15, help="Số dòng nạp (mặc định 15)")
    parser.add_argument(
        "--fixed",
        action="store_true",
        help="Nạp bản đã fix mapping (customer_id hợp lệ) thay vì bản lỗi",
    )
    args = parser.parse_args()

    stats = ingest(rows=args.rows, fixed=args.fixed)
    icon = "✅" if args.fixed else "🐛"
    print(f"{icon} ingest_mobile_app_v3 (sdk {stats['sdk_version']}) đã nạp "
          f"{stats['rows_ingested']} dòng")
    print(f"   fact_orders      : {stats['fact_orders_rows']['before']} "
          f"-> {stats['fact_orders_rows']['after']} dòng")
    print(f"   customer_id NULL : {stats['null_customer_id']['before']} "
          f"-> {stats['null_customer_id']['after']} dòng")
    if not args.fixed:
        print("   Bước tiếp: python -m data.jobs.run_dq_tests  (DQ test sẽ fail)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
