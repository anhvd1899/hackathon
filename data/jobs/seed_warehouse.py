"""
Job: seed_warehouse — tạo lại DuckDB warehouse cho demo.

    python -m data.jobs.seed_warehouse --force
"""

from __future__ import annotations

import argparse

import config
from data.warehouse import bootstrap


def main() -> int:
    parser = argparse.ArgumentParser(description="Khởi tạo DuckDB warehouse + cấy lỗi DQ")
    parser.add_argument("--db", default=config.DUCKDB_PATH, help="Đường dẫn file duckdb")
    parser.add_argument("--force", action="store_true", help="Xoá DB cũ và tạo lại từ đầu")
    args = parser.parse_args()

    stats = bootstrap(args.db, force=args.force)

    print("=" * 72)
    print("✅ DuckDB warehouse đã sẵn sàng:", stats["db_path"])
    print("=" * 72)
    print(f"  fact_orders            : {stats['fact_orders_rows']:>5} dòng")
    print(f"  dim_customers          : {stats['dim_customers_rows']:>5} dòng")
    print("-" * 72)
    print("  LỖI ĐÃ CẤY (chờ Agent phát hiện):")
    print(f"    customer_id IS NULL          : {stats['null_customer_id_rows']:>3} dòng")
    print(f"    order_status sai định dạng   : {stats['invalid_status_rows']:>3} dòng")
    print(f"    total_amount < 0             : {stats['negative_amount_rows']:>3} dòng")
    print(f"    customer_email sai format    : {stats['invalid_email_rows']:>3} dòng")
    print(f"    order_id bị trùng            : {stats['duplicate_order_ids']:>3} khoá")
    print("=" * 72)
    print("  Bước tiếp: python -m data.jobs.run_dq_tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
