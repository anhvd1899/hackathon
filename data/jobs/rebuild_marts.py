"""
Job: rebuild_marts — build lại mart hạ nguồn từ fact_orders.

Dùng sau khi dữ liệu ở bảng fact được vá, để dashboard hết sai.

    python -m data.jobs.rebuild_marts
    python -m data.jobs.rebuild_marts --only mart_daily_revenue
"""

from __future__ import annotations

import argparse
from typing import Dict, List

from data.connection import execute_script, row_count
from data.warehouse import MART_SQL


def rebuild(only: List[str] | None = None) -> Dict[str, int | None]:
    """Build lại các mart (mặc định: tất cả). Trả về số dòng sau khi build."""
    targets = {name: sql for name, sql in MART_SQL.items() if not only or name in only}
    if not targets:
        raise ValueError(f"Không có mart nào khớp {only}. Khả dụng: {sorted(MART_SQL)}")
    execute_script(targets.values())
    return {name: row_count(name) for name in targets}


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild mart hạ nguồn")
    parser.add_argument("--only", nargs="*", help="Chỉ build các mart này")
    args = parser.parse_args()

    result = rebuild(args.only)
    print("✅ Đã rebuild mart:")
    for name, rows in result.items():
        print(f"   - {name}: {rows} dòng")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
