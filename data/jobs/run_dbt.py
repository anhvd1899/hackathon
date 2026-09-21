"""
Job: run_dbt — chạy dbt-duckdb (build model, cổng test, sinh lineage docs)
=========================================================================

    python -m data.jobs.run_dbt --all      # run + test + docs (khuyến nghị)
    python -m data.jobs.run_dbt --run
    python -m data.jobs.run_dbt --test
    python -m data.jobs.run_dbt --docs
    python -m data.jobs.run_dbt --status

Lệnh này tự đóng connection DuckDB dùng chung trước khi gọi dbt (DuckDB chỉ cho một
tiến trình ghi), nên an toàn hơn gọi `dbt` trực tiếp từ terminal.

LƯU Ý: nếu web server đang chạy thì nó đang giữ file DuckDB — hãy tắt server, hoặc dùng
`POST /api/dbt/build` để server tự nhường file.
"""

from __future__ import annotations

import argparse
import json

from data.dbt_runner import (
    dag_summary,
    dbt_docs_generate,
    dbt_run,
    dbt_test,
    executed_tests,
    is_test_failure,
)


def _report(label: str, result: dict, show_tests: bool = True) -> None:
    icon = "✅" if result.get("ok") else "❌"
    print(f"{icon} dbt {label}: exit={result.get('returncode')} · {result.get('duration_ms')}ms")
    if result.get("error"):
        print("   ", result["error"])
    # `docs generate` cũng ghi run_results.json (chỉ compile, không thi hành test) nên
    # nếu in summary ở đó sẽ ra "FAIL 26 test / 0 dòng vi phạm" -> gây hiểu sai.
    tests = executed_tests(result.get("results")) if show_tests else []
    if tests:
        failed = [t for t in tests if is_test_failure(t)]
        print(f"    test: {len(tests)} tổng · PASS {len(tests) - len(failed)} · FAIL {len(failed)}")
        for t in failed:
            print(f"      ❌ {t['name']:<58} {t['failures']} dòng vi phạm")
    models = [r for r in (result.get("results") or []) if r["resource_type"] == "model"]
    if models:
        print(f"    model: {len(models)} node, "
              f"{sum(1 for m in models if m['status'] == 'success')} thành công")
    if not result.get("ok") and result.get("stdout_tail"):
        print("    …", result["stdout_tail"][-600:].replace("\n", "\n    "))


def main() -> int:
    parser = argparse.ArgumentParser(description="Chạy dbt-duckdb")
    parser.add_argument("--all", action="store_true", help="run + test + docs")
    parser.add_argument("--run", action="store_true", help="Build model")
    parser.add_argument("--test", action="store_true", help="Chạy cổng DQ test")
    parser.add_argument("--docs", action="store_true", help="Sinh lineage docs")
    parser.add_argument("--status", action="store_true", help="In tình trạng dbt project")
    args = parser.parse_args()

    if args.status:
        print(json.dumps(dag_summary(), ensure_ascii=False, indent=2))
        return 0

    do_all = args.all or not (args.run or args.test or args.docs)
    failed_tests = 0

    if do_all or args.run:
        _report("run", dbt_run(), show_tests=False)
    if do_all or args.test:
        result = dbt_test()
        _report("test", result)
        failed_tests = sum(1 for r in executed_tests(result.get("results")) if is_test_failure(r))
    if do_all or args.docs:
        docs = dbt_docs_generate()
        _report("docs generate", docs, show_tests=False)
        if docs.get("ok"):
            print("    Lineage Graph: http://localhost:8000/lineage")

    if failed_tests:
        print(f"\n⚠️  {failed_tests} test FAIL -> chạy `python -m data.jobs.run_pipeline --all` "
              "để sinh incident, hoặc mở dashboard để worker nền tự xử lý.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
