"""
Job: run_pipeline — chạy các flow theo lịch (hoặc chạy tay)
===========================================================

    python -m data.jobs.run_pipeline --all          # chạy hết 10 job
    python -m data.jobs.run_pipeline --due          # chỉ job tới hạn
    python -m data.jobs.run_pipeline --job dbt_test_fact_orders
    python -m data.jobs.run_pipeline --list
"""

from __future__ import annotations

import argparse

from data.pipeline import JOBS, job_status_list, run_all_jobs, run_due_jobs, run_job

ICON = {"success": "✅", "failed": "❌", "error": "💥", "pending": "⏳"}


def _print(results) -> int:
    failed = [r for r in results if r["status"] != "success"]
    for r in results:
        print(
            f"  {ICON.get(r['status'], '•')} {r['job_id']:<30} {r['status']:<8} "
            f"{r['duration_ms']:>5}ms  {r['message']}"
            + (f"  -> {r['incident_id']}" if r.get("incident_id") else "")
        )
    print("-" * 78)
    print(f"  Tổng {len(results)} job · FAIL {len(failed)}")
    if failed:
        print("  Incident đã tạo, worker nền sẽ điều tra. Xem dashboard tại /")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Chạy pipeline job")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="Chạy tất cả job")
    group.add_argument("--due", action="store_true", help="Chỉ chạy job tới hạn")
    group.add_argument("--job", help="Chạy đúng một job theo job_id")
    group.add_argument("--list", action="store_true", help="Liệt kê job + trạng thái")
    args = parser.parse_args()

    if args.list:
        print(f"{'job_id':<32}{'layer':<12}{'tier':<9}{'status':<10}dq_checks")
        print("-" * 84)
        for job in job_status_list():
            print(
                f"{job['job_id']:<32}{job['layer']:<12}{job['tier']:<9}"
                f"{ICON.get(job['status'], '•')} {job['status']:<8}{len(job['dq_checks'])}"
            )
        return 0

    if args.job:
        return _print([run_job(args.job, triggered_by="cli")])
    if args.due:
        results = run_due_jobs(triggered_by="cli")
        if not results:
            print("  Không có job nào tới hạn.")
            return 0
        return _print(results)

    print(f"Chạy toàn bộ {len(JOBS)} job…")
    return _print(run_all_jobs(triggered_by="cli"))


if __name__ == "__main__":
    raise SystemExit(main())
