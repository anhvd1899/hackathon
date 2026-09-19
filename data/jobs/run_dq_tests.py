"""
Job: run_dq_tests — chạy Data Quality test
==========================================

Hai đường chạy, CÙNG một output contract (bảng `dq_test_results` + incident payload):

  1. **dbt thật** (`--use-dbt`): chạy `dbt test` trong `data/dbt/` rồi parse
     `target/run_results.json`. Cần cài thêm: `pip install dbt-duckdb`.
  2. **SQL thuần** (mặc định): chạy các check trong `data/dq.py` trực tiếp trên DuckDB.
     Không cần cài dbt — đây là đường mặc định để demo luôn chạy được.

    python -m data.jobs.run_dq_tests
    python -m data.jobs.run_dq_tests --emit-incident
    python -m data.jobs.run_dq_tests --use-dbt
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import config
from data.connection import CONN_LOCK, get_connection
from data.dq import DQ_CHECKS, run_all_checks


def run_with_sql() -> List[Dict[str, Any]]:
    """Chạy DQ test bằng SQL thuần trên DuckDB (không cần dbt)."""
    return run_all_checks()


def run_with_dbt(project_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    Chạy `dbt test` thật rồi parse `run_results.json`.

    Raise RuntimeError nếu chưa cài dbt-duckdb hoặc dbt chạy lỗi — để caller có thể
    fallback sang `run_with_sql()`.
    """
    project = project_dir or config.DBT_PROJECT_DIR
    if not (project / "dbt_project.yml").is_file():
        raise RuntimeError(f"Không tìm thấy dbt project tại {project}")

    command = [
        sys.executable, "-m", "dbt.cli.main", "test",
        "--project-dir", str(project),
        "--profiles-dir", str(project),
    ]
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=600)
    except FileNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(f"Không chạy được dbt: {exc}") from exc

    artifact = project / "target" / "run_results.json"
    if not artifact.is_file():
        raise RuntimeError(
            "dbt không sinh được run_results.json. "
            f"stdout: {proc.stdout[-800:]}\nstderr: {proc.stderr[-400:]}"
        )

    payload = json.loads(artifact.read_text(encoding="utf-8"))
    stamp = datetime.now()
    run_id = f"dbt-run-{stamp:%Y%m%d-%H%M%S}"

    results: List[Dict[str, Any]] = []
    for node in payload.get("results", []):
        unique_id = str(node.get("unique_id", ""))
        if not unique_id.startswith("test."):
            continue
        test_name = unique_id.split(".")[-2] if unique_id.count(".") >= 2 else unique_id
        results.append(
            {
                "run_id": run_id,
                "test_name": test_name,
                "model": "fact_orders",
                "column_name": "",
                "status": "fail" if node.get("status") in {"fail", "error"} else "pass",
                "failures": int(node.get("failures") or 0),
                "executed_at": stamp,
                "message": str(node.get("message") or "")[:500],
            }
        )

    if not results:
        raise RuntimeError("run_results.json không có test nào.")

    con = get_connection()
    with CONN_LOCK:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Chạy DQ test trên warehouse")
    parser.add_argument(
        "--use-dbt",
        action="store_true",
        help="Chạy `dbt test` thật (cần pip install dbt-duckdb); lỗi thì fallback SQL thuần",
    )
    parser.add_argument(
        "--emit-incident",
        action="store_true",
        help="In Incident Envelope JSON cho test fail đầu tiên (để đẩy vào agent)",
    )
    args = parser.parse_args()

    engine = "SQL thuần"
    if args.use_dbt:
        try:
            results = run_with_dbt()
            engine = "dbt test"
        except RuntimeError as exc:
            print(f"⚠️  Không chạy được dbt ({exc}). Fallback sang SQL thuần.\n")
            results = run_with_sql()
    else:
        results = run_with_sql()

    failed = [r for r in results if r["status"] == "fail"]
    print("=" * 72)
    print(f"📋 DQ TEST RESULT (engine: {engine}) — run_id={results[0]['run_id']}")
    print("=" * 72)
    for r in results:
        icon = "❌" if r["status"] == "fail" else "✅"
        print(f"  {icon} {r['test_name']:<52} failures={r['failures']}")
    print("-" * 72)
    print(f"  Tổng: {len(results)} test · PASS {len(results) - len(failed)} · FAIL {len(failed)}")
    print("=" * 72)

    if args.emit_incident and failed:
        from data.incidents import build_incident_payload

        known = {c.test_name for c in DQ_CHECKS}
        target = next((r["test_name"] for r in failed if r["test_name"] in known), None)
        if target:
            print("\n📨 INCIDENT ENVELOPE (đẩy vào agent qua POST /api/incidents):")
            print(json.dumps(build_incident_payload(target), ensure_ascii=False, indent=2,
                             default=str))

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
