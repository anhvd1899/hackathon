"""
data/dbt_runner.py — Chạy dbt & đọc manifest (nguồn sự thật của pipeline)
========================================================================

Module này là lý do hệ thống **không bị fix cứng vào use case demo**:

  - Danh sách DQ check  -> đọc từ `manifest.json` (mọi test dbt, kể cả test mới thêm)
  - Câu SQL đếm vi phạm -> lấy từ **SQL đã compile của chính dbt**, nên test lạ vẫn đếm được
  - Lineage upstream/downstream -> `parent_map` / `child_map` của manifest
  - Danh sách job       -> mỗi model dbt là một job (10 model hay 500 model đều tự sinh)

Thêm một model + vài test vào `data/dbt/models/` là hệ thống tự biết, không phải sửa
một dòng Python nào.

DuckDB chỉ cho MỘT tiến trình ghi vào file, nên mọi lệnh dbt đều chạy bên trong
`exclusive_access()`: đóng connection dùng chung -> gọi dbt (subprocess) -> mở lại.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

import config
from data.connection import CONN_LOCK, close_connection

TARGET_DIR = config.DBT_PROJECT_DIR / "target"
MANIFEST_PATH = TARGET_DIR / "manifest.json"
RUN_RESULTS_PATH = TARGET_DIR / "run_results.json"
CATALOG_PATH = TARGET_DIR / "catalog.json"
DOCS_INDEX_PATH = TARGET_DIR / "index.html"

#: Cache manifest theo mtime để không parse lại file vài MB mỗi request
_manifest_cache: Dict[str, Any] = {}
_manifest_mtime: float = 0.0
_manifest_lock = threading.Lock()


# ---------------------------------------------------------------------------
# 1. Chạy lệnh dbt
# ---------------------------------------------------------------------------


def dbt_project_exists() -> bool:
    return (config.DBT_PROJECT_DIR / "dbt_project.yml").is_file()


def dbt_installed() -> bool:
    """dbt-duckdb có được cài không (nó là dependency TUỲ CHỌN)."""
    try:
        import dbt.version  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


@contextmanager
def exclusive_access() -> Iterator[None]:
    """
    Nhường hẳn file DuckDB cho tiến trình khác (dbt) trong lúc chạy.

    Giữ `CONN_LOCK` suốt thời gian này nên không thread nào mở lại connection giữa chừng;
    lần `get_connection()` kế tiếp sẽ tự kết nối lại.
    """
    with CONN_LOCK:
        close_connection()
        try:
            yield
        finally:
            close_connection()


def run_dbt(
    *args: str, timeout: int = 900, capture_target: bool = True
) -> Dict[str, Any]:
    """
    Chạy một lệnh dbt (ví dụ `run_dbt("test")`) và trả về kết quả đã chuẩn hoá.

    Không raise khi dbt trả exit code khác 0 — `dbt test` fail là **tín hiệu nghiệp vụ
    bình thường** (có sự cố DQ), không phải lỗi hệ thống.
    """
    if not dbt_project_exists():
        return {"ok": False, "error": f"Không tìm thấy dbt project tại {config.DBT_PROJECT_DIR}"}
    if not dbt_installed():
        return {
            "ok": False,
            "error": "Chưa cài dbt-duckdb. Chạy: pip install dbt-duckdb",
            "hint": "Hệ thống vẫn hoạt động ở chế độ fallback (DQ check bằng SQL thuần).",
        }

    command = [
        sys.executable, "-m", "dbt.cli.main", *args,
        "--project-dir", str(config.DBT_PROJECT_DIR),
        "--profiles-dir", str(config.DBT_PROJECT_DIR),
    ]
    env = dict(os.environ)
    # dbt đọc đường dẫn DB từ biến này (xem data/dbt/profiles.yml)
    env["DRA_DUCKDB_PATH"] = str(Path(config.DUCKDB_PATH).resolve())

    started = time.time()
    with exclusive_access():
        try:
            proc = subprocess.run(
                command, capture_output=True, text=True, timeout=timeout, env=env,
                encoding="utf-8", errors="replace",
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": f"dbt {' '.join(args)} quá {timeout}s, đã huỷ"}

    result: Dict[str, Any] = {
        "ok": proc.returncode == 0,
        "command": " ".join(args),
        "returncode": proc.returncode,
        "duration_ms": int((time.time() - started) * 1000),
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }
    if capture_target:
        result["results"] = parse_run_results()
    invalidate_manifest_cache()
    return result


def dbt_run() -> Dict[str, Any]:
    """Build toàn bộ model (không để test chặn) — mart luôn tồn tại cho downstream."""
    return run_dbt("run")


def dbt_test() -> Dict[str, Any]:
    """Chạy toàn bộ test; fail là tín hiệu có sự cố DQ."""
    return run_dbt("test")


def dbt_docs_generate() -> Dict[str, Any]:
    """Sinh `target/index.html` + `manifest.json` + `catalog.json` để nhúng vào web."""
    return run_dbt("docs", "generate")


def docs_ready() -> bool:
    """dbt docs đã sinh chưa (web cần index.html + manifest + catalog)."""
    return DOCS_INDEX_PATH.is_file() and MANIFEST_PATH.is_file() and CATALOG_PATH.is_file()


# ---------------------------------------------------------------------------
# 2. Đọc manifest
# ---------------------------------------------------------------------------


def invalidate_manifest_cache() -> None:
    global _manifest_mtime
    with _manifest_lock:
        _manifest_cache.clear()
        _manifest_mtime = 0.0


def load_manifest() -> Dict[str, Any]:
    """Nạp `manifest.json` (có cache theo mtime). Trả {} nếu chưa sinh."""
    global _manifest_mtime
    if not MANIFEST_PATH.is_file():
        return {}
    mtime = MANIFEST_PATH.stat().st_mtime
    with _manifest_lock:
        if _manifest_cache and mtime == _manifest_mtime:
            return _manifest_cache
        try:
            data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        _manifest_cache.clear()
        _manifest_cache.update(data)
        _manifest_mtime = mtime
        return _manifest_cache


def parse_run_results() -> List[Dict[str, Any]]:
    """
    Đọc `run_results.json` của lần chạy dbt gần nhất.

    Chuẩn hoá về cùng shape với `data/dq.py` để phần downstream (incident, agent)
    không cần biết test chạy bằng dbt hay bằng SQL thuần.
    """
    if not RUN_RESULTS_PATH.is_file():
        return []
    try:
        payload = json.loads(RUN_RESULTS_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []

    manifest = load_manifest()
    nodes = manifest.get("nodes", {})
    out: List[Dict[str, Any]] = []
    for item in payload.get("results", []):
        unique_id = str(item.get("unique_id", ""))
        node = nodes.get(unique_id, {})
        resource = node.get("resource_type") or (
            "test" if unique_id.startswith("test.") else "model"
        )
        out.append(
            {
                "unique_id": unique_id,
                "resource_type": resource,
                "name": node.get("name") or unique_id.split(".")[-1],
                "status": str(item.get("status", "")),
                "failures": int(item.get("failures") or 0),
                "execution_time_ms": int(float(item.get("execution_time") or 0) * 1000),
                "message": str(item.get("message") or "")[:500],
            }
        )
    return out


def is_test_failure(record: Dict[str, Any]) -> bool:
    """
    Một bản ghi test trong run_results có phải là FAIL thật hay không.

    Không dùng `status != "pass"`: `dbt docs generate` cũng ghi lại run_results.json,
    nhưng ở đó mọi node mang status `success` (nó chỉ compile, không thực thi test).
    Nếu so `!= "pass"` thì toàn bộ 26 test sẽ bị báo FAIL oan với 0 dòng vi phạm.
    Chỉ `fail`/`error` (hoặc có số dòng vi phạm) mới là FAIL.
    """
    status = str(record.get("status", "")).strip().lower()
    return status in {"fail", "error"} or int(record.get("failures") or 0) > 0


def executed_tests(results: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Lọc ra các test đã THỰC SỰ được thi hành (status pass/fail/error)."""
    executed = {"pass", "fail", "error", "warn"}
    return [
        r for r in (results or [])
        if r.get("resource_type") == "test" and str(r.get("status", "")).lower() in executed
    ]


@dataclass
class DbtTest:
    """Một test dbt, đã suy ra đủ thông tin để hệ thống tự đếm vi phạm."""

    unique_id: str
    name: str
    test_type: str                    # not_null | unique | accepted_values | singular | ...
    model: str                        # bảng/model bị kiểm
    column_name: str = ""
    severity: str = "error"
    description: str = ""
    compiled_sql: str = ""            # SELECT trả về CÁC DÒNG VI PHẠM (do dbt compile)
    depends_on: List[str] = field(default_factory=list)

    @property
    def count_sql(self) -> str:
        """
        SQL đếm số dòng vi phạm — bọc quanh SQL compile của dbt.

        Nhờ đây mà test nào cũng đếm được, kể cả singular test tự viết mà code Python
        chưa từng biết tới.
        """
        if not self.compiled_sql:
            return ""
        body = self.compiled_sql.strip().rstrip(";")
        return f"SELECT COUNT(*) AS violations FROM (\n{body}\n) AS dbt_test_violations"


def _read_compiled_sql(node: Dict[str, Any]) -> str:
    """Lấy SQL đã compile của một test node (ưu tiên file trong target/compiled)."""
    inline = node.get("compiled_code") or node.get("compiled_sql") or ""
    if inline:
        return str(inline)
    relative = node.get("compiled_path")
    if relative:
        path = config.DBT_PROJECT_DIR / relative
        if path.is_file():
            try:
                return path.read_text(encoding="utf-8")
            except OSError:
                return ""
    return ""


def dbt_tests() -> List[DbtTest]:
    """
    Toàn bộ test trong dbt project, suy ra từ manifest.

    Đây là thứ thay thế danh sách DQ check viết tay: thêm test vào schema.yml là hệ
    thống tự nhận, không cần sửa Python.
    """
    manifest = load_manifest()
    if not manifest:
        return []

    tests: List[DbtTest] = []
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "test":
            continue
        metadata = node.get("test_metadata") or {}
        test_type = str(metadata.get("name") or "singular")
        kwargs = metadata.get("kwargs") or {}

        # Model/bảng bị kiểm: ưu tiên attached_node, sau đó suy từ depends_on
        model = ""
        attached = node.get("attached_node")
        depends = list((node.get("depends_on") or {}).get("nodes") or [])
        candidates = ([attached] if attached else []) + depends
        for candidate in candidates:
            if not candidate:
                continue
            parent = manifest.get("nodes", {}).get(candidate) or manifest.get(
                "sources", {}
            ).get(candidate)
            if parent and parent.get("resource_type") in ("model", "source"):
                model = str(parent.get("name") or "")
                break

        tests.append(
            DbtTest(
                unique_id=unique_id,
                name=str(node.get("name") or unique_id.split(".")[-1]),
                test_type=test_type,
                model=model,
                column_name=str(node.get("column_name") or kwargs.get("column_name") or ""),
                severity=str((node.get("config") or {}).get("severity") or "error").lower(),
                description=str(node.get("description") or ""),
                compiled_sql=_read_compiled_sql(node),
                depends_on=depends,
            )
        )
    return tests


def dbt_models() -> List[Dict[str, Any]]:
    """Danh sách model dbt + metadata (dùng để sinh job và vẽ lineage)."""
    manifest = load_manifest()
    if not manifest:
        return []

    tests_by_model: Dict[str, List[str]] = {}
    for test in dbt_tests():
        tests_by_model.setdefault(test.model, []).append(test.name)

    out: List[Dict[str, Any]] = []
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        name = str(node.get("name"))
        tags = list((node.get("config") or {}).get("tags") or node.get("tags") or [])
        path = str(node.get("path") or "")
        layer = tags[0] if tags else (path.split("/")[0] if "/" in path else "models")
        out.append(
            {
                "unique_id": unique_id,
                "name": name,
                "schema": str(node.get("schema") or "main"),
                "relation": f"{node.get('schema') or 'main'}.{name}",
                "layer": layer,
                "tags": tags,
                "description": str(node.get("description") or ""),
                "materialized": str((node.get("config") or {}).get("materialized") or "view"),
                "depends_on": list((node.get("depends_on") or {}).get("nodes") or []),
                "tests": sorted(tests_by_model.get(name, [])),
            }
        )
    return sorted(out, key=lambda m: (m["layer"], m["name"]))


def compiled_model_sql(model_name: str) -> str:
    """
    SQL đã compile của một model (đã thay `ref()`/`source()` bằng tên bảng thật).

    Dùng để rebuild model **trong tiến trình** cho nhanh, thay vì gọi subprocess dbt mỗi
    lần scheduler chạy job. `dbt run` thật vẫn dùng được qua `POST /api/dbt/run`.
    """
    manifest = load_manifest()
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "model" or node.get("name") != model_name:
            continue
        inline = node.get("compiled_code") or node.get("compiled_sql") or ""
        if inline:
            return str(inline)
        relative = node.get("compiled_path")
        if relative:
            path = config.DBT_PROJECT_DIR / relative
            if path.is_file():
                try:
                    return path.read_text(encoding="utf-8")
                except OSError:
                    return ""
    return ""


def dbt_sources() -> List[Dict[str, Any]]:
    """Danh sách source dbt (bảng do job Python nạp vào)."""
    manifest = load_manifest()
    return [
        {
            "unique_id": unique_id,
            "name": str(node.get("name")),
            "relation": f"{node.get('schema') or 'main'}.{node.get('name')}",
            "description": str(node.get("description") or ""),
        }
        for unique_id, node in (manifest.get("sources") or {}).items()
    ]


# ---------------------------------------------------------------------------
# 3. Lineage — hoạt động cho BẤT KỲ bảng nào trong DAG
# ---------------------------------------------------------------------------


def _node_label(unique_id: str, manifest: Dict[str, Any]) -> Optional[Dict[str, str]]:
    node = (manifest.get("nodes") or {}).get(unique_id) or (
        manifest.get("sources") or {}
    ).get(unique_id)
    if node is None:
        return None
    resource = str(node.get("resource_type") or "")
    if resource == "test":
        return None
    return {
        "unique_id": unique_id,
        "name": str(node.get("name")),
        "relation": f"{node.get('schema') or 'main'}.{node.get('name')}",
        "resource_type": resource,
        "description": str(node.get("description") or "")[:400],
    }


def find_node(table: str) -> Optional[str]:
    """Tìm unique_id của một bảng theo tên (chấp nhận 'main.x', 'x', hoặc unique_id)."""
    manifest = load_manifest()
    if not manifest:
        return None
    bare = table.split(".")[-1].strip().lower()
    if table in (manifest.get("nodes") or {}) or table in (manifest.get("sources") or {}):
        return table
    for collection in ("nodes", "sources"):
        for unique_id, node in (manifest.get(collection) or {}).items():
            if node.get("resource_type") == "test":
                continue
            if str(node.get("name", "")).lower() == bare:
                return unique_id
    return None


def lineage(table: str, depth: int = 3) -> Dict[str, Any]:
    """
    Lineage upstream + downstream của một bảng, lấy từ manifest.

    Dùng cho tool của agent: bất kể giám khảo hỏi bảng nào trong DAG, agent đều trả lời
    được — không phụ thuộc runbook viết tay cho một bảng cụ thể.
    """
    manifest = load_manifest()
    if not manifest:
        return {"ok": False, "error": "Chưa có manifest. Hãy chạy `dbt docs generate` trước."}

    node_id = find_node(table)
    if node_id is None:
        available = [m["name"] for m in dbt_models()] + [s["name"] for s in dbt_sources()]
        return {
            "ok": False,
            "error": f"Không thấy '{table}' trong DAG dbt.",
            "available_tables": sorted(available),
        }

    parents: Dict[str, List[str]] = manifest.get("parent_map") or {}
    children: Dict[str, List[str]] = manifest.get("child_map") or {}

    def walk(graph: Dict[str, List[str]], start: str, limit: int) -> List[Dict[str, Any]]:
        seen: Set[str] = {start}
        frontier = [start]
        collected: List[Dict[str, Any]] = []
        for level in range(1, max(1, limit) + 1):
            next_frontier: List[str] = []
            for current in frontier:
                for neighbour in graph.get(current, []):
                    if neighbour in seen:
                        continue
                    seen.add(neighbour)
                    label = _node_label(neighbour, manifest)
                    if label is None:  # bỏ qua node test
                        continue
                    label["level"] = str(level)
                    collected.append(label)
                    next_frontier.append(neighbour)
            frontier = next_frontier
            if not frontier:
                break
        return collected

    tests_on_node = [
        {"name": t.name, "test_type": t.test_type, "column": t.column_name, "severity": t.severity}
        for t in dbt_tests()
        if t.model and find_node(t.model) == node_id
    ]

    return {
        "ok": True,
        "node": _node_label(node_id, manifest),
        "upstream": walk(parents, node_id, depth),
        "downstream": walk(children, node_id, depth),
        "tests_on_node": tests_on_node,
        "docs_url": "/lineage",
    }


def dag_summary() -> Dict[str, Any]:
    """Thống kê DAG cho dashboard."""
    models = dbt_models()
    tests = dbt_tests()
    manifest = load_manifest()
    generated_at = ""
    if MANIFEST_PATH.is_file():
        generated_at = datetime.fromtimestamp(MANIFEST_PATH.stat().st_mtime).isoformat()
    return {
        "available": bool(manifest),
        "dbt_installed": dbt_installed(),
        "docs_ready": docs_ready(),
        "generated_at": generated_at,
        "models": len(models),
        "sources": len(dbt_sources()),
        "tests": len(tests),
        "layers": sorted({m["layer"] for m in models}),
        "project_dir": str(config.DBT_PROJECT_DIR),
    }


__all__ = [
    "DbtTest",
    "dbt_project_exists",
    "dbt_installed",
    "exclusive_access",
    "run_dbt",
    "dbt_run",
    "dbt_test",
    "dbt_docs_generate",
    "docs_ready",
    "load_manifest",
    "parse_run_results",
    "is_test_failure",
    "executed_tests",
    "dbt_tests",
    "dbt_models",
    "dbt_sources",
    "lineage",
    "find_node",
    "dag_summary",
    "invalidate_manifest_cache",
    "TARGET_DIR",
    "DOCS_INDEX_PATH",
]
