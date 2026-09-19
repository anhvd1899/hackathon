"""Temp: verify 3 scope sau refactor (offline cho nhanh)."""
import importlib
import json
import os
import sys

os.environ["DRA_API_KEY"] = ""  # ep offline

print("=== A. IMPORT 3 SCOPE ===")
import config
import data
import ai
import web.server
from data.dq import run_all_checks
from data.jobs.ingest_mobile_app import ingest
from data.jobs.rebuild_marts import rebuild

print("  config.DUCKDB_PATH :", config.DUCKDB_PATH)
print("  config.RUNBOOK_DIR :", config.RUNBOOK_DIR.name, "|", len(list(config.RUNBOOK_DIR.glob('*.md'))), "runbook")
print("  data / ai / web.server import OK")

print("\n=== B. QUY TAC PHU THUOC MOT CHIEU ===")
import ast
from pathlib import Path


def imports_of(pkg: str):
    found = set()
    for path in Path(pkg).rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    found.add(a.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                found.add(node.module.split(".")[0])
    return found


data_imports = imports_of("data")
ai_imports = imports_of("ai")
web_imports = imports_of("web")
print("  data imports:", sorted(x for x in data_imports if x in {"data", "ai", "web", "config"}))
print("  ai   imports:", sorted(x for x in ai_imports if x in {"data", "ai", "web", "config"}))
print("  web  imports:", sorted(x for x in web_imports if x in {"data", "ai", "web", "config"}))
assert "ai" not in data_imports, "data KHONG duoc import ai"
assert "web" not in data_imports, "data KHONG duoc import web"
assert "web" not in ai_imports, "ai KHONG duoc import web"
print("  -> data (0 dep) <- ai <- web : OK")

print("\n=== C. SCOPE DATA: DQ test + jobs ===")
results = run_all_checks()
failed = [r for r in results if r["status"] == "fail"]
print(f"  DQ: {len(results)} test, FAIL {len(failed)}")
for r in failed:
    print(f"    ❌ {r['test_name']}: {r['failures']}")
assert len(failed) == 5, [r["test_name"] for r in failed]
assert {r["test_name"] for r in results} - {r["test_name"] for r in failed} == {
    "not_null_fact_orders_order_date"
}

payload = data.build_sample_incident_payload()
print("  incident:", payload["incident_id"], "| failures:", payload["evidence_payload"]["failures"])
assert payload["evidence_payload"]["failures"] == 15
assert payload["evidence_payload"]["compiled_sql"].lower().startswith("select")

marts = rebuild()
print("  rebuild marts:", marts)
assert marts["mart_daily_revenue"] and marts["mart_customer_ltv"]

print("\n=== D. SCOPE AI: guard + agent 1 + agent 2 ===")
from ai import tools
from ai.agent import DataReliabilityAgent
from ai.auditor import DataAuditorAgent
from ai.schemas import IncidentInput

blocked = tools.execute_tool("tool_query_duckdb", {"query": "DELETE FROM fact_orders WHERE 1=1"})
assert blocked["ok"] is False
blocked2 = tools.execute_tool("tool_execute_remediation", {"sql_command": "DELETE FROM fact_orders WHERE 1=1"})
assert blocked2["ok"] is False
blocked3 = tools.execute_tool("tool_execute_remediation", {"sql_command": "DELETE FROM fact_orders WHERE 1=1"},
                              allowed_tools=tools.AUDITOR_ALLOWED_TOOLS)
assert blocked3["ok"] is False
print("  guard read-only / HITL / auditor-scope: OK")
print("  catalog snapshot:", len(tools.get_catalog_snapshot().splitlines()), "bang")
print("  runbook topics  :", tools.list_runbook_topics())
assert len(tools.list_runbook_topics()) == 5

incident = IncidentInput(**payload)
agent = DataReliabilityAgent(incident=incident)
report = agent.investigate()
print(f"  Agent 1: {report.impact.affected_row_count} dòng | {report.remediation.action_type.value}")
assert report.impact.affected_row_count == 15

res = agent.approve()
print("  Agent 1 approve:", res["status"], "| violations:", res["verification"].get("violations"))
assert res["ok"] is True
assert res["baseline"]["metrics"]["total_rows_before"] == 1000

auditor = DataAuditorAgent()
audit = auditor.audit(incident=incident, remediation_report=report)
print(f"  Agent 2: {audit.verdict} | {audit.passed_count}/{len(audit.checks)} | SQL: {len(auditor.executed_queries())}")
assert audit.verdict == "AUDIT_PASSED", audit.certification_summary
cats = {c.category for c in audit.checks}
assert {"CLEANLINESS", "DATA_PRESERVATION", "ROW_COUNT_INTEGRITY"} <= cats

print("\n=== E. SCOPE WEB: routes + chainlit handlers ===")
app = web.server.app
from web.backend.api import router as api_router

api_paths = sorted(getattr(r, "path", "") for r in api_router.routes)
print("  api routes:", api_paths)
assert "/health" in api_paths and "/api/audit" in api_paths and "/api/models" in api_paths
mounts = [getattr(r, "path", None) for r in app.routes]
print("  app mounts:", [m for m in mounts if m])
assert "/chat" in mounts, "chua mount Chainlit"

import chainlit as cl
from chainlit.config import config as cl_config
print("  action_callbacks:", sorted(cl_config.code.action_callbacks))
print("  on_chat_start:", cl_config.code.on_chat_start is not None,
      "| on_message:", cl_config.code.on_message is not None)
assert set(cl_config.code.action_callbacks) >= {"approve", "reject", "audit"}
assert cl_config.code.on_chat_start is not None

from web.frontend.rendering import welcome_message, markdown_table, warehouse_snapshot
wm = welcome_message("m1", False, "m2", False, ["m1", "m2"])
assert "Maker" in wm and "Checker" in wm and "Cross-model" in wm
print("  welcome_message / markdown_table / warehouse_snapshot: OK")
print("  snapshot:", warehouse_snapshot("main.fact_orders").replace("\n", " | "))

print("\n=== F. ingest job cay lai loi ===")
before = data.row_count("fact_orders")
stats = ingest(rows=5)
print("  ", json.dumps(stats["null_customer_id"]), "| rows:", before, "->", data.row_count("fact_orders"))
assert stats["null_customer_id"]["after"] == 5

print("\nALL SCOPE CHECKS PASSED")
