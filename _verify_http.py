"""Temp: verify server sau refactor — REST + Chainlit UI (LLM that)."""
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import requests
import socketio

BASE = os.getenv("DRA_TEST_BASE", "http://127.0.0.1:8140")
SESSION_ID = str(uuid.uuid4())
messages, actions_seen, steps = [], [], []
sio = socketio.AsyncClient()


def _rec(d):
    kind = d.get("type") or ""
    if kind == "tool":
        steps.append(d.get("name", ""))
    elif "message" in kind:
        messages.append((d.get("name", ""), d.get("output") or ""))


@sio.on("new_message")
async def _a(d):
    _rec(d)


@sio.on("update_message")
async def _b(d):
    _rec(d)


@sio.on("action")
async def _c(d):
    actions_seen.append(d.get("name"))


async def wait_for(substr, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        if any(substr in c for _, c in messages):
            print(f"    -> '{substr}' sau {time.time() - start:.0f}s ({len(steps)} tool step)")
            return True
        await asyncio.sleep(1)
    print(f"    !! TIMEOUT '{substr}'")
    return False


async def main():
    print("=== A. REST ===")
    h = requests.get(f"{BASE}/health", timeout=30).json()
    print("  health:", json.dumps(h))
    assert h["status"] == "ok" and h["fact_orders_rows"] == 1000

    m = requests.get(f"{BASE}/api/models", timeout=30).json()
    print("  models:", json.dumps(m["maker"]), "|", json.dumps(m["checker"]),
          "| cross:", m["cross_model_enabled"])
    assert m["cross_model_enabled"] is True
    assert m["maker"]["model"] != m["checker"]["model"]

    s = requests.get(f"{BASE}/api/warehouse/summary", timeout=30).json()
    print("  summary:", json.dumps(s))
    assert s["not_null_fact_orders_customer_id"] == 15

    inc = requests.get(f"{BASE}/api/incidents/sample", timeout=60).json()
    print("  incident:", inc["incident_id"], "| failures:", inc["evidence_payload"]["failures"])

    dq = requests.post(f"{BASE}/api/dq/run", timeout=120).json()
    print("  dq run:", dq["total"], "test, FAIL", dq["failed"])
    assert dq["failed"] == 5

    r = requests.get(f"{BASE}/", timeout=30, allow_redirects=False)
    print("  / ->", r.status_code, r.headers.get("location"))
    assert r.status_code in (307, 302) and r.headers.get("location") == "/chat"
    c = requests.get(f"{BASE}/chat", timeout=30)
    print("  /chat ->", c.status_code, len(c.content))
    assert c.status_code == 200

    print("\n=== B. CHAINLIT UI (Maker -> Checker, LLM that) ===")
    await sio.connect(BASE, socketio_path="/chat/ws/socket.io", transports=["websocket"],
                      auth={"sessionId": SESSION_ID, "userEnv": "{}", "clientType": "webapp",
                            "chatProfile": None, "threadId": None})
    await sio.emit("connection_successful")
    assert await wait_for("BÁO CÁO SỰ CỐ")
    await asyncio.sleep(2)
    welcome = messages[0][1]
    assert "Maker" in welcome and "Checker" in welcome and "Cross-model" in welcome
    assert set(actions_seen) >= {"approve", "reject"}
    print("  welcome + bao cao + 2 nut: OK")

    def _click(name):
        return requests.post(
            f"{BASE}/chat/project/action",
            json={"action": {"name": name, "payload": {}, "label": name, "tooltip": "",
                             "id": str(uuid.uuid4()), "forId": ""}, "sessionId": SESSION_ID},
            timeout=1200)

    task = asyncio.create_task(asyncio.to_thread(_click, "approve"))
    assert await wait_for("BIÊN BẢN NGHIỆM THU ĐỘC LẬP", timeout=900)
    assert await wait_for("AUDIT PASSED", timeout=60)
    resp = await task
    assert resp.status_code == 200, resp.text
    await asyncio.sleep(2)
    for a, c in messages:
        print(f"    [{a}] {' '.join(c.split())[:100]}")
    assert "audit" in actions_seen
    assert any(a == "Data Auditor" for a, _ in messages)
    assert any("Maker–Checker" in c for _, c in messages)

    print("\n=== C. Warehouse sau khi va ===")
    s2 = requests.get(f"{BASE}/api/warehouse/summary", timeout=30).json()
    print("  ", json.dumps(s2))
    assert s2["not_null_fact_orders_customer_id"] == 0
    assert s2["quarantined_rows"] == 15

    log = requests.get(f"{BASE}/api/audit-log?limit=200", timeout=30).json()
    counts = {}
    for row in log["rows"]:
        counts[row["tool_name"]] = counts.get(row["tool_name"], 0) + 1
    print("  audit log:", json.dumps(counts))
    assert counts.get("tool_execute_remediation") == 1
    assert counts.get("capture_baseline") == 1

    await sio.disconnect()
    print("\nHTTP + UI CHECKS PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError as exc:
        print("ASSERTION FAILED:", exc)
        sys.exit(1)
