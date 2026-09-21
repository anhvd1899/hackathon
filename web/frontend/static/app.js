/*
 * web/frontend/static/app.js — logic dashboard
 *
 * Luồng: poll GET /api/dashboard mỗi POLL_MS -> render KPI, danh sách job (báo đỏ),
 * badge thông báo. Click job lỗi -> GET /api/incidents/{id} lấy báo cáo ĐÃ LƯU SẴN
 * (worker nền điều tra từ trước) nên hiển thị tức thì, không chờ LLM.
 */
"use strict";

const POLL_MS = 4000;

const state = {
  data: null,
  selectedIncidentId: null,
  onlyFailed: false,
  busy: false,
  lastUnread: 0,
};

/* ---------------------------------------------------------------- helpers */
const $ = (id) => document.getElementById(id);

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function timeAgo(iso) {
  if (!iso) return "—";
  const then = new Date(iso.endsWith("Z") ? iso : iso + "Z");
  const sec = Math.max(0, Math.floor((Date.now() - then.getTime()) / 1000));
  if (sec < 60) return sec + "s trước";
  if (sec < 3600) return Math.floor(sec / 60) + " phút trước";
  if (sec < 86400) return Math.floor(sec / 3600) + " giờ trước";
  return Math.floor(sec / 86400) + " ngày trước";
}

function clock(iso) {
  if (!iso) return "—";
  const d = new Date(iso.endsWith("Z") ? iso : iso + "Z");
  return d.toLocaleTimeString("vi-VN", { hour12: false });
}

async function api(path, options) {
  const res = await fetch(path, options);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) { /* ignore */ }
    throw new Error(detail);
  }
  return res.json();
}

function toast(message, isError) {
  const el = $("toolbar-msg");
  el.textContent = message;
  el.style.color = isError ? "var(--red)" : "var(--muted)";
}

/* ------------------------------------------------------------------- KPI */
function renderKpi(data) {
  const k = data.kpi;
  const cards = [
    { label: "Job lỗi", value: k.jobs_failed, cls: k.jobs_failed ? "alert" : "ok",
      hint: `${k.jobs_success}/${k.jobs_total} job OK` },
    { label: "Sự cố đang mở", value: k.incidents_open, cls: k.incidents_open ? "warn" : "ok",
      hint: `${k.incidents_resolved} đã xử lý` },
    { label: "Chờ duyệt", value: k.incidents_waiting_approval, cls: k.incidents_waiting_approval ? "warn" : "",
      hint: "đã có báo cáo điều tra" },
    { label: "Đang điều tra", value: k.incidents_investigating, cls: "",
      hint: "worker nền đang xử lý" },
    { label: "Dòng bị ảnh hưởng", value: (k.affected_rows || 0).toLocaleString("vi-VN"), cls: "",
      hint: "tổng các sự cố đang mở" },
  ];
  $("kpi").innerHTML = cards.map((c) => `
    <div class="kpi ${c.cls}">
      <div class="label">${esc(c.label)}</div>
      <div class="value">${esc(c.value)}</div>
      <div class="muted">${esc(c.hint)}</div>
    </div>`).join("");
}

/* -------------------------------------------------------------- job list */
function renderJobs(data) {
  const jobs = state.onlyFailed ? data.jobs.filter((j) => j.status === "failed") : data.jobs;
  const failed = data.jobs.filter((j) => j.status === "failed").length;
  $("job-count").textContent = `(${data.jobs.length} flow · ${failed} lỗi)`;

  $("jobs-body").innerHTML = jobs.map((job) => {
    const label = { failed: "LỖI", success: "OK", error: "CRASH", pending: "chưa chạy" }[job.status] || job.status;
    const selected = job.incident_id && job.incident_id === state.selectedIncidentId;
    return `
      <tr class="clickable ${job.status === "failed" ? "row-failed" : ""} ${selected ? "selected" : ""}"
          data-job="${esc(job.job_id)}" data-incident="${esc(job.incident_id || "")}">
        <td><span class="status"><span class="dot ${esc(job.status)}"></span>${esc(label)}</span></td>
        <td>
          <div><strong>${esc(job.name)}</strong></div>
          <div class="muted">${esc(job.job_id)} · ${esc(job.owner)}</div>
        </td>
        <td><span class="tag">${esc(job.layer)}</span></td>
        <td><span class="tag ${job.tier === "Tier-0" ? "tier0" : ""}">${esc(job.tier)}</span></td>
        <td>${job.tests_total ? `${esc(job.tests_failed)}/${esc(job.tests_total)}` : "—"}</td>
        <td class="muted">${esc(timeAgo(job.last_run_at))}</td>
        <td><button class="btn ghost small" data-run="${esc(job.job_id)}">▶</button></td>
      </tr>`;
  }).join("") || `<tr><td colspan="7" class="empty">Không có job nào khớp bộ lọc</td></tr>`;

  $("jobs-body").querySelectorAll("tr.clickable").forEach((tr) => {
    tr.addEventListener("click", (event) => {
      if (event.target.dataset.run) return;
      const incidentId = tr.dataset.incident;
      if (incidentId) openIncident(incidentId);
      else showNoIncident(tr.dataset.job);
    });
  });
  $("jobs-body").querySelectorAll("[data-run]").forEach((btn) => {
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      await runJob(btn.dataset.run);
    });
  });
}

/* -------------------------------------------------------------- dbt DAG */
function renderDbt(data) {
  const d = data.dbt || {};
  $("dbt-sub").textContent = d.available
    ? `(${d.models} model · ${d.sources} source · ${d.tests} test)`
    : "(chưa có manifest)";

  if (!d.dbt_installed) {
    $("dbt-panel").innerHTML = `<div class="banner warn">
      Chưa cài <code>dbt-duckdb</code> — hệ thống đang chạy DQ check ở chế độ fallback
      (SQL thuần trong <code>data/dq.py</code>). Cài bằng <code>pip install dbt-duckdb</code>
      rồi bấm <strong>dbt run + test + docs</strong>.
    </div>`;
    return;
  }
  if (!d.available) {
    $("dbt-panel").innerHTML = `<div class="banner info">
      dbt đã cài nhưng chưa sinh manifest. Bấm <strong>dbt run + test + docs</strong> để
      build DAG — sau đó danh sách job và DQ check sẽ tự sinh từ dbt project.
    </div>`;
    return;
  }

  $("dbt-panel").innerHTML = `
    <div class="banner ok">
      ✅ Pipeline dbt-duckdb đang hoạt động · <strong>${esc(d.models)}</strong> model,
      <strong>${esc(d.tests)}</strong> test, layer <code>${esc((d.layers || []).join(", "))}</code>.
      Danh sách job và DQ check ở trên <strong>sinh tự động từ manifest</strong> —
      thêm model/test vào <code>data/dbt/</code> là hệ thống tự nhận, không phải sửa code.
    </div>
    <dl class="kv" style="margin-top:10px">
      <dt>Project</dt><dd><code>${esc(d.project_dir)}</code></dd>
      <dt>Manifest sinh lúc</dt><dd>${esc(d.generated_at ? new Date(d.generated_at).toLocaleString("vi-VN") : "—")}</dd>
      <dt>dbt docs</dt><dd>${d.docs_ready
        ? '<a href="/lineage">✅ sẵn sàng — mở Lineage Graph</a>'
        : "⏳ chưa sinh (bấm nút dbt run + test + docs)"}</dd>
    </dl>`;
}

/* ----------------------------------------------------------- recent runs */
function renderRuns(data) {
  $("runs-body").innerHTML = (data.recent_runs || []).map((run) => {
    const label = { failed: "LỖI", success: "OK", error: "CRASH" }[run.status] || run.status;
    return `<tr>
      <td class="muted">${esc(clock(run.started_at))}</td>
      <td>${esc(run.job_id)}</td>
      <td><span class="status"><span class="dot ${esc(run.status)}"></span>${esc(label)}</span></td>
      <td class="muted">${esc(run.duration_ms ?? 0)}ms</td>
      <td class="muted">${esc((run.message || "").slice(0, 90))}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="5" class="empty">Chưa có lần chạy nào</td></tr>`;
}

/* --------------------------------------------------------- notifications */
function renderNotifications(data) {
  const unread = data.kpi.unread_notifications || 0;
  const badge = $("bell-badge");
  badge.textContent = unread > 99 ? "99+" : unread;
  badge.classList.toggle("hidden", unread === 0);

  // Có thông báo mới -> tự mở panel để giám khảo thấy ngay
  if (unread > state.lastUnread && state.lastUnread !== 0) {
    $("notif-panel").classList.remove("hidden");
  }
  state.lastUnread = unread;

  $("notif-list").innerHTML = (data.notifications || []).map((n) => `
    <div class="notif ${esc(n.level)} ${n.read_at ? "" : "unread"}"
         data-incident="${esc(n.incident_id || "")}">
      <div class="t">${esc(n.title)}</div>
      <div class="b">${esc(n.body)}</div>
      <div class="ts">${esc(timeAgo(n.created_at))}${n.job_id ? " · " + esc(n.job_id) : ""}</div>
    </div>`).join("") || `<div class="empty">Chưa có thông báo</div>`;

  $("notif-list").querySelectorAll(".notif").forEach((el) => {
    el.addEventListener("click", () => {
      if (el.dataset.incident) {
        openIncident(el.dataset.incident);
        $("notif-panel").classList.add("hidden");
      }
    });
  });
}

/* ------------------------------------------------------- incident detail */
function showNoIncident(jobId) {
  state.selectedIncidentId = null;
  $("detail-hint").textContent = jobId;
  $("detail").innerHTML = `<div class="empty">
    <span class="big">✅</span>Job <strong>${esc(jobId)}</strong> không có sự cố đang mở.
  </div>`;
}

async function openIncident(incidentId) {
  state.selectedIncidentId = incidentId;
  $("detail-hint").textContent = incidentId;
  $("detail").innerHTML = `<div class="empty"><span class="big spin">⏳</span>Đang tải…</div>`;
  try {
    renderIncident(await api(`/api/incidents/${encodeURIComponent(incidentId)}`));
  } catch (err) {
    $("detail").innerHTML = `<div class="banner bad">Không tải được sự cố: ${esc(err.message)}</div>`;
  }
  if (state.data) renderJobs(state.data);
}

function renderIncident(inc) {
  const report = inc.report;
  const audit = inc.audit;
  const parts = [];

  parts.push(`<div class="sec">
    <h3>Tổng quan</h3>
    <dl class="kv">
      <dt>Mã sự cố</dt><dd><strong>${esc(inc.incident_id)}</strong></dd>
      <dt>Job</dt><dd>${esc(inc.job ? inc.job.name : inc.job_id)} <span class="muted">(${esc(inc.job_id)})</span></dd>
      <dt>Trạng thái</dt><dd><span class="tag st-${esc(inc.status)}">${esc(inc.status)}</span></dd>
      <dt>Mức độ</dt><dd><span class="tag sev-${esc(inc.severity)}">${esc(inc.severity)}</span></dd>
      <dt>Bảng</dt><dd><code>${esc(inc.target_table)}</code></dd>
      <dt>Test fail</dt><dd><code>${esc(inc.test_name)}</code> · cột <code>${esc(inc.column_name || "—")}</code></dd>
      <dt>Số dòng vi phạm</dt><dd><strong>${esc(inc.failed_rows)}</strong></dd>
      <dt>Số lần lặp lại</dt><dd>${esc(inc.occurrences)} lần</dd>
      <dt>Phát hiện</dt><dd>${esc(timeAgo(inc.created_at))}</dd>
    </dl>
  </div>`);

  if (!report) {
    const investigating = inc.status === "INVESTIGATING";
    parts.push(`<div class="banner ${investigating ? "info" : "warn"}">
      ${investigating
        ? '<span class="spin">⏳</span> Agent 1 đang điều tra, báo cáo sẽ hiện ở đây khi xong.'
        : "🕐 Chưa điều tra. Worker nền sẽ tự xử lý, hoặc bấm nút dưới để chạy ngay."}
      ${inc.error ? `<div class="muted">Lỗi lần trước: ${esc(inc.error)}</div>` : ""}
    </div>
    <button class="btn" id="btn-investigate">🔎 Điều tra ngay</button>`);
  } else {
    const d = report.diagnosis || {};
    const im = report.impact || {};
    const rm = report.remediation || {};
    const list = (items) => (items || []).map((i) => `<li>${esc(i)}</li>`).join("") || "<li>—</li>";

    parts.push(`<div class="sec">
      <h3>🔎 Nguyên nhân gốc rễ</h3>
      <div>${esc(d.root_cause)}</div>
      <dl class="kv" style="margin-top:8px">
        <dt>Nguồn nghi vấn</dt><dd><code>${esc(d.suspected_source)}</code></dd>
        <dt>Độ tin cậy</dt><dd>${Math.round((d.confidence_score || 0) * 100)}%</dd>
      </dl>
      <ul class="bullets">${list(d.evidence_summary)}</ul>
    </div>`);

    parts.push(`<div class="sec">
      <h3>💥 Phạm vi ảnh hưởng</h3>
      <dl class="kv">
        <dt>Mức độ</dt><dd><span class="tag sev-${esc(im.severity)}">${esc(im.severity)}</span></dd>
        <dt>Dòng ảnh hưởng</dt><dd>${esc(im.affected_row_count)}</dd>
        <dt>Vi phạm SLA</dt><dd>${im.sla_breach ? "⚠️ CÓ" : "Không"}</dd>
      </dl>
      <div class="muted" style="margin-top:6px">${esc(im.business_impact || "")}</div>
      <ul class="bullets">${list((im.affected_downstream_tables || []).concat(im.affected_dashboards || []))}</ul>
    </div>`);

    parts.push(`<div class="sec">
      <h3>🛠️ Cách khắc phục đề xuất</h3>
      <dl class="kv">
        <dt>Hành động</dt><dd><code>${esc(rm.action_type)}</code> · rủi ro <code>${esc(rm.risk_level)}</code></dd>
      </dl>
      <div style="margin-top:6px">${esc(rm.summary || "")}</div>
      <pre>${esc(rm.executable_command || "-- (chưa có)")}</pre>
      <div class="muted" style="margin-top:6px">Verify sau khi vá:</div>
      <pre>${esc(rm.verification_sql || "--")}</pre>
    </div>`);

    if (report.next_steps && report.next_steps.length) {
      parts.push(`<div class="sec"><h3>📌 Việc cần làm tiếp</h3>
        <ul class="bullets">${list(report.next_steps)}</ul></div>`);
    }

    if (audit) {
      const ok = audit.verdict === "AUDIT_PASSED";
      parts.push(`<div class="sec">
        <h3>🧾 Nghiệm thu độc lập (Agent 2)</h3>
        <div class="banner ${ok ? "ok" : "bad"}">
          ${ok ? "🎖️" : "🛑"} <strong>${esc(audit.verdict)}</strong> —
          đạt ${esc(audit.passed_count ?? (audit.checks || []).filter((c) => c.passed).length)}/${(audit.checks || []).length} hạng mục
          ${audit.auditor_model ? `· model <code>${esc(audit.auditor_model)}</code>` : ""}
        </div>
        <ul class="bullets">${(audit.checks || []).map((c) =>
          `<li>${c.passed ? "✅" : "❌"} <strong>${esc(c.check_name)}</strong> — kỳ vọng
           <code>${esc(c.expected_result)}</code>, thực tế <code>${esc(c.actual_result)}</code></li>`
        ).join("") || "<li>—</li>"}</ul>
      </div>`);
    }

    parts.push(`<div class="banner info">
      Muốn duyệt/từ chối phương án này thì mở phiên Human-in-the-loop — ở đó anh chất vấn
      agent được và bấm nút duyệt.
    </div>
    <button class="btn" id="btn-open-chat">💬 Mở phiên xử lý (HITL)</button>`);
  }

  $("detail").innerHTML = parts.join("");

  const investigateBtn = $("btn-investigate");
  if (investigateBtn) {
    investigateBtn.addEventListener("click", async () => {
      investigateBtn.disabled = true;
      investigateBtn.innerHTML = '<span class="spin">⏳</span> Đang điều tra…';
      try {
        await api(`/api/incidents/${encodeURIComponent(inc.incident_id)}/investigate`, { method: "POST" });
        await openIncident(inc.incident_id);
      } catch (err) {
        toast("Điều tra lỗi: " + err.message, true);
        investigateBtn.disabled = false;
        investigateBtn.textContent = "🔎 Điều tra ngay";
      }
    });
  }

  const chatBtn = $("btn-open-chat");
  if (chatBtn) {
    chatBtn.addEventListener("click", async () => {
      try {
        // Ghi nhận incident đang chọn để phiên Chainlit kế tiếp nạp đúng ca này
        await api(`/api/incidents/${encodeURIComponent(inc.incident_id)}/select`, { method: "POST" });
        window.open("/chat", "_blank", "noopener");
      } catch (err) {
        toast("Không mở được phiên: " + err.message, true);
      }
    });
  }
}

/* ------------------------------------------------------------ hành động */
async function runJob(jobId) {
  toast(`Đang chạy ${jobId}…`);
  try {
    const res = await api(`/api/jobs/${encodeURIComponent(jobId)}/run`, { method: "POST" });
    toast(`${jobId}: ${res.status} — ${res.message}`, res.status !== "success");
    await refresh();
  } catch (err) {
    toast("Lỗi: " + err.message, true);
  }
}

async function withBusy(button, label, fn) {
  if (state.busy) return;
  state.busy = true;
  const original = button.textContent;
  button.disabled = true;
  button.innerHTML = `<span class="spin">⏳</span> ${label}`;
  try {
    await fn();
  } catch (err) {
    toast("Lỗi: " + err.message, true);
  } finally {
    state.busy = false;
    button.disabled = false;
    button.textContent = original;
    await refresh();
  }
}

/* -------------------------------------------------------------- vòng lặp */
async function refresh() {
  try {
    const data = await api("/api/dashboard");
    state.data = data;
    renderKpi(data);
    renderJobs(data);
    renderDbt(data);
    renderRuns(data);
    renderNotifications(data);

    const b = data.brains;
    $("brains").textContent = b.offline
      ? "🟡 OFFLINE (chưa cấu hình API key) · số liệu DuckDB vẫn thật"
      : `🟢 Maker ${b.maker_model} · Checker ${b.checker_model}` +
        (b.cross_model ? " · 🔀 cross-model" : " · ⚠️ trùng model") +
        ` · profile ${b.profile}`;
    $("scheduler-pill").textContent = data.scheduler.enabled
      ? `⏱️ scheduler ${data.scheduler.interval_seconds}s`
      : "⏸️ scheduler tắt";
    $("updated-pill").textContent = "cập nhật: " + clock(data.generated_at);
  } catch (err) {
    $("updated-pill").textContent = "mất kết nối";
    toast("Không lấy được dashboard: " + err.message, true);
  }
}

function init() {
  $("bell").addEventListener("click", () => {
    const panel = $("notif-panel");
    panel.classList.toggle("hidden");
    $("bell").setAttribute("aria-expanded", String(!panel.classList.contains("hidden")));
  });
  $("mark-read").addEventListener("click", async () => {
    await api("/api/notifications/read", { method: "POST" });
    await refresh();
  });
  $("only-failed").addEventListener("change", (event) => {
    state.onlyFailed = event.target.checked;
    if (state.data) renderJobs(state.data);
  });
  $("btn-run-all").addEventListener("click", (event) =>
    withBusy(event.target, "Đang chạy 10 job…", async () => {
      const res = await api("/api/jobs/run-all", { method: "POST" });
      toast(`Đã chạy ${res.total} job · ${res.failed} job lỗi`, res.failed > 0);
    })
  );
  $("btn-dbt-build").addEventListener("click", (event) =>
    withBusy(event.target, "dbt đang chạy (30-60s)…", async () => {
      const res = await api("/api/dbt/build", { method: "POST" });
      const tests = (res.test && res.test.results) || [];
      const failed = tests.filter((t) => t.status !== "pass" && t.resource_type === "test");
      toast(
        `dbt run ${res.run && res.run.ok ? "OK" : "lỗi"} · ` +
        `test: ${failed.length}/${tests.filter((t) => t.resource_type === "test").length} FAIL · ` +
        `docs ${res.docs && res.docs.ok ? "đã sinh" : "lỗi"}`,
        failed.length > 0
      );
    })
  );
  $("btn-inject").addEventListener("click", (event) =>
    withBusy(event.target, "Đang cấy lỗi…", async () => {
      const res = await api("/api/demo/inject-defect?rows=8", { method: "POST" });
      toast(`Đã cấy lỗi · job đỏ: ${(res.jobs_failed || []).join(", ") || "không có"}`, true);
    })
  );

  refresh();
  setInterval(refresh, POLL_MS);
}

document.addEventListener("DOMContentLoaded", init);
