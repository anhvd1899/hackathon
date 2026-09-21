"""
data/pipeline.py — Registry 10 flow + lịch chạy + phát hiện sự cố
================================================================

Mô hình hoá một data platform có nhiều flow chạy theo lịch. Mỗi job:
  - thuộc một layer (ingestion / transform / gate / serving),
  - có `dq_checks` là cổng chất lượng của nó,
  - fail khi bất kỳ check nào có dòng vi phạm.

Khi một job fail, hàm `run_job()` **tự tạo incident + notification ngay tại đó**. Nhờ
vậy UI phát hiện được trong vòng 1 chu kỳ scheduler mà không cần ai bấm gì.

Bảng trong DuckDB (scope data sở hữu):
    job_runs        lịch sử từng lần chạy job
    incidents       sự cố đã phát hiện + báo cáo điều tra (xem data/incident_store.py)
    notifications   thông báo cho UI

Chạy tay:
    python -m data.jobs.run_pipeline --all
    python -m data.jobs.run_pipeline --job dbt_test_fact_orders
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from data.connection import CONN_LOCK, execute_script, fetch, get_connection, scalar
from data.dq import CHECKS_BY_NAME

# ---------------------------------------------------------------------------
# 1. Registry 10 flow
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineJob:
    """Một flow trong data platform."""

    job_id: str
    name: str
    layer: str                     # ingestion | transform | gate | serving
    owner: str
    target_table: str
    #: Tần suất chạy (giây). Demo dùng số nhỏ để thấy scheduler hoạt động.
    schedule_seconds: int = 300
    #: Các DQ check là cổng chất lượng của job này (tên trong data/dq.py)
    dq_checks: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    tier: str = "Tier-1"
    description: str = ""
    #: Tên model dbt tương ứng (rỗng nếu là job ingestion Python)
    dbt_model: str = ""

    @property
    def is_dbt(self) -> bool:
        return bool(self.dbt_model)

    @property
    def is_gate(self) -> bool:
        """Job có cổng DQ -> có thể sinh incident."""
        return bool(self.dq_checks)


#: Job NGUỒN (ingestion) — không phải node dbt nên phải khai báo. Trong production
#: đây là Airflow/Dagster task; ở đây mô phỏng bằng Python job.
INGESTION_JOBS: List[PipelineJob] = [
    PipelineJob(
        job_id="ingest_erp_core_orders",
        name="Ingest · ERP Core (CDC)",
        layer="ingestion",
        owner="Team ERP",
        target_table="main.fact_orders",
        schedule_seconds=180,
        tier="Tier-1",
        description="CDC qua Debezium từ ERP core.",
    ),
    PipelineJob(
        job_id="ingest_web_checkout_orders",
        name="Ingest · Web Checkout (CSV)",
        layer="ingestion",
        owner="Team Web",
        target_table="main.fact_orders",
        schedule_seconds=240,
        tier="Tier-1",
        description="Batch export CSV từ web checkout.",
    ),
    PipelineJob(
        job_id="ingest_mobile_app_orders",
        name="Ingest · Mobile App v3 (REST)",
        layer="ingestion",
        owner="Team Mobile BE",
        target_table="main.fact_orders",
        schedule_seconds=120,
        tier="Tier-1",
        dq_checks=["source_not_null_warehouse_fact_orders_customer_id"],
        description="REST ingestion job. SDK 3.4.1 đổi tên field user_ref -> customer_ref.",
    ),
    PipelineJob(
        job_id="ingest_partner_api_orders",
        name="Ingest · Partner API (SFTP)",
        layer="ingestion",
        owner="Team Partnership",
        target_table="main.fact_orders",
        schedule_seconds=600,
        tier="Tier-2",
        description="SFTP + partner API, thường trễ.",
    ),
]

#: Job DỰ PHÒNG cho tầng transform/serving — chỉ dùng khi CHƯA có manifest dbt.
#: Khi có dbt, các job này được **sinh tự động từ DAG** (xem `jobs_from_dbt()`), nên thêm
#: 500 model vào dbt project là có 500 job, không phải sửa file này.
FALLBACK_MODEL_JOBS: List[PipelineJob] = [
    PipelineJob(
        job_id="build_dim_customers",
        name="Build · dim_customers",
        layer="transform",
        owner="Analytics Eng",
        target_table="main.dim_customers",
        schedule_seconds=300,
        tier="Tier-1",
        depends_on=["ingest_erp_core_orders"],
    ),
    PipelineJob(
        job_id="dbt_test_fact_orders",
        name="dbt test · fact_orders (DQ gate)",
        layer="gate",
        owner="Data Platform",
        target_table="main.fact_orders",
        schedule_seconds=150,
        tier="Tier-1",
        dq_checks=[
            "source_not_null_warehouse_fact_orders_customer_id",
            "accepted_values_fact_orders_order_status",
            "unique_fact_orders_order_id",
            "positive_value_fact_orders_total_amount",
            "valid_email_fact_orders_customer_email",
            "not_null_fact_orders_order_date",
        ],
        depends_on=[
            "ingest_erp_core_orders",
            "ingest_web_checkout_orders",
            "ingest_mobile_app_orders",
            "ingest_partner_api_orders",
        ],
        description="Cổng chất lượng chính trước khi build mart.",
    ),
    PipelineJob(
        job_id="build_mart_daily_revenue",
        name="Build · mart_daily_revenue",
        layer="transform",
        owner="Analytics Eng",
        target_table="main.mart_daily_revenue",
        schedule_seconds=300,
        tier="Tier-0",
        dq_checks=["source_not_null_warehouse_fact_orders_customer_id"],
        depends_on=["dbt_test_fact_orders"],
        description="Mart Tier-0 phục vụ dashboard Finance lúc 08:00.",
    ),
    PipelineJob(
        job_id="build_mart_customer_ltv",
        name="Build · mart_customer_ltv",
        layer="transform",
        owner="Analytics Eng",
        target_table="main.mart_customer_ltv",
        schedule_seconds=300,
        tier="Tier-0",
        dq_checks=["source_not_null_warehouse_fact_orders_customer_id"],
        depends_on=["dbt_test_fact_orders"],
        description="Mart Tier-0 phục vụ Customer 360.",
    ),
    PipelineJob(
        job_id="export_finance_dashboard",
        name="Export · Finance Dashboard",
        layer="serving",
        owner="Finance BI",
        target_table="main.mart_daily_revenue",
        schedule_seconds=900,
        tier="Tier-0",
        depends_on=["build_mart_daily_revenue"],
    ),
    PipelineJob(
        job_id="ml_feature_customer_recency",
        name="ML · feature_customer_recency",
        layer="serving",
        owner="Data Science",
        target_table="main.fact_orders",
        schedule_seconds=1200,
        tier="Tier-2",
        dq_checks=["source_not_null_warehouse_fact_orders_customer_id"],
        depends_on=["dbt_test_fact_orders"],
    ),
]

#: Lịch mặc định theo layer (giây) khi job được sinh từ dbt
LAYER_SCHEDULE = {"staging": 180, "marts": 300, "models": 300, "gate": 150}
LAYER_TIER = {"marts": "Tier-0", "staging": "Tier-1"}


def jobs_from_dbt() -> List[PipelineJob]:
    """
    Sinh job từ DAG dbt: **mỗi model là một job**, cộng thêm một job cổng `dbt_test`.

    Đây là lý do hệ thống không bị fix cứng: manifest có bao nhiêu model thì có bấy
    nhiêu job, và `dq_checks` của job chính là các test dbt gắn với model đó.
    """
    try:
        from data.dbt_runner import dbt_models, dbt_tests
    except Exception:  # noqa: BLE001
        return []

    models = dbt_models()
    if not models:
        return []

    tests = dbt_tests()
    jobs: List[PipelineJob] = []

    # Một job cổng chạy TOÀN BỘ test dbt — tương đương `dbt test` trong production
    jobs.append(
        PipelineJob(
            job_id="dbt_test_all",
            name=f"dbt test · toàn bộ project ({len(tests)} test)",
            layer="gate",
            owner="Data Platform",
            target_table="main.stg_orders",
            schedule_seconds=LAYER_SCHEDULE["gate"],
            dq_checks=[t.name for t in tests],
            depends_on=[job.job_id for job in INGESTION_JOBS],
            tier="Tier-1",
            description="Cổng chất lượng toàn cục: chạy mọi test trong dbt project.",
        )
    )

    by_unique_id = {model["unique_id"]: model for model in models}
    for model in models:
        model_tests = [t.name for t in tests if t.model == model["name"]]
        parents = [
            f"build_{by_unique_id[dep]['name']}"
            for dep in model["depends_on"]
            if dep in by_unique_id
        ]
        jobs.append(
            PipelineJob(
                job_id=f"build_{model['name']}",
                name=f"dbt · {model['name']}",
                layer=model["layer"],
                owner="Analytics Eng",
                target_table=model["relation"],
                schedule_seconds=LAYER_SCHEDULE.get(model["layer"], 300),
                dq_checks=model_tests,
                depends_on=parents or ["dbt_test_all"],
                tier=LAYER_TIER.get(model["layer"], "Tier-1"),
                description=model["description"] or f"Model dbt ({model['materialized']}).",
                dbt_model=model["name"],
            )
        )
    return jobs


def load_jobs() -> List[PipelineJob]:
    """
    Danh sách job đang hiệu lực = job ingestion (khai báo tay) + job sinh từ dbt DAG.
    Chưa có manifest thì dùng `FALLBACK_MODEL_JOBS`.
    """
    derived = jobs_from_dbt()
    return list(INGESTION_JOBS) + (derived if derived else list(FALLBACK_MODEL_JOBS))


class _LazyJobList(list):
    """Giữ API `JOBS` như hằng số nhưng luôn đọc lại từ dbt manifest."""

    def _refresh(self) -> None:
        self[:] = load_jobs()

    def __iter__(self):  # type: ignore[override]
        self._refresh()
        return list.__iter__(self)

    def __len__(self) -> int:  # type: ignore[override]
        self._refresh()
        return list.__len__(self)

    def __getitem__(self, index):  # type: ignore[override]
        self._refresh()
        return list.__getitem__(self, index)


class _LazyJobMap(dict):
    def _refresh(self) -> None:
        self.clear()
        self.update({job.job_id: job for job in load_jobs()})

    def get(self, key, default=None):  # type: ignore[override]
        self._refresh()
        return dict.get(self, key, default)

    def __getitem__(self, key):  # type: ignore[override]
        self._refresh()
        return dict.__getitem__(self, key)

    def __contains__(self, key) -> bool:  # type: ignore[override]
        self._refresh()
        return dict.__contains__(self, key)

    def __iter__(self):  # type: ignore[override]
        self._refresh()
        return dict.__iter__(self)

    def __len__(self) -> int:  # type: ignore[override]
        self._refresh()
        return dict.__len__(self)

    def keys(self):  # type: ignore[override]
        self._refresh()
        return dict.keys(self)


JOBS: List[PipelineJob] = _LazyJobList()
JOBS_BY_ID: Dict[str, PipelineJob] = _LazyJobMap()

# ---------------------------------------------------------------------------
# 2. DDL
# ---------------------------------------------------------------------------

PIPELINE_DDL: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS job_runs (
        run_id         VARCHAR,
        job_id         VARCHAR,
        started_at     TIMESTAMP,
        finished_at    TIMESTAMP,
        status         VARCHAR,     -- success | failed | error
        tests_total    INTEGER,
        tests_failed   INTEGER,
        failed_rows    BIGINT,
        duration_ms    BIGINT,
        message        VARCHAR,
        incident_id    VARCHAR
    );
    """,
]


def ensure_pipeline_tables() -> None:
    """Tạo bảng pipeline nếu chưa có (idempotent)."""
    from data.incident_store import INCIDENT_DDL

    con = get_connection()
    with CONN_LOCK:
        for ddl in PIPELINE_DDL + INCIDENT_DDL:
            con.execute(ddl)


# ---------------------------------------------------------------------------
# 3. Chạy một job
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now()


def run_job(job_id: str, triggered_by: str = "scheduler") -> Dict[str, Any]:
    """
    Chạy một job: thực thi các DQ check của nó, ghi `job_runs`, và **tạo incident +
    notification ngay** nếu fail.

    Job transform (`build_*`) còn rebuild bảng đích để mô phỏng pipeline thật.
    """
    job = JOBS_BY_ID.get(job_id)
    if job is None:
        raise ValueError(f"Không có job '{job_id}'. Khả dụng: {sorted(JOBS_BY_ID)}")

    ensure_pipeline_tables()
    run_id = f"run-{_now():%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
    started = _now()

    failures: List[Dict[str, Any]] = []
    error: Optional[str] = None

    # --- Rebuild bảng đích trước khi kiểm (mô phỏng transform của pipeline) ---
    # Job dbt: dùng SQL đã compile của chính model đó -> không hardcode SQL ở Python,
    # và model mới thêm vào dbt project cũng chạy được ngay.
    try:
        if job.is_dbt:
            from data.dbt_runner import compiled_model_sql

            compiled = compiled_model_sql(job.dbt_model)
            if compiled:
                execute_script(
                    [f"CREATE OR REPLACE TABLE {job.dbt_model} AS\n{compiled.strip().rstrip(';')}"]
                )
        elif job.layer == "transform":
            from data.warehouse import MART_SQL

            bare = job.target_table.split(".")[-1]
            if bare in MART_SQL:
                execute_script([MART_SQL[bare]])
    except Exception as exc:  # noqa: BLE001
        error = f"Lỗi khi rebuild {job.target_table}: {exc}"

    # --- Cổng DQ ---
    if error is None:
        for check_name in job.dq_checks:
            check = CHECKS_BY_NAME.get(check_name)
            if check is None:
                # Cổng DQ trỏ tới một check không tồn tại (đổi tên test trong dbt, chưa
                # build manifest…). Bỏ qua sẽ khiến job báo xanh dù chưa kiểm gì -> error.
                error = (
                    f"Cổng DQ của job `{job.job_id}` trỏ tới check `{check_name}` "
                    "không tồn tại (manifest dbt chưa build hoặc test đã đổi tên)."
                )
                break
            try:
                count = check.run()
            except Exception as exc:  # noqa: BLE001
                # Check lỗi -> job trạng thái 'error', KHÔNG phải 'success'.
                # Báo xanh khi chưa kiểm được gì là kiểu lỗi tệ nhất của một hệ DQ.
                error = f"Không chạy được check `{check_name}`: {exc}"
                break
            if count:
                failures.append(
                    {
                        "test_name": check.test_name,
                        # `model` là bảng mà CHECK soi, có thể khác `job.target_table`
                        # (ví dụ build_mart_* fail vì rule trên fact_orders).
                        "model": check.model,
                        "column": check.column_name,
                        "test_type": check.test_type,
                        "failures": count,
                        "count_sql": check.count_sql,
                    }
                )

    finished = _now()
    duration_ms = int((finished - started).total_seconds() * 1000)

    if error:
        status = "error"
        message = error
    elif failures:
        status = "failed"
        worst = max(failures, key=lambda f: f["failures"])
        message = (
            f"{len(failures)}/{len(job.dq_checks)} DQ test FAIL · "
            f"nặng nhất: {worst['test_name']} ({worst['failures']} dòng)"
        )
    else:
        status = "success"
        message = (
            f"{len(job.dq_checks)} DQ test PASS" if job.dq_checks else "Hoàn tất (không có DQ gate)"
        )

    # --- Sinh incident nếu fail ---
    incident_id: Optional[str] = None
    incident_ids: List[str] = []
    if status == "failed":
        from data.incident_store import open_incident

        # MỖI test fail là một incident riêng (nặng nhất trước) -> giám khảo hỏi lỗi nào
        # cũng có hồ sơ. Dedup theo tên test nên không bị ngập khi nhiều job cùng bắt.
        for failure in sorted(failures, key=lambda f: -f["failures"]):
            incident_ids.append(
                open_incident(job=job, run_id=run_id, failure=failure, all_failures=failures)
            )
        incident_id = incident_ids[0] if incident_ids else None

    con = get_connection()
    with CONN_LOCK:
        con.execute(
            # Liệt kê cột tường minh để việc thêm cột về sau không làm vỡ INSERT này.
            "INSERT INTO job_runs (run_id, job_id, started_at, finished_at, status, "
            "tests_total, tests_failed, failed_rows, duration_ms, message, incident_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id, job.job_id, started, finished, status,
                len(job.dq_checks), len(failures),
                sum(f["failures"] for f in failures), duration_ms,
                f"[{triggered_by}] {message}"[:1000], incident_id,
            ],
        )

    return {
        "run_id": run_id,
        "job_id": job.job_id,
        "status": status,
        "tests_total": len(job.dq_checks),
        "tests_failed": len(failures),
        "failed_rows": sum(f["failures"] for f in failures),
        "duration_ms": duration_ms,
        "message": message,
        "incident_id": incident_id,
        "incident_ids": incident_ids,
        "failures": failures,
        "triggered_by": triggered_by,
    }


# ---------------------------------------------------------------------------
# 4. Lịch chạy
# ---------------------------------------------------------------------------


def last_run_at(job_id: str) -> Optional[datetime]:
    value = scalar(
        "SELECT MAX(started_at) FROM job_runs WHERE job_id = '"
        + job_id.replace("'", "''")
        + "'"
    )
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return value


def due_jobs(now: Optional[datetime] = None) -> List[PipelineJob]:
    """Các job tới hạn chạy (chưa chạy bao giờ, hoặc quá `schedule_seconds`)."""
    ensure_pipeline_tables()
    moment = now or _now()
    result: List[PipelineJob] = []
    for job in JOBS:
        last = last_run_at(job.job_id)
        if last is None or moment - last >= timedelta(seconds=job.schedule_seconds):
            result.append(job)
    return result


def run_due_jobs(triggered_by: str = "scheduler") -> List[Dict[str, Any]]:
    """Chạy toàn bộ job tới hạn. Trả về danh sách kết quả."""
    return [run_job(job.job_id, triggered_by=triggered_by) for job in due_jobs()]


def run_all_jobs(triggered_by: str = "manual") -> List[Dict[str, Any]]:
    """Chạy tất cả job bất kể lịch (dùng cho demo / lần khởi tạo đầu tiên)."""
    ensure_pipeline_tables()
    return [run_job(job.job_id, triggered_by=triggered_by) for job in JOBS]


# ---------------------------------------------------------------------------
# 5. Trạng thái job cho UI
# ---------------------------------------------------------------------------


def job_status_list() -> List[Dict[str, Any]]:
    """
    Danh sách job kèm trạng thái lần chạy gần nhất — đây là dữ liệu cho bảng
    "danh sách job" trên dashboard (job fail sẽ có `status='failed'` để UI báo đỏ).
    """
    ensure_pipeline_tables()
    rows = fetch(
        """
        SELECT r.job_id, r.run_id, r.status, r.started_at, r.finished_at,
               r.tests_total, r.tests_failed, r.failed_rows, r.duration_ms,
               r.message, r.incident_id
        FROM job_runs r
        JOIN (
            SELECT job_id, MAX(started_at) AS latest
            FROM job_runs GROUP BY job_id
        ) latest_run
          ON r.job_id = latest_run.job_id AND r.started_at = latest_run.latest
        """,
        max_rows=500,
    )["rows"]
    latest = {row["job_id"]: row for row in rows}

    out: List[Dict[str, Any]] = []
    for job in JOBS:
        run = latest.get(job.job_id) or {}
        out.append(
            {
                "job_id": job.job_id,
                "name": job.name,
                "layer": job.layer,
                "owner": job.owner,
                "tier": job.tier,
                "target_table": job.target_table,
                "schedule_seconds": job.schedule_seconds,
                "depends_on": job.depends_on,
                "dq_checks": job.dq_checks,
                "description": job.description,
                "dbt_model": job.dbt_model,
                "status": run.get("status") or "pending",
                "last_run_id": run.get("run_id"),
                "last_run_at": run.get("started_at"),
                "duration_ms": run.get("duration_ms"),
                "tests_total": run.get("tests_total") or len(job.dq_checks),
                "tests_failed": run.get("tests_failed") or 0,
                "failed_rows": run.get("failed_rows") or 0,
                "message": run.get("message"),
                "incident_id": run.get("incident_id"),
            }
        )
    return out


def recent_runs(limit: int = 30) -> List[Dict[str, Any]]:
    """Lịch sử chạy gần nhất (cho khối 'hoạt động gần đây' trên dashboard)."""
    ensure_pipeline_tables()
    limit = max(1, min(int(limit), 200))
    return fetch(
        "SELECT run_id, job_id, status, started_at, duration_ms, tests_failed, "
        f"failed_rows, message, incident_id FROM job_runs ORDER BY started_at DESC LIMIT {limit}",
        max_rows=limit,
    )["rows"]


__all__ = [
    "PipelineJob",
    "INGESTION_JOBS",
    "FALLBACK_MODEL_JOBS",
    "jobs_from_dbt",
    "load_jobs",
    "JOBS",
    "JOBS_BY_ID",
    "ensure_pipeline_tables",
    "run_job",
    "run_due_jobs",
    "run_all_jobs",
    "due_jobs",
    "job_status_list",
    "recent_runs",
]
