"""
data/jobs — Data flow jobs (chạy bằng CLI)
==========================================

Mô phỏng pipeline thật, mỗi job là một bước trong flow:

    seed_warehouse      Raw dataset -> DuckDB (kèm cấy lỗi để demo)
    ingest_mobile_app   Job ingestion upstream — thủ phạm gây ra sự cố DQ
    run_dq_tests        dbt test (hoặc SQL thuần) -> dq_test_results -> incident payload
    rebuild_marts       Build lại mart hạ nguồn sau khi dữ liệu được vá

Chạy từ thư mục gốc project:

    python -m data.jobs.seed_warehouse --force
    python -m data.jobs.ingest_mobile_app --rows 15
    python -m data.jobs.run_dq_tests --emit-incident
    python -m data.jobs.rebuild_marts
"""
