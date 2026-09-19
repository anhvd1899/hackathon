# =============================================================================
# Data Reliability Squad (Maker - Checker)
# Image san sang deploy len VNG Cloud AgentBase / GreenNode (CPU 2x4GB la du:
# DuckDB in-process, khong can GPU).
# =============================================================================
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1

# Duong dan runtime (co the override khi deploy)
ENV DRA_DUCKDB_PATH=/app/var/warehouse.duckdb \
    DRA_RUNBOOK_DIR=/app/ai/runbooks \
    PORT=8000

WORKDIR /app

# Cai dependencies truoc de tan dung layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Source code theo 3 scope + config/entrypoint
COPY config.py main.py ./
COPY data/ ./data/
COPY ai/ ./ai/
COPY web/ ./web/

# Khoi tao san warehouse DuckDB (1000 dong + cac dong loi de demo)
RUN mkdir -p /app/var \
    && python -m data.jobs.seed_warehouse --force \
    && python -m data.jobs.run_dq_tests || true

# Chay bang user thuong (khong root)
RUN useradd --create-home --shell /bin/bash agent \
    && chown -R agent:agent /app
USER agent

EXPOSE 8000

# Healthcheck: FastAPI /health tra ve trang thai DuckDB
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,os,sys; \
sys.exit(0) if urllib.request.urlopen('http://127.0.0.1:'+os.getenv('PORT','8000')+'/health', timeout=4).status==200 else sys.exit(1)"

# REST API (/health, /api/*) + Chainlit UI (/chat) tren cung 1 port
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
