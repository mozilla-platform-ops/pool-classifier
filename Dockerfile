# Pool classifier web service — Cloud Run image.
# Build context is the repository root.
FROM ghcr.io/astral-sh/uv:0.11.7 AS uv
FROM python:3.14-slim

ARG POOL_CLASSIFIER_COMMIT=unknown

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    POOL_CLASSIFIER_COMMIT=${POOL_CLASSIFIER_COMMIT}

WORKDIR /app

# Install locked production dependencies first so the layer caches across
# source-only changes.
COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# Application package. POOLS_FILE in terraform points at
# /app/worker_health/pool_classifier_web/pools.yaml, so /app is the project dir.
COPY worker_health/ ./worker_health/

# Install the application into the locked production environment.
RUN uv sync --frozen --no-dev --no-editable

COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

# Run as non-root.
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request,os; urllib.request.urlopen(f'http://localhost:{os.environ.get(\"PORT\",\"8080\")}/healthz')"

CMD ["./docker-entrypoint.sh"]
