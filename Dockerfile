# Pool classifier web service — Cloud Run image.
# Build context is the worker_health project dir (this directory).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python deps first so the layer caches across source-only changes.
COPY worker_health/pool_classifier_web/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

# Packaging metadata (setup.py reads README.md at install time).
COPY setup.py README.md ./

# Application package. POOLS_FILE in terraform points at
# /app/worker_health/pool_classifier_web/pools.yaml, so /app is the project dir.
COPY worker_health/ ./worker_health/

# Editable install puts the package on sys.path while leaving data files
# (pools.yaml, patterns.yaml, migrations/*.sql, templates/*.html) in place.
RUN pip install --no-deps -e .

COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

# Run as non-root.
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request,os; urllib.request.urlopen(f'http://localhost:{os.environ.get(\"PORT\",\"8080\")}/healthz')"

CMD ["./docker-entrypoint.sh"]
