#!/bin/sh
# Container entrypoint for the pool classifier Cloud Run service.
#
# Applies pending DB migrations (idempotent — skips already-applied versions),
# then starts gunicorn. Cloud Run sets $PORT; default 8080 for local `docker run`.
set -e

if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
    echo "entrypoint: applying database migrations"
    python -m worker_health.pool_classifier_web.scripts.migrate
fi

# classify_cycle() can run for minutes, so the worker timeout must match the
# Cloud Run request timeout (1800s in run.tf). Work is I/O-bound (Taskcluster
# API + Postgres), so threads give cheap concurrency.
exec gunicorn \
    --bind "0.0.0.0:${PORT:-8080}" \
    --workers "${GUNICORN_WORKERS:-2}" \
    --threads "${GUNICORN_THREADS:-8}" \
    --timeout "${GUNICORN_TIMEOUT:-1800}" \
    --graceful-timeout 30 \
    --access-logfile - \
    --error-logfile - \
    "worker_health.pool_classifier_web.app:create_app()"
