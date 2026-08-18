"""Gunicorn lifecycle hooks for the Pool Classifier web service."""

from __future__ import annotations

import logging

from worker_health.pool_classifier_web.storage import close_postgres_pools


logger = logging.getLogger(__name__)


def worker_exit(server: object, worker: object) -> None:
    """Close psycopg pools before Gunicorn finalizes a worker interpreter."""
    del server, worker
    logger.info("gunicorn worker exiting: closing PostgreSQL connection pools")
    close_postgres_pools()
