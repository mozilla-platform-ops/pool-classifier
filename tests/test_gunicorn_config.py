from __future__ import annotations

import logging

from worker_health.pool_classifier_web import gunicorn_config


def test_worker_exit_closes_postgres_pools(monkeypatch, caplog):
    closed = []
    monkeypatch.setattr(gunicorn_config, "close_postgres_pools", lambda: closed.append(True))

    with caplog.at_level(logging.INFO, logger=gunicorn_config.__name__):
        gunicorn_config.worker_exit(object(), object())

    assert closed == [True]
    assert "gunicorn worker exiting: closing PostgreSQL connection pools" in caplog.text
