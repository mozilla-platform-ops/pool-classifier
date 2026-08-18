from __future__ import annotations

import sys
from types import SimpleNamespace

from worker_health.pool_classifier_web import postgres
from worker_health.pool_classifier_web import storage


def test_application_name_includes_cloud_run_identity(monkeypatch):
    monkeypatch.setenv("K_REVISION", "rev-42")
    monkeypatch.setenv("HOSTNAME", "instance-1")

    assert postgres.application_name("classifier") == (
        "pool-classifier:classifier:r=rev-42:i=instance-1"
    )


def test_application_name_uses_deterministic_local_fallbacks(monkeypatch):
    monkeypatch.delenv("K_REVISION", raising=False)
    monkeypatch.delenv("HOSTNAME", raising=False)

    assert postgres.application_name("web") == "pool-classifier:web:r=local:i=local"


def test_application_name_shortens_long_components_stably(monkeypatch):
    monkeypatch.setenv("K_REVISION", "revision_" * 20)
    monkeypatch.setenv("HOSTNAME", "instance_" * 20)

    name = postgres.application_name("maintenance")

    assert len(name.encode()) <= 63
    assert name == postgres.application_name("maintenance")
    assert "-" in name.split(":r=", 1)[1].split(":i=", 1)[0]


def test_direct_connect_sets_role_identity(monkeypatch):
    calls = []
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda *args, **kwargs: calls.append((args, kwargs))),
    )

    postgres.connect("postgresql://example", "migration", autocommit=True)

    assert calls[0][0] == ("postgresql://example",)
    assert calls[0][1]["autocommit"] is True
    assert ":migration:" in calls[0][1]["application_name"]


def test_postgres_pools_are_separated_by_role(monkeypatch):
    created = []
    closed = []

    class FakePool:
        check_connection = object()

        def __init__(self, **kwargs):
            created.append(kwargs)

        def close(self):
            closed.append(self)

    monkeypatch.setattr(storage, "psycopg_pool", SimpleNamespace(ConnectionPool=FakePool))
    storage.close_postgres_pools()

    web_pool = storage._postgres_pool("postgresql://example", "web")
    classifier_pool = storage._postgres_pool("postgresql://example", "classifier")

    assert web_pool is not classifier_pool
    assert len(created) == 2
    assert ":web:" in created[0]["kwargs"]["application_name"]
    assert ":classifier:" in created[1]["kwargs"]["application_name"]
    storage.close_postgres_pools()
    assert closed == [web_pool, classifier_pool]
    assert storage._PG_POOLS == {}
