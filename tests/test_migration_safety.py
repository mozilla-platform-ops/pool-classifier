"""Unit coverage for deploy-safe Postgres migration operations."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from worker_health.pool_classifier_web import storage as storage_module
from worker_health.pool_classifier_web.scripts import (
    create_unresolved_task_run_index,
    db_maintenance,
    migrate,
)
from worker_health.pool_classifier_web.storage import PostgresStorage


class _Cursor:
    def __init__(self, fetchone_results=(), fetchall_results=()):
        self.executed = []
        self._fetchone_results = iter(fetchone_results)
        self._fetchall_results = iter(fetchall_results)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return next(self._fetchone_results)

    def fetchall(self):
        return next(self._fetchall_results)


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True


def test_apply_migrations_sets_transaction_local_timeouts(monkeypatch, tmp_path: Path):
    (tmp_path / "001_bounded.sql").write_text("SELECT 1;")
    cursor = _Cursor(fetchone_results=(None,))
    connection = _Connection(cursor)
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", tmp_path)
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda _dsn: connection))

    migrate.apply_migrations("postgresql://example")

    assert cursor.executed[:3] == [
        ("SELECT set_config('lock_timeout', %s, true)", ("5s",)),
        ("SELECT set_config('statement_timeout', %s, true)", ("30s",)),
        ("SELECT pg_advisory_xact_lock(%s)", (migrate.MIGRATION_LOCK_ID,)),
    ]
    assert connection.committed is True


def test_concurrent_index_command_uses_autocommit_and_verifies_index(monkeypatch):
    cursor = _Cursor(
        fetchone_results=((1,), None, (True,)),
        fetchall_results=([("observed_at",), ("last_checked_at",)],),
    )
    connection = _Connection(cursor)
    connect_calls = []

    def connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))

    create_unresolved_task_run_index.create_unresolved_task_run_index("postgresql://example")

    assert connect_calls == [(("postgresql://example",), {"autocommit": True})]
    assert any(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_results_unresolved" in sql
        for sql, _params in cursor.executed
    )
    assert ("SELECT pg_advisory_lock(%s)", (migrate.MIGRATION_LOCK_ID,)) in cursor.executed
    assert ("SELECT pg_advisory_unlock(%s)", (migrate.MIGRATION_LOCK_ID,)) in cursor.executed


def test_db_maintenance_dispatches_only_allowlisted_operation(monkeypatch, capsys):
    calls = []
    monkeypatch.setitem(
        db_maintenance.OPERATIONS,
        "create-unresolved-task-run-index",
        lambda dsn: calls.append(dsn),
    )

    db_maintenance.run_operation("create-unresolved-task-run-index", "postgresql://example")

    assert calls == ["postgresql://example"]
    output = capsys.readouterr().out
    assert '"event": "db_maintenance_started"' in output
    assert '"event": "db_maintenance_completed"' in output


def test_db_maintenance_rejects_unknown_operation():
    try:
        db_maintenance.run_operation("drop-everything", "postgresql://example")
    except ValueError as exc:
        assert "unknown maintenance operation" in str(exc)
    else:
        raise AssertionError("unknown operation was accepted")


def test_db_maintenance_main_requires_database_url(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert db_maintenance.main(["--operation", "create-unresolved-task-run-index"]) == 1
    assert "DATABASE_URL not set" in capsys.readouterr().err


def test_postgres_storage_initialization_does_not_apply_migrations(monkeypatch):
    pool = object()
    monkeypatch.setattr(storage_module, "_postgres_pool", lambda _dsn: pool)

    storage = PostgresStorage("test-pool", "postgresql://example")
    storage.init_schema()

    assert storage._pool is pool


def test_web_entrypoint_does_not_run_migrations():
    entrypoint = (Path(__file__).parents[1] / "docker-entrypoint.sh").read_text()

    assert "scripts.migrate" not in entrypoint


def test_cloud_build_does_not_deploy_the_web_service():
    cloudbuild = (Path(__file__).parents[1] / "cloudbuild.yaml").read_text()

    assert "gcloud run deploy" not in cloudbuild
