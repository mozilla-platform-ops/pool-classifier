"""Unit coverage for deploy-safe Postgres migration operations."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from worker_health.pool_classifier_web.scripts import create_unresolved_task_run_index, migrate


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
        ("SET LOCAL lock_timeout = %s", ("5s",)),
        ("SET LOCAL statement_timeout = %s", ("30s",)),
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
