"""Unit coverage for the independently triggered migration-007 backfill."""

from __future__ import annotations

import sys
from types import SimpleNamespace

from worker_health.pool_classifier_web.scripts import backfill_m007_task_timestamps as backfill


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


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor

    def transaction(self):
        return _Transaction()


def test_backfill_updates_a_bounded_null_driven_batch(monkeypatch, capsys):
    cursor = _Cursor(
        fetchone_results=((1,), (5,), (True,), (True,), (0,)),
        fetchall_results=(
            [("observed_at",), ("last_checked_at",)],
            [(2, "pool", "task", "worker")],
            [],
        ),
    )
    connection = _Connection(cursor)
    connect_calls = []
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda *args, **kwargs: connect_calls.append((args, kwargs)) or connection),
    )

    stats = backfill.backfill_task_timestamps(
        "postgresql://example", batch_size=10, batch_delay_seconds=0,
    )

    assert connect_calls == [(('postgresql://example',), {'autocommit': True})]
    assert stats.updated == 2
    assert stats.remaining == 0
    update_sql, update_params = next(
        (sql, params) for sql, params in cursor.executed if "WITH batch AS" in sql
    )
    assert "FOR UPDATE SKIP LOCKED" in update_sql
    assert "result.observed_at IS NULL OR result.last_checked_at IS NULL" in update_sql
    assert update_params == (None, None, None, None, 10)
    assert "(pool_id, task_id, worker_id) >" in update_sql
    assert '"event": "m007_task_timestamp_backfill_completed"' in capsys.readouterr().out


def test_backfill_count_only_does_not_update(monkeypatch):
    cursor = _Cursor(
        fetchone_results=((1,), (7,)),
        fetchall_results=([("observed_at",), ("last_checked_at",)],),
    )
    connection = _Connection(cursor)
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda *_args, **_kwargs: connection))

    stats = backfill.backfill_task_timestamps("postgresql://example", count_only=True)

    assert stats.initial_remaining == stats.remaining == 7
    assert not any("WITH batch AS" in sql for sql, _params in cursor.executed)


def test_backfill_retries_a_failed_batch(monkeypatch):
    cursor = _Cursor(
        fetchone_results=((1,), (1,), (0,)),
        fetchall_results=([("observed_at",), ("last_checked_at",)],),
    )
    connection = _Connection(cursor)
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda *_args, **_kwargs: connection))
    attempts = []

    def update_once_then_succeed(_conn, _batch_size, _cursor):
        attempts.append(None)
        if len(attempts) == 1:
            raise RuntimeError("transient lock failure")
        if len(attempts) == 2:
            return 1, ("pool", "task", "worker")
        return 0, None

    monkeypatch.setattr(backfill, "_update_batch", update_once_then_succeed)
    monkeypatch.setattr(backfill.time, "sleep", lambda _seconds: None)

    stats = backfill.backfill_task_timestamps(
        "postgresql://example", batch_size=10, batch_delay_seconds=0, retries=1,
    )

    assert len(attempts) == 2
    assert stats.retries == 1
    assert stats.updated == 1
