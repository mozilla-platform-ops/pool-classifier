"""Unit coverage for deploy-safe Postgres migration operations."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from worker_health.pool_classifier_web import storage as storage_module
from worker_health.pool_classifier_web.scripts import (
    create_unresolved_task_run_index,
    create_utilization_task_run_index,
    datastore_summary,
    db_maintenance,
    migrate,
    utilization_query_plan,
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
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda _dsn, **_kwargs: connection))

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

    assert connect_calls[0][0] == ("postgresql://example",)
    assert connect_calls[0][1]["autocommit"] is True
    assert ":maintenance:" in connect_calls[0][1]["application_name"]
    assert any(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_results_unresolved" in sql
        for sql, _params in cursor.executed
    )
    assert ("SELECT pg_advisory_lock(%s)", (migrate.MIGRATION_LOCK_ID,)) in cursor.executed
    assert ("SELECT pg_advisory_unlock(%s)", (migrate.MIGRATION_LOCK_ID,)) in cursor.executed


def test_utilization_index_command_uses_autocommit_and_verifies_index(monkeypatch):
    cursor = _Cursor(
        fetchone_results=(None, (True,)),
        fetchall_results=([("pool_id",), ("worker_id",), ("run_started",), ("run_resolved",)],),
    )
    connection = _Connection(cursor)
    connect_calls = []

    def connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))

    create_utilization_task_run_index.create_utilization_task_run_index("postgresql://example")

    assert connect_calls[0][0] == ("postgresql://example",)
    assert connect_calls[0][1]["autocommit"] is True
    assert ":maintenance:" in connect_calls[0][1]["application_name"]
    assert any(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_results_utilization_resolved" in sql
        for sql, _params in cursor.executed
    )
    assert ("SELECT pg_advisory_lock(%s)", (migrate.MIGRATION_LOCK_ID,)) in cursor.executed
    assert ("SELECT pg_advisory_unlock(%s)", (migrate.MIGRATION_LOCK_ID,)) in cursor.executed


def test_db_maintenance_dispatches_only_allowlisted_operation(monkeypatch, capsys):
    calls = []
    monkeypatch.setitem(
        db_maintenance.OPERATIONS,
        "create-unresolved-task-run-index",
        lambda dsn, argv: calls.append((dsn, argv)),
    )

    db_maintenance.run_operation("create-unresolved-task-run-index", "postgresql://example")

    assert calls == [("postgresql://example", [])]
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


def test_db_maintenance_forwards_operation_arguments(monkeypatch):
    calls = []
    monkeypatch.setitem(
        db_maintenance.OPERATIONS,
        "backfill-m007-task-timestamps",
        lambda dsn, argv: calls.append((dsn, argv)),
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")

    assert db_maintenance.main(
        ["--operation", "backfill-m007-task-timestamps", "--batch-size", "200"],
    ) == 0

    assert calls == [("postgresql://example", ["--batch-size", "200"])]


def test_db_maintenance_start_lag_operation_uses_database_url(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db_maintenance.backfill_start_lag_all_pools,
        "main",
        lambda argv: calls.append(argv) or 0,
    )

    db_maintenance.run_operation(
        "backfill-observed-start-lag", "postgresql://example", ["--lookback-days", "30"],
    )

    assert calls == [[
        "--database-url", "postgresql://example",
        "--state-dir", "/tmp/pool-classifier-backfill-start-lag-state",
        "--lookback-days", "30",
    ]]


def test_db_maintenance_job_source_operation_uses_database_url(monkeypatch):
    calls = []
    monkeypatch.setattr(
        db_maintenance.backfill_job_sources_all_pools,
        "main",
        lambda argv: calls.append(argv) or 0,
    )

    db_maintenance.run_operation(
        "backfill-job-sources", "postgresql://example", ["--lookback-days", "30"],
    )

    assert calls == [[
        "--database-url", "postgresql://example", "--lookback-days", "30",
    ]]


def test_db_maintenance_main_requires_database_url(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert db_maintenance.main(["--operation", "create-unresolved-task-run-index"]) == 1
    assert "DATABASE_URL not set" in capsys.readouterr().err


class _SummaryCursor:
    def __init__(self):
        self.executed = []
        self.description = []
        self._result_sets = iter(
            [
                [("16.9",)],
                [("autovacuum", "on")],
                [("task_results", 12, 3, 4096, None, None, None, None)],
                [("idx_task_results_worker", 10, 20, 30, 1024)],
                [("client backend", "idle", "Client", "ClientRead", 2, None)],
                [(0,)],
            ],
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        if "set_config" in sql:
            self.description = []
            self._current = []
            return
        self._current = next(self._result_sets)
        if "SHOW server_version" in sql:
            names = ["server_version"]
        elif "pg_settings" in sql:
            names = ["name", "setting"]
        elif "pg_stat_user_tables" in sql:
            names = [
                "relname", "n_live_tup", "n_dead_tup", "pg_total_relation_size",
                "last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze",
            ]
        elif "pg_stat_user_indexes" in sql:
            names = ["indexrelname", "idx_scan", "idx_tup_read", "idx_tup_fetch", "pg_relation_size"]
        elif "pg_stat_activity" in sql:
            names = ["backend_type", "state", "wait_event_type", "wait_event", "count", "max"]
        else:
            names = ["count"]
        self.description = [SimpleNamespace(name=name) for name in names]

    def fetchone(self):
        return self._current[0]

    def fetchall(self):
        return self._current


def test_datastore_summary_is_read_only_and_structured(monkeypatch):
    cursor = _SummaryCursor()
    connection = _Connection(cursor)
    connect_calls = []

    def connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))

    summary = datastore_summary.collect_datastore_summary("postgresql://example")

    assert connect_calls[0][0] == ("postgresql://example",)
    assert connect_calls[0][1]["autocommit"] is True
    assert ":maintenance:" in connect_calls[0][1]["application_name"]
    assert summary["server_version"] == "16.9"
    assert summary["tables"] == [{"relname": "task_results", "n_live_tup": 12, "n_dead_tup": 3,
                                   "pg_total_relation_size": 4096, "last_vacuum": None,
                                   "last_autovacuum": None, "last_analyze": None, "last_autoanalyze": None}]
    assert summary["task_results_missing_observation_timestamps"] == 0
    sql = "\n".join(statement for statement, _params in cursor.executed)
    assert "default_transaction_read_only" in sql
    assert "statement_timeout" in sql
    assert "INSERT" not in sql
    assert "UPDATE" not in sql


def test_datastore_summary_emits_one_json_record(monkeypatch, capsys):
    monkeypatch.setattr(datastore_summary, "collect_datastore_summary", lambda _dsn: {"tables": []})

    datastore_summary.run("postgresql://example", [])

    assert json.loads(capsys.readouterr().out) == {"event": "datastore_summary", "summary": {"tables": []}}


def test_utilization_plan_is_bounded_read_only_and_parameterized(monkeypatch):
    cursor = _Cursor(fetchone_results=([[{"Plan": {"Node Type": "Index Scan"}}]],))
    connection = _Connection(cursor)
    calls = []

    def connect(*args, **kwargs):
        calls.append((args, kwargs))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))

    result = utilization_query_plan.capture_utilization_task_run_plan(
        "postgresql://example",
        pool_id="releng-hardware/gecko-t-osx-1500-m4",
        start="2026-07-31T00:00:00Z",
        end="2026-07-31T01:00:00Z",
        analyze=True,
    )

    assert calls[0][0] == ("postgresql://example",)
    assert calls[0][1]["autocommit"] is True
    assert ":maintenance:" in calls[0][1]["application_name"]
    assert result["plan"] == [{"Plan": {"Node Type": "Index Scan"}}]
    explain_sql, explain_params = cursor.executed[-1]
    assert "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" in explain_sql
    assert explain_params == ("releng-hardware/gecko-t-osx-1500-m4", "2026-07-31T01:00:00Z", "2026-07-31T00:00:00Z")
    assert "default_transaction_read_only" in "\n".join(sql for sql, _params in cursor.executed)


def test_utilization_plan_rejects_unbounded_or_invalid_windows():
    for start, end in [
        ("2026-07-31T01:00:00Z", "2026-07-31T00:00:00Z"),
        ("2026-07-01T00:00:00Z", "2026-07-09T00:00:00Z"),
        ("2026-07-31T00:00:00", "2026-07-31T01:00:00Z"),
    ]:
        try:
            utilization_query_plan.capture_utilization_task_run_plan(
                "postgresql://example", pool_id="pool", start=start, end=end,
            )
        except ValueError:
            pass
        else:
            raise AssertionError("invalid plan window was accepted")


def test_postgres_storage_initialization_does_not_apply_migrations(monkeypatch):
    pool = object()
    monkeypatch.setattr(storage_module, "_postgres_pool", lambda _dsn, _role: pool)

    storage = PostgresStorage("test-pool", "postgresql://example")
    storage.init_schema()

    assert storage._pool is pool


def test_web_entrypoint_does_not_run_migrations():
    entrypoint = (Path(__file__).parents[1] / "docker-entrypoint.sh").read_text()

    assert "scripts.migrate" not in entrypoint


def test_cloud_build_does_not_deploy_the_web_service():
    cloudbuild = (Path(__file__).parents[1] / "cloudbuild.yaml").read_text()

    assert "gcloud run deploy" not in cloudbuild
