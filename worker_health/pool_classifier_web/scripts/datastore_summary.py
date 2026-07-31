"""Emit a compact, read-only PostgreSQL datastore summary as JSON.

The summary intentionally uses PostgreSQL statistics and catalog views rather
than application tables wherever possible.  It is safe to invoke against a
local Compose database or through the production maintenance job, making the
two environments directly comparable without exposing an HTTP admin surface.
"""

from __future__ import annotations

import argparse
import json

from worker_health.pool_classifier_web.postgres import connect as postgres_connect


LOCK_TIMEOUT = "2s"
STATEMENT_TIMEOUT = "10s"
TABLE_NAMES = (
    "task_results",
    "worker_availability_transitions",
    "collection_coverage_intervals",
)

DATABASE_SETTINGS_SQL = """
SELECT name, setting
FROM pg_settings
WHERE name = ANY(%s)
ORDER BY name
"""
TABLE_STATS_SQL = """
SELECT relname, n_live_tup, n_dead_tup,
       pg_total_relation_size(relid),
       last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
FROM pg_stat_user_tables
WHERE relname = ANY(%s)
ORDER BY relname
"""
TASK_RESULTS_INDEX_STATS_SQL = """
SELECT indexrelname, idx_scan, idx_tup_read, idx_tup_fetch,
       pg_relation_size(indexrelid)
FROM pg_stat_user_indexes
WHERE relname = 'task_results'
ORDER BY indexrelname
"""
ACTIVITY_SQL = """
SELECT COALESCE(backend_type, '') AS backend_type,
       COALESCE(state, '') AS state,
       COALESCE(wait_event_type, '') AS wait_event_type,
       COALESCE(wait_event, '') AS wait_event,
       COUNT(*) AS connections, MAX(now() - query_start) AS longest_query_age
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY backend_type, state, wait_event_type, wait_event
ORDER BY backend_type, state, wait_event_type, wait_event
"""
MISSING_TIMESTAMPS_SQL = """
SELECT COUNT(*) FROM task_results
WHERE observed_at IS NULL OR last_checked_at IS NULL
"""


def _rows(cur, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
    cur.execute(sql, params)
    columns = [column.name for column in cur.description]
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def collect_datastore_summary(dsn: str) -> dict[str, object]:
    """Collect a bounded diagnostic summary without modifying application data."""
    with postgres_connect(dsn, "maintenance", autocommit=True) as conn:
        with conn.cursor() as cur:
            # Session settings apply to each autocommit statement below.  The
            # operation has no DML/DDL and read-only mode makes that contract
            # enforceable at the database boundary.
            cur.execute("SELECT set_config('default_transaction_read_only', 'on', false)")
            cur.execute("SELECT set_config('lock_timeout', %s, false)", (LOCK_TIMEOUT,))
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (STATEMENT_TIMEOUT,))
            cur.execute("SHOW server_version")
            server_version = cur.fetchone()[0]

            settings = _rows(
                cur,
                DATABASE_SETTINGS_SQL,
                (
                    [
                        "autovacuum",
                        "autovacuum_vacuum_threshold",
                        "autovacuum_vacuum_scale_factor",
                        "autovacuum_naptime",
                        "autovacuum_max_workers",
                    ],
                ),
            )
            tables = _rows(cur, TABLE_STATS_SQL, (list(TABLE_NAMES),))
            task_results_indexes = _rows(cur, TASK_RESULTS_INDEX_STATS_SQL)
            activity = _rows(cur, ACTIVITY_SQL)
            cur.execute(MISSING_TIMESTAMPS_SQL)
            missing_timestamps = cur.fetchone()[0]

    return {
        "server_version": server_version,
        "settings": settings,
        "tables": tables,
        "task_results_indexes": task_results_indexes,
        "activity": activity,
        "task_results_missing_observation_timestamps": missing_timestamps,
    }


def run(dsn: str, argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    print(
        json.dumps(
            {"event": "datastore_summary", "summary": collect_datastore_summary(dsn)},
            default=str,
            sort_keys=True,
        ),
    )
