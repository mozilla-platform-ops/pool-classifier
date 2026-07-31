"""Create the measured utilization task-run index outside startup migrations."""

from __future__ import annotations

from worker_health.pool_classifier_web.scripts.migrate import MIGRATION_LOCK_ID


INDEX_NAME = "idx_task_results_utilization_resolved"
LOCK_TIMEOUT = "5s"
STATEMENT_TIMEOUT = "15min"
CREATE_INDEX_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_results_utilization_resolved
    ON task_results (pool_id, run_resolved)
    INCLUDE (worker_id, run_started)
    WHERE run_started IS NOT NULL AND run_resolved IS NOT NULL
"""


def _index_validity(cur):
    cur.execute(
        "SELECT i.indisvalid FROM pg_class c"
        " JOIN pg_index i ON i.indexrelid = c.oid"
        " WHERE c.relname = %s AND c.relnamespace = 'public'::regnamespace",
        (INDEX_NAME,),
    )
    return cur.fetchone()


def create_utilization_task_run_index(dsn: str) -> None:
    """Create the partial covering index selected from production plan evidence."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('lock_timeout', %s, false)", (LOCK_TIMEOUT,))
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (STATEMENT_TIMEOUT,))
            cur.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
            try:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_schema = 'public' AND table_name = 'task_results'"
                    " AND column_name IN ('pool_id', 'worker_id', 'run_started', 'run_resolved')",
                )
                columns = {row[0] for row in cur.fetchall()}
                expected_columns = {"pool_id", "worker_id", "run_started", "run_resolved"}
                if columns != expected_columns:
                    raise RuntimeError(
                        f"task_results is missing columns required for {INDEX_NAME}: "
                        f"{sorted(expected_columns - columns)}",
                    )

                existing = _index_validity(cur)
                if existing is not None:
                    if not existing[0]:
                        raise RuntimeError(
                            f"{INDEX_NAME} exists but is invalid; inspect and remove it before retrying",
                        )
                    print(f"{INDEX_NAME}: already exists and is valid")
                    return

                print(f"creating {INDEX_NAME} concurrently")
                cur.execute(CREATE_INDEX_SQL)
                created = _index_validity(cur)
                if created is None or not created[0]:
                    raise RuntimeError(f"{INDEX_NAME} was not created as a valid index")
                print(f"{INDEX_NAME}: created and valid")
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
