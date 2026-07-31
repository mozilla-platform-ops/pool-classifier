"""Create and verify the unresolved Taskcluster-run index outside migrations.

This command is intentionally separate from ``migrate``. PostgreSQL requires
``CREATE INDEX CONCURRENTLY`` to run outside a transaction, and index creation
must never be part of a Cloud Run web-service startup transaction.
"""

from __future__ import annotations

import os
import sys

from worker_health.pool_classifier_web.scripts.migrate import MIGRATION_LOCK_ID


MIGRATION_VERSION = "007_observed_task_runs"
INDEX_NAME = "idx_task_results_unresolved"
LOCK_TIMEOUT = "5s"
STATEMENT_TIMEOUT = "15min"
CREATE_INDEX_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_results_unresolved
    ON task_results (pool_id, observed_at)
    WHERE run_state NOT IN ('completed', 'failed', 'exception', 'expired')
"""


def _index_validity(cur):
    cur.execute(
        "SELECT i.indisvalid FROM pg_class c"
        " JOIN pg_index i ON i.indexrelid = c.oid"
        " WHERE c.relname = %s AND c.relnamespace = 'public'::regnamespace",
        (INDEX_NAME,),
    )
    return cur.fetchone()


def create_unresolved_task_run_index(dsn: str) -> None:
    """Create the partial index concurrently after migration 007 is applied."""
    import psycopg

    # autocommit is mandatory: CREATE INDEX CONCURRENTLY is rejected inside a
    # transaction block. Do not replace this with a transaction context.
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('lock_timeout', %s, false)", (LOCK_TIMEOUT,))
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (STATEMENT_TIMEOUT,))
            # A session lock survives autocommit statements, unlike the
            # transaction-scoped lock used by the startup migration runner.
            # It prevents a second maintenance job or pending migration from
            # racing this concurrent index operation.
            cur.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
            try:
                cur.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = %s",
                    (MIGRATION_VERSION,),
                )
                if cur.fetchone() is None:
                    raise RuntimeError(
                        f"{MIGRATION_VERSION} is not recorded; apply schema migrations before creating {INDEX_NAME}",
                    )

                cur.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_schema = 'public' AND table_name = 'task_results'"
                    " AND column_name IN ('observed_at', 'last_checked_at')",
                )
                if {row[0] for row in cur.fetchall()} != {"observed_at", "last_checked_at"}:
                    raise RuntimeError("task_results does not have the migration 007 timestamp columns")

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


if __name__ == "__main__":
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    create_unresolved_task_run_index(dsn)
