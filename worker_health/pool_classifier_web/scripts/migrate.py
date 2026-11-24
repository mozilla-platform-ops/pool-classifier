"""Apply pending SQL migrations to a Postgres database.

Usage:
    DATABASE_URL=postgresql://... python -m worker_health.pool_classifier_web.scripts.migrate

Also callable as apply_migrations(dsn) from Python code.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from worker_health.pool_classifier_web.postgres import connect as postgres_connect

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"
MIGRATION_LOCK_ID = 6_061_283
MIGRATION_LOCK_TIMEOUT = "5s"
MIGRATION_STATEMENT_TIMEOUT = "30s"
TASK_EVIDENCE_MIGRATION = "014_task_run_evidence"
TASK_EVIDENCE_CUTOVER_TIMEOUT = "5min"
TASK_EVIDENCE_ONLINE_ROW_LIMIT = 100_000


def _prepare_task_evidence_cutover(cur) -> bool:
    """Keep the slow rewrite an explicit maintenance decision."""
    if os.environ.get("POOL_CLASSIFIER_TASK_EVIDENCE_CUTOVER") == "1":
        cur.execute("SELECT set_config('statement_timeout', %s, true)", (TASK_EVIDENCE_CUTOVER_TIMEOUT,))
        return True
    cur.execute("SELECT COUNT(*) FROM (SELECT 1 FROM task_results LIMIT %s) AS sample", (TASK_EVIDENCE_ONLINE_ROW_LIMIT + 1,))
    if cur.fetchone()[0] > TASK_EVIDENCE_ONLINE_ROW_LIMIT:
        raise RuntimeError(
            "Task-run migration requires a planned cutover for this database. "
            "Drain old application processes before setting POOL_CLASSIFIER_TASK_EVIDENCE_CUTOVER=1."
        )
    return False


def apply_migrations(dsn: str) -> None:
    with postgres_connect(dsn, "migration") as conn:
        with conn.cursor() as cur:
            # Cloud Run can start more than one instance for a new revision.
            # Keep the lock and every migration in one transaction so only one
            # instance can inspect, apply, and record a migration at a time.
            # These are deliberately local to the migration transaction: a
            # blocked schema change must fail the rollout instead of holding
            # application traffic indefinitely.
            cur.execute("SELECT set_config('lock_timeout', %s, true)", (MIGRATION_LOCK_TIMEOUT,))
            cur.execute("SELECT set_config('statement_timeout', %s, true)", (MIGRATION_STATEMENT_TIMEOUT,))
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = sql_file.stem
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (version,))
                if cur.fetchone():
                    print(f"  {version}: already applied")
                    continue
            with conn.cursor() as cur:
                cutover = version == TASK_EVIDENCE_MIGRATION and _prepare_task_evidence_cutover(cur)
                cur.execute(sql_file.read_text())
                if cutover:
                    cur.execute("SELECT set_config('statement_timeout', %s, true)", (MIGRATION_STATEMENT_TIMEOUT,))
                cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
            print(f"  {version}: applied")
        conn.commit()


if __name__ == "__main__":
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    apply_migrations(dsn)
