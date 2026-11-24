"""Apply pending SQL migrations to a Postgres database.

Usage:
    DATABASE_URL=postgresql://... python -m worker_health.pool_classifier_web.scripts.migrate

Also callable as apply_migrations(dsn) from Python code.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from worker_health.pool_classifier_web.postgres import connect as postgres_connect
from worker_health.pool_classifier_web.scripts import task_evidence_cutover as evidence

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"
MIGRATION_LOCK_ID = evidence.MIGRATION_LOCK_ID
MIGRATION_LOCK_TIMEOUT = "5s"
MIGRATION_STATEMENT_TIMEOUT = "30s"


def _apply_evidence_activation(cur, sql_file: Path, *, fresh_database: bool, activate: bool) -> None:
    if not evidence.is_prepared(cur):
        # Some local/test databases already ran the original 014, which
        # activated the canonical columns in a single transaction.
        if evidence.constraint_status(cur) is not True:
            raise RuntimeError("Task evidence is neither prepared nor active.")
        return
    if fresh_database:
        evidence.prepare_empty(cur)
        activate = True
    if not activate:
        raise RuntimeError(
            "Task evidence requires a staged rollout. Run migrate --prepare-task-evidence, "
            "backfill and validate it using the maintenance job, then drain legacy processes "
            "and run migrate --activate-task-evidence.",
        )
    evidence.require_ready(cur)
    cur.execute("LOCK TABLE task_results IN ACCESS EXCLUSIVE MODE NOWAIT")
    cur.execute(sql_file.read_text())


def apply_migrations(dsn: str, *, prepare_task_evidence: bool = False, activate_task_evidence: bool = False) -> None:
    with postgres_connect(dsn, "migration") as conn:
        with conn.cursor() as cur:
            # Serialize migrations and keep the lock and records transactional.
            cur.execute("SELECT set_config('lock_timeout', %s, true)", (MIGRATION_LOCK_TIMEOUT,))
            cur.execute("SELECT set_config('statement_timeout', %s, true)", (MIGRATION_STATEMENT_TIMEOUT,))
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
        fresh_database = False
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = sql_file.stem
            if prepare_task_evidence and version > evidence.PREPARE_VERSION:
                break
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (version,))
                if cur.fetchone():
                    print(f"  {version}: already applied")
                    continue
                if version == evidence.ACTIVATE_VERSION:
                    _apply_evidence_activation(
                        cur, sql_file, fresh_database=fresh_database, activate=activate_task_evidence,
                    )
                else:
                    cur.execute(sql_file.read_text())
                cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
                fresh_database = fresh_database or version == "001_init"
            print(f"  {version}: applied")
        conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    phase = parser.add_mutually_exclusive_group()
    phase.add_argument("--prepare-task-evidence", action="store_true")
    phase.add_argument("--activate-task-evidence", action="store_true")
    args = parser.parse_args()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    apply_migrations(
        dsn, prepare_task_evidence=args.prepare_task_evidence, activate_task_evidence=args.activate_task_evidence,
    )
