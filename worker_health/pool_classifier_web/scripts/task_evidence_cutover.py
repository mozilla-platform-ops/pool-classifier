"""Backfill and validate task evidence while the legacy application is live."""

from __future__ import annotations

import argparse
import json
from time import monotonic, sleep

from worker_health.pool_classifier_web.postgres import connect as postgres_connect

PREPARE_VERSION = "014_task_run_evidence"
ACTIVATE_VERSION = "015_activate_task_run_evidence"
MIGRATION_LOCK_ID = 6_061_283
CONSTRAINT = "task_results_evidence_consistent"
PENDING_INDEX = "idx_task_results_evidence_unmapped"
INDEXES = {
    "idx_task_sources_name_retry": ("task_sources", "(task_id) WHERE name_retry_enabled"),
    "idx_task_results_status_retry": (
        "task_results", "(pool_id, last_checked_at, observed_at, task_id, run_id) WHERE status_retry_enabled",
    ),
    "idx_task_results_classification_pending": (
        "task_results",
        "(pool_id, classification_last_attempt_at NULLS FIRST, task_id, run_id) WHERE classification_state = 'pending'",
    ),
    "idx_task_results_running": ("task_results", "(pool_id, task_id) WHERE evidence_run_state = 'running'"),
    "idx_task_results_evidence_category": ("task_results", "(evidence_category) WHERE evidence_category IS NOT NULL"),
    "idx_task_results_evidence_category_resolved": (
        "task_results",
        "(pool_id, evidence_category, run_resolved DESC, evidence_classified_at DESC) WHERE evidence_category IS NOT NULL",
    ),
}
CONSTRAINT_SQL = f"""
ALTER TABLE task_results ADD CONSTRAINT {CONSTRAINT} CHECK (
    status_fetch_state IS NOT NULL AND status_retry_enabled IS NOT NULL AND classification_state IS NOT NULL
    AND status_fetch_state IN ('pending', 'available', 'not_found', 'run_missing', 'worker_mismatch', 'error', 'unknown')
    AND (evidence_run_state IS NULL OR evidence_run_state IN (
        'pending', 'running', 'unscheduled', 'completed', 'failed', 'exception'
    ))
    AND CASE
        WHEN evidence_run_state = 'completed' THEN classification_state = 'not_applicable' AND NOT status_retry_enabled
            AND evidence_category IS NULL AND evidence_classified_at IS NULL
        WHEN evidence_run_state IN ('failed', 'exception') THEN NOT status_retry_enabled AND (
            (classification_state = 'pending' AND evidence_category IS NULL AND evidence_classified_at IS NULL)
            OR (classification_state = 'classified' AND evidence_category IS NOT NULL AND evidence_classified_at IS NOT NULL))
        ELSE classification_state = 'unknown' AND evidence_category IS NULL AND evidence_classified_at IS NULL END
) NOT VALID
"""
BATCH_SQL = """
UPDATE task_results SET status_fetch_state = NULL
WHERE ctid = ANY (ARRAY(
    SELECT ctid FROM task_results
    WHERE status_fetch_state IS NULL
    ORDER BY pool_id, task_id, COALESCE(run_id, -1)
    LIMIT %s FOR UPDATE SKIP LOCKED
))
RETURNING pool_id, task_id, run_id, status_fetch_state
"""
ROLLBACK_SQL = """
DROP TRIGGER task_results_sync_evidence ON task_results;
ALTER TABLE task_results RENAME COLUMN run_state TO evidence_run_state;
ALTER TABLE task_results RENAME COLUMN category TO evidence_category;
ALTER TABLE task_results RENAME COLUMN classified_at TO evidence_classified_at;
ALTER TABLE task_results RENAME COLUMN legacy_run_state TO run_state;
ALTER TABLE task_results RENAME COLUMN legacy_category TO category;
ALTER TABLE task_results RENAME COLUMN legacy_classified_at TO classified_at;

ALTER TABLE task_results
    ALTER COLUMN status_fetch_state DROP DEFAULT,
    ALTER COLUMN classification_state DROP DEFAULT,
    ALTER COLUMN status_retry_enabled DROP DEFAULT;

CREATE TRIGGER task_results_sync_evidence BEFORE INSERT OR UPDATE ON task_results
FOR EACH ROW EXECUTE FUNCTION task_results_stage_evidence();
"""


def is_prepared(cur) -> bool:
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_attribute WHERE attrelid = 'task_results'::regclass
            AND attname = 'evidence_run_state' AND NOT attisdropped
        )
    """)
    return cur.fetchone()[0]


def constraint_status(cur) -> bool | None:
    cur.execute("""
        SELECT convalidated FROM pg_constraint
        WHERE conrelid = 'task_results'::regclass AND conname = %s
    """, (CONSTRAINT,))
    row = cur.fetchone()
    return None if row is None else row[0]


def _index_status(cur, name: str, table: str) -> bool | None:
    cur.execute("""
        SELECT i.indisvalid FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        WHERE i.indrelid = %s::regclass AND c.relname = %s
    """, (table, name))
    row = cur.fetchone()
    return None if row is None else row[0]


def _create_indexes(cur, indexes, *, concurrently: bool) -> None:
    mode = "CONCURRENTLY " if concurrently else ""
    for name, (table, definition) in indexes.items():
        status = _index_status(cur, name, table)
        if status:
            continue
        if status is False:
            cur.execute(f"DROP INDEX {mode}{name}")
        cur.execute(f"CREATE INDEX {mode}{name} ON {table} {definition}")


def _validate(cur, *, concurrently: bool) -> None:
    _create_indexes(cur, INDEXES, concurrently=concurrently)
    if constraint_status(cur) is None:
        with cur.connection.transaction():
            cur.execute("LOCK TABLE task_results IN ACCESS EXCLUSIVE MODE NOWAIT")
            cur.execute(CONSTRAINT_SQL)
    # This scan runs under SHARE UPDATE EXCLUSIVE, separately from ADD
    # CONSTRAINT in the autocommit maintenance session.
    cur.execute(f"ALTER TABLE task_results VALIDATE CONSTRAINT {CONSTRAINT}")


def prepare_empty(cur) -> None:
    """Build final indexes and validate on a fresh database only."""
    _validate(cur, concurrently=False)


def require_ready(cur) -> None:
    if constraint_status(cur) is not True:
        raise RuntimeError("Run the task-evidence backfill and validation before activation.")
    for name, (table, _) in INDEXES.items():
        if _index_status(cur, name, table) is not True:
            raise RuntimeError(f"Required task-evidence index is missing or invalid: {name}")


def _has_pending(cur) -> bool:
    cur.execute("SELECT EXISTS (SELECT 1 FROM task_results WHERE status_fetch_state IS NULL)")
    return cur.fetchone()[0]


def _backfill(conn, cur, args) -> None:
    started = monotonic()
    _create_indexes(cur, {
        PENDING_INDEX: ("task_results", "(pool_id, task_id, COALESCE(run_id, -1)) WHERE status_fetch_state IS NULL"),
    }, concurrently=True)
    updated = 0
    for batch in range(1, args.max_batches + 1):
        with conn.transaction():
            cur.execute("SET LOCAL statement_timeout = '15s'")
            cur.execute(BATCH_SQL, (args.batch_size,))
            rows = cur.fetchall()
            for pool, task, run, mapped in rows:
                if mapped is None:
                    raise RuntimeError(
                        f"Ambiguous legacy task evidence needs review before retrying: {pool}/{task} run {run}",
                    )
        updated += len(rows)
        if batch % 100 == 0:
            print(json.dumps({"event": "task_evidence_backfill_progress", "rows_updated": updated}), flush=True)
        if not rows:
            break
        sleep(args.pause_seconds)
    print(json.dumps({
        "event": "task_evidence_backfill", "rows_updated": updated,
        "has_pending_rows": _has_pending(cur), "elapsed_seconds": round(monotonic() - started, 2),
    }), flush=True)


def run(dsn: str, argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=("backfill", "validate", "rollback"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-batches", type=int, default=1000)
    parser.add_argument("--pause-seconds", type=float, default=0.05)
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 10000 or args.max_batches < 1 or not 0 <= args.pause_seconds <= 60:
        parser.error("batch size must be 1–10000, max batches positive, and pause 0–60 seconds")

    with postgres_connect(dsn, "maintenance", autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
        if not cur.fetchone()[0]:
            raise RuntimeError("Another migration or maintenance operation already holds the migration lock.")
        cur.execute("SET lock_timeout = '5s'")
        cur.execute("SET statement_timeout = '20min'")
        if args.phase == "rollback":
            _rollback(conn, cur)
        elif not is_prepared(cur):
            raise RuntimeError("Task-evidence preparation must be applied and activation must still be pending.")
        elif args.phase == "backfill":
            _backfill(conn, cur, args)
        else:
            if _has_pending(cur):
                raise RuntimeError("Task-evidence backfill has pending rows. Finish it before validation.")
            started = monotonic()
            _validate(cur, concurrently=True)
            cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {PENDING_INDEX}")
            print(json.dumps({
                "event": "task_evidence_validated", "elapsed_seconds": round(monotonic() - started, 2),
            }), flush=True)


def _rollback(conn, cur) -> None:
    with conn.transaction():
        cur.execute("SET LOCAL statement_timeout = '10s'")
        cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (ACTIVATE_VERSION,))
        if not cur.fetchone():
            raise RuntimeError("Task-evidence activation has not been applied.")
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_attribute WHERE attrelid = 'task_results'::regclass
                AND attname = 'legacy_run_state' AND NOT attisdropped
            )
        """)
        if not cur.fetchone()[0]:
            raise RuntimeError("Legacy task columns are unavailable for an in-place rollback.")
        cur.execute("LOCK TABLE task_results IN ACCESS EXCLUSIVE MODE NOWAIT")
        cur.execute(ROLLBACK_SQL)
        cur.execute("DELETE FROM schema_migrations WHERE version = %s", (ACTIVATE_VERSION,))
    print(json.dumps({"event": "task_evidence_rolled_back"}), flush=True)
