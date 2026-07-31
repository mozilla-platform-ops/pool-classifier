"""Backfill migration-007 task observation timestamps in committed batches.

There is no checkpoint file or durable cursor.  Within one execution, an
in-memory primary-key cursor avoids rescanning earlier batches; a stopped
execution starts over safely and updates only rows still needing either
timestamp.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass

from worker_health.pool_classifier_web.scripts.migrate import MIGRATION_LOCK_ID

MIGRATION_VERSION = "007_observed_task_runs"
DEFAULT_BATCH_SIZE = 1_000
DEFAULT_BATCH_DELAY_SECONDS = 0.2
DEFAULT_RETRIES = 3

COUNT_MISSING_SQL = """
SELECT COUNT(*) FROM task_results
WHERE observed_at IS NULL OR last_checked_at IS NULL
"""
UPDATE_BATCH_SQL = """
WITH batch AS (
    SELECT pool_id, task_id, worker_id
    FROM task_results
    WHERE observed_at IS NULL OR last_checked_at IS NULL
      AND (%s::text IS NULL OR (pool_id, task_id, worker_id) > (%s::text, %s::text, %s::text))
    ORDER BY pool_id, task_id, worker_id
    LIMIT %s
    FOR UPDATE SKIP LOCKED
),
updated AS (
UPDATE task_results AS result
SET observed_at = COALESCE(result.observed_at, result.classified_at),
    last_checked_at = COALESCE(result.last_checked_at, result.classified_at)
FROM batch
WHERE (result.pool_id, result.task_id, result.worker_id) =
      (batch.pool_id, batch.task_id, batch.worker_id)
  AND (result.observed_at IS NULL OR result.last_checked_at IS NULL)
RETURNING 1
)
SELECT (SELECT COUNT(*) FROM updated), pool_id, task_id, worker_id
FROM batch
ORDER BY pool_id, task_id, worker_id
"""


@dataclass
class BackfillStats:
    batches: int = 0
    retries: int = 0
    updated: int = 0
    initial_remaining: int = 0
    remaining: int = 0


def _emit(event: str, **fields: object) -> None:
    print(json.dumps({"event": event, **fields}, sort_keys=True))


def _validate_schema(cur) -> None:
    cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (MIGRATION_VERSION,))
    if cur.fetchone() is None:
        raise RuntimeError(f"{MIGRATION_VERSION} is not recorded; apply schema migrations before backfilling")
    cur.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = 'task_results'"
        " AND column_name IN ('observed_at', 'last_checked_at')",
    )
    if {row[0] for row in cur.fetchall()} != {"observed_at", "last_checked_at"}:
        raise RuntimeError("task_results does not have the migration 007 timestamp columns")


def _count_missing(cur) -> int:
    cur.execute(COUNT_MISSING_SQL)
    return cur.fetchone()[0]


def _update_batch(conn, batch_size: int, cursor: tuple[str, str, str] | None) -> tuple[int, tuple[str, str, str] | None]:
    """Update one short transaction and return its count and next cursor."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
            if not cur.fetchone()[0]:
                raise RuntimeError("another migration or database-maintenance batch holds the operation lock")
            cursor_values = cursor or (None, None, None)
            cur.execute(UPDATE_BATCH_SQL, (cursor_values[0], *cursor_values, batch_size))
            rows = cur.fetchall()
            if not rows:
                return 0, None
            return rows[0][0], tuple(rows[-1][1:])


def backfill_task_timestamps(
    dsn: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_delay_seconds: float = DEFAULT_BATCH_DELAY_SECONDS,
    retries: int = DEFAULT_RETRIES,
    max_batches: int | None = None,
    count_only: bool = False,
    dry_run: bool = False,
) -> BackfillStats:
    """Run the NULL-driven backfill, or report its scope without changing data."""
    import psycopg

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if batch_delay_seconds < 0:
        raise ValueError("batch_delay_seconds cannot be negative")
    if retries < 0:
        raise ValueError("retries cannot be negative")
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be positive")
    if count_only and dry_run:
        raise ValueError("--count-only and --dry-run cannot be combined")

    # Autocommit keeps schema checks and final counts short.  Each actual
    # update below enters its own explicit transaction and commits before the
    # next paced batch begins.
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            _validate_schema(cur)
            initial_remaining = _count_missing(cur)

        stats = BackfillStats(initial_remaining=initial_remaining, remaining=initial_remaining)
        _emit(
            "m007_task_timestamp_backfill_started",
            batch_size=batch_size,
            count_only=count_only,
            dry_run=dry_run,
            remaining=initial_remaining,
        )
        if count_only:
            _emit("m007_task_timestamp_backfill_completed", **asdict(stats))
            return stats
        if dry_run:
            _emit(
                "m007_task_timestamp_backfill_dry_run",
                would_update=min(initial_remaining, batch_size),
                **asdict(stats),
            )
            return stats

        cursor: tuple[str, str, str] | None = None
        while stats.remaining and (max_batches is None or stats.batches < max_batches):
            for attempt in range(retries + 1):
                try:
                    updated, next_cursor = _update_batch(conn, batch_size, cursor)
                    break
                except Exception as exc:
                    if attempt == retries:
                        raise
                    stats.retries += 1
                    delay = min(2**attempt, 30)
                    _emit(
                        "m007_task_timestamp_backfill_retrying",
                        attempt=attempt + 1,
                        delay_seconds=delay,
                        error_type=type(exc).__name__,
                    )
                    time.sleep(delay)
            else:  # pragma: no cover - the loop either breaks or raises.
                raise AssertionError("unreachable retry state")

            if next_cursor is None:
                break

            stats.batches += 1
            stats.updated += updated
            # No cursor is retained: reaching a short/empty batch means this
            # execution's NULL-driven scan is complete.  A later invocation
            # safely rescans for any rows changed concurrently.
            stats.remaining = max(0, stats.initial_remaining - stats.updated)
            _emit(
                "m007_task_timestamp_backfill_progress",
                batch=stats.batches,
                updated=updated,
                updated_total=stats.updated,
                remaining_estimate=stats.remaining,
            )
            cursor = next_cursor
            if batch_delay_seconds:
                time.sleep(batch_delay_seconds)

        with conn.cursor() as cur:
            stats.remaining = _count_missing(cur)
        _emit("m007_task_timestamp_backfill_completed", **asdict(stats))
        return stats


def run(dsn: str, argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-delay-seconds", type=float, default=DEFAULT_BATCH_DELAY_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--count-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    backfill_task_timestamps(dsn, **vars(args))
