"""Backfill compact Taskcluster job-source metadata for recent Postgres task runs."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable

from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web.postgres import connect as postgres_connect
from worker_health.pool_classifier_web.scripts.backfill_start_lag_all_pools import parse_pool_id
from worker_health.pool_classifier_web.storage import ClassifyLockBusy, PostgresStorage, close_postgres_pools


class StopAfterCurrentBatch:
    """Turn the first Ctrl-C into a request to stop after durable work."""

    def __init__(self) -> None:
        self.requested = False

    def handle_signal(self, signum: int, frame: object) -> None:
        if self.requested:
            signal.default_int_handler(signum, frame)
        self.requested = True
        print("Ctrl-C received; finishing the current batch before stopping. Press Ctrl-C again to abort.", file=sys.stderr)


def backlog_by_pool(database_url: str, not_before: str) -> list[tuple[str, int]]:
    """Return missing recent source records, grouped by pool, without fetching tasks."""
    with postgres_connect(database_url, "maintenance") as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT r.pool_id, COUNT(DISTINCT r.task_id) FROM task_results r "
            "LEFT JOIN task_sources s ON s.task_id = r.task_id "
            "WHERE s.task_id IS NULL AND r.run_started IS NOT NULL AND r.run_started >= %s::timestamptz "
            "GROUP BY r.pool_id ORDER BY r.pool_id",
            (not_before,),
        )
        return [(row[0], row[1]) for row in cursor.fetchall()]


def backfill_pool(
    pool_id: str, database_url: str, batch_size: int, concurrency: int, retries: int,
    requests_per_second: float, not_before: str, should_stop: Callable[[], bool],
) -> tuple[bool, bool, str | None]:
    """Drain one pool's source backlog, stopping safely at batch boundaries."""
    provisioner, worker_type = parse_pool_id(pool_id)
    storage = PostgresStorage(pool_id=pool_id, dsn=database_url)
    storage.init_schema()
    classifier = PoolClassifier(provisioner, worker_type, storage=storage, use_color=False)
    try:
        while True:
            if should_stop():
                return True, True, None
            result = classifier.backfill_job_sources(
                batch_size=batch_size, concurrency=concurrency, retries=retries,
                requests_per_second=requests_per_second, not_before=not_before, should_stop=should_stop,
            )
            print(f"{pool_id}: {result}")
            if result.get("stop_requested"):
                return True, True, None
            if result["errors"]:
                return False, False, "transient Taskcluster failures"
            if result["selected_tasks"] == 0:
                return True, False, None
    except ClassifyLockBusy:
        print(f"{pool_id}: skipped because a classifier cycle is already running; rerun it later.", file=sys.stderr)
        return False, False, "classifier lock busy"
    finally:
        storage.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), help="Postgres DSN (default: DATABASE_URL)")
    parser.add_argument("--batch-size", type=int, default=500, metavar="TASKS")
    parser.add_argument("--concurrency", type=int, default=5, metavar="REQUESTS")
    parser.add_argument("--retries", type=int, default=2, metavar="COUNT")
    parser.add_argument("--requests-per-second", type=float, default=5.0, metavar="RATE")
    parser.add_argument("--count-only", action="store_true", help="report eligible tasks without fetching or writing")
    parser.add_argument(
        "--lookback-days", type=int, default=14, metavar="DAYS",
        help="only backfill tasks started within this trailing window (default: 14)",
    )
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    if args.batch_size <= 0 or args.concurrency <= 0 or args.retries < 0 or args.requests_per_second <= 0 or args.lookback_days <= 0:
        parser.error("batch-size, concurrency, requests-per-second, and lookback-days must be positive; retries must not be negative")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    stop = StopAfterCurrentBatch()
    previous_handler = signal.signal(signal.SIGINT, stop.handle_signal)
    try:
        not_before = (datetime.now(timezone.utc) - timedelta(days=args.lookback_days)).isoformat()
        backlog = backlog_by_pool(args.database_url, not_before)
        if not backlog:
            print("No eligible job-source backlog found.")
            return 0
        if args.count_only:
            total = sum(tasks for _pool_id, tasks in backlog)
            print(f"Eligible job-source backlog: {total} task(s) across {len(backlog)} pool(s) from the last {args.lookback_days} days.")
            for pool_id, tasks in backlog:
                print(f"{pool_id}: {tasks} task(s)")
            return 0
        pool_ids = [pool_id for pool_id, _tasks in backlog]
        print(f"Backfilling {len(pool_ids)} pool(s) from the last {args.lookback_days} days.")
        skipped = []
        for pool_id in pool_ids:
            if stop.requested:
                return 130
            print(f"=== {pool_id} ===")
            succeeded, stopped, reason = backfill_pool(
                pool_id, args.database_url, args.batch_size, args.concurrency, args.retries,
                args.requests_per_second, not_before, lambda: stop.requested,
            )
            if stopped:
                return 130
            if not succeeded:
                skipped.append(f"{pool_id} ({reason})")
        if skipped:
            print(f"Incomplete backfill in {len(skipped)} pool(s): {', '.join(skipped)}", file=sys.stderr)
            return 1
        return 0
    finally:
        signal.signal(signal.SIGINT, previous_handler)
        close_postgres_pools()


if __name__ == "__main__":
    sys.exit(main())
