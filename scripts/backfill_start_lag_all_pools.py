#!/usr/bin/env python3
"""Backfill Queue schedule metadata for every Postgres pool with a backlog."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Callable

import psycopg

from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web.storage import PostgresStorage


class StopAfterCurrentBatch:
    """Turn the first Ctrl-C into a request to stop after durable work."""

    def __init__(self) -> None:
        self.requested = False

    def handle_signal(self, signum: int, _frame: object) -> None:
        if self.requested:
            signal.default_int_handler(signum, _frame)
        self.requested = True
        print(
            "Ctrl-C received; finishing the current batch before stopping. Press Ctrl-C again to abort.",
            file=sys.stderr,
        )


def pool_ids_with_backlog(database_url: str) -> list[str]:
    """Return stored pools that still have runs eligible for enrichment."""
    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT pool_id FROM task_results "
            "WHERE run_scheduled IS NULL AND run_id IS NOT NULL "
            "GROUP BY pool_id ORDER BY pool_id",
        )
        return [row[0] for row in cursor.fetchall()]


def parse_pool_id(pool_id: str) -> tuple[str, str]:
    """Split the Taskcluster provisioner/worker-type identifier."""
    provisioner, separator, worker_type = pool_id.partition("/")
    if not separator or not provisioner or not worker_type or "/" in worker_type:
        raise ValueError(f"invalid pool ID in task_results: {pool_id!r}")
    return provisioner, worker_type


def state_file(state_dir: Path, pool_id: str) -> Path:
    return state_dir / f"{pool_id.replace('/', '--')}.json"


def backfill_pool(
    pool_id: str,
    database_url: str,
    batch_size: int,
    concurrency: int,
    retries: int,
    requests_per_second: float,
    state_dir: Path,
    should_stop: Callable[[], bool],
) -> tuple[bool, bool]:
    """Drain one pool's eligible backlog; return false after transient failures."""
    provisioner, worker_type = parse_pool_id(pool_id)
    storage = PostgresStorage(pool_id=pool_id, dsn=database_url)
    storage.init_schema()
    classifier = PoolClassifier(
        provisioner=provisioner,
        worker_type=worker_type,
        storage=storage,
        use_color=False,
    )
    try:
        while True:
            if should_stop():
                return True, True
            result = classifier.backfill_start_lag(
                batch_size=batch_size,
                concurrency=concurrency,
                retries=retries,
                requests_per_second=requests_per_second,
                state_file=state_file(state_dir, pool_id),
                should_stop=should_stop,
            )
            print(f"{pool_id}: {result}")
            if result.get("stop_requested"):
                return True, True
            if result["transient_failures"]:
                print(
                    f"{pool_id}: stopped after transient Queue failures; rerun this script to retry.",
                    file=sys.stderr,
                )
                return False, False
            if result["selected_runs"] == 0:
                return True, False
    finally:
        storage.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill observed start-lag metadata for every Postgres pool with eligible runs.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN (default: DATABASE_URL)",
    )
    parser.add_argument("--batch-size", type=int, default=500, metavar="RUNS")
    parser.add_argument("--concurrency", type=int, default=5, metavar="REQUESTS")
    parser.add_argument("--retries", type=int, default=2, metavar="COUNT")
    parser.add_argument("--requests-per-second", type=float, default=5.0, metavar="RATE")
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(".backfill-start-lag-state"),
        help="directory for per-pool Queue skip state",
    )
    args = parser.parse_args()

    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    if args.batch_size <= 0 or args.concurrency <= 0 or args.retries < 0 or args.requests_per_second <= 0:
        parser.error("batch-size, concurrency, and requests-per-second must be positive; retries must not be negative")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    stop = StopAfterCurrentBatch()
    previous_handler = signal.signal(signal.SIGINT, stop.handle_signal)
    try:
        args.state_dir.mkdir(parents=True, exist_ok=True)
        pool_ids = pool_ids_with_backlog(args.database_url)
        if not pool_ids:
            print("No eligible start-lag backlog found.")
            return 0

        print(f"Backfilling {len(pool_ids)} pool(s).")
        failures = []
        for pool_id in pool_ids:
            if stop.requested:
                return 130
            print(f"=== {pool_id} ===")
            succeeded, stopped = backfill_pool(
                pool_id,
                args.database_url,
                args.batch_size,
                args.concurrency,
                args.retries,
                args.requests_per_second,
                args.state_dir,
                lambda: stop.requested,
            )
            if stopped:
                return 130
            if not succeeded:
                failures.append(pool_id)

        if failures:
            print(f"Transient failures in {len(failures)} pool(s): {', '.join(failures)}", file=sys.stderr)
            return 1
        return 0
    finally:
        signal.signal(signal.SIGINT, previous_handler)


if __name__ == "__main__":
    raise SystemExit(main())
