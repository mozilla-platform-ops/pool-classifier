"""Re-enable Queue status collection for one unavailable run."""

import argparse
import os
from pathlib import Path

from worker_health.pool_classifier_web.storage import PostgresStorage, SqliteStorage


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool", required=True, help="Provisioner/worker type")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--run-id", required=True, help="Run number, or 'legacy' for a NULL run ID")
    parser.add_argument("--results-dir", type=Path, help="Use this pool's SQLite results directory")
    args = parser.parse_args(argv)
    parts = args.pool.split("/")
    if len(parts) != 2 or not all(parts):
        parser.error("pool must be provisioner/worker-type")
    if args.run_id != "legacy" and (not args.run_id.isascii() or not args.run_id.isdecimal() or int(args.run_id) > 2147483647):
        parser.error("run-id must be a non-negative 32-bit integer or 'legacy'")
    run_id = None if args.run_id == "legacy" else int(args.run_id)
    if args.results_dir is not None:
        if not (args.results_dir / "pool_classifier.db").is_file():
            parser.error("results-dir must contain the existing SQLite database for the specified pool")
        storage = SqliteStorage(args.pool, args.results_dir)
    else:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            parser.error("set DATABASE_URL or supply --results-dir")
        storage = PostgresStorage(args.pool, dsn, role="classifier")
    try:
        storage.init_schema()
        with storage.classify_lock():
            changed = storage.retry_task_status(args.task_id, run_id)
            storage.commit()
    finally:
        storage.close()
    if changed:
        print("Status retry enabled for the selected run.")
        return 0
    print("No stopped, unavailable run matched the supplied identity.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
