"""Run an explicitly selected, reviewed production database operation.

Cloud Run's ``pool-classifier-db-maintenance`` job is deliberately a durable
runner, not a job per database change.  Operations are allowlisted here and
selected through the execution's arguments, so adding an operation requires a
reviewed release-image change but not a Terraform change.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable

from worker_health.pool_classifier_web.scripts.create_unresolved_task_run_index import (
    create_unresolved_task_run_index,
)

Operation = Callable[[str], None]

OPERATIONS: dict[str, Operation] = {
    "create-unresolved-task-run-index": create_unresolved_task_run_index,
}


def run_operation(operation: str, dsn: str) -> None:
    """Run one allowlisted operation and record its identity in job logs."""
    try:
        handler = OPERATIONS[operation]
    except KeyError as exc:
        available = ", ".join(sorted(OPERATIONS))
        raise ValueError(f"unknown maintenance operation {operation!r}; available: {available}") from exc

    print(json.dumps({"event": "db_maintenance_started", "operation": operation}, sort_keys=True))
    try:
        handler(dsn)
    except Exception:
        print(json.dumps({"event": "db_maintenance_failed", "operation": operation}, sort_keys=True), file=sys.stderr)
        raise
    print(json.dumps({"event": "db_maintenance_completed", "operation": operation}, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", required=True, choices=sorted(OPERATIONS))
    args = parser.parse_args(argv)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        return 1
    run_operation(args.operation, dsn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
