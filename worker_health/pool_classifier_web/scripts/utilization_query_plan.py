"""Capture a bounded PostgreSQL plan for the utilization task-run overlap query."""

from __future__ import annotations

import argparse
import json
from datetime import datetime


DEFAULT_STATEMENT_TIMEOUT_SECONDS = 30
MAX_WINDOW_SECONDS = 7 * 24 * 60 * 60
UTILIZATION_TASK_RUNS_SQL = """
SELECT worker_id, run_started AS start_at, run_resolved AS end_at
FROM task_results
WHERE pool_id = %s
  AND run_started IS NOT NULL
  AND run_resolved IS NOT NULL
  AND run_started < %s::timestamptz
  AND run_resolved > %s::timestamptz
"""


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return parsed


def _validate_window(start: str, end: str) -> None:
    start_dt, end_dt = _parse_timestamp(start), _parse_timestamp(end)
    if end_dt <= start_dt:
        raise ValueError("end must be after start")
    if (end_dt - start_dt).total_seconds() > MAX_WINDOW_SECONDS:
        raise ValueError("window must be no longer than seven days")


def capture_utilization_task_run_plan(
    dsn: str,
    *,
    pool_id: str,
    start: str,
    end: str,
    analyze: bool = False,
    statement_timeout_seconds: int = DEFAULT_STATEMENT_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Return PostgreSQL's JSON plan for the exact utilization overlap query.

    ``--analyze`` is opt-in because it executes the read-only SELECT.  Both
    modes use a range cap and a session statement timeout, so production plan
    collection cannot silently become an unbounded maintenance operation.
    """
    import psycopg

    if not pool_id:
        raise ValueError("pool_id must not be empty")
    _validate_window(start, end)
    if statement_timeout_seconds < 1 or statement_timeout_seconds > 120:
        raise ValueError("statement timeout must be between 1 and 120 seconds")

    options = "ANALYZE, BUFFERS, FORMAT JSON" if analyze else "FORMAT JSON"
    explain_sql = f"EXPLAIN ({options}) {UTILIZATION_TASK_RUNS_SQL}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('default_transaction_read_only', 'on', false)")
            cur.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (f"{statement_timeout_seconds}s",),
            )
            cur.execute(explain_sql, (pool_id, end, start))
            plan = cur.fetchone()[0]

    return {
        "pool_id": pool_id,
        "start": start,
        "end": end,
        "analyze": analyze,
        "statement_timeout_seconds": statement_timeout_seconds,
        "plan": plan,
    }


def run(dsn: str, argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool-id", required=True)
    parser.add_argument("--start", required=True, help="ISO-8601 timestamp with timezone")
    parser.add_argument("--end", required=True, help="ISO-8601 timestamp with timezone")
    parser.add_argument("--analyze", action="store_true", help="Execute the read-only SELECT")
    parser.add_argument(
        "--statement-timeout-seconds",
        type=int,
        default=DEFAULT_STATEMENT_TIMEOUT_SECONDS,
    )
    args = parser.parse_args(argv)
    print(
        json.dumps(
            {
                "event": "utilization_task_run_query_plan",
                "summary": capture_utilization_task_run_plan(
                    dsn,
                    pool_id=args.pool_id,
                    start=args.start,
                    end=args.end,
                    analyze=args.analyze,
                    statement_timeout_seconds=args.statement_timeout_seconds,
                ),
            },
            default=str,
            sort_keys=True,
        ),
    )
