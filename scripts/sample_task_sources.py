#!/usr/bin/env python3
"""Sample locally stored Taskcluster jobs and report their source metadata.

This is an investigation tool, not part of the classifier's collection path.
It reads task IDs from a local PostgreSQL datastore and fetches each selected
task definition from Taskcluster.  The generated report is ignored by Git by
default, so it can be used to validate source taxonomy before a deployment.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import json
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


DEFAULT_DSN = "postgresql://pc:pc@127.0.0.1:5433/pool_classifier"
DEFAULT_OUTPUT = Path("debugging_results/task-source-sample.json")
TASK_URL = "https://firefox-ci-tc.services.mozilla.com/api/queue/v1/task/{task_id}"
AUDIT_WORKER_SOURCE = "https://github.com/taskcluster/mozilla-history/tree/master/audit-worker-versions"

SAMPLE_SQL = """
WITH distinct_tasks AS (
    SELECT DISTINCT ON (pool_id, task_id)
           pool_id, task_id, run_started
    FROM task_results
    WHERE run_started >= now() - make_interval(days => %s)
    ORDER BY pool_id, task_id, run_started DESC
), ranked AS (
    SELECT pool_id, task_id, run_started,
           row_number() OVER (PARTITION BY pool_id ORDER BY md5(task_id)) AS rank
    FROM distinct_tasks
)
SELECT pool_id, task_id, run_started
FROM ranked
WHERE rank <= %s
ORDER BY pool_id, task_id
"""

POPULATION_SQL = """
SELECT COUNT(*) AS runs, COUNT(DISTINCT task_id) AS tasks
FROM task_results
WHERE run_started >= now() - make_interval(days => %s)
"""


def classify_source(task: dict[str, Any]) -> tuple[str, str, str | None]:
    """Return (source, method, project) without inferring source from scheduling."""
    tags = task.get("tags")
    project = tags.get("project") if isinstance(tags, dict) else None
    if isinstance(project, str) and project.strip():
        return project, "project_tag", project

    metadata = task.get("metadata")
    source_url = metadata.get("source") if isinstance(metadata, dict) else None
    if source_url == AUDIT_WORKER_SOURCE:
        return "audit-worker", "metadata_source", None

    return "unknown", "missing_project_tag", None


def fetch_task(row: dict[str, Any]) -> dict[str, Any]:
    pool_id = row["pool_id"]
    task_id = row["task_id"]
    run_started = row["run_started"]
    try:
        with urllib.request.urlopen(TASK_URL.format(task_id=task_id), timeout=20) as response:
            task = json.load(response)
        source, method, project = classify_source(task)
        return {
            "pool_id": pool_id,
            "task_id": task_id,
            "run_started": run_started.isoformat() if run_started else None,
            "source": source,
            "classification_method": method,
            "project_tag": project,
            "metadata_source": (task.get("metadata") or {}).get("source"),
            "scheduler_id": task.get("schedulerId"),
            "error": None,
        }
    except urllib.error.HTTPError as error:
        error_text = f"HTTP {error.code}"
    except urllib.error.URLError as error:
        error_text = f"network error: {error.reason}"
    except TimeoutError:
        error_text = "timeout"
    except json.JSONDecodeError:
        error_text = "invalid JSON"

    return {
        "pool_id": pool_id,
        "task_id": task_id,
        "run_started": run_started.isoformat() if run_started else None,
        "source": "unknown",
        "classification_method": "unavailable_task_definition",
        "project_tag": None,
        "metadata_source": None,
        "scheduler_id": None,
        "error": error_text,
    }


def _counter(records: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(collections.Counter(record[key] for record in records).items()))


def sample_sources(
    dsn: str, days: int, sample_per_pool: int, workers: int,
) -> dict[str, Any]:
    if days <= 0 or sample_per_pool <= 0 or workers <= 0:
        raise ValueError("days, sample-per-pool, and workers must be positive")

    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as connection:
        connection.execute("SET default_transaction_read_only = on")
        connection.execute("SET statement_timeout = '15s'")
        population = dict(connection.execute(POPULATION_SQL, (days,)).fetchone())
        rows = connection.execute(SAMPLE_SQL, (days, sample_per_pool)).fetchall()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        records = list(executor.map(fetch_task, rows))

    fetched = [record for record in records if record["error"] is None]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "window_days": days,
        "sample_per_pool": sample_per_pool,
        "population": population,
        "sample": {
            "task_definitions": len(records),
            "fetchable_task_definitions": len(fetched),
            "project_tag_coverage_pct": round(
                100 * sum(record["classification_method"] == "project_tag" for record in fetched) / len(fetched), 1,
            ) if fetched else 0.0,
            "sources": _counter(records, "source"),
            "classification_methods": _counter(records, "classification_method"),
            "errors": _counter([record for record in records if record["error"]], "error"),
        },
        "records": records,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL", DEFAULT_DSN))
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--sample-per-pool", type=int, default=3)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    report = sample_sources(args.dsn, args.days, args.sample_per_pool, args.workers)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "sample": report["sample"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
