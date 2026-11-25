"""Refresh stored performance catch-all labels from existing task names."""

from __future__ import annotations

import argparse
import json
from collections import Counter

from worker_health.pool_classifier_web.job_families import OTHER_PERFORMANCE_TESTS, job_family_for_name
from worker_health.pool_classifier_web.postgres import connect as postgres_connect

BATCH_SQL = """
SELECT task_id, task_name FROM task_sources
WHERE task_id > %s AND job_family = %s AND task_name IS NOT NULL
ORDER BY task_id LIMIT %s
"""
UPDATE_SQL = """
UPDATE task_sources AS stored SET job_family = changed.family
FROM unnest(%s::text[], %s::text[], %s::text[]) AS changed(id, name, family)
WHERE stored.task_id = changed.id AND stored.task_name = changed.name AND stored.job_family = %s
RETURNING stored.job_family
"""


def run(dsn: str, argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 10000:
        parser.error("--batch-size must be from 1 to 10000")

    checked = 0
    previous_id = ""
    labels: Counter[str] = Counter()
    with postgres_connect(dsn, "maintenance", autocommit=True) as conn:
        conn.execute("SET lock_timeout = '3s'")
        conn.execute("SET statement_timeout = '30s'")
        while True:
            rows = conn.execute(BATCH_SQL, (previous_id, OTHER_PERFORMANCE_TESTS, args.batch_size)).fetchall()
            if not rows:
                break
            previous_id = rows[-1][0]
            checked += len(rows)
            changes = []
            for task_id, name in rows:
                family = job_family_for_name(name)
                if family in {"Talos", "Benchmarks"}:
                    changes.append((task_id, name, family))
            if args.dry_run:
                labels.update(family for _, _, family in changes)
            elif changes:
                ids, names, families = zip(*changes, strict=True)
                updated = conn.execute(
                    UPDATE_SQL, (list(ids), list(names), list(families), OTHER_PERFORMANCE_TESTS),
                ).fetchall()
                labels.update(family for (family,) in updated)
    print(json.dumps({
        "event": "job_families_reclassified", "dry_run": args.dry_run,
        "checked": checked, "labels": labels,
    }, sort_keys=True))
