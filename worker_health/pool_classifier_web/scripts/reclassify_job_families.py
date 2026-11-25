"""Refresh stored catch-all suite labels from existing task names."""

from __future__ import annotations

import argparse
import json
from collections import Counter

from worker_health.pool_classifier_web.job_families import OTHER_PERFORMANCE_TESTS, job_family_for_name
from worker_health.pool_classifier_web.postgres import connect as postgres_connect

RECLASSIFICATIONS = {
    OTHER_PERFORMANCE_TESTS: {"Talos", "Benchmarks", "Networking", "Resource and power"},
    "Other tests": {"GTest", "Marionette", "Test Verify", "Source tests"},
    "Unknown": {"Resource and power"},
}
BATCH_SQL = """
SELECT task_id, task_name, COALESCE(job_family, 'Unknown') AS family
FROM task_sources AS stored
WHERE task_id > %s AND COALESCE(job_family, 'Unknown') = ANY(%s) AND task_name IS NOT NULL
AND (cardinality(%s::text[]) = 0 OR EXISTS (
    SELECT 1 FROM task_results AS run
    WHERE run.task_id = stored.task_id AND run.pool_id = ANY(%s)
))
ORDER BY task_id LIMIT %s
"""
UPDATE_SQL = """
UPDATE task_sources AS stored SET job_family = changed.family
FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[]) AS changed(id, name, old_family, family)
WHERE stored.task_id = changed.id AND stored.task_name = changed.name
AND COALESCE(stored.job_family, 'Unknown') = changed.old_family
RETURNING stored.job_family
"""


def run(dsn: str, argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--pool", action="append", default=[], help="Scope to a pool ID. Repeat for multiple pools.")
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
            rows = conn.execute(
                BATCH_SQL, (previous_id, list(RECLASSIFICATIONS), args.pool, args.pool, args.batch_size),
            ).fetchall()
            if not rows:
                break
            previous_id = rows[-1][0]
            checked += len(rows)
            changes = []
            for task_id, name, old_family in rows:
                family = job_family_for_name(name)
                if family in RECLASSIFICATIONS[old_family]:
                    changes.append((task_id, name, old_family, family))
            if args.dry_run:
                labels.update(family for _, _, _, family in changes)
            elif changes:
                columns = tuple(list(column) for column in zip(*changes, strict=True))
                updated = conn.execute(UPDATE_SQL, columns).fetchall()
                labels.update(family for (family,) in updated)
    print(json.dumps({
        "event": "job_families_reclassified", "dry_run": args.dry_run,
        "checked": checked, "labels": labels,
    }, sort_keys=True))
