# Task-run evidence cutover

Migration 014 separates the last Queue lookup result, the most recent matching
Queue evidence, and the last completed log classification. It preserves the
original observation and attempt timestamps. It leaves historical successful
fetch timestamps null.

## Check the existing data

Inventory the states and whether a category, Queue time or Queue reason is
present. On the local PostgreSQL copy examined for this change there were
1,506,034 completed runs, 107,127 classified failures or exceptions, 447
observed references, and 653 legacy expiry markers. None of the expiry
markers carried a Queue outcome field.

The old scanner only wrote an expiry marker on a Queue 404. Migration 014
maps that shape to a stopped `not_found` lookup when it has a coherent
observation and attempt timestamp and no category, Queue reason or run time.
It aborts and reports the count if expiry rows have other evidence. Inspect
those rows and their provenance before attempting the migration again.
The migration also rejects unrecognized legacy state strings.

The SQLite launcher rebuilds the per-pool task table within one transaction.
Stop old SQLite processes before opening that database with the new code.
Keep a backup of the database before the first open.

## PostgreSQL rollout

1. Pause the scan scheduler and drain classifiers, web readers, and database
   maintenance processes using the old revision. Take a recoverable backup.
2. Run the migration job from the candidate image. For databases over 100,000
   run rows the runner requires `POOL_CLASSIFIER_TASK_EVIDENCE_CUTOVER=1` on
   the **migration job**. This is a controlled cutover. The migration takes an
   exclusive table lock and rewrites existing rows. The lock wait remains five
   seconds and this migration gets up to five minutes to complete.
3. Verify the successful migration before starting only the new reader and
   classifier. The new schema rejects old sentinel and terminal-row writes.
   Keep prior revisions drained because they still interpret task states and
   classification timestamps using the old rules.
4. Check the first scan, verify that pending classifications recover, and then
   resume the scheduler. Remove the cutover setting from the migration job.

A failed migration rolls back as a unit, including its migration record. If
validation fails after the migration commits, keep the scanner paused. Repair
with schema-compatible code or restore the pre-cutover database before using
the old application. Restoring that backup after new writes loses those writes.

The retry and running indexes support the collector. The existing pool/start
and pool/worker indexes are the starting point for the later history endpoints.
On the local A55 data, a seven-day worker lookup with a left join on
`(pool_id, worker_id)` and a 50-run cap took about 5 ms. Worker discovery
over the same period took about 1.5 ms. Both used the pool/start index.
Benchmark the final cursor queries before adding larger history-specific indexes.

## Retry one unavailable run

```sh
DATABASE_URL=... uv run --frozen python -m worker_health.pool_classifier_web.scripts.retry_task_status \
  --pool provisioner/worker-type --task-id TASK_ID --run-id 0
```

Use `--run-id legacy` for a retained reference with a null run ID. For a local
SQLite database, pass its pool's `--results-dir` instead of `DATABASE_URL`.
The command re-enables the next scanner lookup and preserves the previous
lookup result and all stored Queue evidence. Finished outcomes stay closed.
