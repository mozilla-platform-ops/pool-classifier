# Task-run evidence cutover

Task-run evidence separates the last Queue lookup result, the most recent
matching Queue evidence, and the last completed log classification. Production
had 4,372,467 task-result rows at the November 24, 2026 inventory. Prepare and
backfill while the legacy revision is serving traffic. Activation is a separate
metadata-only transaction.

## Prepare

Use a reviewed image containing both migrations and the maintenance command.
Point the migration and maintenance jobs at that image before executing them.
Take a recoverable backup and check Cloud SQL disk headroom. Allow room for new
row versions, indexes, and WAL. Check free disk and application latency
throughout the backfill. Stop between executions if they approach operating
limits. Autovacuum can make freed space reusable inside PostgreSQL.

Use the following execution arguments for the migration job.

```text
-m,worker_health.pool_classifier_web.scripts.migrate,--prepare-task-evidence
```

Migration 014 adds columns and a trigger. It leaves legacy columns and their
constraints in place. The new columns use null to mark existing rows awaiting
backfill. Inserts and updates from the legacy scanner populate the evidence
columns in the same transaction. Leave the legacy app and scheduler running
during preparation, backfill, and validation.

## Backfill and validate

Use the following execution arguments for the maintenance job.

```text
-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,task-evidence-cutover,--phase,backfill
```

The backfill uses one transaction per batch. The defaults are 1,000 rows per
batch, at most 1,000 batches per execution, and a 50 ms pause between batches.
Use `--batch-size`, `--max-batches`, or `--pause-seconds` to tune the load.
Each invocation reports committed updates and whether pending rows remain.
Rerun until `has_pending_rows` is false. Rows locked by an active scan are
skipped and retried later. An interrupted execution keeps its committed
batches. The command takes an advisory lock to serialize migration jobs.

Legacy completed and failed/exception rows retain their Queue outcomes.
Legacy classified failures keep their category and classification timestamp.
Observed references and synthetic expiry markers keep their observation and
attempt timestamps. Historical successful fetch times remain null. Ambiguous
legacy expiry markers and unrecognized states stop the batch for review.
Inspect the reported pool, task, and run, and correct or resolve the legacy
data before retrying.

Then run the validation phase in the maintenance job.

```text
-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,task-evidence-cutover,--phase,validate
```

This checks for pending rows, builds indexes concurrently, and validates the
evidence constraint with a lock that allows normal reads and writes. The
constraint is installed in a separate short transaction, without scanning
under the exclusive lock. Validation fails if there is a pending or invalid
row. If a concurrent index build is interrupted, rerunning the phase rebuilds
the invalid index.

## Activate and verify

1. Pause the scan scheduler and prevent direct classifier invocations. Drain
   in-flight readers and writers on the old revision before switching schema.
   Review the backup and the rollback plan before taking this step.
2. Run the migration job with
   `-m,worker_health.pool_classifier_web.scripts.migrate,--activate-task-evidence`.
   The runner checks the validated constraint and required indexes before
   asking for an exclusive table lock with `NOWAIT`. If the table is in use,
   the transaction rolls back immediately. Wait for the blocker to drain
   before retrying. Migration 015 only switches columns, defaults, and
   triggers under the lock. It performs no backfill, index build, or table
   scan in that transaction.
3. Verify migration 015 in `schema_migrations`. Start and verify only the new
   revision. Check the first scan, including pending classification recovery,
   then resume scheduling.

Use the normal migration command for a new empty local database. It builds
the indexes and activates automatically. For an existing local database,
start PostgreSQL with `./pc_db.sh up`. Run the same phases directly against
its local DSN using the Python modules listed above. Stop both local web
instances and any active scanner before activation.

## Roll back during the compatibility window

Activation retains the original columns and corresponding indexes. A trigger
keeps a legacy representation of new writes. Pause and drain the new app
before running the rollback phase in the maintenance job.

```text
-m,worker_health.pool_classifier_web.scripts.db_maintenance,--operation,task-evidence-cutover,--phase,rollback
```

Rollback switches the columns and trigger back under a short `NOWAIT` lock
and removes the migration 015 record. Restore the legacy revision after it
succeeds. Detailed evidence stays in the staged columns, although the legacy
app uses its prior outcome and classification semantics. The old scanner
resynchronizes evidence for rows whose legacy values change. A later activation
can reuse the validated constraint and indexes.

Databases that already ran the original single-transaction 014 can record
015 as already active. They have no original columns to switch back to.
Remove the legacy columns and their indexes from staged databases in a
separate reviewed change after the rollback window closes.

## Local rehearsal

A November 24, 2026 rehearsal used an isolated PostgreSQL copy scaled from
1,617,827 local rows to the 4,372,467 production row count. With batches of
2,000 and a 20 ms pause, backfill took 12 minutes 47 seconds across two
executions. Preparation took 27 ms, validation and concurrent index builds
took 9.3 seconds, activation took 31 ms, and rollback took 11 ms. These are
wall-clock timings for each operation. Reactivation kept the same row count.
A sample legacy read completed 2,962 times during the online phases and hit
its 500 ms timeout once.

Sampled PostgreSQL database size rose from 2.23 GiB to a peak of 3.05 GiB and
finished at 2.65 GiB. Those database sizes exclude WAL. The rehearsal matched
production's row count with a smaller starting table and index footprint and
sampled reads. Use the result to budget the online work and check production
disk, WAL and request latency before selecting batch settings.

## Retry one unavailable run

```sh
DATABASE_URL=... uv run --frozen python -m worker_health.pool_classifier_web.scripts.retry_task_status \
  --pool provisioner/worker-type --task-id TASK_ID --run-id 0
```

Use `--run-id legacy` for a retained reference with a null run ID. For a local
SQLite database, pass its pool's `--results-dir` instead of `DATABASE_URL`.
The command re-enables the next scanner lookup and preserves existing Queue
evidence. Finished outcomes stay closed.
