# Backfilling observed start-lag metadata

Existing terminal task-run rows created before observed start lag was added do
not have the Queue `scheduled` timestamp. Run the explicit backfill command to
enrich a bounded batch:

```bash
python pool_classifier.py --backfill-start-lag \
  --provisioner proj-autophone --worker-type gecko-t-bitbar-gw-perf-a51 \
  --results-dir pool_classifier_results \
  --backfill-batch-size 500 --backfill-concurrency 5 \
  --backfill-requests-per-second 5 \
  --backfill-state-file .backfill-start-lag-state.json
```

For the deployed Postgres store, set `DATABASE_URL` (or pass
`--database-url`); the command will apply any pending migrations and use the
same per-pool advisory lock as Cloud Run classification.

The command shares the per-pool classifier lock, fetches Queue status once per
unique task, and changes only `run_scheduled` and `reason_created`. It retries
transient failures with exponential backoff and reports enriched runs, expired
Queue status documents, unmatched runs, and exhausted transient failures.

It selects the newest rows still missing `run_scheduled`; successful enrichment
removes those rows from later batches, so it is safe to rerun after an
interruption. Expired or unmatched Queue status documents are reported but are
not reconstructed: this backfill only covers terminal runs already stored by
the classifier.

The state file records Queue 404 task IDs and runs that lack a Queue
`scheduled` timestamp. Later invocations skip those known-unavailable records
and page past them to reach older rows. Delete the file (or choose another path
with `--backfill-state-file`) to retry them.

## Every Postgres pool

To drain the eligible backlog for every pool already stored in Postgres, use
the repository script. It discovers only pools with at least one run missing
`run_scheduled`, keeps separate retry state for each pool, and repeats bounded
batches until each pool is drained.

```bash
DATABASE_URL=postgresql://pc:pc@127.0.0.1:5433/pool_classifier \
uv run --frozen python scripts/backfill_start_lag_all_pools.py
```

Use `--batch-size`, `--concurrency`, and `--requests-per-second` to tune the
Queue request load. The script exits nonzero if a pool has exhausted transient
Queue retries or has an active classifier cycle; rerun it to retry that pool.

Press Ctrl-C once to stop after the current pool batch has finished and its
database updates and state file are durable. The script exits with status 130;
press Ctrl-C a second time to abort immediately.
