# Backfilling observed start-lag metadata

Existing terminal task-run rows created before observed start lag was added do
not have the Queue `scheduled` timestamp. Run the explicit backfill command to
enrich a bounded batch:

```bash
python pool_classifier.py --backfill-start-lag \
  --provisioner proj-autophone --worker-type gecko-t-bitbar-gw-perf-a51 \
  --results-dir pool_classifier_results \
  --backfill-batch-size 500 --backfill-concurrency 5 \
  --backfill-requests-per-second 5
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
