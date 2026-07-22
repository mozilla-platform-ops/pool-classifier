# Utilization API

The utilization API returns duration-weighted pool usage for rightsizing:

```http
GET /api/v1/pools/{provisioner}/{worker_type}/utilization
    ?start=2026-07-21T10:00:00Z
    &end=2026-07-21T12:00:00Z
    &bucket_seconds=3600
```

`start` is inclusive and `end` is exclusive. Both must be ISO 8601 timestamps
with a timezone. `bucket_seconds` must be a positive integer no larger than 90
days. A request may span at most 90 days and return at most 2,000 buckets.
Buckets start at the requested `start`; the final bucket is shorter when the
range is not evenly divisible.

Unknown pools return `404`. Invalid parameters return `400`:

```json
{
  "error": {
    "code": "invalid_parameter",
    "message": "bucket_seconds must be greater than zero"
  }
}
```

## Response

```json
{
  "api_version": 1,
  "pool_id": "proj-autophone/gecko-t-lambda-perf-a55",
  "start_at": "2026-07-21T10:00:00+00:00",
  "end_at": "2026-07-21T12:00:00+00:00",
  "bucket_seconds": 3600,
  "collection_started": "2026-07-21T09:00:00+00:00",
  "coverage_pct": 100.0,
  "complete": true,
  "buckets": [
    {
      "start_at": "2026-07-21T10:00:00+00:00",
      "end_at": "2026-07-21T11:00:00+00:00",
      "coverage_pct": 100.0,
      "complete": true,
      "status": "available",
      "busy_worker_hours": 2.0,
      "available_worker_hours": 11.0,
      "worker_equivalents": 2.0,
      "utilization_pct": 18.181818181818183
    }
  ]
}
```

Timestamps are normalized to UTC. Top-level coverage describes the selected
range; each bucket carries its own coverage and status.

## Formulas and units

For each request or bucket boundary, task and availability intervals are
clipped before their durations are calculated.

- `busy_worker_seconds` is the sum of clipped terminal task-run intervals.
  Overlapping task runs are additive because each run contributes its actual
  execution duration.
- `available_worker_seconds` is the time integral of available,
  non-quarantined workers. Eleven available workers for one hour contribute 11
  worker-hours.
- `busy_worker_hours = busy_worker_seconds / 3600`.
- `available_worker_hours = available_worker_seconds / 3600`.
- `worker_equivalents = busy_worker_seconds / bucket_duration_seconds`. This is
  the average number of concurrently busy workers during the bucket.
- `utilization_pct = busy_worker_seconds / available_worker_seconds * 100`.

A complete bucket with no available worker time has `status: "unavailable"`
and `utilization_pct: null`; this avoids dividing by zero. Its other duration
metrics remain numeric. An incomplete bucket has `status: "incomplete"` and all
four utilization metrics are `null`, so partial collection is not presented as
authoritative rightsizing data.

## Availability semantics

Availability comes from Taskcluster Queue worker contact and quarantine data,
not configured capacity or the number of retained worker records.

- A worker is available when `lastDateActive` is within the recent-contact
  threshold and `quarantineUntil` is not in the future.
- `WORKER_CONTACT_THRESHOLD_SECONDS` configures the threshold and defaults to
  3,600 seconds (60 minutes). Taskcluster updates `lastDateActive` periodically,
  so tune this using observed contact ages for known healthy workers.
- A contact timeout becomes effective at `lastDateActive + threshold`.
- An online or returning worker becomes effective at its new
  `lastDateActive`.
- Quarantine and unquarantine changes become effective when observed.
- Workers that disappear from a listing retain their last known contact time,
  allowing a timeout followed by a later return to be represented correctly.

Only state transitions are retained as history; the current observation is an
upserted row per worker. Storage therefore grows with availability changes,
not polling frequency.

## Coverage semantics

Task-run and worker-availability collection are tracked independently. API
coverage is their intersection: a period is complete only when both inputs are
continuous. `collection_started` is the later of their first successful
observations.

Successful observations within `COLLECTION_COVERAGE_MAX_GAP_SECONDS` coalesce
into one interval. The default is twice the classifier poll interval. A failed
poll, a partial recent-task scan, or a longer process outage starts a coverage
gap. Startup incompleteness disappears naturally once the requested range is
entirely inside continuous coverage.

## Utilization versus throughput

Job counts and jobs per active worker (tracked separately by
`pool-classifier-49f`) are throughput metrics. They answer how many tasks were
completed, but not how long workers were busy. A pool can process many short
jobs or a few long jobs with the same count, so throughput complements rather
than replaces duration-weighted utilization.
