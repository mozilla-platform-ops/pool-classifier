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
  "availability_mode": "listed",
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

The `availability_mode` field states how the availability denominator is
derived. It is configured per pool:

- `recent_contact` requires `lastDateActive` to be within the recent-contact
  threshold and `quarantineUntil` not to be in the future. This is the default.
- `WORKER_CONTACT_THRESHOLD_SECONDS` configures the threshold and defaults to
  3,600 seconds (60 minutes). Taskcluster updates `lastDateActive` periodically,
  so tune this using observed contact ages for known healthy workers.
- A contact timeout becomes effective at `lastDateActive + threshold`.
- An online or returning worker becomes effective at its new
  `lastDateActive`.
- Quarantine and unquarantine changes become effective when observed.
- Workers that disappear from a listing retain their last known contact time,
  allowing a timeout followed by a later return to be represented correctly.

`listed` is used for wake-on-dispatch pools such as `proj-autophone`. Every
worker returned by a successful, complete Taskcluster `listWorkers` observation
is treated as eligible capacity while it is not quarantined, regardless of
`lastDateActive`. A missing worker becomes unavailable when the complete listing
is observed. Failed listings create coverage gaps and do not remove workers.

Listed availability is not a liveness or health signal. Taskcluster cannot
distinguish a dormant wake-on-dispatch device from a physically dead device
that remains listed. Known-bad devices must be quarantined (or eventually
filtered using an external health source); otherwise they remain in the
denominator and can make utilization look lower than the usable pool really is.

Only state transitions are retained as history; the current observation is an
upserted row per worker. Storage therefore grows with availability changes,
not polling frequency.

When a pool changes modes, its availability state, transition history, and
availability coverage are reset while task-run history is preserved. This
prevents utilization from combining incompatible denominator semantics; API
coverage becomes complete again only after fresh observations under the new
mode.

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
