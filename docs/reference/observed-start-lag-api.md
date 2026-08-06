# Observed scheduled-to-start lag API

`GET /api/v1/pools/{provisioner}/{worker_type}/observed-start-lag` accepts ISO
8601 `start` and `end` query parameters and an optional positive `slo_seconds`
parameter. If omitted, the SLO is configurable with
`OBSERVED_START_LAG_SLO_SECONDS` (default: 14,400 seconds / 4 hours). This
fleet-wide default is calibrated to the observed 95th-percentile lag; it is
intended as a meaningful SLO line, not a normal-case target.

The result reports a bounded sample count, nearest-rank p50/p95, and the share
of samples that started within the selected SLO. A sample is included only when
the raw Queue `runs[].scheduled` and `runs[].started` timestamps are both
present, ordered, and `scheduled` falls within the requested window.

This is **observed scheduled-to-start lag**, not a total queued-task count,
drop/expiry rate, or pool-health verdict. It includes only task runs that
started and were later observed terminal by per-worker polling; jobs that never
ran are invisible. `scheduled` is retained as the raw Queue scheduled timestamp
and is not relabeled as a general task-readiness timestamp.

`GET /api/v1/pools/{provisioner}/{worker_type}/observed-start-lag/visualization`
uses the same window and SLO parameters and adds `min_samples` (default: 5).
It returns hourly p50/p95 and sample-count buckets plus UTC weekday/hour p95
cells. Cells below `min_samples` are marked insufficient rather than colored
as a reliable percentile; the pool dashboard uses this endpoint for its chart
and heatmap.
