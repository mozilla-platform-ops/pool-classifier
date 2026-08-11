# Capacity scenarios API

`GET /api/v1/pools/{provisioner}/{worker_type}/capacity-scenarios` estimates
the observed start-lag outcome when healthy hosts are added to a pool. It is a
read-only, counterfactual planning aid for investigating an overloaded pool;
it is not a provisioning command or a complete Taskcluster queue simulation.

The endpoint requires timezone-aware ISO 8601 `start` and `end` parameters.
The interval is start-inclusive and end-exclusive and may be at most 90 days.
`target_p95_seconds` is optional and defaults to 14,400 seconds (four hours).
`additional_hosts` is an optional comma-separated list of non-negative
integers, defaulting to `0,1,2,4,8,12`; each value is capped at 100.
`turnaround_seconds` is an optional non-negative post-run host turnaround
assumption, defaulting to 120 seconds and capped at 1,800 seconds. It is added
to every observed run duration before its modeled host becomes available.

For every requested increment, the result reports modeled p50/p95 start lag,
the share of modeled tasks within the selected target, peak waiting queue
depth, and whether every modeled task starts and the p95 meets the target.
`minimum_additional_hosts_meeting_target` is the first requested increment
that meets the target, refined to the smallest integer host count between the
previous failing displayed increment and that first passing increment. It is
`null` if none of the displayed increments meets the target.
`observed_baseline` reports the actual p50/p95 lag of the replay input, so a
consumer can judge how closely the zero-added-host scenario reproduces the
observed period before relying on other scenarios.
`calibration` makes that difference machine-readable. The initial model is
explicitly `uncalibrated`; its host-count result must not be used as a sizing
recommendation until it has been validated against a known capacity change.

`turnaround_sensitivity` compares a fixed two-minute turnaround with the
per-pool busy-worker-cycle median when at least 30 eligible consecutive worker
cycles are available. A cycle is eligible only if its following observed task
was already scheduled when the prior task resolved. Variants include their own
calibration and host-count results; the distribution is evidence about normal
turnaround, not a model of long reboot or readiness failures.

The model replays task runs in scheduled-time FIFO order using their observed
started-to-resolved duration. Its capacity at each point is the number of
historically observed healthy workers plus the requested additional hosts.
The response includes task-run and worker-availability collection coverage and
a versioned `model` object that documents the scope.

This data is incomplete by design: the collector only observes runs that
started and subsequently became terminal. Tasks that never started, expired,
or were cancelled before starting are absent. The model also does not account
for routing/capability constraints, retries, host wake-up time, or changes in
task duration under a different load. Treat its results as a conservative
starting point for an upsize investigation, and do not use them for a
downsize decision.
