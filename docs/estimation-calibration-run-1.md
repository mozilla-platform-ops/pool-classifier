# Estimation calibration run 1: move 10 Talos hosts from 1804 to 2404

## Purpose

Validate the capacity-scenarios model against a controlled, steady-state host
transfer. This run measures two independent effects:

- Removing 10 healthy hosts from `gecko-t-linux-talos-1804`.
- Adding 10 healthy hosts to `gecko-t-linux-talos-2404` after imaging and
  registration complete.

The imaging interval is intentionally not part of either estimate: the model
represents healthy, ready capacity and does not model reimage or readiness
time.

## Frozen pre-change prediction

Window: `2026-08-10T18:34:28Z` to `2026-08-17T18:34:28Z`.

| Pool | Host delta | Modeled p95 lag | Observed baseline p95 | 1-hour target | Coverage |
| --- | ---: | ---: | ---: | --- | --- |
| `gecko-t-linux-talos-1804` | -10 | 0s | 0.182s | pass | task runs 100.0%; availability 88.1% |
| `gecko-t-linux-talos-2404` | +10 | 1h 47m 10s | 2h 20m 49s | fail | task runs 88.1%; availability 87.8% |

Both primary estimates use the configured 120-second turnaround assumption.
2404's observed busy-worker-cycle median is about 125 seconds and produces a
very similar +10 prediction (1h 47m 38s). 1804 has no eligible busy-worker
cycles, so it has no observed turnaround alternative.

The model is uncalibrated. It excludes tasks that never started, and the
availability coverage is incomplete. Treat these predictions as a recorded
hypothesis, not a guarantee.

## Host inventory

- Host IDs:
  - `t-linux64-ms-229.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-230.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-231.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-232.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-233.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-234.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-235.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-236.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-237.test.releng.mdc1.mozilla.com`
  - `t-linux64-ms-238.test.releng.mdc1.mozilla.com`
- Operator: <!-- fill -->
- Change reference: <!-- fill -->

## Procedure

1. [x] Before changing capacity, save the source and target summary,
   observed-start-lag, and capacity-scenarios responses. Record the exact
   query windows used. Completed `2026-08-17T21:12:13Z`; the frozen forecast
   window is recorded above.
2. Quarantine all ten selected hosts in 1804 to stop new work. Record both the
   operator action time and the time Pool Classifier first observes all ten as
   quarantined.
3. Let any active work drain after quarantine. The hosts should be empty within
   one hour. Record the drain-complete time; begin the steady-state 1804
   observation window only after both the quarantine transition and drain are
   complete.
4. Keep the hosts quarantined/offline while they are reimaged. Do not include
   this interval when evaluating either steady-state estimate.
5. Register the hosts in 2404. Record each ready time and the time Pool
   Classifier first observes all ten as healthy, available 2404 workers. This
   is the 2404 capacity-addition event.
6. Monitor observed p95 lag and worker availability after each event. Use an
   early 24-hour safety check, then a seven-day primary comparison once the
   relevant pool has been stable for the whole window.
7. Compare observed p95 with the frozen prediction, record the error, and
   record whether the one-hour SLO classification was correct.

## Event log

| Event | UTC time | Notes |
| --- | --- | --- |
| Pre-change API snapshots captured | 2026-08-17T21:12:13Z | Forecast window and model outputs recorded above. |
| All 10 hosts quarantined in 1804 | 2026-08-17T21:55:00Z | Operator action time. |
| Classifier observed 10 quarantined in 1804 | <!-- fill --> | |
| All 10 hosts drained in 1804 | 2026-08-17T21:57:05Z | Operator confirmed all selected hosts idle. |
| Imaging started | <!-- fill --> | |
| Imaging completed | <!-- fill --> | |
| All 10 hosts ready in 2404 | 2026-08-18T16:48:29Z | Confirmed online in Taskcluster by this time; individual ready times were not captured. |
| Classifier observed 10 available in 2404 | 2026-08-18T16:48:29Z | All hosts `229`–`238` listed on the 2404 pool page generated at this time. |

## Results

### 24-hour safety check

| Pool | Window | Observed p95 | Within 1 hour? | Coverage | Notes |
| --- | --- | ---: | --- | --- | --- |
| 1804 | <!-- fill --> | | | | |
| 2404 | <!-- fill --> | | | | |

### Seven-day calibration comparison

| Pool | Forecast p95 | Observed p95 | Error (observed - forecast) | Correct 1-hour classification? | Task / availability coverage | Notes |
| --- | ---: | ---: | ---: | --- | --- | --- |
| 1804 (-10) | 0s | <!-- fill --> | <!-- fill --> | <!-- fill --> | <!-- fill --> | |
| 2404 (+10) | 1h 47m 10s | <!-- fill --> | <!-- fill --> | <!-- fill --> | <!-- fill --> | |

## Design lessons for future runs

This one-way host transfer cannot produce independent seven-day steady-state
measurements for both its source and target pools. Once imaging begins, the
1804 removal window ends; once the hosts are registered in 2404, 1804 no
longer has the planned capacity state. Therefore this run can calibrate the
2404 `+10` estimate, but it cannot provide the planned seven-day calibration
of the 1804 `-10` estimate.

Future capacity-change calibrations must use separate, stable experiments for
each signed delta:

1. For a removal estimate, quarantine or otherwise remove the selected healthy
   hosts and hold the source pool at that reduced capacity for the complete
   observation window before restoring or transferring them.
2. For an addition estimate, add healthy, available hosts to the target pool
   and hold the target pool at the increased capacity for the complete
   observation window.
3. Do not combine the source-removal and target-addition measurements in a
   one-way transfer unless both required steady-state windows can actually be
   completed. Treat imaging and registration as excluded transition time.
4. Before any capacity action, name the single signed delta being calibrated,
   its event timestamp, its 24-hour and seven-day checkpoint timestamps, and
   the rollback or next-transition time. Do not begin the next transition
   before the required window closes.
5. Record the operator action, the first Classifier observation, and the
   source API responses at each checkpoint. If only a later confirmation is
   available, label it as a conservative "confirmed by" time rather than the
   actual transition time.
6. Archive a reproducibility bundle for the baseline and every checkpoint:
   the full request (endpoint, query parameters, headers or request body as
   applicable), fetch timestamp, and unmodified raw response. The summary
   values copied into this document are not sufficient to reproduce or audit a
   result later. Store the artifact location in the event log.

## Outcome

This run is a 2404 `+10` calibration only. Its 24-hour and seven-day results
remain pending. The 1804 `-10` estimate requires a separate controlled,
seven-day removal run before it can be calibrated.
