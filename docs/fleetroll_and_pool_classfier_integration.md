# Fleetroll and Pool Classifier integration

Fleetroll and Pool Classifier should be complementary operational systems, not
competing sources of truth. Fleetroll manages persistent host state and safe
configuration changes. Pool Classifier measures Taskcluster workload outcomes,
queue health, and capacity pressure.

## Ownership boundary

| Fleetroll owns | Pool Classifier owns |
| --- | --- |
| Is a persistent host reachable, correctly configured, and in its intended rollout state? | Is a Taskcluster worker pool delivering reliable, timely work? |
| Host inventory, expected roles, drift, and lifecycle state | Task outcomes, failure classification, task-source attribution, and systemic diagnosis |
| Puppet/configuration provenance and rollout cohorts | Queue health, utilization, demand, throughput, and pool-level SLOs |
| Safe host actions, staged rollouts, approvals, and audit trails | Analysis of the Taskcluster task population and its dispositions |

Pool Classifier should not independently implement SSH reachability probes,
host inventory/role drift, host mutation, Puppet execution, or rollout state
machines. Those capabilities belong in Fleetroll.

## Why both systems are needed

Fleetroll can explain whether a persistent host is reachable, has applied the
desired Puppet/configuration revision, and is in the expected rollout cohort.
That alone cannot explain whether queued Taskcluster work is starting promptly,
whether tasks are expiring before they start, or whether failures come from a
test, task definition, external service, or a fleet regression.

Pool Classifier can expose those workload and outcome signals. It cannot, on
its own, establish that a Taskcluster-listed wake-on-dispatch device is
physically healthy or correctly configured. Combining the two resolves this
ambiguity.

For example, a queue-SLO breach combined with sufficient Fleetroll-healthy
hosts points to scheduling, routing, or workload demand. The same breach with
a reduced healthy-host count points to fleet remediation or a rollout problem.

## Integration contract

Fleetroll should expose a read-only, versioned interface that Pool Classifier
can consume. It should provide the latest observation and freshness for each
persistent host, with a stable mapping to its Taskcluster worker identity.

Minimum host fields:

- Stable Fleetroll host ID, hostname, and Taskcluster worker ID/group/pool.
- Expected and observed role, enabled/disabled state, and disabled reason.
- Reachability, privilege/probe status, and last successful observation time.
- Configuration and rollout provenance: Puppet SHA, override SHA, image or
  configuration revision, and rollout/cohort ID when applicable.
- Physical/infrastructure attributes where known: site, rack, hardware model,
  operating-system version, and device class.

Pool Classifier should retain its own Taskcluster collection and coverage
semantics. Fleetroll health is an additional physical-host signal; it must not
silently replace Pool Classifier's explicit `recent_contact` or `listed`
availability definitions.

Useful presentation links flow both ways:

- A Pool Classifier worker or pool page links to a Fleetroll filtered host view.
- A Fleetroll host or rollout view links to Pool Classifier outcomes for the
  matching worker, worker pool, and rollout cohort.

## Pool Classifier priorities

### 1. Complete queue-population visibility

Observed scheduled-to-start lag intentionally includes only runs that started
and were later seen as terminal. Add durable Taskcluster Pulse task-status
ingestion plus Queue reconciliation to measure the complete population:
ready-to-start lag, backlog age/depth, expiry and cancellation dispositions,
and work stranded by routing or capability mismatch.

### 2. Capacity and demand intelligence

Combine queue age, arrival rate, completion rate, utilization, and usable
capacity. When Fleetroll data is available, distinguish Taskcluster-listed
capacity from Fleetroll-healthy physical capacity. The resulting answer should
be operational, such as: “demand requires ten healthy workers; eight are
currently healthy.”

### 3. Rollout-aware workload gates

Expose cohort-aware metrics for Fleetroll rollout assessment:

- success-rate change relative to a baseline;
- categorized failure regressions;
- p95 queue lag and SLO attainment;
- expiration/cancellation rate; and
- sample size and collection coverage.

Fleetroll remains responsible for deciding whether to advance a rollout and
for recording that decision. Pool Classifier provides the workload evidence.

### 4. Cohort correlation and task-source diagnosis

Associate outcomes with Fleetroll rollout/configuration cohorts, hardware
attributes, image revisions, and sites. Continue improving task classification
and job-source segmentation so an operator can separate a host/image regression
from a bad test, task definition, or upstream service failure.

### 5. Per-pool SLOs and actionable alerts

Use pool-specific queue and capacity thresholds. Alerts should include the
relevant confidence/coverage, affected workers, Fleetroll-health context, and
links to both systems rather than issuing an unqualified “pool unhealthy”
verdict.

## Phased delivery

1. Define stable host-to-Taskcluster identity mapping and add deep links in
   both products.
2. Publish Fleetroll's read-only host snapshot with freshness and provenance.
3. Add Pool Classifier cohort dimensions and join them to worker/task outcomes.
4. Implement complete Taskcluster queue-population capture and disposition
   reporting.
5. Use Pool Classifier cohort metrics as Fleetroll rollout-gate inputs.

## Current-scope caveats

Fleetroll MVP currently uses operator-run collection with local SQLite. Its
centralized read API, shared PostgreSQL state, and rollout orchestration are
planned architecture rather than capabilities to depend on today. It is also
focused on long-lived Linux and macOS hosts. Pool Classifier must continue to
cover the Taskcluster-wide fleet independently, including Android device pools
and any worker types outside Fleetroll's host coverage, until the integration
proves complete and fresh for those populations.
