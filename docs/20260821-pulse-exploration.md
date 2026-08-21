# Taskcluster Pulse task-lifecycle exploration

**Beads:** `pool-classifier-xs9`
**Status:** preliminary investigation; no consumer implementation is proposed
here.

## Problem being investigated

Pool Classifier's current terminal-run collector begins with each worker's
`recentTasks` list.  It therefore has no record of work that is eligible for a
pool but is never claimed by a worker.  The immediate gap is a task that stays
pending for Taskcluster's 24-hour service window and then reaches a terminal
expiry disposition.  Such a task must be included in the pool's workload and
disposition metrics; it must not simply vanish.

This is different from a **Queue API record expiry**: the existing collector
may see a previously observed task's Queue status become 404 after Taskcluster
has retained the record for its configured lifetime.  Pool Classifier already
records that as `run_state = expired` to stop futile retries.  That is a data
availability condition, not evidence that the task was unserviced for 24
hours.  Any Pulse design must keep these two concepts separate:

| Concept | Evidence | Proposed representation |
| --- | --- | --- |
| Unserviced task expiry | Queue lifecycle event/status showing a terminal disposition without a run start | `terminal_disposition` and its Taskcluster reason/state |
| Queue record no longer retained | Queue status request returns 404 for an already-observed task/run | `record_unavailable` / existing retry-stop state; never count as workload expiry |

## Candidate Taskcluster Pulse subscriptions

Taskcluster Queue publishes status-transition messages on its versioned Queue
Pulse exchanges.  The current `taskcluster` Python client's generated
`QueueEvents` definitions establish this starting set:

| Exchange | Why subscribe | Pool Classifier use |
| --- | --- | --- |
| `exchange/taskcluster-queue/v1/task-defined` | Task enters Queue | Establish the task universe and creation metadata. |
| `exchange/taskcluster-queue/v1/task-pending` | Task becomes schedulable/pending | Candidate start of ready-to-start wait; validate exact Queue semantics before using it as the SLO clock. |
| `exchange/taskcluster-queue/v1/task-running` | A run is claimed/started | Associate task/run with worker group and worker; close pending wait. |
| `exchange/taskcluster-queue/v1/task-completed` | Terminal success | Close the run and reconcile with `recentTasks`. |
| `exchange/taskcluster-queue/v1/task-failed` | Terminal task failure | Close the run and record disposition. |
| `exchange/taskcluster-queue/v1/task-exception` | Exceptional terminal outcome | This includes a task that was not completed by its deadline, as well as cancellation, exhausted retries, and malformed payload. Treeherder treats a run whose `reasonCreated == "exception"` as deadline-exceeded; confirm that invariant on a live sample and preserve both creation and resolution reasons. |

Each task exchange uses the same primary routing key:

```text
primary.<taskId>.<runId-or-_>.<workerGroup-or-_>.<workerId-or-_>.<provisionerId>.<workerType>.<schedulerId>.<taskGroupId>.#
```

This means a pool-specific AMQP binding can be exact without losing tasks that
have not yet been assigned a worker:

```text
primary.*.*.*.*.<provisionerId>.<workerType>.#
```

Task-specific `task.routes` can add a separate `route.*` routing key, but do
not replace this `primary.*` key.  The initial live experiment should bind the
six exchanges above using that pool-specific pattern.  It should also retain
payload-side validation, because the live deployment and schema version remain
the authority.

Historic examples show the message payload contains a Queue status snapshot,
`runId`, `workerGroup`, `workerId`, and a message version.  The payload should
be treated as a state observation, not as a complete ordered event log.

## Pulse operating model and implications

Pulse is Mozilla's TLS AMQP 0-9-1 service.  A consumer receives a credential
from PulseGuardian and connects with the client ID as AMQP username and its
secret access token as the password.  Consumer queues are named
`queue/<clientId>/<name>`; queues can be durable or auto-delete, must be
bounded, and should set a prefetch limit.  The service guidance is to aim for
at-least-once delivery, so duplicates and redelivery are normal.  An unattended
or unbounded queue can be warned about or deleted. [Mozilla Pulse
documentation](https://wiki.mozilla.org/Auto-tools/Projects/Pulse)

Consequences for this project:

- A durable production queue needs a documented maximum length/age, alerting,
  and a recovery procedure for its retained window.
- ACK only after the message and its idempotency key are committed.  A crash
  before ACK deliberately permits redelivery.
- Pulse is live delivery, not an archival replay API.  Rebuilding a missed
  interval requires Queue/API reconciliation or a separate retained event
  store, not merely reconnecting a consumer.
- A consumer credential has read access but not `configure` permission on
  Taskcluster-owned exchanges.  Construct Queue exchanges as passive (the
  Treeherder pattern) so their declaration verifies existence without trying
  to configure them; declare and bind only the consumer-owned queue.  An
  ordinary exchange declaration receives AMQP 403.
- MozillaPulse is not a candidate production dependency: PyPI's newest 1.3
  release is from June 2017.  Evaluate a maintained AMQP 0-9-1 library and the
  current Taskcluster schema/client support instead. [MozillaPulse on
  PyPI](https://pypi.org/project/MozillaPulse/)

### Existing Mozilla consumer reference

Treeherder is a useful current reference in the adjacent checkout.  It binds a
durable `queue/<clientId>/<suffix>` to all six Queue task exchanges, uses
`kombu==5.6.1` and `ConsumerMixin`, passes each received message to durable
application work, and only then ACKs it.  It also calls PulseGuardian's queue
bindings endpoint to prune stale bindings.  Pool Classifier should reuse the
maintained-Kombu approach, not the 2017 MozillaPulse wrapper; it must improve
on Treeherder's broad binding by using the pool-specific primary key above and
must commit its ledger before ACK.

MozillaPulse did successfully perform a read-only TLS authentication handshake
against `pulse.mozilla.org:5671` with the `relops-pool-classifier` credential
on 2026-08-21.  That verifies the credential and broker path, but does not
change the production-library recommendation.

The same day, a 12-second Kombu probe created an exclusive, auto-delete queue
with a 100-message bound and bound all six exchanges using
`primary.*.*.*.*.releng-hardware.gecko-t-linux-talos-2404.#`.  Every passive
exchange check and queue binding succeeded; no messages arrived in that short
window.  This validates the pool-specific binding and permissions, but is not
a volume measurement and provides no deadline-expiry sample.

## Minimum validation experiment

1. Create a temporary auto-delete queue under
   `queue/relops-pool-classifier/…`, with a small prefetch and an explicit
   expiry/length bound.
2. Bind each candidate Queue exchange and record a short, sanitized sample of
   routing keys and message keys.  Do not commit task payloads unless reviewed:
   Pulse payloads are public but can still contain operational identifiers.
3. For a known configured pool, compare Pulse counts and task/run identities
   for a 30–60 minute window against Queue status and the current
   `recentTasks` collector.
4. Specifically find a terminal event for a task that never ran, and establish
   its Queue state and `reasonResolved`.  This is the acceptance condition for
   the 24-hour-expiry gap.
5. Measure delivery lag, duplicate rate, out-of-order transitions, queue depth,
   and unmatched Pulse/current-collector records.

## Direction if the experiment succeeds

Pulse should initially be a **complementary task-universe and reconciliation
feed**, not the immediate replacement for per-worker `recentTasks` polling.
Persist a durable event ledger keyed by a stable message/event identity when
available, with a fallback content identity covering task ID, run ID, observed
state, lifecycle timestamp, and source exchange.  Maintain one task record and
one run record per `(task_id, run_id)`; state transitions must be monotonic by
their Queue lifecycle timestamp, while retaining out-of-order observations for
diagnosis.  Reconcile non-terminal records periodically with Queue and retain
the existing worker collector as the correctness check until coverage proves
otherwise.

Success metrics for a staged rollout are: task-universe coverage, unserviced
terminal-disposition coverage, Pulse-vs-polling run agreement, delivery lag,
duplicate/redelivery rate, reconciliation repair rate, and consumer queue
depth/age.  Only measured coverage can justify changing the current collector's
role.

## Open questions

- Is the live unserviced-expiry invariant exactly `task-exception` plus
  `reasonCreated == "exception"`, and does that final run have no `started`
  timestamp?  Record `reasonResolved` too; do not reduce the event to one
  historical interpretation.
- Does `task-pending` accurately mean eligible for this worker pool, or must
  the ready clock be derived from another Queue field?
- What live routing-key filters provide the required pool selectivity without
  losing a task whose worker is never assigned?
- What production secret and queue ownership/alerting model fits Cloud Run?
- What is the actual per-pool and all-Queue message volume under the broad
  bindings needed for task-universe capture?
