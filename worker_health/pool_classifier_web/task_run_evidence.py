"""Task-run write transitions shared by SQLite and PostgreSQL."""

import logging

from worker_health.pool_classifier_web.job_sources import SourceMethod


logger = logging.getLogger(__name__)
TERMINAL_STATES = {"completed", "failed", "exception"}
QUEUE_STATES = TERMINAL_STATES | {"pending", "running", "unscheduled"}


class TaskRunEvidence:
    """Subclasses scope statements to their pool and own the scan transaction."""

    def record_task_result(
        self, task_id, worker_id, run_id, run_state, category, reason_resolved,
        run_started, run_resolved, classified_at, run_scheduled=None, reason_created=None,
    ):
        """Import an already processed result without counting it again."""
        self.record_observed_task_run(task_id, worker_id, run_id, classified_at)
        self.record_task_run_check(task_id, run_id, classified_at)
        self.record_queue_evidence(task_id, run_id, "available", None, {
            "state": run_state, "workerId": worker_id, "started": run_started,
            "resolved": run_resolved, "scheduled": run_scheduled,
            "reasonCreated": reason_created, "reasonResolved": reason_resolved,
        }, count_outcome=False)
        if category and run_state in {"failed", "exception"}:
            self.complete_task_classification(task_id, run_id, category, classified_at)

    def record_observed_task_run(self, task_id, worker_id, run_id, observed_at):
        self._evidence_sql(
            "INSERT INTO task_results ({insert_column}task_id, worker_id, run_id, observed_at)"
            " VALUES ({insert_value}?,?,?,?) ON CONFLICT DO NOTHING",
            (task_id, worker_id, run_id, observed_at),
        )

    def record_task_run_check(self, task_id, run_id, checked_at):
        return bool(self._evidence_sql(
            "UPDATE task_results SET last_checked_at = ? WHERE {scope}task_id = ?"
            " AND COALESCE(run_id, -1) = COALESCE(?, -1) AND status_retry_enabled",
            (checked_at, task_id, run_id),
        ))

    def list_unresolved_task_runs(self, limit):
        return self._evidence_sql(
            "SELECT task_id, worker_id, run_id, run_state, observed_at, last_checked_at FROM task_results"
            " WHERE {scope}status_retry_enabled ORDER BY last_checked_at NULLS FIRST, observed_at, task_id, run_id LIMIT ?",
            (limit,), read=True,
        )

    def expire_task_run(self, task_id, run_id, checked_at):
        self.record_task_run_check(task_id, run_id, checked_at)
        return self.record_queue_evidence(task_id, run_id, "not_found", None)

    def retry_task_status(self, task_id, run_id):
        return bool(self._evidence_sql(
            "UPDATE task_results SET status_retry_enabled = true WHERE {scope}task_id = ?"
            " AND COALESCE(run_id, -1) = COALESCE(?, -1) AND NOT status_retry_enabled"
            " AND status_fetch_state = 'not_found' AND classification_state = 'unknown'",
            (task_id, run_id),
        ))

    def record_queue_evidence(self, task_id, run_id, result, fetched_at, run=None, worker_group=None, *, count_outcome=True):
        """Apply one lookup result. Outcomes and counter updates share a transaction."""
        identity = (task_id, run_id)
        if result != "available":
            return bool(self._evidence_sql(
                "UPDATE task_results SET status_fetch_state = ?, status_retry_enabled ="
                " CASE WHEN ? = 'not_found' THEN false ELSE status_retry_enabled END"
                " WHERE {scope}task_id = ? AND COALESCE(run_id, -1) = COALESCE(?, -1)",
                (result, result, *identity),
            ))

        state = run["state"]
        rows = self._evidence_sql(
            "SELECT run_state, status_retry_enabled, worker_id, run_started FROM task_results WHERE {scope}task_id = ?"
            " AND COALESCE(run_id, -1) = COALESCE(?, -1){row_lock}",
            identity, read=True,
        )
        if not rows:
            return False
        previous = rows[0]
        old_state = previous["run_state"]
        if previous["worker_id"] != run["workerId"]:
            self.record_queue_evidence(task_id, run_id, "worker_mismatch", None)
            return False

        terminal = state in TERMINAL_STATES
        if old_state in TERMINAL_STATES:
            if terminal and old_state != state:
                logger.warning("Conflicting Queue outcomes for %s/%s: kept %s, received %s", task_id, run_id, old_state, state)
                run = {}
            self._evidence_sql(
                "UPDATE task_results SET status_fetch_state = 'available', status_fetched_at = COALESCE(?, status_fetched_at),"
                " run_started = COALESCE(run_started, ?), run_resolved = COALESCE(run_resolved, ?),"
                " run_scheduled = COALESCE(run_scheduled, ?), reason_created = COALESCE(reason_created, ?),"
                " reason_resolved = COALESCE(reason_resolved, ?)"
                " WHERE {scope}task_id = ? AND COALESCE(run_id, -1) = COALESCE(?, -1)",
                (fetched_at, run.get("started"), run.get("resolved"), run.get("scheduled"),
                 run.get("reasonCreated"), run.get("reasonResolved"), *identity),
            )
            return False

        if not previous["status_retry_enabled"]:
            return False
        classification = "not_applicable" if state == "completed" else "pending" if terminal else "unknown"
        self._evidence_sql(
            "UPDATE task_results SET status_fetch_state = 'available', status_fetched_at = COALESCE(?, status_fetched_at),"
            " run_state = ?, classification_state = ?, status_retry_enabled = ?,"
            " run_started = COALESCE(run_started, ?), run_resolved = COALESCE(run_resolved, ?),"
            " run_scheduled = COALESCE(run_scheduled, ?), reason_created = COALESCE(reason_created, ?),"
            " reason_resolved = COALESCE(reason_resolved, ?)"
            " WHERE {scope}task_id = ? AND COALESCE(run_id, -1) = COALESCE(?, -1)",
            (fetched_at, state, classification, not terminal, run.get("started"), run.get("resolved"),
             run.get("scheduled"), run.get("reasonCreated"), run.get("reasonResolved"), *identity),
        )
        if count_outcome and fetched_at is not None and (terminal or (state == "running" and old_state != "running")):
            self._evidence_sql(
                "INSERT INTO task_sources (task_id, source, source_method, fetched_at, name_retry_enabled)"
                " VALUES (?, 'unknown', ?, ?, true) ON CONFLICT (task_id) DO NOTHING",
                (task_id, int(SourceMethod.TASK_FETCH_FAILED), fetched_at),
            )
        if terminal and count_outcome:
            worker = previous["worker_id"]
            started = previous["run_started"] or run.get("started")
            self.upsert_worker(worker, worker_group)
            if state == "completed":
                self.increment_success(worker, started)
            else:
                self.increment_failure(worker, started, None)
        return terminal

    def list_pending_classifications(self, limit):
        return self._evidence_sql(
            "SELECT task_id, run_id, worker_id, run_state, run_started, run_resolved, reason_resolved,"
            " run_scheduled, reason_created FROM task_results WHERE {scope}classification_state = 'pending'"
            " ORDER BY classification_last_attempt_at NULLS FIRST, task_id, run_id LIMIT ?",
            (limit,), read=True,
        )

    def defer_task_classification(self, task_id, run_id, attempted_at):
        self._evidence_sql(
            "UPDATE task_results SET classification_last_attempt_at = ? WHERE {scope}task_id = ?"
            " AND COALESCE(run_id, -1) = COALESCE(?, -1) AND classification_state = 'pending'",
            (attempted_at, task_id, run_id),
        )

    def complete_task_classification(self, task_id, run_id, category, classified_at):
        return bool(self._evidence_sql(
            "UPDATE task_results SET category = ?, classified_at = ?, classification_state = 'classified'"
            " WHERE {scope}task_id = ? AND COALESCE(run_id, -1) = COALESCE(?, -1)"
            " AND classification_state = 'pending'",
            (category, classified_at, task_id, run_id),
        ))

    def replace_task_classification(self, task_id, run_id, category, classified_at):
        return bool(self._evidence_sql(
            "UPDATE task_results SET category = ?, classified_at = ? WHERE {scope}task_id = ?"
            " AND COALESCE(run_id, -1) = COALESCE(?, -1) AND classification_state = 'classified'",
            (category, classified_at, task_id, run_id),
        ))

    def has_task_metadata(self, task_id):
        return bool(self._evidence_sql(
            "SELECT 1 AS cached FROM task_sources WHERE task_id = ? AND NOT name_retry_enabled",
            (task_id,), read=True,
        ))

    def list_task_name_candidates(self, limit):
        retry = self._evidence_sql(
            "SELECT s.task_id FROM task_sources s WHERE s.name_retry_enabled AND EXISTS"
            " (SELECT 1 FROM task_results r WHERE {scope}r.task_id = s.task_id)"
            " ORDER BY s.fetched_at, s.task_id LIMIT ?",
            (limit,), read=True,
        )
        active = self._evidence_sql(
            "SELECT DISTINCT r.task_id FROM task_results r WHERE {scope}r.run_state = 'running'"
            " AND NOT EXISTS (SELECT 1 FROM task_sources s WHERE r.task_id = s.task_id)"
            " ORDER BY r.task_id LIMIT ?",
            (limit - len(retry),), read=True,
        )
        return active + retry
