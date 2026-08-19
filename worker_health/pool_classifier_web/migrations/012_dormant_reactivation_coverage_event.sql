ALTER TABLE task_run_coverage_events
    DROP CONSTRAINT task_run_coverage_events_reason_check;

ALTER TABLE task_run_coverage_events
    ADD CONSTRAINT task_run_coverage_events_reason_check
    CHECK (reason IN ('get_worker_error', 'recent_tasks_no_overlap', 'incomplete_poll', 'dormant_reactivation'));
