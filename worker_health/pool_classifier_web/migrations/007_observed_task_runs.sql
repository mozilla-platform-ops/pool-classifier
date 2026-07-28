ALTER TABLE task_results ADD COLUMN observed_at TIMESTAMPTZ;
ALTER TABLE task_results ADD COLUMN last_checked_at TIMESTAMPTZ;

UPDATE task_results
SET observed_at = classified_at,
    last_checked_at = classified_at
WHERE observed_at IS NULL OR last_checked_at IS NULL;

CREATE INDEX idx_task_results_unresolved
    ON task_results (pool_id, observed_at)
    WHERE run_state NOT IN ('completed', 'failed', 'exception', 'expired');
