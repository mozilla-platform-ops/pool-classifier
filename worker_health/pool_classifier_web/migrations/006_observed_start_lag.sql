ALTER TABLE task_results ADD COLUMN reason_created TEXT;
ALTER TABLE task_results ADD COLUMN run_scheduled TIMESTAMPTZ;

CREATE INDEX idx_task_results_scheduled ON task_results (pool_id, run_scheduled);
