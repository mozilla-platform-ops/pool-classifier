ALTER TABLE task_results ADD COLUMN run_resolved TIMESTAMPTZ;

ALTER TABLE task_results DROP CONSTRAINT task_results_pkey;

CREATE UNIQUE INDEX idx_task_results_task_run
    ON task_results (pool_id, task_id, COALESCE(run_id, -1));
