ALTER TABLE task_results ADD COLUMN observed_at TIMESTAMPTZ;
ALTER TABLE task_results ADD COLUMN last_checked_at TIMESTAMPTZ;
