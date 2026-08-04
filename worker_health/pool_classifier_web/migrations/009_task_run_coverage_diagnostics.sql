CREATE TABLE worker_recent_task_windows (
    pool_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_group TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    recent_tasks JSONB NOT NULL,
    PRIMARY KEY (pool_id, worker_id)
);

CREATE TABLE task_run_coverage_events (
    id BIGSERIAL PRIMARY KEY,
    pool_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('get_worker_error', 'recent_tasks_no_overlap', 'incomplete_poll')),
    worker_id TEXT,
    worker_group TEXT,
    previous_observed_at TIMESTAMPTZ,
    previous_recent_tasks JSONB,
    current_recent_tasks JSONB,
    previous_window_count INTEGER,
    current_window_count INTEGER,
    overlap_count INTEGER,
    error_type TEXT
);

CREATE INDEX idx_task_run_coverage_events_pool_time
    ON task_run_coverage_events (pool_id, observed_at DESC);
