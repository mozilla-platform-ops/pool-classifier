CREATE TABLE workers (
    pool_id TEXT NOT NULL, worker_id TEXT NOT NULL, worker_group TEXT,
    successes INT NOT NULL DEFAULT 0, failures INT NOT NULL DEFAULT 0,
    consecutive_failures INT NOT NULL DEFAULT 0,
    last_active TIMESTAMPTZ, last_success TIMESTAMPTZ, last_failure TIMESTAMPTZ,
    last_failure_category TEXT,
    PRIMARY KEY (pool_id, worker_id)
);
CREATE TABLE task_results (
    pool_id TEXT NOT NULL, task_id TEXT NOT NULL, worker_id TEXT NOT NULL,
    run_id INT, run_state TEXT NOT NULL, category TEXT, reason_resolved TEXT,
    run_started TIMESTAMPTZ, classified_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pool_id, task_id, worker_id)
);
CREATE INDEX idx_task_results_worker  ON task_results (pool_id, worker_id);
CREATE INDEX idx_task_results_started ON task_results (pool_id, run_started);
CREATE INDEX idx_task_results_cat     ON task_results (pool_id, category);
CREATE TABLE quarantine_cache (
    pool_id TEXT NOT NULL, worker_id TEXT NOT NULL,
    quarantine_until TIMESTAMPTZ NOT NULL, reason TEXT,
    set_at TIMESTAMPTZ, client_id TEXT, fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pool_id, worker_id)
);
CREATE TABLE unclassified_logs (
    pool_id TEXT NOT NULL, task_id TEXT NOT NULL, run_id INT,
    worker_id TEXT NOT NULL, log_text TEXT NOT NULL,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, task_id)
);
