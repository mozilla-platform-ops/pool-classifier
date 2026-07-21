CREATE TABLE collection_coverage_intervals (
    id BIGSERIAL PRIMARY KEY,
    pool_id TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('task_runs', 'worker_availability')),
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    CHECK (end_at >= start_at)
);

CREATE INDEX idx_collection_coverage_source
    ON collection_coverage_intervals (pool_id, source, start_at, end_at);

CREATE TABLE collection_coverage_state (
    pool_id TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('task_runs', 'worker_availability')),
    last_observed_at TIMESTAMPTZ NOT NULL,
    last_success BOOLEAN NOT NULL,
    current_interval_id BIGINT REFERENCES collection_coverage_intervals(id),
    PRIMARY KEY (pool_id, source)
);
