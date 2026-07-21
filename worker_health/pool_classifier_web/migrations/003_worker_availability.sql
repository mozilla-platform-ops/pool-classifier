CREATE TABLE worker_availability_state (
    pool_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_group TEXT,
    available BOOLEAN NOT NULL,
    quarantined BOOLEAN NOT NULL,
    last_contact TIMESTAMPTZ,
    quarantine_until TIMESTAMPTZ,
    reason TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pool_id, worker_id)
);

CREATE TABLE worker_availability_transitions (
    id BIGSERIAL PRIMARY KEY,
    pool_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_group TEXT,
    available BOOLEAN NOT NULL,
    quarantined BOOLEAN NOT NULL,
    last_contact TIMESTAMPTZ,
    quarantine_until TIMESTAMPTZ,
    reason TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_worker_availability_effective
    ON worker_availability_transitions (pool_id, effective_at);
CREATE INDEX idx_worker_availability_worker
    ON worker_availability_transitions (pool_id, worker_id, effective_at);
