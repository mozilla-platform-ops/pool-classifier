CREATE TABLE worker_availability_mode (
    pool_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode IN ('recent_contact', 'listed')),
    changed_at TIMESTAMPTZ NOT NULL
);
