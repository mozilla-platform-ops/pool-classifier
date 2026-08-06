CREATE TABLE task_sources (
    task_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_method SMALLINT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);
