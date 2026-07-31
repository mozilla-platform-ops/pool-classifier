-- Fixed dashboard views are rebuilt after successful scans.  Keeping exactly
-- one complete version per scope makes replacement atomic: readers see either
-- the previous complete snapshot or the new complete snapshot, never a mix.
CREATE TABLE dashboard_snapshots (
    scope TEXT NOT NULL,
    pool_id TEXT NOT NULL DEFAULT '',
    schema_version INTEGER NOT NULL,
    source_at TIMESTAMPTZ NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB NOT NULL,
    PRIMARY KEY (scope, pool_id)
);
