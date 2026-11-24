-- Run during a controlled cutover with the previous revision drained.
LOCK TABLE task_results IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    ambiguous_count bigint;
BEGIN
    SELECT count(*) INTO ambiguous_count FROM task_results
    WHERE run_state = 'expired' AND (
        category IS NOT NULL OR run_started IS NOT NULL OR run_resolved IS NOT NULL
        OR run_scheduled IS NOT NULL OR reason_created IS NOT NULL OR reason_resolved IS NOT NULL
        OR observed_at IS NULL OR last_checked_at IS NULL OR last_checked_at < observed_at
    );
    IF ambiguous_count > 0 THEN
        RAISE EXCEPTION '% ambiguous legacy expired rows require review before migration', ambiguous_count;
    END IF;
    IF EXISTS (SELECT 1 FROM task_results WHERE run_state NOT IN (
        'observed', 'expired', 'pending', 'running', 'unscheduled', 'completed', 'failed', 'exception'
    )) THEN
        RAISE EXCEPTION 'Unrecognized legacy run states require review before migration';
    END IF;
END $$;

ALTER TABLE task_results
    ALTER COLUMN run_state DROP NOT NULL,
    ALTER COLUMN classified_at DROP NOT NULL,
    ADD COLUMN status_fetch_state TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN status_fetched_at TIMESTAMPTZ,
    ADD COLUMN classification_state TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN classification_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN status_retry_enabled BOOLEAN NOT NULL DEFAULT true;

UPDATE task_results SET
    status_fetch_state = CASE
        WHEN run_state = 'expired' THEN 'not_found'
        WHEN run_state IN ('completed', 'failed', 'exception') THEN 'available'
        ELSE 'unknown' END,
    classification_state = CASE
        WHEN run_state = 'completed' THEN 'not_applicable'
        WHEN run_state IN ('failed', 'exception') AND category IS NOT NULL THEN 'classified'
        WHEN run_state IN ('failed', 'exception') THEN 'pending'
        ELSE 'unknown' END,
    status_retry_enabled = run_state NOT IN ('expired', 'completed', 'failed', 'exception'),
    classified_at = CASE WHEN run_state IN ('failed', 'exception') AND category IS NOT NULL THEN classified_at END,
    category = CASE WHEN run_state IN ('failed', 'exception') THEN category END,
    run_state = CASE WHEN run_state NOT IN ('observed', 'expired') THEN run_state END;

ALTER TABLE task_results ADD CONSTRAINT task_results_evidence_consistent CHECK (
    status_fetch_state IN ('pending', 'available', 'not_found', 'run_missing', 'worker_mismatch', 'error', 'unknown')
    AND (run_state IS NULL OR run_state IN ('pending', 'running', 'unscheduled', 'completed', 'failed', 'exception'))
    AND CASE
        WHEN run_state = 'completed' THEN classification_state = 'not_applicable' AND NOT status_retry_enabled
            AND category IS NULL AND classified_at IS NULL
        WHEN run_state IN ('failed', 'exception') THEN NOT status_retry_enabled AND (
            (classification_state = 'pending' AND category IS NULL AND classified_at IS NULL)
            OR (classification_state = 'classified' AND category IS NOT NULL AND classified_at IS NOT NULL))
        ELSE classification_state = 'unknown' AND category IS NULL AND classified_at IS NULL END
);

ALTER TABLE task_sources ADD COLUMN name_retry_enabled BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX idx_task_sources_name_retry ON task_sources (task_id) WHERE name_retry_enabled;
CREATE INDEX idx_task_results_status_retry ON task_results (pool_id, last_checked_at, observed_at, task_id, run_id) WHERE status_retry_enabled;
CREATE INDEX idx_task_results_classification_pending ON task_results (pool_id, classification_last_attempt_at NULLS FIRST, task_id, run_id) WHERE classification_state = 'pending';
CREATE INDEX idx_task_results_running ON task_results (pool_id, task_id) WHERE run_state = 'running';
