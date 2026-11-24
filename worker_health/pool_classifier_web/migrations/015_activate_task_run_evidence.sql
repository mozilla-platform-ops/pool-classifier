-- The runner has checked the validated constraint and prebuilt indexes.
-- Keep the original columns and their indexes through the rollback window.
DROP TRIGGER task_results_sync_evidence ON task_results;
ALTER TABLE task_results RENAME COLUMN run_state TO legacy_run_state;
ALTER TABLE task_results RENAME COLUMN category TO legacy_category;
ALTER TABLE task_results RENAME COLUMN classified_at TO legacy_classified_at;
ALTER TABLE task_results RENAME COLUMN evidence_run_state TO run_state;
ALTER TABLE task_results RENAME COLUMN evidence_category TO category;
ALTER TABLE task_results RENAME COLUMN evidence_classified_at TO classified_at;

ALTER TABLE task_results
    ALTER COLUMN status_fetch_state SET DEFAULT 'pending',
    ALTER COLUMN classification_state SET DEFAULT 'unknown',
    ALTER COLUMN status_retry_enabled SET DEFAULT true;

CREATE OR REPLACE FUNCTION task_results_sync_legacy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND
        ROW(NEW.run_state, NEW.category, NEW.classified_at, NEW.status_fetch_state, NEW.status_retry_enabled)
        IS NOT DISTINCT FROM
        ROW(OLD.run_state, OLD.category, OLD.classified_at, OLD.status_fetch_state, OLD.status_retry_enabled)
    THEN
        RETURN NEW;
    END IF;

    -- Only use the legacy expired sentinel for the shape the legacy scanner
    -- could have produced. A running or terminal observation takes precedence.
    NEW.legacy_run_state := COALESCE(NEW.run_state, CASE
        WHEN NEW.status_fetch_state = 'not_found' AND NOT NEW.status_retry_enabled
            AND NEW.run_started IS NULL AND NEW.run_resolved IS NULL AND NEW.run_scheduled IS NULL
            AND NEW.reason_created IS NULL AND NEW.reason_resolved IS NULL
            AND NEW.last_checked_at >= NEW.observed_at
        THEN 'expired' ELSE 'observed' END);
    NEW.legacy_category := NEW.category;
    NEW.legacy_classified_at := COALESCE(NEW.classified_at, NEW.last_checked_at, NEW.observed_at, now());
    RETURN NEW;
END;
$$;

CREATE TRIGGER task_results_sync_evidence BEFORE INSERT OR UPDATE ON task_results
FOR EACH ROW EXECUTE FUNCTION task_results_sync_legacy();
