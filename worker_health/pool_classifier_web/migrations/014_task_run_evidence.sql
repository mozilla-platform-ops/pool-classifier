-- Keep legacy readers and writers on the original columns during preparation.
ALTER TABLE task_results
    ADD COLUMN evidence_run_state TEXT,
    ADD COLUMN evidence_category TEXT,
    ADD COLUMN evidence_classified_at TIMESTAMPTZ,
    ADD COLUMN status_fetch_state TEXT,
    ADD COLUMN status_fetched_at TIMESTAMPTZ,
    ADD COLUMN classification_state TEXT,
    ADD COLUMN classification_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN status_retry_enabled BOOLEAN;

ALTER TABLE task_sources ADD COLUMN name_retry_enabled BOOLEAN NOT NULL DEFAULT false;

CREATE FUNCTION task_results_stage_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Leave questionable rows pending so the backfill reports them without
    -- changing legacy behavior or inventing an outcome.
    IF NEW.run_state NOT IN (
        'observed', 'expired', 'pending', 'running', 'unscheduled', 'completed', 'failed', 'exception'
    ) OR (NEW.run_state = 'expired' AND (
        NEW.category IS NOT NULL OR NEW.run_started IS NOT NULL OR NEW.run_resolved IS NOT NULL
        OR NEW.run_scheduled IS NOT NULL OR NEW.reason_created IS NOT NULL OR NEW.reason_resolved IS NOT NULL
        OR NEW.observed_at IS NULL OR NEW.last_checked_at IS NULL OR NEW.last_checked_at < NEW.observed_at
    )) THEN
        NEW.status_fetch_state := NULL;
        RETURN NEW;
    END IF;

    -- After a rollback, a legacy attempt timestamp update alone need
    -- not discard the evidence written by the newer scanner.
    IF TG_OP = 'UPDATE' AND NEW.status_fetch_state IS NOT NULL AND
        ROW(NEW.pool_id, NEW.task_id, NEW.run_id, NEW.worker_id, NEW.run_state, NEW.category, NEW.classified_at)
        IS NOT DISTINCT FROM
        ROW(OLD.pool_id, OLD.task_id, OLD.run_id, OLD.worker_id, OLD.run_state, OLD.category, OLD.classified_at)
    THEN
        RETURN NEW;
    END IF;

    NEW.evidence_run_state := CASE WHEN NEW.run_state NOT IN ('observed', 'expired') THEN NEW.run_state END;
    NEW.evidence_category := CASE WHEN NEW.run_state IN ('failed', 'exception') THEN NEW.category END;
    NEW.evidence_classified_at := CASE
        WHEN NEW.run_state IN ('failed', 'exception') AND NEW.category IS NOT NULL THEN NEW.classified_at END;
    NEW.status_fetch_state := CASE
        WHEN NEW.run_state = 'expired' THEN 'not_found'
        WHEN NEW.run_state IN ('completed', 'failed', 'exception') THEN 'available'
        ELSE 'unknown' END;
    NEW.classification_state := CASE
        WHEN NEW.run_state = 'completed' THEN 'not_applicable'
        WHEN NEW.run_state IN ('failed', 'exception') AND NEW.category IS NOT NULL THEN 'classified'
        WHEN NEW.run_state IN ('failed', 'exception') THEN 'pending'
        ELSE 'unknown' END;
    NEW.status_retry_enabled := NEW.run_state NOT IN ('expired', 'completed', 'failed', 'exception');
    NEW.status_fetched_at := NULL;
    NEW.classification_last_attempt_at := NULL;
    RETURN NEW;
END;
$$;

CREATE TRIGGER task_results_sync_evidence BEFORE INSERT OR UPDATE ON task_results
FOR EACH ROW EXECUTE FUNCTION task_results_stage_evidence();
