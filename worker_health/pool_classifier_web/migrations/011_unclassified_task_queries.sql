CREATE INDEX idx_task_results_pool_category_resolved
    ON task_results (pool_id, category, run_resolved DESC, classified_at DESC);
