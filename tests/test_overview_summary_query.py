"""Regression coverage for bounded, current overview aggregates."""

from worker_health.pool_classifier_web.storage import (
    CURRENT_POOL_SUMMARY_SQL,
    LIVE_WORKER_AND_ALERT_SUMMARY_SQL,
)


def test_current_pool_summary_query_is_bounded_by_requested_pool_and_window():
    assert "WHERE task_results.pool_id = requested_pools.pool_id" in CURRENT_POOL_SUMMARY_SQL
    assert "AND task_results.run_resolved >= %(s24h)s" in CURRENT_POOL_SUMMARY_SQL
    assert "FROM task_results LEFT JOIN" not in CURRENT_POOL_SUMMARY_SQL


def test_live_hosts_query_counts_only_latest_available_workers():
    assert "FROM worker_availability_state" in LIVE_WORKER_AND_ALERT_SUMMARY_SQL
    assert "AND available = TRUE" in LIVE_WORKER_AND_ALERT_SUMMARY_SQL
    assert "FROM workers" in LIVE_WORKER_AND_ALERT_SUMMARY_SQL
    assert "consecutive_failures >= %(alert_threshold)s" in LIVE_WORKER_AND_ALERT_SUMMARY_SQL
