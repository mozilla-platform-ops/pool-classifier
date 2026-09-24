from unittest.mock import Mock

from worker_health.pool_classifier import PoolClassifier


def test_web_classification_refreshes_quarantine_without_building_reports(monkeypatch):
    monkeypatch.setattr(PoolClassifier, "_init_tc", lambda self: None)
    storage = Mock()
    classifier = PoolClassifier("provisioner", "worker-type", storage=storage)
    quarantined = {"worker-1": "2026-09-24T00:00:00Z"}
    details = {"worker-1": {"quarantine_until": quarantined["worker-1"]}}
    classifier._list_quarantined_workers = Mock(return_value=quarantined)
    classifier._update_quarantine_cache = Mock(return_value=details)
    for name in (
        "_query_workers", "_query_windowed_sr", "_query_heatmap",
        "_recent_failure_summary", "_write_md", "_write_html",
    ):
        setattr(classifier, name, Mock())

    classifier._update_reports()

    classifier._list_quarantined_workers.assert_called_once_with()
    classifier._update_quarantine_cache.assert_called_once_with(quarantined)
    assert classifier._cached_quarantined == quarantined
    assert classifier._cached_quarantine_details == details
    assert classifier._last_quarantine_refresh > 0
    for name in (
        "_query_workers", "_query_windowed_sr", "_query_heatmap",
        "_recent_failure_summary", "_write_md", "_write_html",
    ):
        getattr(classifier, name).assert_not_called()
    storage.get_busy_turnaround.assert_not_called()


def test_file_report_mode_writes_markdown_and_html(monkeypatch, tmp_path):
    monkeypatch.setattr(PoolClassifier, "_init_tc", lambda self: None)
    storage = Mock()
    classifier = PoolClassifier("provisioner", "worker-type", results_dir=tmp_path, storage=storage)
    classifier._list_quarantined_workers = Mock(return_value={})
    classifier._update_quarantine_cache = Mock(return_value={})
    classifier._query_workers = Mock(return_value={})
    classifier._query_windowed_sr = Mock(return_value={})
    classifier._query_heatmap = Mock(return_value={})
    classifier._recent_failure_summary = Mock(return_value={})
    classifier._write_md = Mock(return_value="# Report\n")
    classifier._write_html = Mock(return_value="<html>Report</html>")

    classifier._update_reports()

    assert (tmp_path / "OVERVIEW.md").read_text() == "# Report\n"
    assert (tmp_path / "OVERVIEW.html").read_text() == "<html>Report</html>"
    classifier._query_workers.assert_called_once_with()
    classifier._write_md.assert_called_once()
    classifier._write_html.assert_called_once()
