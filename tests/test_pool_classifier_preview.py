import pytest

from worker_health.pool_classifier_preview import compare_classification, select_terminal_run
from worker_health.pool_classifier_web.patterns_registry import Pattern


def pattern(name, regex, severity="high"):
    return Pattern(name=name, regex=regex, severity=severity)


def test_preview_uses_production_severity_then_file_order():
    comparison = compare_classification(
        [pattern("high", "match", "high"), pattern("critical", "match", "critical")],
        [pattern("high", "match", "high"), pattern("critical", "match", "critical")],
        "match", "failed", None,
    )

    assert comparison.before.category == "critical"
    assert comparison.proposed.category == "critical"


def test_preview_reports_proposed_rule_shadowing_prior_match():
    old = pattern("old", "match", "high")
    new = pattern("new", "match", "critical")

    comparison = compare_classification([old], [old, new], "match", "failed", None)

    assert comparison.before.category == "old"
    assert comparison.proposed.category == "new"
    assert comparison.proposed_shadows_before is True


def test_preview_preserves_exception_fallback_without_matching_rules():
    comparison = compare_classification([], [], "", "exception", "worker-shutdown")

    assert comparison.before.category == "exception_worker-shutdown"
    assert comparison.before.pattern is None


def test_select_terminal_run_uses_newest_or_requested_run():
    status = {"status": {"runs": [
        {"runId": 0, "state": "failed"},
        {"runId": 1, "state": "running"},
        {"runId": 2, "state": "completed"},
    ]}}

    assert select_terminal_run(status)["runId"] == 2
    assert select_terminal_run(status, 0)["state"] == "failed"
    with pytest.raises(ValueError, match="no terminal run 1"):
        select_terminal_run(status, 1)
