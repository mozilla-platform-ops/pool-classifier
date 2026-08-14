from worker_health.pool_classifier import PoolClassifier
from worker_health.pool_classifier_web.patterns_registry import all_patterns, classify_patterns


def test_macos_refresh_rate_mismatch_ignores_incidental_payload_validation_text():
    category, pattern = classify_patterns(
        all_patterns(),
        "task payload does not declare a required value, so content authenticity cannot be verified\n"
        "ERROR: expected refresh rate = 60.00, instead got 75.00.",
        "failed",
        None,
    )

    assert category == "macos_refresh_rate_mismatch"
    assert pattern is not None
    assert pattern.severity == "high"


def test_payload_validation_text_alone_is_unclassified():
    category, pattern = classify_patterns(
        all_patterns(),
        "task payload does not declare a required value, so content authenticity cannot be verified",
        "failed",
        None,
    )

    assert category == "unclassified"
    assert pattern is None


def test_mozperftest_missing_results_is_a_low_severity_test_outcome():
    category, pattern = classify_patterns(
        all_patterns(),
        "mozperftest.metrics.exceptions.MetricsMissingResultsError: Could not find any results to process.",
        "failed",
        None,
    )

    assert category == "mozperftest-missing-results"
    assert pattern is not None
    assert pattern.severity == "low"


def test_raptor_test_failure_is_a_low_severity_fallback():
    category, pattern = classify_patterns(
        all_patterns(),
        "raptor-main Critical: TEST-UNEXPECTED-FAIL | Some visual metrics have an erroneous value of 0.",
        "failed",
        None,
    )

    assert category == "raptor-test-failure"
    assert pattern is not None
    assert pattern.severity == "low"


def test_raptor_benchmark_timeout_beats_generic_raptor_failure():
    category, pattern = classify_patterns(
        all_patterns(),
        "CRITICAL -  raptor-browsertime Critical: Benchmark timed out. Aborting...\n"
        "raptor-main Critical: TEST-UNEXPECTED-FAIL",
        "failed",
        None,
    )

    assert category == "raptor-benchmark-timeout"
    assert pattern is not None
    assert pattern.severity == "low"


def test_raptor_browsertime_failure_is_a_low_severity_fallback():
    category, pattern = classify_patterns(
        all_patterns(),
        "raptor-browsertime Error: Browsertime failed to run",
        "failed",
        None,
    )

    assert category == "raptor-browsertime-failure"
    assert pattern is not None
    assert pattern.severity == "low"


def test_taskcluster_command_abort_at_max_runtime_beats_test_outcome():
    category, pattern = classify_patterns(
        all_patterns(),
        "raptor-browsertime Error: Browsertime failed to run\n"
        "[taskcluster 2026-08-14T15:46:41.804Z] Command ABORTED after "
        "34m59.7774463s: process aborted",
        "failed",
        None,
    )

    assert category == "task-aborted-max-run-time"
    assert pattern is not None
    assert pattern.severity == "high"


def test_test_outcomes_are_low_priority_but_max_runtime_remains_high():
    severity_by_name = {pattern.name: pattern.severity for pattern in all_patterns()}

    assert {
        "browsertime_samples",
        "test-unexpected-timeout",
        "browsertime-device-timeout",
        "raptor-no-data-to-collect",
        "mozperftest-missing-results",
        "test-failure-unexpected-crashes",
        "test-failure-unexpected-statuses",
        "wpt-unexpected-results",
        "wpt-errorsummary-unexpected-result",
        "test-failure-unexpected-server-start-timeout",
        "test-exception-image-difference-too-high",
        "tests-failed",
        "app-crashed-minidump",
        "build-commands-failed",
        "raptor-benchmark-timeout",
        "raptor-browsertime-failure",
        "raptor-test-failure",
    } <= {name for name, severity in severity_by_name.items() if severity == "low"}
    assert severity_by_name["task-aborted-max-run-time"] == "high"


def test_wpt_terminal_summary_classifies_bounded_log_without_specific_failure_line():
    category, pattern = classify_patterns(
        all_patterns(),
        "17:52:36     INFO - Got 3 unexpected results, with 0 unexpected passes",
        "failed",
        None,
    )

    assert category == "wpt-unexpected-results"
    assert pattern is not None
    assert pattern.severity == "low"


def test_unexpected_statuses_is_platform_neutral_test_runner_rule():
    category, pattern = classify_patterns(
        all_patterns(),
        "17:52:37  WARNING - Got 11 unexpected statuses",
        "failed",
        None,
    )

    assert category == "test-failure-unexpected-statuses"
    assert pattern is not None
    assert pattern.tags == ["test"]


def test_wpt_error_summary_detects_unexpected_non_intermittent_result():
    summary = (
        '{"status": "FAIL", "expected": "PASS", "known_intermittent": ["FAIL"], '
        '"action": "test_result"}\n'
        '{"status": "TIMEOUT", "expected": "PASS", "known_intermittent": [], '
        '"action": "test_result"}\n'
    )

    assert PoolClassifier._wpt_error_summary_has_unexpected_result(summary)


def test_wpt_error_summary_ignores_known_intermittents():
    summary = '{"status": "FAIL", "expected": "PASS", "known_intermittent": ["FAIL"], "action": "test_result"}\n'

    assert not PoolClassifier._wpt_error_summary_has_unexpected_result(summary)
