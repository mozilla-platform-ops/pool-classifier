from worker_health.pool_classifier_web.patterns_registry import all_patterns, classify_patterns


def test_macos_refresh_rate_mismatch_beats_incidental_low_severity_payload_text():
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
