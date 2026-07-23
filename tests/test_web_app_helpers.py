from datetime import datetime, timezone

from worker_health.pool_classifier_web.app import _coverage_label


def test_coverage_label_shows_duration_when_latest_data_is_fresh():
    label, seconds = _coverage_label(
        "2026-07-11T12:00:00+00:00",
        "2026-07-23T12:00:00+00:00",
        datetime(2026, 7, 23, 12, 30, tzinfo=timezone.utc),
    )

    assert (label, seconds) == ("12d", 12 * 24 * 60 * 60)


def test_coverage_label_includes_staleness_of_latest_data():
    label, seconds = _coverage_label(
        "2026-07-11T08:00:00+00:00",
        "2026-07-23T08:00:00+00:00",
        datetime(2026, 7, 23, 12, 30, tzinfo=timezone.utc),
    )

    assert (label, seconds) == ("12d \u00b7 4h stale", 12 * 24 * 60 * 60)


def test_coverage_label_uses_successful_collection_for_freshness():
    label, seconds = _coverage_label(
        "2026-07-11T08:00:00+00:00",
        "2026-07-23T08:00:00+00:00",
        datetime(2026, 7, 23, 12, 30, tzinfo=timezone.utc),
        "2026-07-23T12:15:00+00:00",
    )

    assert (label, seconds) == ("12d", 12 * 24 * 60 * 60)
