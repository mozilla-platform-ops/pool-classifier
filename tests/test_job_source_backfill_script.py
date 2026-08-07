"""Tests for the operational job-source backfill runner."""

from worker_health.pool_classifier_web.scripts import backfill_job_sources_all_pools as backfill


def test_count_only_reports_backlog_without_starting_pool_backfills(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        backfill,
        "backlog_by_pool",
        lambda dsn, not_before: calls.append((dsn, not_before)) or [("a/worker", 3), ("b/worker", 2)],
    )
    monkeypatch.setattr(backfill, "backfill_pool", lambda *_args: (_ for _ in ()).throw(AssertionError("should not run")))

    assert backfill.main(["--database-url", "postgresql://example", "--count-only"]) == 0

    output = capsys.readouterr().out
    assert "Eligible job-source backlog: 5 task(s) across 2 pool(s) from the last 14 days." in output
    assert "a/worker: 3 task(s)" in output
    assert calls[0][0] == "postgresql://example"


def test_count_only_reports_an_empty_backlog(monkeypatch, capsys):
    closed = []
    monkeypatch.setattr(backfill, "backlog_by_pool", lambda *_args: [])
    monkeypatch.setattr(backfill, "close_postgres_pools", lambda: closed.append(True))

    assert backfill.main(["--database-url", "postgresql://example", "--count-only", "--lookback-days", "30"]) == 0
    assert capsys.readouterr().out.strip() == "No eligible job-source backlog found."
    assert closed == [True]


def test_defaults_use_moderate_parallelism(monkeypatch):
    captured = {}
    monkeypatch.setattr(backfill, "backlog_by_pool", lambda *_args: [("a/worker", 1)])
    monkeypatch.setattr(
        backfill,
        "backfill_pool",
        lambda pool_id, _dsn, batch_size, concurrency, retries, requests_per_second, *_args: (
            captured.update(
                pool_id=pool_id,
                batch_size=batch_size,
                concurrency=concurrency,
                retries=retries,
                requests_per_second=requests_per_second,
            )
            or (True, False, None)
        ),
    )

    assert backfill.main(["--database-url", "postgresql://example"]) == 0

    assert captured == {
        "pool_id": "a/worker",
        "batch_size": 500,
        "concurrency": 12,
        "retries": 2,
        "requests_per_second": 8.0,
    }
