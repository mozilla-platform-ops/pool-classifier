"""Flask web application for the pool classifier Cloud Run service."""

from __future__ import annotations

from collections import Counter
import logging
import math
import os
from datetime import datetime, timedelta, timezone

from flask import Flask, Response, abort, jsonify, render_template, request

from worker_health.pool_classifier import CONSECUTIVE_FAILURE_ALERT, PoolClassifier
from worker_health.pool_classifier_web import registry
from worker_health.pool_classifier_web.auth import require_scheduler_oidc
from worker_health.pool_classifier_web.registry import detect_os
from worker_health.pool_classifier_web import patterns_registry
from worker_health.pool_classifier_web.storage import (
    ClassifyLockBusy,
    PostgresStorage,
    count_category_hits_global,
    pool_summaries_global,
)

logger = logging.getLogger(__name__)

# Keyed by (provisioner, worker_type).
_classifiers: dict[tuple[str, str], PoolClassifier] = {}
MAX_UTILIZATION_RANGE_SECONDS = 90 * 24 * 60 * 60
MAX_UTILIZATION_BUCKETS = 2000
UTILIZATION_WINDOWS = {"1h": 60 * 60, "24h": 24 * 60 * 60, "7d": 7 * 24 * 60 * 60, "30d": 30 * 24 * 60 * 60}
COVERAGE_STALE_AFTER = timedelta(hours=1)


def _parse_utilization_datetime(name: str, value: str | None) -> datetime:
    if not value:
        raise ValueError(f"{name} is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _utilization_parameters() -> tuple[str, str, int]:
    start = _parse_utilization_datetime("start", request.args.get("start"))
    end = _parse_utilization_datetime("end", request.args.get("end"))
    if end <= start:
        raise ValueError("end must be after start")
    range_seconds = (end - start).total_seconds()
    if range_seconds > MAX_UTILIZATION_RANGE_SECONDS:
        raise ValueError("time range must not exceed 90 days")

    bucket_value = request.args.get("bucket_seconds")
    if not bucket_value:
        raise ValueError("bucket_seconds is required")
    try:
        bucket_seconds = int(bucket_value)
    except ValueError as exc:
        raise ValueError("bucket_seconds must be an integer") from exc
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be greater than zero")
    if bucket_seconds > MAX_UTILIZATION_RANGE_SECONDS:
        raise ValueError(f"bucket_seconds must not exceed {MAX_UTILIZATION_RANGE_SECONDS}")
    if math.ceil(range_seconds / bucket_seconds) > MAX_UTILIZATION_BUCKETS:
        raise ValueError(f"bucket_seconds would produce more than {MAX_UTILIZATION_BUCKETS} buckets")
    return start.isoformat(), end.isoformat(), bucket_seconds


def _get_classifier(provisioner: str, worker_type: str) -> PoolClassifier | None:
    key = (provisioner, worker_type)
    if key not in _classifiers:
        pool = registry.get_pool(provisioner, worker_type)
        if pool is None:
            return None
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        storage = PostgresStorage(
            pool_id=f"{provisioner}/{worker_type}",
            dsn=dsn,
        )
        pc = PoolClassifier(
            provisioner=provisioner,
            worker_type=worker_type,
            storage=storage,
            availability_mode=pool.availability_mode,
        )
        pc._init_db()
        _classifiers[key] = pc
    return _classifiers[key]


def _humanize_cron(expr: str) -> str:
    parts = expr.strip().split()
    if len(parts) != 5:
        return expr
    minute, hour, dom, month, dow = parts
    if dom == "*" and month == "*" and dow == "*":
        if minute.startswith("*/") and hour == "*":
            return f"every {minute[2:]}m"
        if minute == "0" and hour.startswith("*/"):
            return f"every {hour[2:]}h"
        if minute == "0" and hour == "0":
            return "daily"
        if minute == "0" and hour == "*":
            return "every 1h"
    return expr


def _format_elapsed(delta: timedelta) -> str:
    """Return a compact, whole-unit elapsed-time label."""
    seconds = max(0, int(delta.total_seconds()))
    if seconds >= 24 * 60 * 60:
        return f"{seconds // (24 * 60 * 60)}d"
    if seconds >= 60 * 60:
        return f"{seconds // (60 * 60)}h"
    return f"{seconds // 60}m"


def _coverage_label(
    oldest: str | None,
    latest: str | None,
    now: datetime,
    collection_latest: str | None = None,
) -> tuple[str | None, int | None]:
    """Format the task-result range and flag stale successful collection coverage."""
    if not oldest or not latest:
        return None, None
    start = _parse_utilization_datetime("oldest", oldest)
    end = _parse_utilization_datetime("latest", latest)
    coverage_seconds = max(0, int((end - start).total_seconds()))
    label = _format_elapsed(timedelta(seconds=coverage_seconds))
    freshness_at = _parse_utilization_datetime("collection_latest", collection_latest) if collection_latest else end
    staleness = now - freshness_at
    if staleness > COVERAGE_STALE_AFTER:
        label += f" \u00b7 {_format_elapsed(staleness)} stale"
    return label, coverage_seconds


def create_app() -> Flask:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    app = Flask(__name__)
    app.jinja_env.filters["humanize_cron"] = _humanize_cron

    # Warn at startup if TC credentials are missing, but don't fail.
    try:
        token_file = os.path.expanduser(os.environ.get("TC_TOKEN_FILE", "~/.tc_token"))
        has_tc = bool(os.environ.get("TC_TOKEN_JSON")) or os.path.exists(token_file)
        if not has_tc:
            logger.warning(
                "No TC credentials found (TC_TOKEN_JSON env or %s). "
                "POST /classify/<provisioner>/<worker_type> will fail until credentials are provided.",
                token_file,
            )
    except Exception:
        pass

    @app.get("/healthz")
    def healthz():
        return "ok", 200, {"Content-Type": "text/plain"}

    @app.get("/favicon.ico")
    def favicon():
        return app.send_static_file("favicon.svg")

    @app.get("/")
    def index():
        now_dt = datetime.now(timezone.utc)
        now = now_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        since_1h = (now_dt.replace(microsecond=0) - timedelta(hours=1)).isoformat()
        since_24h = (now_dt.replace(microsecond=0) - timedelta(hours=24)).isoformat()
        # One pair of GROUP BY pool_id queries for every pool, on one connection
        # (vs ~7 queries per pool on a per-pool connection). See PC_DB_REFACTOR.md.
        summaries: dict = {}
        dsn = os.environ.get("DATABASE_URL")
        if dsn:
            try:
                summaries = pool_summaries_global(dsn, CONSECUTIVE_FAILURE_ALERT, since_1h, since_24h)
            except Exception as e:
                logger.warning("index: pool_summaries_global failed: %s", e)

        def _eph(errors, workers):
            return round(errors / workers, 2) if workers else None

        def _sr(errors, successes):
            total = errors + successes
            return round(successes / total * 100, 1) if total > 0 else None

        rows = []
        for pool in registry.all_pools_including_disabled():
            if not pool.enabled:
                rows.append(
                    {
                        "pool": pool,
                        "os": detect_os(pool),
                        "alerting": None,
                        "coverage": None,
                        "coverage_seconds": None,
                        "workers": None,
                        "errors_per_host_1h": None,
                        "success_rate_1h": None,
                        "errors_per_host_24h": None,
                        "success_rate_24h": None,
                    },
                )
                continue
            s = summaries.get(f"{pool.provisioner}/{pool.worker_type}")
            if s is None:
                # No rows yet for this pool (never classified).
                workers = alerting = oldest = latest = None
                errors_per_host_1h = success_rate_1h = errors_per_host_24h = success_rate_24h = None
                collection_latest = None
            else:
                workers, alerting, oldest, latest = s["workers"], s["alerting"], s["oldest"], s["latest"]
                collection_latest = s["collection_latest"]
                errors_per_host_1h, success_rate_1h = _eph(s["err_1h"], workers), _sr(s["err_1h"], s["ok_1h"])
                errors_per_host_24h, success_rate_24h = _eph(s["err_24h"], workers), _sr(s["err_24h"], s["ok_24h"])
            coverage, coverage_seconds = _coverage_label(oldest, latest, now_dt, collection_latest)
            rows.append(
                {
                    "pool": pool,
                    "os": detect_os(pool),
                    "alerting": alerting,
                    "coverage": coverage,
                    "coverage_seconds": coverage_seconds,
                    "workers": workers,
                    "errors_per_host_1h": errors_per_host_1h,
                    "success_rate_1h": success_rate_1h,
                    "errors_per_host_24h": errors_per_host_24h,
                    "success_rate_24h": success_rate_24h,
                },
            )
        return render_template(
            "index.html",
            pools=rows,
            generated=now,
        )

    @app.get("/patterns")
    def patterns():
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(microsecond=0).isoformat()
        hits: dict[str, int] = {}
        try:
            dsn = os.environ.get("DATABASE_URL")
            if dsn:
                hits = count_category_hits_global(dsn, since)
        except Exception as e:
            logger.warning("patterns: hit-count query failed: %s", e)
        # All patterns, including disabled — the page is for inspecting config.
        all_pats = patterns_registry._patterns  # noqa: SLF001  (intentional: surface disabled too)
        sev_rank = {"critical": 0, "high": 1, "low": 2}
        rows = sorted(all_pats, key=lambda p: sev_rank.get(p.severity, 99))
        return render_template(
            "patterns.html",
            patterns=rows,
            hits=hits,
            generated=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        )

    @app.get("/pools/<provisioner>/<worker_type>")
    def pool_html(provisioner: str, worker_type: str):
        pool = registry.get_pool(provisioner, worker_type)
        if pool is None:
            abort(404)
        if not pool.enabled:
            reason_html = f"<p><strong>Reason:</strong> {pool.reason}</p>" if pool.reason else ""
            return Response(
                f"<!DOCTYPE html><html><head><meta charset='utf-8'>"
                f"<link rel='icon' href='/static/favicon.svg' type='image/svg+xml'>"
                f"<title>{pool.worker_type} — disabled</title>"
                f"<style>body{{font-family:monospace;background:#111;color:#ccc;padding:1.5rem}}"
                f"h1{{color:#f90}}a{{color:#888}}</style></head><body>"
                f"<p><a href='/'>← back</a></p>"
                f"<h1>{pool.worker_type}</h1>"
                f"<p>This pool is <strong>disabled</strong> and is not being classified.</p>"
                f"{reason_html}"
                f"</body></html>",
                content_type="text/html; charset=utf-8",
            )
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        os_label = detect_os(pool)
        return Response(pc.render_html(os_label=os_label), content_type="text/html; charset=utf-8")

    @app.get("/pools/<provisioner>/<worker_type>/overview.md")
    def pool_md(provisioner: str, worker_type: str):
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        return Response(pc.render_md(), content_type="text/markdown; charset=utf-8")

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/utilization")
    def pool_utilization(provisioner: str, worker_type: str):
        try:
            start, end, bucket_seconds = _utilization_parameters()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        result = pc.storage.get_utilization(start, end, bucket_seconds)
        result["api_version"] = 1
        result["availability_mode"] = pc.availability_mode
        return jsonify(result)

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/utilization/summary")
    def pool_utilization_summary(provisioner: str, worker_type: str):
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        result = pc.storage.get_utilization_summary(UTILIZATION_WINDOWS)
        result.update({"api_version": 1, "availability_mode": pc.availability_mode})
        return jsonify(result)

    @app.get("/pools/<provisioner>/<worker_type>/utilization")
    def pool_utilization_guide(provisioner: str, worker_type: str):
        pool = registry.get_pool(provisioner, worker_type)
        if pool is None:
            abort(404)
        return render_template("utilization_guide.html", pool=pool)

    @app.post("/classify/<provisioner>/<worker_type>")
    @require_scheduler_oidc
    def classify(provisioner: str, worker_type: str):
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        try:
            summary = pc.classify_cycle()
        except ClassifyLockBusy:
            return jsonify({"error": "classify cycle already running for this pool"}), 409
        return jsonify(summary)

    @app.post("/classify-all")
    @require_scheduler_oidc
    def classify_all():
        # Sequential fan-out over all enabled pools, driven by a single Cloud
        # Scheduler job. Mirrors pc_fetch_data.sh (proj-autophone first, then the
        # rest) and runs one pool at a time on purpose — concurrent per-pool jobs
        # exhausted the Postgres connection budget. Per-pool failures are caught
        # so one bad pool doesn't abort the run; the advisory lock makes
        # overlapping runs safe (busy pools are skipped).
        pools = sorted(
            registry.all_pools(),
            key=lambda p: (p.provisioner != "proj-autophone", p.provisioner, p.worker_type),
        )
        results = []
        for pool in pools:
            label = f"{pool.provisioner}/{pool.worker_type}"
            try:
                pc = _get_classifier(pool.provisioner, pool.worker_type)
                if pc is None:
                    results.append({"pool": label, "status": "not_found"})
                    continue
                summary = pc.classify_cycle()
                results.append({"pool": label, "status": "ok", "summary": summary})
            except ClassifyLockBusy:
                results.append({"pool": label, "status": "busy"})
            except Exception as e:  # noqa: BLE001 - one pool must not abort the rest
                logger.exception("classify-all: pool %s failed", label)
                results.append({"pool": label, "status": "error", "error": str(e)})
        ok = sum(1 for r in results if r["status"] == "ok")
        counts = Counter(r["status"] for r in results)
        body = {"pools": len(results), "ok": ok, "status_counts": dict(counts), "results": results}
        log_msg = "classify-all summary: pools=%d ok=%d busy=%d error=%d not_found=%d"
        log_args = (
            len(results),
            counts["ok"],
            counts["busy"],
            counts["error"],
            counts["not_found"],
        )
        if counts["error"] or counts["not_found"]:
            logger.warning(log_msg, *log_args)
        else:
            logger.info(log_msg, *log_args)
        # Surface a systemic failure (e.g. DB down) as a failed run; partial
        # failures still return 200 so the scheduler isn't spammed with retries.
        status_code = 200 if (ok > 0 or not results) else 500
        return jsonify(body), status_code

    @app.get("/pools/<provisioner>/<worker_type>/unclassified/<task_id>.log")
    def unclassified_log(provisioner: str, worker_type: str, task_id: str):
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        for tid, log_text, _ref in pc.storage.list_unclassified_logs():
            if tid == task_id:
                return Response(log_text, content_type="text/plain; charset=utf-8")
        abort(404)

    return app
