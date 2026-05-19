"""Flask web application for the pool classifier Cloud Run service."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from flask import Flask, Response, abort, jsonify, render_template

from worker_health.pool_classifier import CONSECUTIVE_FAILURE_ALERT, PoolClassifier
from worker_health.pool_classifier_web import registry
from worker_health.pool_classifier_web.registry import detect_os
from worker_health.pool_classifier_web.storage import ClassifyLockBusy, PostgresStorage

logger = logging.getLogger(__name__)

# Keyed by (provisioner, worker_type).
_classifiers: dict[tuple[str, str], PoolClassifier] = {}


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

    @app.get("/")
    def index():
        now_dt = datetime.now(timezone.utc)
        now = now_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        since_1h = (now_dt.replace(microsecond=0) - timedelta(hours=1)).isoformat()
        since_24h = (now_dt.replace(microsecond=0) - timedelta(hours=24)).isoformat()
        rows = []
        for pool in registry.all_pools_including_disabled():
            if not pool.enabled:
                rows.append(
                    {
                        "pool": pool,
                        "os": detect_os(pool),
                        "alerting": None,
                        "oldest": None,
                        "workers": None,
                        "errors_per_host_1h": None,
                        "success_rate_1h": None,
                        "errors_per_host_24h": None,
                        "success_rate_24h": None,
                    },
                )
                continue
            try:
                pc = _get_classifier(pool.provisioner, pool.worker_type)
                alerting = pc.storage.count_alerting(CONSECUTIVE_FAILURE_ALERT) if pc else None
                oldest = pc.storage.oldest_classified_at() if pc else None
                workers = pc.storage.count_workers() if pc else None

                def _rates(since):
                    errors = pc.storage.count_recent_errors(since)
                    successes = pc.storage.count_recent_successes(since)
                    eph = round(errors / workers, 2) if workers and errors is not None else None
                    total = (errors or 0) + (successes or 0)
                    sr = round(successes / total * 100, 1) if total > 0 else None
                    return eph, sr

                errors_per_host_1h, success_rate_1h = _rates(since_1h) if pc else (None, None)
                errors_per_host_24h, success_rate_24h = _rates(since_24h) if pc else (None, None)
            except Exception as e:
                logger.warning("Failed to fetch summary for pool %s/%s: %s", pool.provisioner, pool.worker_type, e)
                alerting = oldest = workers = None
                errors_per_host_1h = success_rate_1h = None
                errors_per_host_24h = success_rate_24h = None
            rows.append(
                {
                    "pool": pool,
                    "os": detect_os(pool),
                    "alerting": alerting,
                    "oldest": oldest,
                    "workers": workers,
                    "errors_per_host_1h": errors_per_host_1h,
                    "success_rate_1h": success_rate_1h,
                    "errors_per_host_24h": errors_per_host_24h,
                    "success_rate_24h": success_rate_24h,
                },
            )
        return render_template("index.html", pools=rows, generated=now)

    @app.get("/pools/<provisioner>/<worker_type>")
    def pool_html(provisioner: str, worker_type: str):
        pool = registry.get_pool(provisioner, worker_type)
        if pool is None:
            abort(404)
        if not pool.enabled:
            reason_html = f"<p><strong>Reason:</strong> {pool.reason}</p>" if pool.reason else ""
            return Response(
                f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{pool.worker_type} — disabled</title>"
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

    @app.post("/classify/<provisioner>/<worker_type>")
    def classify(provisioner: str, worker_type: str):
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        try:
            summary = pc.classify_cycle()
        except ClassifyLockBusy:
            return jsonify({"error": "classify cycle already running for this pool"}), 409
        return jsonify(summary)

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
