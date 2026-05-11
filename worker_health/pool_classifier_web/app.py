"""Flask web application for the pool classifier Cloud Run service."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from flask import Flask, Response, abort, jsonify, render_template

from worker_health.pool_classifier import CONSECUTIVE_FAILURE_ALERT, PoolClassifier
from worker_health.pool_classifier_web import registry
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
        rows = []
        for pool in registry.all_pools():
            try:
                pc = _get_classifier(pool.provisioner, pool.worker_type)
                alerting = pc.storage.count_alerting(CONSECUTIVE_FAILURE_ALERT) if pc else None
                oldest = pc.storage.oldest_classified_at() if pc else None
                workers = pc.storage.count_workers() if pc else None
                errors_1h = pc.storage.count_recent_errors(since_1h) if pc else None
            except Exception as e:
                logger.warning("Failed to fetch summary for pool %s/%s: %s", pool.provisioner, pool.worker_type, e)
                alerting = None
                oldest = None
                workers = None
                errors_1h = None
            rows.append(
                {"pool": pool, "alerting": alerting, "oldest": oldest, "workers": workers, "errors_1h": errors_1h},
            )
        return render_template("index.html", pools=rows, generated=now)

    @app.get("/pools/<provisioner>/<worker_type>")
    def pool_html(provisioner: str, worker_type: str):
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        return Response(pc.render_html(), content_type="text/html; charset=utf-8")

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
