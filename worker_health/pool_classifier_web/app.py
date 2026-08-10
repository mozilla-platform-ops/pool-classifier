"""Flask web application for the pool classifier Cloud Run service."""

from __future__ import annotations

from collections import Counter
import base64
import binascii
import copy
from concurrent.futures import ThreadPoolExecutor
from html import escape
import json
from importlib.metadata import PackageNotFoundError, version
import logging
import math
import os
import re
from threading import Lock
from time import monotonic
from datetime import datetime, timedelta, timezone

from flask import Flask, Response, abort, jsonify, render_template, request

from worker_health.pool_classifier import CONSECUTIVE_FAILURE_ALERT, PoolClassifier
from worker_health.pool_classifier_web import registry
from worker_health.pool_classifier_web import discovery
from worker_health.pool_classifier_web.auth import (
    is_admin_iap_user_hint,
    require_admin_iap,
    require_scheduler_oidc,
)
from worker_health.pool_classifier_web.postgres import connect as postgres_connect
from worker_health.pool_classifier_web.registry import detect_os
from worker_health.pool_classifier_web.scripts.migrate import MIGRATIONS_DIR
from worker_health.pool_classifier_web import patterns_registry
from worker_health.pool_classifier_web.snapshots import (
    OVERVIEW_SCOPE,
    POOL_SCOPE,
    read_snapshot,
    write_snapshot,
)
from worker_health.pool_classifier_web.storage import (
    ClassifyLockBusy,
    PostgresStorage,
    count_category_hits_global,
    observed_start_lag_summaries_global,
    pool_summaries_global,
)

logger = logging.getLogger(__name__)

# Keyed by (provisioner, worker_type, database workload role).
_classifiers: dict[tuple[str, str, str], PoolClassifier] = {}
MAX_UTILIZATION_RANGE_SECONDS = 90 * 24 * 60 * 60
MAX_UTILIZATION_BUCKETS = 2000
DEFAULT_WORKERS_WINDOW_SECONDS = 24 * 60 * 60
DEFAULT_WORKERS_LIMIT = 50
MAX_WORKERS_LIMIT = 200
UTILIZATION_WINDOWS = {"1h": 60 * 60, "24h": 24 * 60 * 60, "7d": 7 * 24 * 60 * 60, "30d": 30 * 24 * 60 * 60}
DEFAULT_OBSERVED_START_LAG_SLO_SECONDS = 4 * 60 * 60
DEFAULT_OBSERVED_START_LAG_MIN_SAMPLES = 5
COVERAGE_STALE_AFTER = timedelta(hours=1)
REPOSITORY_URL = "https://github.com/mozilla-platform-ops/pool-classifier"
DEFAULT_OVERVIEW_CACHE_TTL_SECONDS = 30
DEFAULT_OVERVIEW_UTILIZATION_CONCURRENCY = 4
DEBUG_INSTANCE_COLORS = (
    "#1f6feb",
    "#a371f7",
    "#d29922",
    "#3fb950",
    "#f85149",
    "#39c5cf",
)
DEBUG_INSTANCE_PORT_COLORS = {"8080": "#58a6ff"}
DEBUG_INSTANCE_BADGE_COLOR = "#39c5cf"

# Dashboard aggregates are intentionally process-local: metric definitions are
# still evolving, so a short TTL is safer and simpler than persisted rollups.
_overview_cache: dict[tuple, tuple[float, object]] = {}
_overview_cache_lock = Lock()


def _overview_cache_ttl_seconds() -> float:
    """Return the configured short-lived cache duration, or disable caching."""
    try:
        return max(0, float(os.environ.get("OVERVIEW_CACHE_TTL_SECONDS", DEFAULT_OVERVIEW_CACHE_TTL_SECONDS)))
    except ValueError:
        logger.warning("OVERVIEW_CACHE_TTL_SECONDS must be numeric; disabling overview cache")
        return 0


def _overview_utilization_concurrency() -> int:
    """Return the bounded worker count for a cold overview utilization batch."""
    try:
        return max(1, int(os.environ.get("OVERVIEW_UTILIZATION_CONCURRENCY", DEFAULT_OVERVIEW_UTILIZATION_CONCURRENCY)))
    except ValueError:
        logger.warning("OVERVIEW_UTILIZATION_CONCURRENCY must be an integer; using %d", DEFAULT_OVERVIEW_UTILIZATION_CONCURRENCY)
        return DEFAULT_OVERVIEW_UTILIZATION_CONCURRENCY


def _cached_overview_result(key: tuple, calculate):
    """Return a defensive copy of a short-lived process-local aggregate."""
    ttl = _overview_cache_ttl_seconds()
    if not ttl:
        return calculate()
    now = monotonic()
    with _overview_cache_lock:
        cached = _overview_cache.get(key)
        if cached is not None and cached[0] > now:
            return copy.deepcopy(cached[1])

    result = calculate()
    with _overview_cache_lock:
        _overview_cache[key] = (now + ttl, copy.deepcopy(result))
    return result


def _reset_overview_cache() -> None:
    """Clear process-local aggregate state (primarily useful to tests)."""
    with _overview_cache_lock:
        _overview_cache.clear()


def _application_version() -> str:
    """Return the installed application version without requiring package metadata in development."""
    if configured_version := os.environ.get("POOL_CLASSIFIER_VERSION"):
        return configured_version
    try:
        return version("worker_health")
    except PackageNotFoundError:
        return "unknown"


def _instance_identity_enabled(debug_enabled: bool) -> bool:
    """Return whether this local runtime should mark its HTML responses."""
    return debug_enabled or os.environ.get("PC_INSTANCE_IDENTITY") == "1"


def _debug_instance_identity(port: str, debug_enabled: bool) -> tuple[str, str, str]:
    """Return the label, color, and translucent tint for a local instance."""
    configured_color = os.environ.get("PC_INSTANCE_COLOR", "")
    if re.fullmatch(r"#[0-9a-fA-F]{6}", configured_color):
        color = configured_color.lower()
    elif port in DEBUG_INSTANCE_PORT_COLORS:
        color = DEBUG_INSTANCE_PORT_COLORS[port]
    else:
        try:
            color = DEBUG_INSTANCE_COLORS[int(port) % len(DEBUG_INSTANCE_COLORS)]
        except ValueError:
            color = DEBUG_INSTANCE_COLORS[sum(map(ord, port)) % len(DEBUG_INSTANCE_COLORS)]
    label = os.environ.get("PC_INSTANCE_LABEL") or (f"DEBUG {port}" if debug_enabled else port)
    return label, color, f"{color}20"


def _add_debug_instance_identity(html: str, port: str, debug_enabled: bool) -> str:
    """Add a request-specific local marker without changing stored page artifacts."""
    label, color, tint = _debug_instance_identity(port, debug_enabled)
    styles = (
        '<style id="debug-instance-identity">'
        f"body {{ background-color: #111 !important; background-image: linear-gradient({tint}, {tint}) !important; }}"
        ".debug-instance-indicator { position: fixed; z-index: 9999; top: .65rem; right: .65rem; "
        "padding: .28rem .5rem; border: 1px solid var(--debug-instance-color); border-radius: 3px; "
        "background: #111e; color: var(--debug-instance-color); font: 700 .75rem/1 monospace; "
        "letter-spacing: .04em; box-shadow: 0 .15rem .6rem #0008; }"
        "</style>"
    )
    marker = (
        f'<div class="debug-instance-indicator" style="--debug-instance-color: {DEBUG_INSTANCE_BADGE_COLOR}" '
        f'aria-label="Local debug instance: {escape(label)}">{escape(label)}</div>'
    )
    if "debug-instance-identity" in html:
        return html
    if "</head>" in html:
        html = html.replace("</head>", f"{styles}</head>", 1)
        injection = marker
    else:
        injection = f"{styles}{marker}"
    return re.sub(r"(<body\b[^>]*>)", lambda match: f"{match.group(1)}{injection}", html, count=1, flags=re.IGNORECASE)


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


def _utilization_summary_windows() -> dict[str, int]:
    """Return the requested named utilization windows in canonical order.

    Omitting ``windows`` preserves the detail-page and public API default of
    returning every standard window.  Callers that need only a compact summary
    can request a comma-separated subset, for example ``windows=1h,24h``.
    """
    requested = request.args.get("windows")
    if requested is None:
        return UTILIZATION_WINDOWS

    names = {name.strip() for name in requested.split(",") if name.strip()}
    if not names:
        raise ValueError("windows must include at least one supported window")
    unknown = names - UTILIZATION_WINDOWS.keys()
    if unknown:
        raise ValueError(f"windows contains unsupported value: {sorted(unknown)[0]}")
    return {name: seconds for name, seconds in UTILIZATION_WINDOWS.items() if name in names}


def _observed_start_lag_parameters(default_window_seconds: int | None = None) -> tuple[str, str, int]:
    start_value = request.args.get("start")
    end_value = request.args.get("end")
    if start_value is None and end_value is None and default_window_seconds is not None:
        end = datetime.now(timezone.utc).replace(microsecond=0)
        start = end - timedelta(seconds=default_window_seconds)
    else:
        start = _parse_utilization_datetime("start", start_value)
        end = _parse_utilization_datetime("end", end_value)
    if end <= start:
        raise ValueError("end must be after start")
    if (end - start).total_seconds() > MAX_UTILIZATION_RANGE_SECONDS:
        raise ValueError("time range must not exceed 90 days")
    configured_slo = os.environ.get("OBSERVED_START_LAG_SLO_SECONDS", str(DEFAULT_OBSERVED_START_LAG_SLO_SECONDS))
    value = request.args.get("slo_seconds", configured_slo)
    try:
        slo_seconds = int(value)
    except ValueError as exc:
        raise ValueError("slo_seconds must be an integer") from exc
    if slo_seconds <= 0:
        raise ValueError("slo_seconds must be greater than zero")
    return start.isoformat(), end.isoformat(), slo_seconds


def _observed_start_lag_min_samples() -> int:
    value = request.args.get("min_samples", str(DEFAULT_OBSERVED_START_LAG_MIN_SAMPLES))
    try:
        min_samples = int(value)
    except ValueError as exc:
        raise ValueError("min_samples must be an integer") from exc
    if min_samples <= 0:
        raise ValueError("min_samples must be greater than zero")
    return min_samples


def _bounded_failure_window(default_seconds: int | None = None) -> tuple[str, str]:
    start_value = request.args.get("start")
    end_value = request.args.get("end")
    if not start_value and not end_value and default_seconds is not None:
        end = datetime.now(timezone.utc).replace(microsecond=0)
        start = end - timedelta(seconds=default_seconds)
    elif not start_value or not end_value:
        raise ValueError("start and end must be provided together")
    else:
        start = _parse_utilization_datetime("start", start_value)
        end = _parse_utilization_datetime("end", end_value)
    if end <= start:
        raise ValueError("end must be after start")
    if (end - start).total_seconds() > MAX_UTILIZATION_RANGE_SECONDS:
        raise ValueError("time range must not exceed 90 days")
    return start.isoformat(), end.isoformat()


def _optional_bool(name: str) -> bool | None:
    value = request.args.get(name)
    if value is None:
        return None
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"{name} must be true or false")


def _workers_parameters() -> tuple[str, str, bool | None, bool | None, str | None, int, tuple[bool, str] | None]:
    start, end = _bounded_failure_window(DEFAULT_WORKERS_WINDOW_SECONDS)
    quarantined = _optional_bool("quarantined")
    alerting = _optional_bool("alerting")
    category = request.args.get("category") or None
    try:
        limit = int(request.args.get("limit", str(DEFAULT_WORKERS_LIMIT)))
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc
    if not 1 <= limit <= MAX_WORKERS_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_WORKERS_LIMIT}")
    cursor = request.args.get("cursor")
    after = None
    if cursor:
        try:
            padded_cursor = cursor + "=" * (-len(cursor) % 4)
            decoded = base64.urlsafe_b64decode(padded_cursor)
            value = json.loads(decoded)
            if not isinstance(value["alerting"], bool) or not isinstance(value["worker_id"], str):
                raise ValueError
            after = (value["alerting"], value["worker_id"])
        except (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("cursor is invalid") from exc
    return start, end, quarantined, alerting, category, limit, after


def _workers_cursor(alerting: bool, worker_id: str) -> str:
    value = json.dumps({"alerting": alerting, "worker_id": worker_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def _get_classifier(provisioner: str, worker_type: str, role: str = "web") -> PoolClassifier | None:
    key = (provisioner, worker_type, role)
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
            role=role,
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


def _format_lag(seconds: float | int) -> str:
    """Return a compact lag label while retaining useful sub-minute precision."""
    seconds = max(0, round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, remaining_seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m" if not remaining_seconds else f"{minutes}m {remaining_seconds}s"
    hours, remaining_minutes = divmod(minutes, 60)
    return f"{hours}h" if not remaining_minutes else f"{hours}h {remaining_minutes}m"


def _lag_color_class(seconds: float | int) -> str:
    """Return the overview color band for an observed p95 lag value."""
    if seconds >= 12 * 60 * 60:
        return "bad"
    if seconds >= 4 * 60 * 60:
        return "warn"
    if seconds >= 2 * 60 * 60:
        return "lag-yellow"
    return "ok"


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


def _pool_config(pool) -> dict:
    """Public, stable representation of a configured pool."""
    return {
        "id": pool.id,
        "provisioner": pool.provisioner,
        "worker_type": pool.worker_type,
        "os": detect_os(pool),
        "enabled": pool.enabled,
        "reason": pool.reason or None,
        "schedule": pool.schedule,
        "availability_mode": pool.availability_mode,
    }


def _success_rate(successes: int, errors: int) -> float | None:
    total = successes + errors
    return round(successes / total * 100, 1) if total else None


def _public_pool_summary(pool, summary: dict | None, now: datetime) -> dict:
    """Shape storage aggregates into the versioned per-pool API contract."""
    summary = summary or {}
    task_latest = summary.get("collection_latest")
    availability_latest = summary.get("availability_collection_latest")
    freshness_values = [value for value in (task_latest, availability_latest) if value]
    collected_at = (
        max(freshness_values, key=lambda value: _parse_utilization_datetime("collected_at", value))
        if freshness_values
        else None
    )
    stale = None
    if collected_at:
        stale = now - _parse_utilization_datetime("collected_at", collected_at) > COVERAGE_STALE_AFTER

    def _coverage(started: str | None, latest: str | None) -> dict:
        return {"started_at": started, "through": latest}

    def _window(successes: int, errors: int) -> dict:
        return {"successes": successes, "errors": errors, "success_rate_pct": _success_rate(successes, errors)}

    successes = summary.get("successes", 0)
    errors = summary.get("errors", 0)
    return {
        "api_version": 1,
        "pool": _pool_config(pool),
        "metrics": {
            "summary_window": "24h",
            "workers": summary.get("workers", 0),
            "alerting_workers": summary.get("alerting", 0),
            "task_runs": summary.get("task_runs", 0),
            "successes": successes,
            "errors": errors,
            "success_rate_pct": _success_rate(successes, errors),
            "windows": {
                "1h": _window(summary.get("ok_1h", 0), summary.get("err_1h", 0)),
                "24h": _window(summary.get("ok_24h", 0), summary.get("err_24h", 0)),
            },
        },
        "coverage": {
            "task_runs": _coverage(summary.get("task_collection_started"), task_latest),
            "worker_availability": _coverage(summary.get("availability_collection_started"), availability_latest),
        },
        "freshness": {"collected_at": collected_at, "stale": stale},
    }


def _global_pool_summaries(dsn: str, pool_ids: tuple[str, ...]) -> dict:
    """Query current dashboard windows once per short cache lifetime."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return pool_summaries_global(
        dsn,
        pool_ids,
        CONSECUTIVE_FAILURE_ALERT,
        (now - timedelta(hours=1)).isoformat(),
        (now - timedelta(hours=24)).isoformat(),
    )


def _global_observed_start_lag_summaries(dsn: str) -> dict:
    """Query the overview's fixed seven-day lag window once per TTL."""
    end = datetime.now(timezone.utc).replace(microsecond=0)
    return observed_start_lag_summaries_global(dsn, (end - timedelta(days=7)).isoformat(), end.isoformat())


def _overview_utilization_summaries(windows: dict[str, int]) -> dict:
    """Return cached utilization summaries for every enabled overview pool."""
    pools = [pool for pool in registry.all_pools_including_disabled() if pool.enabled]
    pool_keys = tuple((pool.provisioner, pool.worker_type) for pool in pools)

    def calculate() -> dict:
        classifiers = []
        for pool in pools:
            pc = _get_classifier(pool.provisioner, pool.worker_type)
            if pc is not None:
                classifiers.append((pool, pc))

        def summarize(pool_and_classifier):
            pool, pc = pool_and_classifier
            result = _cached_overview_result(
                ("utilization-summary", pool.provisioner, pool.worker_type, tuple(windows.items()), id(pc.storage)),
                lambda: pc.storage.get_utilization_summary(windows),
            )
            result.update({"availability_mode": pc.availability_mode})
            return f"{pool.provisioner}/{pool.worker_type}", result

        if not classifiers:
            return {}
        # Cold summaries are independent per pool. Match the previous browser
        # ceiling while keeping the whole operation behind one HTTP request.
        with ThreadPoolExecutor(max_workers=min(_overview_utilization_concurrency(), len(classifiers))) as executor:
            return dict(executor.map(summarize, classifiers))

    return _cached_overview_result(
        ("overview-utilization-summaries", pool_keys, tuple(windows.items())),
        calculate,
    )


def _snapshot_metadata(snapshot: dict) -> dict:
    """Expose the freshness boundary without leaking snapshot storage details."""
    return {
        "source_at": snapshot["source_at"],
        "generated_at": snapshot["generated_at"],
        "stale": datetime.now(timezone.utc) - _parse_utilization_datetime("source_at", snapshot["source_at"])
        > COVERAGE_STALE_AFTER,
    }


def _snapshot_freshness_label(metadata: dict) -> str:
    """Return the concise, user-facing freshness label for a snapshot."""
    source_at = _parse_utilization_datetime("source_at", metadata["source_at"])
    label = _timestamp_label("data from", source_at)
    return f"{label} (stale)" if metadata["stale"] else label


def _relative_age(timestamp: datetime, *, now: datetime | None = None) -> str:
    """Return a compact, human-readable elapsed-time label."""
    seconds = int(((now or datetime.now(timezone.utc)) - timestamp).total_seconds())
    if seconds < 0:
        return f"in {abs(seconds)}s"
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def _format_runtime(seconds: float) -> str:
    """Format a scan runtime compactly while retaining useful second precision."""
    whole_seconds = max(0, round(seconds))
    if whole_seconds < 60:
        return f"{whole_seconds}s"
    minutes, seconds = divmod(whole_seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _classify_all_pool_order(pools, overview_snapshot: dict | None) -> list:
    """Order pools by the workers refreshed per second in the prior full scan.

    Snapshot data is advisory: a first run, a partial preceding run, or a
    manually edited snapshot must retain deterministic provisioner/worker-type
    ordering rather than preventing a classify cycle from starting.
    """
    timings = (overview_snapshot or {}).get("payload", {}).get("pool_timings", {})

    def sort_key(pool) -> tuple:
        label = f"{pool.provisioner}/{pool.worker_type}"
        timing = timings.get(label, {}) if isinstance(timings, dict) else {}
        try:
            duration = timing["duration_seconds"]
            workers = timing["total_workers"]
        except (KeyError, TypeError, ValueError):
            return (1, 0.0, pool.provisioner, pool.worker_type)
        if (
            isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or isinstance(workers, bool)
            or not isinstance(workers, int)
            or not math.isfinite(duration)
            or duration <= 0
            or workers < 0
        ):
            return (1, 0.0, pool.provisioner, pool.worker_type)
        return (0, -(workers / duration), pool.provisioner, pool.worker_type)

    return sorted(pools, key=sort_key)


def _admin_dashboard_data(dsn: str) -> dict:
    """Read the small operational status dataset needed by the admin page."""
    with postgres_connect(dsn, "web") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version, applied_at FROM schema_migrations ORDER BY version")
            applied = {version: applied_at for version, applied_at in cur.fetchall()}
            cur.execute(
                "SELECT pool_id, source_at, generated_at FROM dashboard_snapshots "
                "WHERE scope = %s AND schema_version = %s",
                (POOL_SCOPE, 1),
            )
            snapshots = {
                pool_id: {"source_at": source_at, "generated_at": generated_at}
                for pool_id, source_at, generated_at in cur.fetchall()
            }
            cur.execute(
                "SELECT source_at, generated_at, payload FROM dashboard_snapshots "
                "WHERE scope = %s AND pool_id = '' AND schema_version = %s",
                (OVERVIEW_SCOPE, 1),
            )
            overview_row = cur.fetchone()

    migrations = [
        {"version": path.stem, "applied_at": applied.get(path.stem)}
        for path in sorted(MIGRATIONS_DIR.glob("*.sql"))
    ]
    overview = None
    if overview_row is not None:
        source_at, generated_at, payload = overview_row
        overview = {"source_at": source_at, "generated_at": generated_at, "payload": payload}
    return {"migrations": migrations, "snapshots": snapshots, "overview": overview}


def _timestamp_label(prefix: str, timestamp: datetime) -> str:
    """Format the timestamp shown in dashboard footers."""
    return f"{prefix} {timestamp.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"


def _read_dashboard_snapshot(dsn: str | None, scope: str, pool_id: str = "") -> dict | None:
    if _dashboard_snapshot_reads_disabled():
        return None
    if not dsn:
        return None
    try:
        return read_snapshot(dsn, scope, pool_id)
    except Exception as exc:  # noqa: BLE001 - snapshot absence must not hide the dashboard
        logger.warning("dashboard snapshot read failed for %s/%s: %s", scope, pool_id, exc)
        return None


def _dashboard_snapshot_reads_disabled() -> bool:
    """Return whether dashboard requests must render live instead of reading snapshots."""
    return os.environ.get("POOL_CLASSIFIER_DISABLE_DASHBOARD_SNAPSHOTS") == "1"


def _replace_detail_navigation(detail_html: str, navigation_html: str) -> str:
    """Replace a stored detail page's header with request-specific navigation."""
    return re.sub(
        r'<header class="site-header">.*?</header>',
        lambda _match: navigation_html,
        detail_html,
        count=1,
        flags=re.DOTALL,
    )


def create_app() -> Flask:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    app = Flask(__name__)
    app.jinja_env.filters["humanize_cron"] = _humanize_cron
    app.jinja_env.filters["format_lag"] = _format_lag
    app.jinja_env.filters["lag_color_class"] = _lag_color_class

    @app.after_request
    def add_debug_instance_identity(response: Response) -> Response:
        """Mark local HTML so concurrent instances are distinguishable."""
        if _instance_identity_enabled(app.debug) and response.mimetype == "text/html" and not response.is_streamed:
            response.set_data(
                _add_debug_instance_identity(response.get_data(as_text=True), request.environ.get("SERVER_PORT", "?"), app.debug),
            )
        return response

    @app.context_processor
    def navigation_context() -> dict[str, bool]:
        return {"show_admin_navigation": is_admin_iap_user_hint(request.headers)}

    def navigation_html(current: str) -> str:
        base_template = app.jinja_env.get_template("base.html")
        return str(
            base_template.module.navigation(
                current,
                show_admin_navigation=is_admin_iap_user_hint(request.headers),
            ),
        )

    def publish_pool_snapshot(pc: PoolClassifier, pool) -> None:
        """Build every fixed detail artifact before atomically publishing it."""
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            return
        generated_at = datetime.now(timezone.utc).replace(microsecond=0)
        summary = pc.storage.get_utilization_summary(UTILIZATION_WINDOWS)
        summary.update({"api_version": 1, "availability_mode": pc.availability_mode})
        timeline = None
        if data_through := summary.get("data_through"):
            end = _parse_utilization_datetime("data_through", data_through)
            timeline = pc.storage.get_utilization(
                (end - timedelta(hours=24)).isoformat(), end.isoformat(), 3600,
            )
            timeline.update({"api_version": 1, "availability_mode": pc.availability_mode})
        lag_end = generated_at
        lag = pc.storage.get_observed_start_lag_visualization(
            (lag_end - timedelta(days=7)).isoformat(), lag_end.isoformat(),
            DEFAULT_OBSERVED_START_LAG_SLO_SECONDS, DEFAULT_OBSERVED_START_LAG_MIN_SAMPLES,
        )
        lag["api_version"] = 1
        job_sources_end = generated_at
        job_sources_start = job_sources_end - timedelta(days=14)
        job_sources = {
            "api_version": 1,
            "start_at": job_sources_start.isoformat(),
            "end_at": job_sources_end.isoformat(),
            "days": 14,
            "buckets": pc.storage.get_job_source_volume(
                job_sources_start.isoformat(), job_sources_end.isoformat(),
            ),
        }
        detail_html = pc.render_html(
            os_label=detect_os(pool),
            navigation_html=navigation_html(f"{pool.provisioner}/{pool.worker_type}"),
            navigation_styles=str(app.jinja_env.get_template("base.html").module.navigation_styles()),
        )
        write_snapshot(
            dsn,
            POOL_SCOPE,
            {
                "detail_html": detail_html,
                "utilization_summary": summary,
                "utilization_timeline_24h": timeline,
                "observed_start_lag_visualization_7d": lag,
                "job_source_volume_14d": job_sources,
            },
            pool_id=f"{pool.provisioner}/{pool.worker_type}",
            source_at=generated_at,
        )

    def publish_overview_snapshot(
        *,
        classify_all_started_at: datetime | None = None,
        classify_all_started_monotonic: float | None = None,
        pool_timings: dict[str, dict] | None = None,
    ) -> None:
        """Publish the fixed overview only after a complete aggregate scan."""
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            return
        pool_ids = tuple(
            f"{pool.provisioner}/{pool.worker_type}"
            for pool in registry.all_pools_including_disabled()
            if pool.enabled
        )
        generated_at = datetime.now(timezone.utc).replace(microsecond=0)
        _reset_overview_cache()
        payload = {
            "pool_summaries": _global_pool_summaries(dsn, pool_ids),
            "lag_summaries": _global_observed_start_lag_summaries(dsn),
            "utilization_summaries": _overview_utilization_summaries({"1h": 3600, "24h": 86400}),
        }
        if classify_all_started_at is not None and classify_all_started_monotonic is not None and pool_timings is not None:
            completed_at = datetime.now(timezone.utc)
            payload.update(
                {
                    "classify_all_started_at": classify_all_started_at.isoformat(),
                    "classify_all_completed_at": completed_at.isoformat(),
                    "classify_all_duration_seconds": monotonic() - classify_all_started_monotonic,
                    "pool_timings": pool_timings,
                }
            )
        write_snapshot(dsn, OVERVIEW_SCOPE, payload, source_at=generated_at)

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

    @app.errorhandler(401)
    def unauthorized(_error):
        return render_template(
            "access_error.html",
            title="Authentication required",
            heading="Authentication required",
            message="Sign in through the dashboard access proxy, then try again.",
        ), 401

    @app.errorhandler(403)
    def forbidden(_error):
        return render_template(
            "access_error.html",
            title="Access denied",
            heading="Access denied",
            message="Your account is not authorized to view this page.",
        ), 403

    @app.errorhandler(404)
    def not_found(_error):
        return render_template("not_found.html"), 404

    @app.get("/admin")
    @require_admin_iap
    def admin():
        now = datetime.now(timezone.utc)
        database_error = None
        data = {"migrations": [], "snapshots": {}}
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            database_error = "DATABASE_URL is not configured"
        else:
            try:
                data = _admin_dashboard_data(dsn)
            except Exception as exc:  # noqa: BLE001 - return an authenticated diagnostic page
                logger.warning("admin: status query failed: %s", exc)
                database_error = "Database status is unavailable"

        migration_rows = data["migrations"]
        pools = registry.all_pools_including_disabled()
        pools_by_id = {f"{pool.provisioner}/{pool.worker_type}": pool for pool in pools}
        snapshot_rows = []
        for pool in pools:
            pool_id = f"{pool.provisioner}/{pool.worker_type}"
            snapshot = data["snapshots"].get(pool_id)
            source_at = snapshot["source_at"] if snapshot else None
            snapshot_rows.append(
                {
                    "pool_id": pool_id,
                    "pool": pool,
                    "enabled": pool.enabled,
                    "source_at": source_at,
                    "age": _relative_age(source_at, now=now) if source_at else "never",
                    "stale": bool(source_at and now - source_at > COVERAGE_STALE_AFTER),
                }
            )
        overview_payload = (data.get("overview") or {}).get("payload", {})
        completion_value = overview_payload.get("classify_all_completed_at")
        completed_at = _parse_utilization_datetime("classify_all_completed_at", completion_value) if completion_value else None
        pool_timings = [
            {
                "pool_id": pool_id,
                "pool": pools_by_id.get(pool_id),
                "duration_seconds": timing["duration_seconds"],
                "duration": _format_runtime(timing["duration_seconds"]),
                "completed_at": _parse_utilization_datetime("pool completed_at", timing["completed_at"]),
            }
            for pool_id, timing in overview_payload.get("pool_timings", {}).items()
        ]
        pool_timings.sort(key=lambda timing: timing["duration_seconds"], reverse=True)
        return render_template(
            "admin.html",
            runtime_mode={
                "debug_enabled": app.debug,
                "detail_pages_live": _dashboard_snapshot_reads_disabled(),
                "request_host": request.host,
            },
            migrations=migration_rows,
            snapshots=snapshot_rows,
            classify_all_duration=_format_runtime(overview_payload["classify_all_duration_seconds"])
            if "classify_all_duration_seconds" in overview_payload
            else None,
            classify_all_completed_at=completed_at,
            pool_timings=pool_timings,
            database_error=database_error,
            generated=now,
        )

    @app.get("/favicon.ico")
    def favicon():
        return app.send_static_file("favicon.svg")

    @app.get("/")
    def index():
        now_dt = datetime.now(timezone.utc)
        now = _timestamp_label("generated on", now_dt)
        # One pair of GROUP BY pool_id queries for every pool, on one connection
        # (vs ~7 queries per pool on a per-pool connection). See the dashboard
        # query refactor history in docs/history/dashboard-query-refactor.md.
        summaries: dict = {}
        lag_summaries: dict = {}
        dsn = os.environ.get("DATABASE_URL")
        if dsn:
            snapshot = _read_dashboard_snapshot(dsn, OVERVIEW_SCOPE)
            if snapshot:
                payload = snapshot["payload"]
                summaries = payload.get("pool_summaries", {})
                lag_summaries = payload.get("lag_summaries", {})
                metadata = _snapshot_metadata(snapshot)
                now = _snapshot_freshness_label(metadata)
            else:
                overview_pool_ids = tuple(
                    f"{pool.provisioner}/{pool.worker_type}"
                    for pool in registry.all_pools_including_disabled()
                    if pool.enabled
                )
                try:
                    summaries = _cached_overview_result(
                        ("pool-summaries", dsn, overview_pool_ids, ("1h", "24h")),
                        lambda: _global_pool_summaries(dsn, overview_pool_ids),
                    )
                except Exception as e:
                    logger.warning("index: current pool summaries failed: %s", e)
                try:
                    lag_summaries = _cached_overview_result(
                        ("observed-start-lag", dsn, "7d"),
                        lambda: _global_observed_start_lag_summaries(dsn),
                    )
                except Exception as e:
                    logger.warning("index: observed_start_lag_summaries_global failed: %s", e)

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
                        "start_lag": None,
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
                workers, alerting = s["workers"], s["alerting"]
                oldest = s["task_collection_started"] or s["oldest"]
                latest = s["collection_latest"] or s["latest"]
                collection_latest = s["collection_latest"]
                errors_per_host_1h, success_rate_1h = _eph(s["err_1h"], workers), _sr(s["err_1h"], s["ok_1h"])
                errors_per_host_24h, success_rate_24h = _eph(s["err_24h"], workers), _sr(s["err_24h"], s["ok_24h"])
            coverage, coverage_seconds = _coverage_label(oldest, latest, now_dt, collection_latest)
            lag = lag_summaries.get(f"{pool.provisioner}/{pool.worker_type}")
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
                    "start_lag": lag,
                },
            )
        return render_template(
            "index.html",
            pools=rows,
            generated=now,
        )

    @app.get("/patterns")
    def patterns():
        hits: dict[str, int] = {}
        try:
            dsn = os.environ.get("DATABASE_URL")
            if dsn:
                hits = _cached_overview_result(
                    ("category-hits", dsn, "24h"),
                    lambda: count_category_hits_global(
                        dsn,
                        (datetime.now(timezone.utc) - timedelta(hours=24)).replace(microsecond=0).isoformat(),
                    ),
                )
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
            generated=_timestamp_label("generated on", datetime.now(timezone.utc)),
        )

    @app.get("/pool-discovery")
    def pool_discovery():
        return render_template("coverage.html", data=discovery.discover())

    @app.post("/pool-discovery/refetch")
    def pool_discovery_refetch():
        return render_template("coverage.html", data=discovery.discover(force=True))

    @app.get("/api")
    def api_overview():
        return render_template("api.html")

    @app.get("/api/v1")
    def api_v1_discovery():
        return jsonify(
            {
                "api_version": 1,
                "endpoints": [
                    {"path": "/api/v1/pools", "description": "Configured pool discovery."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/summary", "description": "Pool health summary."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/failures", "description": "Failure category counts."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/workers", "description": "Filterable, paginated worker health."},
                    {"path": "/api/v1/patterns", "description": "Classification-pattern registry."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/utilization", "description": "Duration-weighted utilization."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/utilization/summary", "description": "Standard utilization windows."},
                    {"path": "/api/v1/overview/utilization", "description": "Batched overview utilization summaries."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/observed-start-lag", "description": "Observed task-run start lag."},
                    {"path": "/api/v1/pools/{provisioner}/{worker_type}/observed-start-lag/visualization", "description": "Chart-ready observed start lag."},
                ],
            },
        )

    @app.get("/api/v1/pools")
    def pools_api():
        return jsonify({"api_version": 1, "pools": [_pool_config(pool) for pool in registry.all_pools_including_disabled()]})

    @app.get("/api/v1/patterns")
    def patterns_api():
        return jsonify(
            {
                "api_version": 1,
                "patterns": [
                    {
                        "name": pattern.name,
                        "severity": pattern.severity,
                        "tags": pattern.tags,
                        "description": pattern.description,
                        "enabled": pattern.enabled,
                    }
                    for pattern in patterns_registry._patterns  # noqa: SLF001 - public registry includes disabled patterns
                ],
            },
        )

    @app.get("/about")
    def about():
        commit = os.environ.get("POOL_CLASSIFIER_COMMIT", "unknown")
        return render_template(
            "about.html",
            repository_url=REPOSITORY_URL,
            version=_application_version(),
            commit=commit,
            commit_url=f"{REPOSITORY_URL}/commit/{commit}" if commit != "unknown" else None,
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
        snapshot = _read_dashboard_snapshot(
            os.environ.get("DATABASE_URL"), POOL_SCOPE, f"{provisioner}/{worker_type}",
        )
        if snapshot and (detail_html := snapshot["payload"].get("detail_html")):
            metadata = _snapshot_metadata(snapshot)
            detail_html = _replace_detail_navigation(
                detail_html,
                navigation_html(f"{provisioner}/{worker_type}"),
            )
            detail_html = detail_html.replace('<p class="footer">Generated: ', '<p class="footer">data from ', 1)
            detail_html = detail_html.replace('<p class="footer">generated on ', '<p class="footer">data from ', 1)
            if metadata["stale"]:
                detail_html = detail_html.replace(
                    "</body>", f'<p class="gen">{_snapshot_freshness_label(metadata)}</p></body>', 1,
                )
            response = Response(
                detail_html,
                content_type="text/html; charset=utf-8",
            )
            response.headers["X-Pool-Classifier-Snapshot-Source"] = snapshot["source_at"]
            return response
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            abort(404)
        os_label = detect_os(pool)
        return Response(
            pc.render_html(
                os_label=os_label,
                navigation_html=navigation_html(f"{provisioner}/{worker_type}"),
                navigation_styles=str(app.jinja_env.get_template("base.html").module.navigation_styles()),
            ),
            content_type="text/html; charset=utf-8",
        )

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
        snapshot = _read_dashboard_snapshot(
            os.environ.get("DATABASE_URL"), POOL_SCOPE, f"{provisioner}/{worker_type}",
        )
        if snapshot:
            timeline = snapshot["payload"].get("utilization_timeline_24h")
            if timeline and (timeline["start_at"], timeline["end_at"], timeline["bucket_seconds"]) == (start, end, bucket_seconds):
                result = dict(timeline)
                result["snapshot"] = _snapshot_metadata(snapshot)
                return jsonify(result)
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        result = pc.storage.get_utilization(start, end, bucket_seconds)
        result["api_version"] = 1
        result["availability_mode"] = pc.availability_mode
        return jsonify(result)

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/summary")
    def pool_summary(provisioner: str, worker_type: str):
        pool = registry.get_pool(provisioner, worker_type)
        if pool is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        summaries = {}
        dsn = os.environ.get("DATABASE_URL")
        if dsn:
            now = datetime.now(timezone.utc).replace(microsecond=0)
            try:
                summaries = _cached_overview_result(
                    ("pool-summaries", dsn, (f"{provisioner}/{worker_type}",), ("1h", "24h")),
                    lambda: _global_pool_summaries(dsn, (f"{provisioner}/{worker_type}",)),
                )
            except Exception as exc:  # noqa: BLE001 - read-only dashboard endpoint remains available
                logger.warning("pool summary: aggregate query failed: %s", exc)
        else:
            now = datetime.now(timezone.utc).replace(microsecond=0)
        return jsonify(_public_pool_summary(pool, summaries.get(f"{provisioner}/{worker_type}"), now))

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/failures")
    def pool_failures(provisioner: str, worker_type: str):
        try:
            start, end = _bounded_failure_window()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        category = request.args.get("category") or None
        return jsonify(
            {
                "api_version": 1,
                "pool_id": f"{provisioner}/{worker_type}",
                "start_at": start,
                "end_at": end,
                "category": category,
                "failures": pc.storage.get_public_failures(start, end, category),
            },
        )

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/coverage-breaks")
    def pool_coverage_breaks(provisioner: str, worker_type: str):
        try:
            start, end = _bounded_failure_window()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        return jsonify({
            "api_version": 1,
            "pool_id": f"{provisioner}/{worker_type}",
            "start_at": start,
            "end_at": end,
            "events": pc.storage.list_task_run_coverage_events(start, end),
        })

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/workers")
    def pool_workers(provisioner: str, worker_type: str):
        try:
            start, end, quarantined, alerting, category, limit, after = _workers_parameters()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        workers = pc.storage.get_public_workers(
            start, end, CONSECUTIVE_FAILURE_ALERT, quarantined, alerting, category, limit + 1, after,
        )
        has_next = len(workers) > limit
        workers = workers[:limit]
        next_cursor = None
        if has_next:
            last = workers[-1]
            next_cursor = _workers_cursor(bool(last["alerting"]), last["worker_id"])
        return jsonify(
            {
                "api_version": 1,
                "pool_id": f"{provisioner}/{worker_type}",
                "start_at": start,
                "end_at": end,
                "filters": {"quarantined": quarantined, "alerting": alerting, "category": category},
                "pagination": {"limit": limit, "next_cursor": next_cursor},
                "workers": workers,
            },
        )

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/utilization/summary")
    def pool_utilization_summary(provisioner: str, worker_type: str):
        try:
            windows = _utilization_summary_windows()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        snapshot = _read_dashboard_snapshot(
            os.environ.get("DATABASE_URL"), POOL_SCOPE, f"{provisioner}/{worker_type}",
        )
        if snapshot and (stored := snapshot["payload"].get("utilization_summary")):
            result = dict(stored)
            result["windows"] = {name: stored.get("windows", {}).get(name) for name in windows}
            result["snapshot"] = _snapshot_metadata(snapshot)
            return jsonify(result)
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        result = _cached_overview_result(
            ("utilization-summary", provisioner, worker_type, tuple(windows.items()), id(pc.storage)),
            lambda: pc.storage.get_utilization_summary(windows),
        )
        result.update({"api_version": 1, "availability_mode": pc.availability_mode})
        return jsonify(result)

    @app.get("/api/v1/overview/utilization")
    def overview_utilization_summaries():
        try:
            windows = _utilization_summary_windows()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        snapshot = _read_dashboard_snapshot(os.environ.get("DATABASE_URL"), OVERVIEW_SCOPE)
        if (
            snapshot
            and set(windows) <= {"1h", "24h"}
            and (stored := snapshot["payload"].get("utilization_summaries"))
        ):
            pools = {
                pool_id: {
                    **summary,
                    "windows": {name: summary.get("windows", {}).get(name) for name in windows},
                }
                for pool_id, summary in stored.items()
            }
            return jsonify({
                "api_version": 1,
                "windows": list(windows),
                "pools": pools,
                "snapshot": _snapshot_metadata(snapshot),
            })
        return jsonify({"api_version": 1, "windows": list(windows), "pools": _overview_utilization_summaries(windows)})

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/observed-start-lag")
    def pool_observed_start_lag(provisioner: str, worker_type: str):
        try:
            start, end, slo_seconds = _observed_start_lag_parameters()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        result = pc.storage.get_observed_start_lag(start, end, slo_seconds)
        result["api_version"] = 1
        return jsonify(result)

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/job-sources")
    def pool_job_sources(provisioner: str, worker_type: str):
        try:
            days = int(request.args.get("days", "7"))
        except ValueError:
            days = 0
        if days not in {7, 14}:
            return jsonify({"error": {"code": "invalid_parameter", "message": "days must be 7 or 14"}}), 400
        snapshot = _read_dashboard_snapshot(
            os.environ.get("DATABASE_URL"), POOL_SCOPE, f"{provisioner}/{worker_type}",
        )
        if snapshot and (stored := snapshot["payload"].get("job_source_volume_14d")):
            try:
                if stored["days"] != 14:
                    raise ValueError("expected a 14-day payload")
                start = _parse_utilization_datetime("start_at", stored["start_at"])
                end = _parse_utilization_datetime("end_at", stored["end_at"])
                if end - start != timedelta(days=14):
                    raise ValueError("expected a 14-day range")
                buckets = stored["buckets"]
                if not isinstance(buckets, list):
                    raise ValueError("buckets must be a list")
                final_days = sorted({bucket["day"] for bucket in buckets})[-7:]
            except (AttributeError, KeyError, TypeError, ValueError):
                logger.warning("invalid job-source data in snapshot for %s/%s", provisioner, worker_type)
            else:
                if days == 7:
                    buckets = [bucket for bucket in buckets if bucket["day"] in final_days]
                    start = end - timedelta(days=7)
                return jsonify({
                    "api_version": 1,
                    "start_at": start.isoformat(),
                    "end_at": end.isoformat(),
                    "days": days,
                    "buckets": buckets,
                    "snapshot": _snapshot_metadata(snapshot),
                })
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        return jsonify({"api_version": 1, "start_at": start.isoformat(), "end_at": end.isoformat(), "days": days,
                        "buckets": pc.storage.get_job_source_volume(start.isoformat(), end.isoformat())})

    @app.get("/api/v1/pools/<provisioner>/<worker_type>/observed-start-lag/visualization")
    def pool_observed_start_lag_visualization(provisioner: str, worker_type: str):
        if request.args.get("start") is None and request.args.get("end") is None:
            snapshot = _read_dashboard_snapshot(
                os.environ.get("DATABASE_URL"), POOL_SCOPE, f"{provisioner}/{worker_type}",
            )
            if snapshot and (stored := snapshot["payload"].get("observed_start_lag_visualization_7d")):
                result = dict(stored)
                result["snapshot"] = _snapshot_metadata(snapshot)
                return jsonify(result)
        try:
            start, end, slo_seconds = _observed_start_lag_parameters(default_window_seconds=7 * 24 * 60 * 60)
            min_samples = _observed_start_lag_min_samples()
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_parameter", "message": str(exc)}}), 400
        pc = _get_classifier(provisioner, worker_type)
        if pc is None:
            return jsonify({"error": {"code": "not_found", "message": "pool not found"}}), 404
        result = pc.storage.get_observed_start_lag_visualization(start, end, slo_seconds, min_samples)
        result["api_version"] = 1
        return jsonify(result)

    @app.get("/pools/<provisioner>/<worker_type>/utilization-api-guide")
    def pool_utilization_guide(provisioner: str, worker_type: str):
        pool = registry.get_pool(provisioner, worker_type)
        if pool is None:
            abort(404)
        return render_template("utilization_guide.html", pool=pool)

    @app.post("/classify/<provisioner>/<worker_type>")
    @require_scheduler_oidc
    def classify(provisioner: str, worker_type: str):
        pc = _get_classifier(provisioner, worker_type, role="classifier")
        if pc is None:
            abort(404)
        try:
            summary = pc.classify_cycle()
            pool = registry.get_pool(provisioner, worker_type)
            if pool is not None:
                try:
                    publish_pool_snapshot(pc, pool)
                except Exception:  # noqa: BLE001 - a snapshot failure must retain the prior view
                    logger.exception("classify: snapshot build failed for %s/%s", provisioner, worker_type)
        except ClassifyLockBusy:
            return jsonify({"error": "classify cycle already running for this pool"}), 409
        return jsonify(summary)

    @app.post("/classify-all")
    @require_scheduler_oidc
    def classify_all():
        # Sequential fan-out over all enabled pools, driven by a single Cloud
        # Scheduler job. It runs one pool at a time on purpose — concurrent
        # per-pool jobs exhausted the Postgres connection budget. The last
        # complete scan ranks pools by workers refreshed per second; missing or
        # invalid history falls back to stable registry order. Per-pool failures
        # are caught so one bad pool doesn't abort the run; the advisory lock
        # makes overlapping runs safe (busy pools are skipped).
        previous_overview = _read_dashboard_snapshot(
            os.environ.get("DATABASE_URL"), OVERVIEW_SCOPE,
        )
        pools = _classify_all_pool_order(registry.all_pools(), previous_overview)
        classify_all_started_at = datetime.now(timezone.utc)
        classify_all_started_monotonic = monotonic()
        results = []
        pool_timings = {}
        for pool in pools:
            label = f"{pool.provisioner}/{pool.worker_type}"
            try:
                pc = _get_classifier(pool.provisioner, pool.worker_type, role="classifier")
                if pc is None:
                    results.append({"pool": label, "status": "not_found"})
                    continue
                pool_started_at = datetime.now(timezone.utc)
                pool_started_monotonic = monotonic()
                summary = pc.classify_cycle()
                try:
                    publish_pool_snapshot(pc, pool)
                except Exception:  # noqa: BLE001 - do not turn a successful scan into a retry
                    logger.exception("classify-all: snapshot build failed for %s", label)
                pool_completed_at = datetime.now(timezone.utc)
                timing = {
                    "started_at": pool_started_at.isoformat(),
                    "completed_at": pool_completed_at.isoformat(),
                    "duration_seconds": monotonic() - pool_started_monotonic,
                }
                if isinstance(summary.get("total_workers"), int) and not isinstance(summary["total_workers"], bool):
                    timing["total_workers"] = summary["total_workers"]
                pool_timings[label] = timing
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
        if counts["ok"] == len(pools):
            try:
                publish_overview_snapshot(
                    classify_all_started_at=classify_all_started_at,
                    classify_all_started_monotonic=classify_all_started_monotonic,
                    pool_timings=pool_timings,
                )
            except Exception:  # noqa: BLE001 - retain the previous aggregate snapshot
                logger.exception("classify-all: overview snapshot build failed")
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
