#!/usr/bin/env python3
"""Benchmark the overview and detail-page request workflows against a running app.

The first run measures the current cache state (normally cold after a process
restart); later runs capture warm-cache behavior. Save the JSON output before a
change and compare it with a run after the change.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any
from urllib.parse import urlencode

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_POOL = "releng-hardware/gecko-t-osx-1500-m4"


def request(session: requests.Session, name: str, url: str, timeout: float) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Fetch one endpoint and return its timing record plus decoded JSON if any."""
    started = perf_counter()
    try:
        response = session.get(url, timeout=timeout)
        elapsed_ms = round((perf_counter() - started) * 1000, 1)
        record = {
            "name": name,
            "url": url,
            "status": response.status_code,
            "elapsed_ms": elapsed_ms,
            "bytes": len(response.content),
        }
        if response.ok and response.headers.get("content-type", "").startswith("application/json"):
            return record, response.json()
        return record, None
    except requests.RequestException as exc:
        return {
            "name": name,
            "url": url,
            "status": None,
            "elapsed_ms": round((perf_counter() - started) * 1000, 1),
            "bytes": 0,
            "error": str(exc),
        }, None


def request_with_new_session(name: str, url: str, timeout: float) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Fetch concurrently without sharing a requests session across threads."""
    with requests.Session() as session:
        return request(session, name, url, timeout)


def percentile(values: list[float], fraction: float) -> float:
    """Return a nearest-rank percentile without requiring a large sample."""
    return sorted(values)[max(0, int((len(values) * fraction) - 0.000001))]


def url_with_query(base_url: str, path: str, query: dict[str, str]) -> str:
    """Build a URL with correctly encoded ISO 8601 query values."""
    return f"{base_url}{path}?{urlencode(query)}"


def summarize(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build comparable latency summaries for each named request."""
    grouped: dict[str, list[float]] = {}
    for record in records:
        if record["status"] == 200:
            grouped.setdefault(record["name"], []).append(record["elapsed_ms"])
    return [
        {
            "name": name,
            "count": len(values),
            "min_ms": min(values),
            "median_ms": round(median(values), 1),
            "p95_ms": percentile(values, 0.95),
            "max_ms": max(values),
        }
        for name, values in sorted(grouped.items())
    ]


def benchmark_run(session: requests.Session, base_url: str, pool: str, timeout: float) -> list[dict[str, Any]]:
    """Measure the browser request sequence for the overview and one detail page."""
    records: list[dict[str, Any]] = []
    detail_url = f"{base_url}/pools/{pool}"
    summary_url = f"{base_url}/api/v1/pools/{pool}/utilization/summary"
    now = datetime.now(timezone.utc)
    lag_url = url_with_query(
        base_url,
        f"/api/v1/pools/{pool}/observed-start-lag/visualization",
        {
            "start": (now - timedelta(days=7)).isoformat(),
            "end": now.isoformat(),
            "min_samples": "5",
        },
    )

    overview, _ = request(session, "overview_html", f"{base_url}/", timeout)
    records.append(overview)
    overview_utilization, _ = request(
        session,
        "overview_utilization_batch",
        f"{base_url}/api/v1/overview/utilization?windows=1h,24h",
        timeout,
    )
    records.append(overview_utilization)

    detail, _ = request(session, "detail_html", detail_url, timeout)
    records.append(detail)
    with ThreadPoolExecutor(max_workers=2) as executor:
        summary_future = executor.submit(request_with_new_session, "detail_utilization_summary", summary_url, timeout)
        lag_future = executor.submit(request_with_new_session, "detail_lag_visualization", lag_url, timeout)
        summary, summary_json = summary_future.result()
        lag, _ = lag_future.result()
    records.extend((summary, lag))

    data_through = (summary_json or {}).get("data_through")
    if data_through:
        end = datetime.fromisoformat(data_through.replace("Z", "+00:00"))
        timeline_url = url_with_query(
            base_url,
            f"/api/v1/pools/{pool}/utilization",
            {
                "start": (end - timedelta(hours=24)).isoformat(),
                "end": end.isoformat(),
                "bucket_seconds": "3600",
            },
        )
        timeline, _ = request(session, "detail_utilization_timeline_24h", timeline_url, timeout)
        records.append(timeline)
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--pool", default=DEFAULT_POOL, help="provisioner/worker-type")
    parser.add_argument("--runs", type=int, default=3, help="number of complete page workflows")
    parser.add_argument("--timeout", type=float, default=180, help="per-request timeout in seconds")
    parser.add_argument("--output", type=Path, help="write machine-readable results to this JSON file")
    args = parser.parse_args()
    if args.runs < 1 or args.timeout <= 0:
        parser.error("runs and timeout must be positive")

    base_url = args.base_url.rstrip("/")
    all_records: list[dict[str, Any]] = []
    with requests.Session() as session:
        for run in range(1, args.runs + 1):
            for record in benchmark_run(session, base_url, args.pool, args.timeout):
                record["run"] = run
                all_records.append(record)

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "pool": args.pool,
        "runs": args.runs,
        "requests": all_records,
        "summary": summarize(all_records),
    }
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{encoded}\n")
    print(encoded)
    return 0 if all(record["status"] == 200 for record in all_records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
