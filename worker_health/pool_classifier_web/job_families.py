"""Identify job families from exact Taskcluster names and job kind."""

import re
from collections import defaultdict
from datetime import datetime, timedelta

from worker_health.pool_classifier_web.utilization import _is_complete, _merge, _parse


WORKLOAD_RANGES = {
    "3h": (timedelta(hours=3), 900),
    "12h": (timedelta(hours=12), 1800),
    "24h": (timedelta(days=1), 3600),
    "3d": (timedelta(days=3), 10800),
    "7d": (timedelta(days=7), 25200),
}
WORKLOAD_BASE_SECONDS = 900
OTHER_PERFORMANCE_TESTS = "Other performance tests"


def build_job_family_trend(storage, start: datetime, end: datetime, bucket_seconds: int) -> dict:
    """Roll up the selected window and keep a common scale based on 15-minute peaks."""
    scale_start = end - WORKLOAD_RANGES["7d"][0]
    base_rows = storage.get_job_family_buckets(scale_start.isoformat(), end.isoformat(), WORKLOAD_BASE_SECONDS)
    raw_coverage = storage.get_collection_coverage("task_runs", start.isoformat(), end.isoformat())
    coverage = _merge((_parse(interval["start_at"]), _parse(interval["end_at"])) for interval in raw_coverage["intervals"])
    first_bin = (start - scale_start) // timedelta(seconds=WORKLOAD_BASE_SECONDS)
    bins_per_bucket = bucket_seconds // WORKLOAD_BASE_SECONDS
    totals = defaultdict(lambda: defaultdict(float))
    for row in base_rows:
        if row["bucket"] >= first_bin:
            bucket = (row["bucket"] - first_bin) // bins_per_bucket
            totals[bucket][row["job_family"]] += row["worker_seconds"]

    step = timedelta(seconds=bucket_seconds)
    buckets = []
    for index in range((end - start) // step):
        bucket_start = start + index * step
        buckets.append({
            "start_at": bucket_start.isoformat(),
            "covered": _is_complete(coverage, bucket_start, bucket_start + step),
            "worker_seconds": dict(totals[index]),
        })
    return {
        "bucket_seconds": bucket_seconds,
        "scale_peak_workers": max((row["worker_seconds"] / WORKLOAD_BASE_SECONDS for row in base_rows), default=0),
        "buckets": buckets,
    }


_FAMILIES = tuple(
    (family, re.compile(rf"(?<![a-z0-9])(?:{pattern})(?![a-z0-9])"))
    for family, pattern in (
        ("Page load", r"tp6m?|pageload|page-load"),
        ("Video playback", r"video-playback|youtube-playback|youtubep"),
        ("Startup", r"startup|cold-start|warm-start"),
        ("Benchmarks", r"speedometer[0-9]*|jetstream[0-9]*|motionmark[0-9]*|ares6|benchmark|nav-bench"),
        ("Networking", r"http3|h3|doh|trr|networking|network-bench"),
        ("Resource and power", r"resource-usage|foreground-resource|background-resource|power|battery"),
        ("Mochitest media", r"mochitest-media"),
        ("Mochitest", r"mochitest"),
        ("JS jitest", r"jittest|jit-test|jitest|jsreftest"),
        ("WebGL", r"webgl[0-9]*"),
        ("WebRender", r"webrender"),
        ("Crashtest", r"crashtest"),
        ("Reftest", r"reftest"),
        ("Xpcshell", r"xpcshell"),
        ("Web platform tests", r"web-platform-tests|wpt"),
        ("GTest", r"gtest"),
        ("Marionette", r"marionette"),
        ("Test Verify", r"test-verify"),
        ("Source tests", r"^source-test"),
        ("Talos", r"talos"),
        (OTHER_PERFORMANCE_TESTS, r"browsertime|raptor"),
    )
)


def job_family_for_name(name: str) -> str | None:
    label = name.lower()
    for family, pattern in _FAMILIES:
        if pattern.search(label):
            return family
    return None


def task_job_metadata(task: dict | None) -> tuple[str | None, str | None]:
    """Keep the exact name and a suite label, with an explicit Unknown fallback."""
    if task is None:
        return None, None
    metadata = task.get("metadata")
    name = metadata.get("name") if isinstance(metadata, dict) else None
    name = name if isinstance(name, str) and name.strip() else None
    extra = task.get("extra")
    treeherder = extra.get("treeherder") if isinstance(extra, dict) else None
    kind = treeherder.get("jobKind") if isinstance(treeherder, dict) else None
    if kind == "build" or (name and name.lower().startswith("build-")):
        return name, "Builds"
    family = job_family_for_name(name) if name else None
    return name, family or ("Other tests" if kind == "test" else "Unknown")
