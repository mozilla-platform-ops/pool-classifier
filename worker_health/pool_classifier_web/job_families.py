"""Identify job families from exact Taskcluster names and job kind."""

import re


_FAMILIES = tuple(
    (family, re.compile(rf"(?<![a-z0-9])(?:{pattern})(?![a-z0-9])"))
    for family, pattern in (
        ("Page load", r"tp6m?|pageload|page-load"),
        ("Video playback", r"video-playback|youtube-playback|youtubep"),
        ("Startup", r"startup|cold-start|warm-start"),
        ("Benchmarks", r"speedometer[0-9]*|jetstream[0-9]*|motionmark[0-9]*|ares6|benchmark"),
        ("Networking", r"http3|h3|doh|trr|networking"),
        ("Resource and power", r"resource-usage|power|battery"),
        ("Mochitest media", r"mochitest-media"),
        ("Mochitest", r"mochitest"),
        ("JS jitest", r"jittest|jit-test|jitest|jsreftest"),
        ("WebGL", r"webgl[0-9]*"),
        ("WebRender", r"webrender"),
        ("Crashtest", r"crashtest"),
        ("Reftest", r"reftest"),
        ("Xpcshell", r"xpcshell"),
        ("Web platform tests", r"web-platform-tests|wpt"),
        ("Other performance tests", r"browsertime|raptor|talos"),
    )
)


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
    if name:
        label = name.lower()
        for family, pattern in _FAMILIES:
            if pattern.search(label):
                return name, family
    return name, "Other tests" if kind == "test" else "Unknown"
