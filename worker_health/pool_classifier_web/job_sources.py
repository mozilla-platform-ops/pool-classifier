"""Derive compact, reviewable job-source labels from Taskcluster task definitions."""

from __future__ import annotations

from enum import IntEnum
from typing import Any, NamedTuple


AUDIT_WORKER_SOURCE = "https://github.com/taskcluster/mozilla-history/tree/master/audit-worker-versions"


class SourceMethod(IntEnum):
    PROJECT_TAG = 0
    METADATA_SOURCE = 1
    MISSING_PROJECT_TAG = 2
    TASK_FETCH_FAILED = 3


class JobSource(NamedTuple):
    source: str
    method: SourceMethod


def classify_job_source(task: dict[str, Any] | None) -> JobSource:
    """Classify without scheduler-name inference or retaining the task payload."""
    if task is None:
        return JobSource("unknown", SourceMethod.TASK_FETCH_FAILED)
    tags = task.get("tags")
    project = tags.get("project") if isinstance(tags, dict) else None
    if isinstance(project, str) and project.strip():
        return JobSource(project, SourceMethod.PROJECT_TAG)
    metadata = task.get("metadata")
    if isinstance(metadata, dict) and metadata.get("source") == AUDIT_WORKER_SOURCE:
        return JobSource("audit-worker", SourceMethod.METADATA_SOURCE)
    return JobSource("unknown", SourceMethod.MISSING_PROJECT_TAG)
