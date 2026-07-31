"""PostgreSQL connection identity helpers for operational diagnosis."""

from __future__ import annotations

import hashlib
import os
import re
from typing import Any


_MAX_APPLICATION_NAME_BYTES = 63
_SAFE_COMPONENT = re.compile(r"[^A-Za-z0-9_.-]+")


def _component(value: str, fallback: str, limit: int) -> str:
    """Return a readable, bounded identifier with a stable collision suffix."""
    normalized = _SAFE_COMPONENT.sub("-", value).strip("-._") or fallback
    if len(normalized) <= limit:
        return normalized
    digest = hashlib.sha256(normalized.encode()).hexdigest()[:8]
    return f"{normalized[:limit - len(digest) - 1]}-{digest}"


def application_name(role: str) -> str:
    """Identify this backend by workload, Cloud Run revision, and instance.

    PostgreSQL limits ``application_name`` to 63 bytes.  The components are
    ASCII-normalized and independently shortened with hashes, so operators can
    still distinguish long Cloud Run revision and instance names in
    ``pg_stat_activity``.
    """
    role_name = _component(role, "unknown", 16)
    revision = _component(os.environ.get("K_REVISION", ""), "local", 15)
    instance = _component(os.environ.get("HOSTNAME", ""), "local", 15)
    name = f"pool-classifier:{role_name}:r={revision}:i={instance}"
    assert len(name.encode()) <= _MAX_APPLICATION_NAME_BYTES
    return name


def connect(dsn: str, role: str, **kwargs: Any):
    """Open a direct psycopg connection with the standard identity."""
    import psycopg

    return psycopg.connect(dsn, application_name=application_name(role), **kwargs)
