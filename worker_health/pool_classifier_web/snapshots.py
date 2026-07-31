"""Versioned, atomically replaced fixed-dashboard snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from worker_health.pool_classifier_web.postgres import connect as postgres_connect


SCHEMA_VERSION = 1
POOL_SCOPE = "pool-dashboard"
OVERVIEW_SCOPE = "overview-dashboard"


def read_snapshot(dsn: str, scope: str, pool_id: str = "") -> dict[str, Any] | None:
    """Return the current compatible snapshot, or ``None`` when absent/stale."""
    with postgres_connect(dsn, "web") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_at, generated_at, payload FROM dashboard_snapshots"
                " WHERE scope = %s AND pool_id = %s AND schema_version = %s",
                (scope, pool_id, SCHEMA_VERSION),
            )
            row = cur.fetchone()
    if row is None:
        return None
    source_at, generated_at, payload = row
    return {
        "schema_version": SCHEMA_VERSION,
        "source_at": source_at.isoformat(),
        "generated_at": generated_at.isoformat(),
        "payload": payload,
    }


def write_snapshot(
    dsn: str,
    scope: str,
    payload: dict[str, Any],
    *,
    source_at: datetime | None = None,
    pool_id: str = "",
) -> None:
    """Atomically publish a fully-built snapshot.

    Callers must construct ``payload`` before calling this function.  Therefore
    a failed build leaves the previous committed snapshot untouched.
    """
    source_at = (source_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    with postgres_connect(dsn, "snapshot") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dashboard_snapshots (scope, pool_id, schema_version, source_at, payload)"
                " VALUES (%s, %s, %s, %s, %s::jsonb)"
                " ON CONFLICT (scope, pool_id) DO UPDATE SET"
                " schema_version = EXCLUDED.schema_version, source_at = EXCLUDED.source_at,"
                " generated_at = now(), payload = EXCLUDED.payload",
                (scope, pool_id, SCHEMA_VERSION, source_at, json.dumps(payload)),
            )
        conn.commit()
