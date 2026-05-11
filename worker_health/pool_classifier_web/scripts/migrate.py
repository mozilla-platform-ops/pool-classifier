"""Apply pending SQL migrations to a Postgres database.

Usage:
    DATABASE_URL=postgresql://... python -m worker_health.pool_classifier_web.scripts.migrate

Also callable as apply_migrations(dsn) from Python code.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def apply_migrations(dsn: str) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)

    with psycopg.connect(dsn) as conn:
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = sql_file.stem
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (version,))
                if cur.fetchone():
                    print(f"  {version}: already applied")
                    continue
            with conn.cursor() as cur:
                cur.execute(sql_file.read_text())
                cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
            conn.commit()
            print(f"  {version}: applied")


if __name__ == "__main__":
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    apply_migrations(dsn)
