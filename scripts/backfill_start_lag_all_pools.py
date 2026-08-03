#!/usr/bin/env python3
"""Local compatibility wrapper for the packaged start-lag backfill runner."""

from worker_health.pool_classifier_web.scripts.backfill_start_lag_all_pools import main


if __name__ == "__main__":
    raise SystemExit(main())
