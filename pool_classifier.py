#!/usr/bin/env python3

import argparse
import logging
from pathlib import Path

from worker_health.pool_classifier import (
    DEFAULT_POLL_INTERVAL,
    DEFAULT_PROVISIONER,
    DEFAULT_WORKER_TYPE,
    PoolClassifier,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor a TC worker pool and classify task failures from logs.")
    parser.add_argument("-p", "--provisioner", default=DEFAULT_PROVISIONER, help="TC provisioner ID")
    parser.add_argument("-w", "--worker-type", default=DEFAULT_WORKER_TYPE, help="TC worker type")
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        metavar="SECONDS",
        help=f"seconds between polls (default: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("pool_classifier_results"),
        metavar="DIR",
        help="directory for state.json and OVERVIEW reports (default: pool_classifier_results/)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args()

    # font: smbraille
    print(" ⣀⡀ ⢀⡀ ⢀⡀ ⡇   ⢀⣀ ⡇ ⢀⣀ ⢀⣀ ⢀⣀ ⠄ ⣰⡁ ⠄ ⢀⡀ ⡀⣀")
    print(" ⡧⠜ ⠣⠜ ⠣⠜ ⠣   ⠣⠤ ⠣ ⠣⠼ ⠭⠕ ⠭⠕ ⠇ ⢸  ⠇ ⠣⠭ ⠏ ")
    print()

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    classifier = PoolClassifier(
        provisioner=args.provisioner,
        worker_type=args.worker_type,
        results_dir=args.results_dir,
        poll_interval=args.poll_interval,
    )
    classifier.run()
