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

# ANSI helpers
_use_color = True


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _use_color else text


CYAN = lambda t: _c("1;36", t)  # noqa: E731


class ColorFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.DEBUG: "2",  # dim
        logging.WARNING: "1;33",  # bold yellow
        logging.ERROR: "1;31",  # bold red
        logging.CRITICAL: "1;31",
    }

    def format(self, record):
        msg = super().format(record)
        code = self.LEVEL_COLORS.get(record.levelno)
        return _c(code, msg) if code else msg


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
        help="directory for DB and OVERVIEW reports (default: pool_classifier_results/)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    parser.add_argument("--no-color", action="store_true", help="disable color output")
    args = parser.parse_args()

    if args.no_color:
        _use_color = False

    # font: smbraille
    print(CYAN(" ⣀⡀ ⢀⡀ ⢀⡀ ⡇   ⢀⣀ ⡇ ⢀⣀ ⢀⣀ ⢀⣀ ⠄ ⣰⡁ ⠄ ⢀⡀ ⡀⣀"))
    print(CYAN(" ⡧⠜ ⠣⠜ ⠣⠜ ⠣   ⠣⠤ ⠣ ⠣⠼ ⠭⠕ ⠭⠕ ⠇ ⢸  ⠇ ⠣⠭ ⠏ "))
    print()

    handler = logging.StreamHandler()
    handler.setFormatter(
        ColorFormatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
    )
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        handlers=[handler],
    )

    classifier = PoolClassifier(
        provisioner=args.provisioner,
        worker_type=args.worker_type,
        results_dir=args.results_dir,
        poll_interval=args.poll_interval,
        use_color=_use_color,
    )
    classifier.run()
