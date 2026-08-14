#!/usr/bin/env python3

import argparse
import logging
import os
import signal
from pathlib import Path

from worker_health.pool_classifier import (
    DEFAULT_POLL_INTERVAL,
    DEFAULT_PROVISIONER,
    DEFAULT_WORKER_TYPE,
    PoolClassifier,
)
from worker_health.pool_classifier_web import registry
from worker_health.pool_classifier_web.storage import PostgresStorage
from worker_health.pool_classifier_preview import (
    compare_classification,
    load_committed_patterns,
    load_working_tree_patterns,
    select_terminal_run,
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


class StopAfterCurrentBatch:
    """Turn the first Ctrl-C into a request to stop after durable work."""

    def __init__(self) -> None:
        self.requested = False

    def handle_signal(self, signum: int, frame: object) -> None:
        if self.requested:
            signal.default_int_handler(signum, frame)
        self.requested = True
        print(
            "Ctrl-C received; finishing the current batch before stopping. Press Ctrl-C again to abort.",
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
        help="directory for DB and OVERVIEW reports (default: pool_classifier_results/)",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        metavar="URL",
        help="use Postgres storage at URL instead of --results-dir (default: DATABASE_URL)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    parser.add_argument("--no-color", action="store_true", help="disable color output")
    parser.add_argument(
        "-u",
        "--update-only",
        action="store_true",
        help="fetch quarantine state, write report, and exit",
    )
    parser.add_argument(
        "--reclassify",
        action="store_true",
        help="re-run patterns against saved/fetched logs for a category and update the DB, then exit",
    )
    parser.add_argument(
        "--reclassify-category",
        default="unclassified",
        metavar="CATEGORY",
        help="category to target with --reclassify (default: unclassified)",
    )
    parser.add_argument(
        "--save-unmatched-logs",
        action="store_true",
        help="save re-fetched logs that still don't match any pattern to reclassify_logs/{category}/",
    )
    parser.add_argument("--backfill-start-lag", action="store_true", help="enrich one batch of stored runs with Queue scheduled metadata, then exit")
    parser.add_argument("--backfill-batch-size", type=int, default=500, metavar="RUNS", help="runs to inspect for --backfill-start-lag (default: 500)")
    parser.add_argument("--backfill-concurrency", type=int, default=5, metavar="REQUESTS", help="concurrent Queue status requests for --backfill-start-lag (default: 5)")
    parser.add_argument("--backfill-retries", type=int, default=2, metavar="COUNT", help="retries per transient status request (default: 2)")
    parser.add_argument("--backfill-requests-per-second", type=float, default=5.0, metavar="RATE", help="maximum Queue status requests per second for --backfill-start-lag (default: 5)")
    parser.add_argument("--backfill-state-file", type=Path, default=Path(".backfill-start-lag-state.json"), metavar="FILE", help="persist Queue 404 and unmatched-run skips here (default: .backfill-start-lag-state.json)")
    parser.add_argument(
        "--preview-task", metavar="TASK_ID",
        help="read-only: compare worker_health/pool_classifier_web/patterns.yaml at a Git ref and in the working tree",
    )
    parser.add_argument("--preview-run", type=int, metavar="RUN_ID", help="run ID to preview (default: newest terminal run)")
    parser.add_argument("--base-ref", default="HEAD", metavar="REF", help="Git ref for preview baseline patterns (default: HEAD)")
    args = parser.parse_args()

    if args.preview_run is not None and not args.preview_task:
        parser.error("--preview-run requires --preview-task")

    if args.no_color:
        _use_color = False

    if not args.preview_task:
        # font: smbraille
        print()
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
        level=(logging.WARNING if args.preview_task else (logging.DEBUG if args.verbose else logging.INFO)),
        handlers=[handler],
    )

    configured_pool = registry.get_pool(args.provisioner, args.worker_type)
    if args.preview_task:
        repo_root = Path(__file__).resolve().parent
        classifier = PoolClassifier(
            provisioner=args.provisioner,
            worker_type=args.worker_type,
            storage=object(),
            poll_interval=args.poll_interval,
            use_color=_use_color,
            availability_mode=(configured_pool.availability_mode if configured_pool else "recent_contact"),
        )
        status = classifier._get_task_status(args.preview_task)
        if status is None:
            parser.error(f"task {args.preview_task} does not exist or has expired")
        try:
            run = select_terminal_run(status, args.preview_run)
            log_text, log_status = classifier._fetch_log_tail(args.preview_task, run["runId"])
            if log_status != "ok":
                parser.error(f"could not fetch a usable log for task {args.preview_task} run {run['runId']} ({log_status})")
            before_patterns = load_committed_patterns(repo_root, args.base_ref)
            proposed_patterns = load_working_tree_patterns(repo_root)
            comparison = compare_classification(
                before_patterns, proposed_patterns,
                log_text,
                run["state"],
                run.get("reasonResolved"),
            )
        except ValueError as exc:
            parser.error(str(exc))

        def describe(label, result):
            rule = result.pattern.name if result.pattern else "no matching pattern"
            severity = result.severity or "n/a"
            print(f"{label}: {result.category} (severity: {severity}; rule: {rule})")

        print("Classification preview (read-only)")
        print("Rules: worker_health/pool_classifier_web/patterns.yaml")
        print(f"Task: {args.preview_task} run {run['runId']} ({run['state']})")
        describe(f"Baseline ({args.base_ref})", comparison.before)
        describe("Working tree", comparison.proposed)
        if before_patterns == proposed_patterns:
            print("Rule file is unchanged; both results use identical rules.")
        elif comparison.before == comparison.proposed:
            print("Classification is unchanged by the proposed rules.")
        elif comparison.proposed_shadows_before:
            print("Proposed winner shadows the prior matching rule.")
        raise SystemExit(0)

    storage = (
        PostgresStorage(pool_id=f"{args.provisioner}/{args.worker_type}", dsn=args.database_url)
        if args.database_url
        else None
    )
    classifier = PoolClassifier(
        provisioner=args.provisioner,
        worker_type=args.worker_type,
        results_dir=args.results_dir,
        storage=storage,
        poll_interval=args.poll_interval,
        use_color=_use_color,
        availability_mode=(configured_pool.availability_mode if configured_pool else "recent_contact"),
    )
    if args.backfill_start_lag:
        classifier._init_db()
        stop = StopAfterCurrentBatch()
        previous_handler = signal.signal(signal.SIGINT, stop.handle_signal)
        try:
            result = classifier.backfill_start_lag(
                args.backfill_batch_size,
                args.backfill_concurrency,
                args.backfill_retries,
                args.backfill_requests_per_second,
                args.backfill_state_file,
                should_stop=lambda: stop.requested,
            )
            print(result)
        finally:
            signal.signal(signal.SIGINT, previous_handler)
            classifier.storage.close()
        if stop.requested:
            raise SystemExit(130)
    elif args.reclassify:
        classifier.reclassify_unclassified(
            target_category=args.reclassify_category,
            save_unmatched_logs=args.save_unmatched_logs,
        )
    elif args.update_only:
        classifier.update_report()
    else:
        classifier.run()
