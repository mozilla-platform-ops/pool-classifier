"""Read-only command-line preview of Taskcluster task classification."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from worker_health.pool_classifier import (
    DEFAULT_POLL_INTERVAL,
    DEFAULT_PROVISIONER,
    DEFAULT_WORKER_TYPE,
    PoolClassifier,
)
from worker_health.pool_classifier_preview import (
    compare_classification,
    load_committed_patterns,
    load_working_tree_patterns,
    select_terminal_run,
)
from worker_health.pool_classifier_web import registry


def build_parser() -> argparse.ArgumentParser:
    """Build the parser for a single-task classification preview."""
    parser = argparse.ArgumentParser(
        description="Read-only preview of a Taskcluster task's classification.",
    )
    parser.add_argument("task", metavar="TASK_ID", help="Taskcluster task ID to inspect")
    parser.add_argument("--run", type=int, metavar="RUN_ID", help="terminal run to preview (default: newest)")
    parser.add_argument("--base-ref", default="HEAD", metavar="REF", help="Git ref for baseline patterns (default: HEAD)")
    parser.add_argument("-p", "--provisioner", default=DEFAULT_PROVISIONER, help="TC provisioner ID")
    parser.add_argument("-w", "--worker-type", default=DEFAULT_WORKER_TYPE, help="TC worker type")
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        metavar="SECONDS",
        help=f"seconds between polls (default: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument("--no-color", action="store_true", help="disable color output")
    return parser


def _describe(label: str, result: object) -> None:
    rule = result.pattern.name if result.pattern else "no matching pattern"
    severity = result.severity or "n/a"
    print(f"{label}: {result.category} (severity: {severity}; rule: {rule})")


def main(argv: Sequence[str] | None = None) -> None:
    """Preview a task using committed and working-tree pattern files."""
    parser = build_parser()
    args = parser.parse_args(argv)
    configured_pool = registry.get_pool(args.provisioner, args.worker_type)
    classifier = PoolClassifier(
        provisioner=args.provisioner,
        worker_type=args.worker_type,
        storage=object(),
        poll_interval=args.poll_interval,
        use_color=not args.no_color,
        availability_mode=(configured_pool.availability_mode if configured_pool else "recent_contact"),
    )
    status = classifier._get_task_status(args.task)
    if status is None:
        parser.error(f"task {args.task} does not exist or has expired")
    try:
        run = select_terminal_run(status, args.run)
        log_text, log_status = classifier._fetch_log_tail(args.task, run["runId"])
        if log_status != "ok":
            parser.error(f"could not fetch a usable log for task {args.task} run {run['runId']} ({log_status})")
        repo_root = Path(__file__).resolve().parent.parent
        before_patterns = load_committed_patterns(repo_root, args.base_ref)
        proposed_patterns = load_working_tree_patterns(repo_root)
        comparison = compare_classification(
            before_patterns,
            proposed_patterns,
            log_text,
            run["state"],
            run.get("reasonResolved"),
        )
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(level=logging.WARNING)
    print("Classification preview (read-only)")
    print("Rules: worker_health/pool_classifier_web/patterns.yaml")
    print(f"Task: {args.task} run {run['runId']} ({run['state']})")
    _describe(f"Baseline ({args.base_ref})", comparison.before)
    _describe("Working tree", comparison.proposed)
    if before_patterns == proposed_patterns:
        print("Rule file is unchanged; both results use identical rules.")
    elif comparison.before == comparison.proposed:
        print("Classification is unchanged by the proposed rules.")
    elif comparison.proposed_shadows_before:
        print("Proposed winner shadows the prior matching rule.")
