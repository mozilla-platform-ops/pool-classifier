"""Command-line launcher for the Pool Classifier web application."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence


APP_TARGET = "worker_health.pool_classifier_web.app:create_app()"


def _environment_int(name: str, default: int) -> int:
    """Read a positive integer configuration value from the environment."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError(f"{name} must be at least 1")
    return parsed


def _positive_int(value: str) -> int:
    """Parse a positive command-line integer."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser using the deployment defaults."""
    parser = argparse.ArgumentParser(description="Run the Pool Classifier web application with Gunicorn.")
    parser.add_argument(
        "--host",
        default=os.environ.get("POOL_CLASSIFIER_HOST", "0.0.0.0"),
        help="address to bind (default: POOL_CLASSIFIER_HOST or 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=_positive_int,
        default=_environment_int("PORT", 8080),
        help="TCP port to bind (default: PORT or 8080)",
    )
    parser.add_argument(
        "--workers",
        type=_positive_int,
        default=_environment_int("GUNICORN_WORKERS", 2),
        help="Gunicorn worker processes (default: GUNICORN_WORKERS or 2)",
    )
    parser.add_argument(
        "--threads",
        type=_positive_int,
        default=_environment_int("GUNICORN_THREADS", 8),
        help="threads per Gunicorn worker (default: GUNICORN_THREADS or 8)",
    )
    parser.add_argument(
        "--timeout",
        type=_positive_int,
        default=_environment_int("GUNICORN_TIMEOUT", 1800),
        help="Gunicorn request timeout in seconds (default: GUNICORN_TIMEOUT or 1800)",
    )
    return parser


def gunicorn_command(arguments: argparse.Namespace) -> list[str]:
    """Return the Gunicorn command for parsed launcher arguments."""
    host = f"[{arguments.host}]" if ":" in arguments.host and not arguments.host.startswith("[") else arguments.host
    return [
        "gunicorn",
        "--config", "python:worker_health.pool_classifier_web.gunicorn_config",
        "--bind", f"{host}:{arguments.port}",
        "--workers", str(arguments.workers),
        "--threads", str(arguments.threads),
        "--timeout", str(arguments.timeout),
        "--graceful-timeout", "30",
        "--access-logfile", "-",
        "--error-logfile", "-",
        APP_TARGET,
    ]


def main(argv: Sequence[str] | None = None) -> None:
    """Replace this process with the configured Gunicorn web server."""
    arguments = build_parser().parse_args(argv)
    command = gunicorn_command(arguments)
    os.execvp(command[0], command)
