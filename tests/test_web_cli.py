from __future__ import annotations

import argparse

import pytest

from worker_health.pool_classifier_web import cli


def test_gunicorn_command_uses_deployment_defaults(monkeypatch):
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("POOL_CLASSIFIER_HOST", raising=False)
    monkeypatch.delenv("GUNICORN_WORKERS", raising=False)
    monkeypatch.delenv("GUNICORN_THREADS", raising=False)
    monkeypatch.delenv("GUNICORN_TIMEOUT", raising=False)

    arguments = cli.build_parser().parse_args([])

    assert cli.gunicorn_command(arguments) == [
        "gunicorn",
        "--bind", "0.0.0.0:8080",
        "--workers", "2",
        "--threads", "8",
        "--timeout", "1800",
        "--graceful-timeout", "30",
        "--access-logfile", "-",
        "--error-logfile", "-",
        "worker_health.pool_classifier_web.app:create_app()",
    ]


def test_command_line_options_override_environment(monkeypatch):
    monkeypatch.setenv("PORT", "9000")
    monkeypatch.setenv("POOL_CLASSIFIER_HOST", "127.0.0.1")
    monkeypatch.setenv("GUNICORN_WORKERS", "3")
    monkeypatch.setenv("GUNICORN_THREADS", "4")
    monkeypatch.setenv("GUNICORN_TIMEOUT", "60")

    arguments = cli.build_parser().parse_args(["--host", "::1", "--port", "9001", "--workers", "1", "--threads", "2", "--timeout", "10"])

    assert cli.gunicorn_command(arguments)[2] == "[::1]:9001"
    assert (arguments.workers, arguments.threads, arguments.timeout) == (1, 2, 10)


def test_main_replaces_process_with_gunicorn(monkeypatch):
    executed = {}
    monkeypatch.setattr(cli.os, "execvp", lambda executable, command: executed.update(executable=executable, command=command))

    cli.main(["--port", "8081"])

    assert executed == {
        "executable": "gunicorn",
        "command": cli.gunicorn_command(cli.build_parser().parse_args(["--port", "8081"])),
    }


@pytest.mark.parametrize("value", ["zero", "0", "-1"])
def test_invalid_environment_integer_is_rejected(monkeypatch, value):
    monkeypatch.setenv("PORT", value)

    with pytest.raises(argparse.ArgumentTypeError, match="PORT must be"):
        cli.build_parser()
