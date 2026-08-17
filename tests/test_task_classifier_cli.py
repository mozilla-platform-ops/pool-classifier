import pytest

from worker_health import task_classifier


def test_parser_accepts_task_and_preview_options():
    arguments = task_classifier.build_parser().parse_args(
        ["task-id", "--run", "2", "--base-ref", "origin/main", "--no-color"],
    )

    assert arguments.task == "task-id"
    assert arguments.run == 2
    assert arguments.base_ref == "origin/main"
    assert arguments.no_color is True


def test_parser_requires_task_id():
    with pytest.raises(SystemExit, match="2"):
        task_classifier.build_parser().parse_args([])
