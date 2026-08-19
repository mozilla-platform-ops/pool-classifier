from __future__ import annotations

import json
import os
import subprocess
from types import SimpleNamespace

import pytest

from worker_health import utils


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (None, "--"),
        (59, "59s"),
        (60, "1m 0s"),
        (3600, "1h"),
        (86400, "1d 0h"),
        (604800, "1w 0d"),
        (2592000, "1mo 0d"),
        (31536000, "~1y"),
    ],
)
def test_human_delta_formats_each_unit_boundary(seconds, expected):
    assert utils.human_delta(seconds) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "just now"),
        (1, "1 second ago"),
        (120, "2 minutes ago"),
        (3600, "1 hour ago"),
        (60 * 60 * 24 * 30, "1 month ago"),
        (60 * 60 * 24 * 365 * 2, "2 years ago"),
    ],
)
def test_humanize_time_period_formats_largest_whole_unit(seconds, expected):
    assert utils.humanize_time_period(seconds) == expected


def test_date_in_past_handles_none_past_and_future_dates():
    assert utils.date_in_past(None) is False
    assert utils.date_in_past("2000-01-01T00:00:00Z") is True
    assert utils.date_in_past("2999-01-01T00:00:00Z") is False


def test_run_cmd_returns_decoded_stripped_output(monkeypatch):
    monkeypatch.setattr(utils.subprocess, "check_output", lambda *_args, **_kwargs: b" result\n")

    assert utils.run_cmd("anything") == "result"


def test_bitbar_systemd_service_reports_absence_at_requested_levels(monkeypatch, caplog):
    def unavailable(_command):
        raise subprocess.CalledProcessError(1, "systemctl")

    monkeypatch.setattr(utils, "run_cmd", unavailable)

    assert utils.bitbar_systemd_service_present(warn=True, error=True) is False
    assert "primary devicepool host" in caplog.text
    assert "must be run on the primary devicepool host" in caplog.text


def test_bitbar_systemd_service_returns_true_when_command_succeeds(monkeypatch):
    monkeypatch.setattr(utils, "run_cmd", lambda _command: "active")

    assert utils.bitbar_systemd_service_present() is True


def test_list_intersection_returns_shared_values():
    assert set(utils.list_intersection(["a", "b", "b"], ["b", "c"])) == {"b"}


def test_get_jsonc_retries_invalid_json_and_merges_worker_pages(monkeypatch):
    responses = iter(
        [
            "not json",
            json.dumps({"workers": [{"id": 1}], "continuationToken": "next"}),
            json.dumps({"workers": [{"id": 2}]}),
        ]
    )
    requests = []

    def get(url, **kwargs):
        requests.append((url, kwargs))
        return SimpleNamespace(text=next(responses))

    monkeypatch.setattr(utils.requests, "get", get)

    assert utils.get_jsonc("https://example.invalid/workers") == {"workers": [{"id": 1}, {"id": 2}], "continuationToken": "next"}
    assert len(requests) == 3
    assert requests[-1][1]["params"] == {"continuationToken": "next"}


def test_get_jsonc_returns_empty_mapping_after_all_decode_retries_fail(monkeypatch):
    monkeypatch.setattr(utils.requests, "get", lambda *_args, **_kwargs: SimpleNamespace(text="not json"))

    assert utils.get_jsonc("https://example.invalid") == {}


def test_consecutive_non_ones_from_end_counts_trailing_values():
    values = [1, 2, 3]

    assert utils.consecutive_non_ones_from_end(values) == 2
    assert values == [3, 2, 1]


def test_graph_percentage_supports_labels_and_rounding():
    assert utils.graph_percentage(0.25) == "[==        ]"
    assert utils.graph_percentage(0.26, round_value=True) == "[==        ]"
    assert utils.graph_percentage(1, show_label=True) == "%s: [==========]"


def test_fetch_url_returns_response_data_or_exception(monkeypatch):
    monkeypatch.setattr(utils, "urlopen", lambda _url: SimpleNamespace(read=lambda: b"body"))
    assert utils.fetch_url("https://example.invalid") == ("https://example.invalid", b"body", None)

    failure = RuntimeError("offline")
    monkeypatch.setattr(utils, "urlopen", lambda _url: (_ for _ in ()).throw(failure))
    assert utils.fetch_url("https://example.invalid") == ("https://example.invalid", None, failure)


def test_pformat_term_uses_available_terminal_width(monkeypatch):
    monkeypatch.setattr(utils.shutil, "get_terminal_size", lambda fallback: os.terminal_size((20, 50)))

    assert utils.pformat_term({"key": "value"}) == "{'key': 'value'}"


def test_requests_retry_session_configures_both_http_adapters():
    class Session:
        def __init__(self):
            self.mounts = []

        def mount(self, prefix, adapter):
            self.mounts.append((prefix, adapter))

    session = Session()
    configured = utils.requests_retry_session(retries=4, backoff_factor=1, status_forcelist=(429,), session=session)

    assert configured is session
    assert [prefix for prefix, _adapter in session.mounts] == ["http://", "https://"]
    retry = session.mounts[0][1].max_retries
    assert (retry.total, retry.read, retry.connect, retry.backoff_factor, retry.status_forcelist) == (4, 4, 4, 1, (429,))


def test_get_jsonc2_returns_paginated_response(monkeypatch):
    responses = iter(
        [
            json.dumps({"values": [1], "continuationToken": "next"}),
            json.dumps({"values": [2]}),
        ]
    )

    class Session:
        def get(self, _url, **_kwargs):
            return SimpleNamespace(text=next(responses))

    monkeypatch.setattr(utils, "requests_retry_session", Session)

    assert utils.get_jsonc2("https://example.invalid") == ("https://example.invalid", {"values": [2]}, None)


def test_get_jsonc2_returns_decode_error_after_retries(monkeypatch):
    class Session:
        def get(self, _url, **_kwargs):
            return SimpleNamespace(text="not json")

    monkeypatch.setattr(utils, "requests_retry_session", Session)

    url, result, error = utils.get_jsonc2("https://example.invalid")

    assert url == "https://example.invalid"
    assert result is None
    assert isinstance(error, json.JSONDecodeError)


def test_mkdir_p_runs_mkdir_command(monkeypatch):
    calls = []
    monkeypatch.setattr(utils.subprocess, "run", lambda *args, **kwargs: calls.append((args, kwargs)))

    utils.mkdir_p("new directory")

    assert calls == [(("mkdir -p new directory",), {"shell": True})]


def test_array_helpers_return_requested_portions_and_report_errors():
    values = ["a", "b", "c", "d"]

    assert utils.arr_get_followers(values, "b", 2) == ["b", "c", "d"]
    assert utils.arr_get_followers(values, "missing", 1) == []
    assert utils.arr_get_slice_from_item(values, "c") == ["c", "d"]

    with pytest.raises(Exception, match="empty array"):
        utils.arr_get_followers([], "a", 1, raise_on_errors=True)
    with pytest.raises(Exception, match="too many followers"):
        utils.arr_get_followers(values, "a", 5)
    with pytest.raises(Exception, match="item not in array"):
        utils.arr_get_slice_from_item(values, "missing")
