import json
import os
import sys
import types

import pytest

from community_metrics import lambda_handler


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(lambda_handler, "_secrets_loaded", False)
    monkeypatch.delenv("SECRET_ID", raising=False)


def _fake_jobs(monkeypatch, behaviours):
    """behaviours: job name -> callable(argv) run as that job's main()."""
    calls = []

    def fake_import(name):
        def main():
            calls.append((name, list(sys.argv)))
            behaviours.get(name, lambda argv: None)(sys.argv)

        return main

    monkeypatch.setattr(lambda_handler, "_import_job", fake_import)
    return calls


def test_daily_chain_runs_steps_in_order_with_cli_args(monkeypatch):
    calls = _fake_jobs(monkeypatch, {})

    result = lambda_handler.handler({"job": "daily"})

    assert calls == [
        ("daily_refresh", ["daily_refresh"]),
        ("collect_hn_evidence", ["collect_hn_evidence", "--lookback-days", "30"]),
        ("derive_dashboard", ["derive_dashboard"]),
    ]
    assert result["ok"] is True
    assert [s["step"] for s in result["steps"]] == [
        "daily_refresh",
        "collect_hn_evidence",
        "derive_dashboard",
    ]


def test_chain_keeps_going_after_a_failure_and_names_failed_steps(monkeypatch):
    def exit_1(argv):
        raise SystemExit(1)

    def boom(argv):
        raise ValueError("bad")

    calls = _fake_jobs(monkeypatch, {"daily_refresh": exit_1, "derive_dashboard": boom})

    with pytest.raises(RuntimeError) as excinfo:
        lambda_handler.handler({"job": "daily"})

    assert [name for name, _ in calls] == [
        "daily_refresh",
        "collect_hn_evidence",
        "derive_dashboard",
    ]
    message = str(excinfo.value)
    assert "failed steps: daily_refresh, derive_dashboard" in message
    assert "collect_hn_evidence" not in message.split(";")[0]


def test_system_exit_zero_is_success(monkeypatch):
    def exit_0(argv):
        raise SystemExit(0)

    _fake_jobs(monkeypatch, {"update_duckdb_extension_downloads": exit_0})

    result = lambda_handler.handler({"job": "update_duckdb_extension_downloads"})

    assert result["ok"] is True


def test_single_job_receives_args_and_argv_is_restored(monkeypatch):
    calls = _fake_jobs(monkeypatch, {})
    original_argv = list(sys.argv)

    lambda_handler.handler(
        {"job": "generate_signal_guidance", "args": ["--window-days", "7"]}
    )

    assert calls == [
        ("generate_signal_guidance", ["generate_signal_guidance", "--window-days", "7"])
    ]
    assert sys.argv == original_argv


@pytest.mark.parametrize(
    "event",
    [
        {},
        {"job": "bootstrap_tables"},  # destructive; never allowed from Lambda
        {"job": "../config"},
        {"job": "daily_refresh", "args": "--lookback-days 7"},
        {"job": "daily", "args": ["--lookback-days", "7"]},
    ],
)
def test_rejects_bad_events_without_running_anything(monkeypatch, event):
    calls = _fake_jobs(monkeypatch, {})

    with pytest.raises(ValueError):
        lambda_handler.handler(event)

    assert calls == []


def test_secret_is_loaded_into_env_before_jobs_run(monkeypatch):
    seen = {}

    class FakeClient:
        def get_secret_value(self, SecretId):
            assert SecretId == "community-metrics/test"
            return {"SecretString": json.dumps({"CM_TEST_SECRET": "from-secret"})}

    fake_boto3 = types.SimpleNamespace(client=lambda service: FakeClient())
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setenv("SECRET_ID", "community-metrics/test")
    monkeypatch.delenv("CM_TEST_SECRET", raising=False)
    _fake_jobs(
        monkeypatch,
        {
            "derive_dashboard": lambda argv: seen.update(
                v=os.environ.get("CM_TEST_SECRET")
            )
        },
    )

    try:
        lambda_handler.handler({"job": "derive_dashboard"})
    finally:
        os.environ.pop("CM_TEST_SECRET", None)

    assert seen == {"v": "from-secret"}


def test_real_job_modules_are_importable():
    for name in lambda_handler.ALLOWED_JOBS:
        assert callable(lambda_handler._import_job(name))
