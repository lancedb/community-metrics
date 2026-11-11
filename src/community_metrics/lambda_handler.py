"""AWS Lambda entry point for the scheduled ingestion jobs.

Each invocation runs one job, or a named chain of jobs, by calling the job's
existing CLI ``main()`` with ``sys.argv`` set, so Lambda runs use the same
defaults as the old cron commands (e.g. ``daily_refresh --lookback-days 1``).

Event shape::

    {"job": "daily"}                                      # the daily chain
    {"job": "generate_signal_guidance", "args": ["--window-days", "7"]}

Secrets: when ``SECRET_ID`` is set, the JSON secret it names is loaded from
AWS Secrets Manager into the environment *before* any job module is imported,
because ``community_metrics.config`` reads the environment at import time.
Without ``SECRET_ID`` the environment (or a local ``.env``) is used as-is.

Do not import ``community_metrics.config`` (or anything that imports it) at
module level in this file.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from collections.abc import Callable
from typing import Any

ALLOWED_JOBS = frozenset(
    {
        "daily_refresh",
        "collect_hn_evidence",
        "derive_dashboard",
        "generate_signal_guidance",
        "update_duckdb_extension_downloads",
    }
)

# Every step in a chain runs even if an earlier one fails; the invocation then
# fails at the end and names the failed steps.
CHAINS: dict[str, list[tuple[str, list[str]]]] = {
    "daily": [
        ("daily_refresh", []),
        ("collect_hn_evidence", ["--lookback-days", "30"]),
        ("derive_dashboard", []),
    ],
}

_secrets_loaded = False


def _log(message: str) -> None:
    print(f"[lambda_handler] {message}", flush=True)


def _load_secrets() -> None:
    global _secrets_loaded
    if _secrets_loaded:
        return
    secret_id = os.environ.get("SECRET_ID", "").strip()
    if secret_id:
        import boto3  # provided by the Lambda Python base image

        response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        secret = json.loads(response["SecretString"])
        os.environ.update({str(k): str(v) for k, v in secret.items()})
        _log(f"loaded {len(secret)} values from secret {secret_id}")
    _secrets_loaded = True


def _import_job(name: str) -> Callable[[], None]:
    module = importlib.import_module(f"community_metrics.jobs.{name}")
    return module.main


def _plan(event: dict[str, Any]) -> list[tuple[str, list[str]]]:
    job = event.get("job")
    if not isinstance(job, str) or not job:
        raise ValueError('event must include a "job" name')
    args = event.get("args", [])
    if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
        raise ValueError('"args" must be a list of strings')
    if job in CHAINS:
        if args:
            raise ValueError(f'chain "{job}" does not take args')
        return CHAINS[job]
    if job not in ALLOWED_JOBS:
        raise ValueError(f"unknown job {job!r}")
    return [(job, args)]


def _run_step(name: str, args: list[str]) -> None:
    main = _import_job(name)
    saved_argv = sys.argv
    sys.argv = [name, *args]
    try:
        main()
    except SystemExit as exc:
        # argparse and the jobs signal results via SystemExit; 0/None is success.
        if exc.code not in (0, None):
            raise RuntimeError(f"{name} exited with status {exc.code}") from exc
    finally:
        sys.argv = saved_argv


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    steps = _plan(event)  # validate before doing anything
    _load_secrets()

    results: list[dict[str, Any]] = []
    for name, args in steps:
        _log(f"start {name} {' '.join(args)}".rstrip())
        started = time.monotonic()
        try:
            _run_step(name, args)
        except Exception as exc:  # noqa: BLE001 - keep going; report failures at the end
            elapsed = round(time.monotonic() - started, 1)
            _log(f"FAILED {name} after {elapsed}s: {exc!r}")
            results.append(
                {"step": name, "ok": False, "seconds": elapsed, "error": repr(exc)}
            )
        else:
            elapsed = round(time.monotonic() - started, 1)
            _log(f"ok {name} in {elapsed}s")
            results.append({"step": name, "ok": True, "seconds": elapsed})

    failed = [r["step"] for r in results if not r["ok"]]
    if failed:
        raise RuntimeError(f"failed steps: {', '.join(failed)}; results: {results}")
    return {"ok": True, "job": event["job"], "steps": results}


if __name__ == "__main__":
    # Local run without Docker, e.g.:
    #   uv run python -m community_metrics.lambda_handler '{"job": "daily"}'
    if len(sys.argv) != 2:
        raise SystemExit(
            'usage: python -m community_metrics.lambda_handler \'{"job": "..."}\''
        )
    print(json.dumps(handler(json.loads(sys.argv[1])), indent=2))
