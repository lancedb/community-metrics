from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from typing import Any

import requests


def _request_json(base_url: str, path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str))


def _default_start_date(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simple debug helper that queries FastAPI endpoints only."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="FastAPI base URL",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("metrics", help="Query metrics table view (/api/v1/definitions)")

    stats = subparsers.add_parser(
        "stats", help="Query stats table view (/api/v1/series/{metric_id})"
    )
    stats.add_argument(
        "--metric-id",
        required=True,
        help="Metric ID, e.g. downloads:lance:python",
    )
    stats.add_argument("--days", type=int, default=30)

    history = subparsers.add_parser(
        "history",
        help="Query history table errors view (/api/v1/history/refresh-errors)",
    )
    history.add_argument(
        "--start-date",
        default=None,
        help="YYYY-MM-DD (default: 30 days ago)",
    )
    history.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="YYYY-MM-DD (default: today)",
    )
    history.add_argument("--limit", type=int, default=200)

    subparsers.add_parser("all", help="Query metrics, one stats series, and history errors")
    parser.add_argument(
        "--sample-metric-id",
        default="downloads:lance:python",
        help="Metric used by 'all' for the stats query",
    )
    parser.add_argument(
        "--sample-days",
        type=int,
        default=30,
        help="Days used by 'all' for the stats query",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=30,
        help="Days-back used by 'all' and history default start_date",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command == "metrics":
        _print(_request_json(args.base_url, "/api/v1/definitions"))
        return

    if args.command == "stats":
        _print(
            _request_json(
                args.base_url,
                f"/api/v1/series/{args.metric_id}",
                params={"days": args.days},
            )
        )
        return

    if args.command == "history":
        start_date = args.start_date or _default_start_date(args.history_days)
        _print(
            _request_json(
                args.base_url,
                "/api/v1/history/refresh-errors",
                params={
                    "start_date": start_date,
                    "end_date": args.end_date,
                    "limit": args.limit,
                },
            )
        )
        return

    if args.command == "all":
        start_date = _default_start_date(args.history_days)
        payload = {
            "metrics": _request_json(args.base_url, "/api/v1/definitions"),
            "stats": _request_json(
                args.base_url,
                f"/api/v1/series/{args.sample_metric_id}",
                params={"days": args.sample_days},
            ),
            "history": _request_json(
                args.base_url,
                "/api/v1/history/refresh-errors",
                params={
                    "start_date": start_date,
                    "end_date": date.today().isoformat(),
                    "limit": 200,
                },
            ),
        }
        _print(payload)
        return

    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
