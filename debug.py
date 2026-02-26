from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from typing import Any

from community_metrics.storage.lancedb_store import LanceDBStore
from community_metrics.utils.time import parse_iso_date


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str))


def _default_start_date(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _coerce_day(value: object) -> date:
    return parse_iso_date(str(value)[:10])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Simple debug helper that queries LanceDB Enterprise tables directly. "
            "Requires LANCEDB_* env vars."
        )
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("metrics", help="Query metric definitions from metrics table")

    stats = subparsers.add_parser("stats", help="Query stats rows for one metric_id")
    stats.add_argument(
        "--metric-id",
        required=True,
        help="Metric ID, e.g. downloads:lance:python",
    )
    stats.add_argument("--days", type=int, default=30, help="Trailing day window (UTC)")

    history = subparsers.add_parser(
        "history",
        help="Query refresh-run errors from history table",
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

    subparsers.add_parser(
        "all", help="Query metrics, one stats series, and history errors"
    )
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


def _metrics_payload(store: LanceDBStore) -> list[dict[str, Any]]:
    rows = store.get_metrics_df()
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("product", "")),
            str(row.get("metric_family", "")),
            str(row.get("display_name", "")),
            str(row.get("metric_id", "")),
        ),
    )


def _stats_payload(store: LanceDBStore, metric_id: str, days: int) -> dict[str, Any]:
    rows = store.get_stats_for_metric(metric_id)
    if rows and days > 0:
        end_day = date.today()
        start_day = end_day - timedelta(days=days - 1)
        rows = [
            row
            for row in rows
            if start_day <= _coerce_day(row.get("period_end")) <= end_day
        ]

    points = [
        {
            "period_start": str(row.get("period_start"))[:10],
            "period_end": str(row.get("period_end"))[:10],
            "value": int(row.get("value", 0)),
            "provenance": row.get("provenance"),
            "source_window": row.get("source_window"),
            "source_ref": row.get("source_ref"),
            "ingestion_run_id": row.get("ingestion_run_id"),
        }
        for row in rows
    ]

    return {
        "metric_id": metric_id,
        "days": days,
        "points": points,
    }


def _history_payload(
    store: LanceDBStore,
    *,
    start_date: str,
    end_date: str,
    limit: int,
) -> dict[str, Any]:
    start_day = parse_iso_date(start_date)
    end_day = parse_iso_date(end_date)
    rows = store.list_refresh_errors(start_day=start_day, end_day=end_day, limit=limit)
    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "count": len(rows),
        "errors": rows,
    }


def main() -> None:
    args = parse_args()
    store = LanceDBStore()

    if args.command == "metrics":
        _print(_metrics_payload(store))
        return

    if args.command == "stats":
        _print(_stats_payload(store, args.metric_id, args.days))
        return

    if args.command == "history":
        start_date = args.start_date or _default_start_date(args.history_days)
        _print(
            _history_payload(
                store,
                start_date=start_date,
                end_date=args.end_date,
                limit=args.limit,
            )
        )
        return

    if args.command == "all":
        start_date = _default_start_date(args.history_days)
        payload = {
            "metrics": _metrics_payload(store),
            "stats": _stats_payload(store, args.sample_metric_id, args.sample_days),
            "history": _history_payload(
                store,
                start_date=start_date,
                end_date=date.today().isoformat(),
                limit=200,
            ),
        }
        _print(payload)
        return

    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
