from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

from community_metrics.sources.duckdb_extensions_client import (
    DuckDBExtensionsClient,
    WeeklyExtensionDownloads,
)
from community_metrics.storage.lancedb_store import LanceDBStore

START_DAY = date(2026, 1, 1)


def build_monthly_rows(
    weekly_rows: list[WeeklyExtensionDownloads],
    *,
    today: date | None = None,
) -> list[dict[str, Any]]:
    current_day = today or datetime.now(tz=timezone.utc).date()
    current_month_start = current_day.replace(day=1)
    buckets: dict[date, dict[str, Any]] = defaultdict(
        lambda: {
            "core_downloads": 0,
            "community_downloads": 0,
            "latest_source_update_at": None,
        }
    )

    for row in weekly_rows:
        if row.source_update_at.date() < START_DAY:
            continue
        month_start = row.source_update_at.date().replace(day=1)
        bucket = buckets[month_start]
        downloads_key = f"{row.repo}_downloads"
        bucket[downloads_key] += row.downloads
        latest_update = bucket["latest_source_update_at"]
        if latest_update is None or row.source_update_at > latest_update:
            bucket["latest_source_update_at"] = row.source_update_at

    rows: list[dict[str, Any]] = []
    for month_start, bucket in sorted(buckets.items()):
        core_downloads = int(bucket["core_downloads"])
        community_downloads = int(bucket["community_downloads"])
        rows.append(
            {
                "month_start": month_start,
                "month_label": month_start.strftime("%Y-%m"),
                "core_downloads": core_downloads,
                "community_downloads": community_downloads,
                "total_downloads": core_downloads + community_downloads,
                "is_partial_month": month_start == current_month_start,
                "latest_source_update_at": bucket["latest_source_update_at"],
            }
        )

    return rows


def run() -> dict[str, int]:
    today = datetime.now(tz=timezone.utc).date()
    client = DuckDBExtensionsClient()
    weekly_rows = client.fetch_lance_weekly_downloads(
        start_day=START_DAY,
        end_day=today,
    )
    monthly_rows = build_monthly_rows(weekly_rows, today=today)

    store = LanceDBStore()
    store.ensure_duckdb_extension_downloads_table()
    upsert = store.upsert_duckdb_extension_downloads_monthly(monthly_rows)
    return {
        "inserted": upsert["inserted"],
        "updated": upsert["updated"],
        "weekly_rows": len(weekly_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update monthly DuckDB lance extension download stats"
    )
    parser.parse_args()

    result = run()
    print(
        "update_duckdb_extension_downloads complete: "
        f"weekly_rows={result['weekly_rows']} "
        f"inserted={result['inserted']} updated={result['updated']}"
    )


if __name__ == "__main__":
    main()
