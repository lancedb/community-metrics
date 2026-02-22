from __future__ import annotations

import argparse
from datetime import datetime, timezone

from community_metrics.jobs.common import (
    DOWNLOAD_METRIC_SOURCE_MAP,
    build_store,
    days_to_refresh,
    finish_run,
    latest_period_end_for_metric,
    start_run,
    to_daily_stat_row,
)
from community_metrics.sources.crates_client import CratesClient
from community_metrics.sources.npm_client import NpmDownloadsClient
from community_metrics.sources.pypistats_client import PyPIStatsClient
from community_metrics.utils.time import latest_completed_day, parse_iso_date


def run(*, run_id: str | None = None, lookback_days: int = 0) -> dict[str, int]:
    store = build_store()
    run_ctx = start_run("update_daily_downloads", run_id=run_id)
    latest_day = latest_completed_day()

    pypi = PyPIStatsClient()
    npm = NpmDownloadsClient()
    crates = CratesClient()

    rows: list[dict[str, object]] = []
    errors: list[str] = []

    for metric_id, (source, subject) in DOWNLOAD_METRIC_SOURCE_MAP.items():
        try:
            latest_existing = latest_period_end_for_metric(store, metric_id)
            target_days = days_to_refresh(
                latest_existing_period_end=latest_existing,
                latest_completed_period_end=latest_day,
                lookback_days=lookback_days,
            )
            if not target_days:
                continue

            observed_at = datetime.now(tz=timezone.utc)
            daily_totals: dict = {}

            if source == "pypi":
                daily_rows = pypi.fetch_daily_downloads(subject)
                daily_totals = {
                    parse_iso_date(row.day): row.downloads for row in daily_rows
                }
                for day in target_days:
                    rows.append(
                        to_daily_stat_row(
                            metric_id=metric_id,
                            day=day,
                            observed_at=observed_at,
                            value=daily_totals.get(day, 0),
                            provenance="api_daily",
                            source_window="1d",
                            ingestion_run_id=run_ctx.run_id,
                            source_ref=f"{source}:{subject}",
                        )
                    )

            elif source == "npm":
                range_start = target_days[0]
                range_end = target_days[-1]
                daily_rows = npm.fetch_daily_downloads(
                    subject, start=range_start, end=range_end
                )
                daily_totals = {
                    parse_iso_date(row.day): row.downloads for row in daily_rows
                }
                for day in target_days:
                    rows.append(
                        to_daily_stat_row(
                            metric_id=metric_id,
                            day=day,
                            observed_at=observed_at,
                            value=daily_totals.get(day, 0),
                            provenance="api_daily",
                            source_window="1d",
                            ingestion_run_id=run_ctx.run_id,
                            source_ref=f"{source}:{subject}",
                        )
                    )

            elif source == "crates":
                daily_rows = crates.fetch_daily_downloads(subject)
                daily_totals = {
                    parse_iso_date(row.day): row.downloads for row in daily_rows
                }
                for day in target_days:
                    # crates.io may not provide every day in a long lookback window.
                    if day not in daily_totals:
                        continue
                    rows.append(
                        to_daily_stat_row(
                            metric_id=metric_id,
                            day=day,
                            observed_at=observed_at,
                            value=daily_totals[day],
                            provenance="api_daily",
                            source_window="1d",
                            ingestion_run_id=run_ctx.run_id,
                            source_ref=f"{source}:{subject}",
                        )
                    )

        except Exception as exc:
            errors.append(f"{metric_id}: {exc}")

    upsert = store.upsert_stats(rows)
    status = "success" if not errors else "partial"
    error_summary = None if not errors else " | ".join(errors)
    finish_run(
        store,
        run_ctx,
        status=status,
        rows_inserted=upsert["inserted"],
        rows_updated=upsert["updated"],
        error_summary=error_summary,
    )

    return {
        "inserted": upsert["inserted"],
        "updated": upsert["updated"],
        "errors": len(errors),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Update daily download metrics")
    parser.add_argument("--run-id", default=None, help="Optional ingestion run id")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Also recompute this many recent days (0 = only days newer than latest period_end)",
    )
    args = parser.parse_args()

    result = run(run_id=args.run_id, lookback_days=args.lookback_days)
    print(
        "update_daily_downloads complete: "
        f"inserted={result['inserted']} updated={result['updated']} errors={result['errors']}"
    )


if __name__ == "__main__":
    main()
