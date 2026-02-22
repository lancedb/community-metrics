from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, time, timedelta, timezone

from community_metrics.jobs.common import (
    DOWNLOAD_COLUMN_TO_METRIC,
    DOWNLOAD_METRIC_SOURCE_MAP,
    REPO_TO_STAR_METRIC,
    STARS_METRIC_SOURCE_MAP,
    clean_int,
    finish_run,
    parse_day,
    resolve_seed_path,
    start_run,
    to_daily_stat_row,
)
from community_metrics.jobs.update_daily_stars import _daily_cumulative_stars
from community_metrics.sources.crates_client import CratesClient
from community_metrics.sources.github_client import GitHubClient
from community_metrics.sources.npm_client import NpmDownloadsClient
from community_metrics.sources.pypistats_client import PyPIStatsClient
from community_metrics.storage.lancedb_store import LanceDBStore
from community_metrics.utils.time import (
    latest_completed_day,
    parse_iso_date,
    parse_seed_star_timestamp,
)


def _daily_range(start_day: date, end_day: date) -> list[date]:
    days: list[date] = []
    cursor = start_day
    while cursor <= end_day:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _seed_rows_older_than(cutoff_day: date, run_id: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    download_path = resolve_seed_path("download_stats.csv")
    with download_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            day = parse_day(raw["Day"])
            if day >= cutoff_day:
                continue
            observed_at = datetime.combine(day, time.min, tzinfo=timezone.utc)
            for column, metric_id in DOWNLOAD_COLUMN_TO_METRIC.items():
                rows.append(
                    to_daily_stat_row(
                        metric_id=metric_id,
                        day=day,
                        observed_at=observed_at,
                        value=clean_int(raw[column]),
                        provenance="csv_seed",
                        source_window="discrete_snapshot",
                        ingestion_run_id=run_id,
                        source_ref=f"seed_data/{download_path.name}",
                    )
                )

    for filename in [
        "lance-star-history-2026220.csv",
        "lancedb-star-history-2026220.csv",
    ]:
        star_path = resolve_seed_path(filename)
        with star_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                observed_at = parse_seed_star_timestamp(raw["Date"])
                if observed_at.date() >= cutoff_day:
                    continue
                metric_id = REPO_TO_STAR_METRIC.get(raw["Repository"].strip())
                if not metric_id:
                    continue
                rows.append(
                    to_daily_stat_row(
                        metric_id=metric_id,
                        day=observed_at.date(),
                        observed_at=observed_at,
                        value=clean_int(raw["Stars"]),
                        provenance="csv_seed",
                        source_window="discrete_snapshot",
                        ingestion_run_id=run_id,
                        source_ref=f"seed_data/{star_path.name}",
                    )
                )

    return rows


def _api_rows_for_window(
    *,
    start_day: date,
    end_day: date,
    run_id: str,
) -> tuple[list[dict[str, object]], list[str]]:
    rows: list[dict[str, object]] = []
    errors: list[str] = []

    pypi = PyPIStatsClient()
    npm = NpmDownloadsClient()
    crates = CratesClient()
    github = GitHubClient()

    target_days = _daily_range(start_day, end_day)

    for metric_id, (source, subject) in DOWNLOAD_METRIC_SOURCE_MAP.items():
        try:
            print(
                f"[downloads] request source={source} subject={subject} range={start_day}..{end_day}",
                flush=True,
            )

            daily_totals: dict[date, int] = {}
            if source == "pypi":
                source_rows = pypi.fetch_daily_downloads(subject)
                daily_totals = {parse_iso_date(r.day): r.downloads for r in source_rows}
            elif source == "npm":
                source_rows = npm.fetch_daily_downloads(
                    subject, start=start_day, end=end_day
                )
                daily_totals = {parse_iso_date(r.day): r.downloads for r in source_rows}
            elif source == "crates":
                source_rows = crates.fetch_daily_downloads(subject)
                daily_totals = {parse_iso_date(r.day): r.downloads for r in source_rows}

            for day in target_days:
                if day not in daily_totals and source == "crates":
                    continue
                rows.append(
                    to_daily_stat_row(
                        metric_id=metric_id,
                        day=day,
                        observed_at=datetime.combine(
                            day, time.min, tzinfo=timezone.utc
                        ),
                        value=daily_totals.get(day, 0),
                        provenance="recomputed",
                        source_window="1d",
                        ingestion_run_id=run_id,
                        source_ref=f"{source}:{subject}",
                    )
                )
        except Exception as exc:
            errors.append(f"{metric_id}: {exc}")

    for metric_id, repo in STARS_METRIC_SOURCE_MAP.items():
        try:
            print(
                f"[stars] request source=github-stargazers repo={repo} range={start_day}..{end_day}",
                flush=True,
            )
            totals = _daily_cumulative_stars(github, repo, target_days)
            source_ref = f"github-stargazers:{repo}"

            for day in target_days:
                rows.append(
                    to_daily_stat_row(
                        metric_id=metric_id,
                        day=day,
                        observed_at=datetime.combine(
                            day, time.min, tzinfo=timezone.utc
                        ),
                        value=totals.get(day, 0),
                        provenance="recomputed",
                        source_window="cumulative_snapshot",
                        ingestion_run_id=run_id,
                        source_ref=source_ref,
                    )
                )
        except Exception as exc:
            errors.append(f"{metric_id}: {exc}")

    return rows, errors


def run(
    *,
    strict: bool = False,
    lookback_days: int | None = None,
    reset_tables: bool = False,
) -> dict[str, int]:
    effective_lookback_days = lookback_days if lookback_days is not None else 90
    if effective_lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")

    latest_day = latest_completed_day()
    lookback_start = latest_day - timedelta(days=effective_lookback_days - 1)

    run_ctx = start_run("recompute_history")
    store = LanceDBStore()

    if reset_tables:
        print("[bootstrap] resetting tables: metrics, stats, history", flush=True)
        store.reset_tables()

    print("[bootstrap] creating required tables", flush=True)
    store.create_required_tables(
        on_table=lambda table_name: print(
            f"[bootstrap] ensuring table: {table_name}", flush=True
        )
    )

    metrics_result = store.seed_metrics()
    print(
        f"[bootstrap] metrics table written inserted={metrics_result['inserted']} updated={metrics_result['updated']}",
        flush=True,
    )
    print("[bootstrap] history table ready", flush=True)

    all_rows: list[dict[str, object]] = []

    seed_rows = _seed_rows_older_than(lookback_start, run_ctx.run_id)
    if seed_rows:
        print(
            f"[bootstrap] seeded older discrete snapshots rows={len(seed_rows)}",
            flush=True,
        )
        all_rows.extend(seed_rows)

    api_rows, errors = _api_rows_for_window(
        start_day=lookback_start,
        end_day=latest_day,
        run_id=run_ctx.run_id,
    )
    all_rows.extend(api_rows)

    print(f"[bootstrap] writing stats rows={len(all_rows)}", flush=True)
    if reset_tables:
        upsert = store.append_stats(all_rows)
    else:
        upsert = store.upsert_stats(all_rows)

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

    if strict and errors:
        raise RuntimeError(error_summary)

    return {
        "lookback_days": effective_lookback_days,
        "inserted": upsert["inserted"],
        "updated": upsert["updated"],
        "errors": len(errors),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recompute source-supported historical daily metrics"
    )
    parser.add_argument(
        "--strict", action="store_true", help="Fail if any source returns errors"
    )
    parser.add_argument(
        "--reset-tables",
        action="store_true",
        help="Drop and recreate metrics/stats/history before recomputing",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        help="Days to recompute",
    )
    args = parser.parse_args()

    result = run(
        strict=args.strict,
        lookback_days=args.lookback_days,
        reset_tables=args.reset_tables,
    )
    print(
        "recompute_history complete: "
        f"lookback_days={result['lookback_days']} "
        f"inserted={result['inserted']} updated={result['updated']} errors={result['errors']}"
    )


if __name__ == "__main__":
    main()
