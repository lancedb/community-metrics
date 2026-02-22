from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone

from community_metrics.jobs.common import (
    STARS_METRIC_SOURCE_MAP,
    build_store,
    days_to_refresh,
    finish_run,
    latest_period_end_for_metric,
    start_run,
    to_daily_stat_row,
)
from community_metrics.sources.github_client import GitHubClient
from community_metrics.utils.time import latest_completed_day


def _daily_cumulative_stars(
    github: GitHubClient, repo: str, target_days: list[date]
) -> dict[date, int]:
    events = sorted(github.iter_stargazer_events(repo), key=lambda ev: ev.starred_at)
    totals: dict[date, int] = {}
    idx = 0
    cumulative = 0

    for day in target_days:
        cutoff_exclusive = datetime.combine(
            day + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
        while idx < len(events) and events[idx].starred_at < cutoff_exclusive:
            cumulative += 1
            idx += 1
        totals[day] = cumulative
    return totals


def run(*, run_id: str | None = None, lookback_days: int = 0) -> dict[str, int]:
    store = build_store()
    run_ctx = start_run("update_daily_stars", run_id=run_id)
    latest_day = latest_completed_day()

    github = GitHubClient()

    rows: list[dict[str, object]] = []
    errors: list[str] = []

    for metric_id, repo in STARS_METRIC_SOURCE_MAP.items():
        try:
            latest_existing = latest_period_end_for_metric(store, metric_id)
            target_days = days_to_refresh(
                latest_existing_period_end=latest_existing,
                latest_completed_period_end=latest_day,
                lookback_days=lookback_days,
            )
            if not target_days:
                continue

            if len(target_days) == 1 and lookback_days == 0:
                totals = {target_days[0]: github.get_repo_stars(repo)}
                source_ref = f"github:{repo}"
            else:
                try:
                    totals = _daily_cumulative_stars(github, repo, target_days)
                    source_ref = f"github-stargazers:{repo}"
                except Exception as exc:
                    snapshot = github.get_repo_stars(repo)
                    totals = {day: snapshot for day in target_days}
                    source_ref = f"github:{repo}"
                    errors.append(
                        f"{metric_id}: stargazer backfill failed ({exc}); fell back to snapshot"
                    )

            observed_at = datetime.now(tz=timezone.utc)
            for day in target_days:
                rows.append(
                    to_daily_stat_row(
                        metric_id=metric_id,
                        day=day,
                        observed_at=observed_at,
                        value=totals.get(day, 0),
                        provenance="api_daily",
                        source_window="cumulative_snapshot",
                        ingestion_run_id=run_ctx.run_id,
                        source_ref=source_ref,
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
    parser = argparse.ArgumentParser(description="Update daily GitHub star metrics")
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
        "update_daily_stars complete: "
        f"inserted={result['inserted']} updated={result['updated']} errors={result['errors']}"
    )


if __name__ == "__main__":
    main()
