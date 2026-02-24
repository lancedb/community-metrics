from __future__ import annotations

import argparse

from community_metrics.jobs.update_daily_downloads import run as run_downloads
from community_metrics.jobs.update_daily_stars import run as run_stars
from community_metrics.utils.ids import new_ingestion_run_id


def run(*, lookback_days: int = 0) -> dict[str, int]:
    shared_run_id = new_ingestion_run_id("daily_refresh")
    print(
        f"[daily_refresh] starting downloads lookback_days={lookback_days}", flush=True
    )
    downloads = run_downloads(
        run_id=f"{shared_run_id}:downloads", lookback_days=lookback_days
    )
    print(
        "[daily_refresh] downloads complete: "
        f"inserted={downloads['inserted']} updated={downloads['updated']} errors={downloads['errors']}",
        flush=True,
    )
    print(f"[daily_refresh] starting stars lookback_days={lookback_days}", flush=True)
    stars = run_stars(run_id=f"{shared_run_id}:stars", lookback_days=lookback_days)
    print(
        "[daily_refresh] stars complete: "
        f"inserted={stars['inserted']} updated={stars['updated']} errors={stars['errors']}",
        flush=True,
    )

    return {
        "inserted": downloads["inserted"] + stars["inserted"],
        "updated": downloads["updated"] + stars["updated"],
        "errors": downloads["errors"] + stars["errors"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run daily download and star refresh jobs"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1,
        help="Also recompute this many recent days (0 = only days newer than latest period_end)",
    )
    args = parser.parse_args()

    result = run(lookback_days=args.lookback_days)
    print(
        "daily_refresh complete: "
        f"inserted={result['inserted']} updated={result['updated']} errors={result['errors']}"
    )


if __name__ == "__main__":
    main()
