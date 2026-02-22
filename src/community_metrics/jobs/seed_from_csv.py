from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone

from community_metrics.jobs.common import (
    DOWNLOAD_COLUMN_TO_METRIC,
    REPO_TO_STAR_METRIC,
    build_store,
    clean_int,
    finish_run,
    parse_day,
    resolve_seed_path,
    start_run,
    to_daily_stat_row,
)
from community_metrics.utils.time import parse_seed_star_timestamp


def run() -> dict[str, int]:
    store = build_store()
    run_ctx = start_run("seed_from_csv")
    inserted = 0
    updated = 0

    try:
        rows: list[dict[str, object]] = []

        download_path = resolve_seed_path("download_stats.csv")
        with download_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                day = parse_day(raw["Day"])
                observed_at = datetime.combine(
                    day, datetime.min.time(), tzinfo=timezone.utc
                )
                for column, metric_id in DOWNLOAD_COLUMN_TO_METRIC.items():
                    value = clean_int(raw[column])
                    rows.append(
                        to_daily_stat_row(
                            metric_id=metric_id,
                            day=day,
                            observed_at=observed_at,
                            value=value,
                            provenance="csv_seed",
                            source_window="legacy_unknown",
                            ingestion_run_id=run_ctx.run_id,
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
                    repo = raw["Repository"].strip()
                    metric_id = REPO_TO_STAR_METRIC.get(repo)
                    if not metric_id:
                        continue
                    observed_at = parse_seed_star_timestamp(raw["Date"])
                    rows.append(
                        to_daily_stat_row(
                            metric_id=metric_id,
                            day=observed_at.date(),
                            observed_at=observed_at,
                            value=clean_int(raw["Stars"]),
                            provenance="csv_seed",
                            source_window="cumulative_snapshot",
                            ingestion_run_id=run_ctx.run_id,
                            source_ref=f"seed_data/{star_path.name}",
                        )
                    )

        upsert = store.upsert_stats(rows)
        inserted += upsert["inserted"]
        updated += upsert["updated"]
        finish_run(
            store,
            run_ctx,
            status="success",
            rows_inserted=inserted,
            rows_updated=updated,
        )
        return {"inserted": inserted, "updated": updated}
    except Exception as exc:
        finish_run(
            store,
            run_ctx,
            status="failed",
            rows_inserted=inserted,
            rows_updated=updated,
            error_summary=str(exc),
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed LanceDB stats table from CSV snapshots"
    )
    parser.parse_args()
    result = run()
    print(
        f"seed_from_csv complete: inserted={result['inserted']} updated={result['updated']}"
    )


if __name__ == "__main__":
    main()
