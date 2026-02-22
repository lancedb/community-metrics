from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from community_metrics.config import SEED_DATA_DIR
from community_metrics.storage.lancedb_store import LanceDBStore
from community_metrics.utils.ids import new_ingestion_run_id
from community_metrics.utils.time import parse_iso_date

DOWNLOAD_METRIC_SOURCE_MAP = {
    "downloads:lance:python": ("pypi", "pylance"),
    "downloads:lance:rust": ("crates", "lance"),
    "downloads:lancedb:python": ("pypi", "lancedb"),
    "downloads:lancedb:nodejs": ("npm", "@lancedb/lancedb"),
    "downloads:lancedb:rust": ("crates", "lancedb"),
}

DOWNLOAD_COLUMN_TO_METRIC = {
    "Python (lance)": "downloads:lance:python",
    "Rust (lance)": "downloads:lance:rust",
    "Python (lanceDB)": "downloads:lancedb:python",
    "NodeJS (lancedb)": "downloads:lancedb:nodejs",
    "Rust (lancedb)": "downloads:lancedb:rust",
}

STARS_METRIC_SOURCE_MAP = {
    "stars:lance:github": "lance-format/lance",
    "stars:lancedb:github": "lancedb/lancedb",
}

REPO_TO_STAR_METRIC = {
    "lance-format/lance": "stars:lance:github",
    "lancedb/lancedb": "stars:lancedb:github",
}


@dataclass
class RunContext:
    job_name: str
    run_id: str
    started_at: datetime


def build_store(*, reset_tables: bool = False) -> LanceDBStore:
    store = LanceDBStore()
    if reset_tables:
        store.reset_tables()
    store.ensure_tables()
    store.seed_metrics()
    return store


def start_run(job_name: str, run_id: str | None = None) -> RunContext:
    return RunContext(
        job_name=job_name,
        run_id=run_id or new_ingestion_run_id(job_name),
        started_at=datetime.now(tz=timezone.utc),
    )


def finish_run(
    store: LanceDBStore,
    run: RunContext,
    *,
    status: str,
    rows_inserted: int,
    rows_updated: int,
    error_summary: str | None = None,
) -> None:
    store.upsert_history(
        {
            "ingestion_run_id": run.run_id,
            "job_name": run.job_name,
            "started_at": run.started_at,
            "finished_at": datetime.now(tz=timezone.utc),
            "status": status,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "error_summary": error_summary,
        }
    )


def to_daily_stat_row(
    *,
    metric_id: str,
    day: date,
    observed_at: datetime,
    value: int,
    provenance: str,
    source_window: str,
    ingestion_run_id: str,
    source_ref: str,
) -> dict[str, object]:
    day_iso = day.isoformat()
    return {
        "metric_id": metric_id,
        "period_start": day_iso,
        "period_end": day_iso,
        "observed_at": observed_at.astimezone(timezone.utc),
        "value": int(value),
        "provenance": provenance,
        "source_window": source_window,
        "ingestion_run_id": ingestion_run_id,
        "source_ref": source_ref,
    }


def resolve_seed_path(filename: str) -> Path:
    path = SEED_DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Seed file not found: {path}")
    return path


def clean_int(value: str | int | float) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    return int(str(value).strip().replace(",", ""))


def parse_day(value: str) -> date:
    return parse_iso_date(value)


def latest_period_end_for_metric(store: LanceDBStore, metric_id: str) -> date | None:
    stats = store.get_stats_for_metric(metric_id)
    if not stats:
        return None

    def _to_date(value: object) -> date | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return parse_iso_date(str(value)[:10])

    latest = max(
        (
            parsed
            for parsed in (_to_date(row.get("period_end")) for row in stats)
            if parsed
        ),
        default=None,
    )
    return latest


def days_to_refresh(
    *,
    latest_existing_period_end: date | None,
    latest_completed_period_end: date,
    lookback_days: int = 0,
) -> list[date]:
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")

    start = (
        latest_existing_period_end + timedelta(days=1)
        if latest_existing_period_end is not None
        else latest_completed_period_end
    )
    if lookback_days > 0:
        lookback_start = latest_completed_period_end - timedelta(days=lookback_days - 1)
        if lookback_start < start:
            start = lookback_start

    if start > latest_completed_period_end:
        return []

    days: list[date] = []
    cursor = start
    while cursor <= latest_completed_period_end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days
