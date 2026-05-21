from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

import requests

from community_metrics.config import REQUEST_TIMEOUT_SECONDS

DuckDBExtensionRepo = Literal["core", "community"]


@dataclass(frozen=True)
class WeeklyExtensionDownloads:
    repo: DuckDBExtensionRepo
    week_date: date
    source_url: str
    source_update_at: datetime
    downloads: int


class DuckDBExtensionsClient:
    base_urls: dict[DuckDBExtensionRepo, str] = {
        "core": "https://extensions.duckdb.org",
        "community": "https://community-extensions.duckdb.org",
    }

    def __init__(self, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    def fetch_lance_weekly_downloads(
        self,
        *,
        start_day: date,
        end_day: date,
    ) -> list[WeeklyExtensionDownloads]:
        rows: list[WeeklyExtensionDownloads] = []
        for week_date in weekly_snapshot_dates(start_day=start_day, end_day=end_day):
            for repo in ("core", "community"):
                row = self._fetch_week(repo, week_date)
                if row is not None:
                    rows.append(row)
        return rows

    def _fetch_week(
        self,
        repo: DuckDBExtensionRepo,
        week_date: date,
    ) -> WeeklyExtensionDownloads | None:
        url = weekly_download_url(repo, week_date)
        response = requests.get(url, timeout=self.timeout_seconds)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected DuckDB extension payload at {url}")

        source_update_raw = payload.get("_last_update")
        if not source_update_raw:
            raise ValueError(
                f"Missing _last_update in DuckDB extension payload at {url}"
            )

        return WeeklyExtensionDownloads(
            repo=repo,
            week_date=week_date,
            source_url=url,
            source_update_at=parse_source_update_at(source_update_raw),
            downloads=_coerce_downloads(payload.get("lance")),
        )


def weekly_snapshot_dates(*, start_day: date, end_day: date) -> list[date]:
    if end_day < start_day:
        return []

    dates: list[date] = []
    cursor = start_day
    while cursor <= end_day:
        iso_week = cursor.isocalendar().week
        if iso_week != 53:
            dates.append(cursor)
        cursor += timedelta(days=7)
    return dates


def weekly_download_url(repo: DuckDBExtensionRepo, week_date: date) -> str:
    year = week_date.strftime("%Y")
    week_num = str(week_date.isocalendar().week)
    base_url = DuckDBExtensionsClient.base_urls[repo]
    return f"{base_url}/download-stats-weekly/{year}/{week_num}.json"


def parse_source_update_at(value: Any) -> datetime:
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_downloads(value: Any) -> int:
    if value is None:
        return 0
    return int(value)
