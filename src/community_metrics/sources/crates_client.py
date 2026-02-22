from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import requests

from community_metrics.config import REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class DailyDownload:
    day: str
    downloads: int


class CratesClient:
    base_url = "https://crates.io/api/v1/crates"

    def __init__(self, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    def fetch_daily_downloads(self, crate_name: str) -> list[DailyDownload]:
        url = f"{self.base_url}/{crate_name}/downloads"
        response = requests.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        totals_by_day: dict[str, int] = defaultdict(int)

        # Per-version daily downloads for recent versions.
        for row in payload.get("version_downloads", []):
            day = row.get("date")
            downloads = row.get("downloads")
            if not day or downloads is None:
                continue
            totals_by_day[str(day)] += int(downloads)

        # Additional daily downloads not included in version_downloads
        # (typically older versions bucketed together).
        meta = payload.get("meta", {}) or {}
        for row in meta.get("extra_downloads", []):
            day = row.get("date")
            downloads = row.get("downloads")
            if not day or downloads is None:
                continue
            totals_by_day[str(day)] += int(downloads)

        rows: list[DailyDownload] = [
            DailyDownload(day=day, downloads=downloads)
            for day, downloads in sorted(totals_by_day.items())
        ]
        return rows
