from __future__ import annotations

from dataclasses import dataclass

import requests

from community_metrics.config import REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class DailyDownload:
    day: str
    downloads: int


class PyPIStatsClient:
    base_url = "https://pypistats.org/api/packages"

    def __init__(self, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    def fetch_daily_downloads(self, package: str) -> list[DailyDownload]:
        url = f"{self.base_url}/{package}/overall"
        response = requests.get(
            url, params={"mirrors": "true"}, timeout=self.timeout_seconds
        )
        response.raise_for_status()

        payload = response.json()
        raw_rows = payload.get("data", [])
        if isinstance(raw_rows, dict):
            raw_rows = raw_rows.get("data", [])

        rows: list[DailyDownload] = []
        for row in raw_rows:
            day = row.get("date") or row.get("day")
            downloads = row.get("downloads") or row.get("count")
            if not day or downloads is None:
                continue
            rows.append(DailyDownload(day=str(day), downloads=int(downloads)))
        return rows
