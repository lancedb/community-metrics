from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from urllib.parse import quote

import requests

from community_metrics.config import REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class DailyDownload:
    day: str
    downloads: int


class NpmDownloadsClient:
    base_url = "https://api.npmjs.org/downloads/range"

    def __init__(self, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    def fetch_daily_downloads(
        self, package: str, start: date, end: date
    ) -> list[DailyDownload]:
        encoded_package = quote(package, safe="")
        date_range = f"{start.isoformat()}:{end.isoformat()}"
        url = f"{self.base_url}/{date_range}/{encoded_package}"

        response = requests.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        rows: list[DailyDownload] = []
        for row in payload.get("downloads", []):
            day = row.get("day")
            downloads = row.get("downloads")
            if not day or downloads is None:
                continue
            rows.append(DailyDownload(day=str(day), downloads=int(downloads)))
        return rows
