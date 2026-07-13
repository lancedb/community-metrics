from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import time

import requests

from community_metrics.config import REQUEST_TIMEOUT_SECONDS
from community_metrics.sources.http_client import (
    COMMUNITY_METRICS_USER_AGENT,
    build_retrying_session,
)


@dataclass(frozen=True)
class DailyDownload:
    day: str
    downloads: int


class CratesClient:
    base_url = "https://crates.io/api/v1/crates"

    def __init__(
        self,
        timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        session: requests.Session | None = None,
        min_request_interval_seconds: float = 1.0,
    ):
        self.timeout_seconds = timeout_seconds
        self.session = session or build_retrying_session()
        self.session.headers.update({"User-Agent": COMMUNITY_METRICS_USER_AGENT})
        self.min_request_interval_seconds = max(0.0, min_request_interval_seconds)
        self._last_request_started_at: float | None = None

    def _respect_request_interval(self) -> None:
        now = time.monotonic()
        if self._last_request_started_at is not None:
            remaining = self.min_request_interval_seconds - (
                now - self._last_request_started_at
            )
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_started_at = time.monotonic()

    def fetch_daily_downloads(self, crate_name: str) -> list[DailyDownload]:
        url = f"{self.base_url}/{crate_name}/downloads"
        self._respect_request_interval()
        response = self.session.get(url, timeout=self.timeout_seconds)
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
        if not rows:
            raise RuntimeError(
                f"crates.io returned no daily rows for crate '{crate_name}'"
            )
        return rows
