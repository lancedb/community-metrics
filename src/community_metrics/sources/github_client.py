from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

import requests

from community_metrics.config import GITHUB_TOKEN, REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class StargazerEvent:
    starred_at: datetime


class GitHubClient:
    base_url = "https://api.github.com"

    def __init__(
        self,
        token: str | None = GITHUB_TOKEN,
        timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
    ):
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.github+json"})
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def get_repo_stars(self, repo: str) -> int:
        url = f"{self.base_url}/repos/{repo}"
        response = self.session.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        return int(payload.get("stargazers_count", 0))

    def iter_stargazer_events(
        self, repo: str, per_page: int = 100
    ) -> Iterator[StargazerEvent]:
        url = f"{self.base_url}/repos/{repo}/stargazers"
        headers = {"Accept": "application/vnd.github.star+json"}
        page = 1
        while True:
            response = self.session.get(
                url,
                params={"per_page": per_page, "page": page},
                headers=headers,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            rows = response.json()
            if not rows:
                break
            for row in rows:
                starred_at = row.get("starred_at")
                if not starred_at:
                    continue
                dt = datetime.fromisoformat(
                    starred_at.replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                yield StargazerEvent(starred_at=dt)
            page += 1
