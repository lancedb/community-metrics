from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Any

import requests

from community_metrics.config import REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class HackerNewsHit:
    object_id: str
    created_at: datetime
    title: str
    url: str
    text: str
    author: str
    points: int
    num_comments: int
    tags: tuple[str, ...]


class HackerNewsClient:
    base_url = "https://hn.algolia.com/api/v1/search_by_date"

    def __init__(self, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    def search_mentions(
        self,
        *,
        query: str,
        start: datetime,
        end: datetime,
        tags: tuple[str, ...] = ("story", "comment"),
        hits_per_page: int = 100,
        max_pages: int = 5,
    ) -> list[HackerNewsHit]:
        hits: list[HackerNewsHit] = []
        for tag in tags:
            for page in range(max_pages):
                payload = self._search_page(
                    query=query,
                    start=start,
                    end=end,
                    tag=tag,
                    hits_per_page=hits_per_page,
                    page=page,
                )
                raw_hits = payload.get("hits", [])
                hits.extend(self._parse_hit(hit) for hit in raw_hits)
                if page + 1 >= int(payload.get("nbPages", 0) or 0):
                    break
        return hits

    def _search_page(
        self,
        *,
        query: str,
        start: datetime,
        end: datetime,
        tag: str,
        hits_per_page: int,
        page: int,
    ) -> dict[str, Any]:
        params = {
            "query": query,
            "tags": tag,
            "hitsPerPage": str(hits_per_page),
            "page": str(page),
            "numericFilters": (
                f"created_at_i>={int(start.timestamp())},"
                f"created_at_i<={int(end.timestamp())}"
            ),
        }
        response = requests.get(
            self.base_url, params=params, timeout=self.timeout_seconds
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _parse_hit(raw: dict[str, Any]) -> HackerNewsHit:
        object_id = str(raw.get("objectID") or "")
        created_at = HackerNewsClient._parse_created_at(raw)
        title = str(
            raw.get("title")
            or raw.get("story_title")
            or raw.get("comment_text")
            or "Hacker News mention"
        )
        url = str(raw.get("url") or raw.get("story_url") or "")
        text = str(raw.get("comment_text") or raw.get("story_text") or "")
        author = str(raw.get("author") or "")
        tags = tuple(str(tag) for tag in raw.get("_tags", []) if tag)
        return HackerNewsHit(
            object_id=object_id,
            created_at=created_at,
            title=HackerNewsClient._clean_text(title),
            url=url,
            text=HackerNewsClient._clean_text(text),
            author=author,
            points=int(raw.get("points") or 0),
            num_comments=int(raw.get("num_comments") or 0),
            tags=tags,
        )

    @staticmethod
    def _parse_created_at(raw: dict[str, Any]) -> datetime:
        epoch = raw.get("created_at_i")
        if epoch is not None:
            return datetime.fromtimestamp(int(epoch), tz=timezone.utc)
        created_at = str(raw.get("created_at") or "")
        parsed = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _clean_text(value: str) -> str:
        text = unescape(value)
        cleaned: list[str] = []
        in_tag = False
        for char in text:
            if char == "<":
                in_tag = True
                continue
            if char == ">":
                in_tag = False
                cleaned.append(" ")
                continue
            if not in_tag:
                cleaned.append(char)
        return " ".join("".join(cleaned).split())
