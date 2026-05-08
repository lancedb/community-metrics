from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from community_metrics.sources.hn_client import HackerNewsClient, HackerNewsHit
from community_metrics.storage.lancedb_store import LanceDBStore

HN_SEARCH_TERMS = (
    "LanceDB",
    "lancedb",
    "@lancedb/lancedb",
    "lance format",
    "lance file format",
    "memory-lancedb",
    "memory-lancedb-pro",
    "OpenClaw lancedb",
)


def run(*, lookback_days: int = 30) -> dict[str, int]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")

    store = LanceDBStore()
    store.ensure_derived_tables()
    client = HackerNewsClient()
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=lookback_days)

    rows: dict[str, dict[str, Any]] = {}
    for term in HN_SEARCH_TERMS:
        hits = client.search_mentions(query=term, start=start, end=end)
        for hit in hits:
            if not hit.object_id:
                continue
            row = evidence_row_for_hn_hit(hit, matched_term=term)
            rows[str(row["evidence_id"])] = row

    result = store.upsert_evidence_items(list(rows.values()))
    return {"inserted": result["inserted"], "updated": result["updated"]}


def evidence_row_for_hn_hit(
    hit: HackerNewsHit,
    *,
    matched_term: str,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    related = _related_for_term(matched_term)
    hn_url = f"https://news.ycombinator.com/item?id={hit.object_id}"
    url = hit.url or hn_url
    title = hit.title or "Hacker News mention"
    snippet = _snippet(hit.text or title)
    strength = _evidence_strength(hit)
    raw_ref = {
        "object_id": hit.object_id,
        "hn_url": hn_url,
        "author": hit.author,
        "points": hit.points,
        "num_comments": hit.num_comments,
        "tags": list(hit.tags),
    }
    return {
        "evidence_id": _evidence_id(hit.object_id, matched_term),
        "source_type": "hackernews",
        "source_name": "HN Algolia",
        "observed_at": observed_at or datetime.now(tz=timezone.utc),
        "occurred_at": hit.created_at,
        "title": title,
        "url": url,
        "snippet": snippet,
        "matched_terms": [matched_term],
        "related_metrics": related["metrics"],
        "related_packages": related["packages"],
        "related_repos": [],
        "communities": related["communities"],
        "evidence_strength": strength,
        "raw_ref": json.dumps(raw_ref, sort_keys=True),
    }


def _related_for_term(term: str) -> dict[str, list[str]]:
    normalized = term.lower()
    if (
        "@lancedb/lancedb" in normalized
        or "memory-lancedb" in normalized
        or "openclaw" in normalized
    ):
        return {
            "metrics": ["downloads:lancedb:nodejs"],
            "packages": ["@lancedb/lancedb"],
            "communities": ["typescript", "agent-memory"],
        }
    if normalized in {"lance format", "lance file format"}:
        return {
            "metrics": ["downloads:lance:python", "downloads:lance:rust"],
            "packages": ["pylance", "lance"],
            "communities": ["data-infrastructure"],
        }
    return {
        "metrics": [
            "downloads:lancedb:python",
            "downloads:lancedb:nodejs",
            "downloads:lancedb:rust",
            "stars:lancedb:github",
        ],
        "packages": ["lancedb", "@lancedb/lancedb"],
        "communities": ["vector-database"],
    }


def _evidence_strength(hit: HackerNewsHit) -> str:
    if hit.points >= 50 or hit.num_comments >= 25:
        return "strong"
    if hit.points >= 10 or hit.num_comments >= 5 or "story" in hit.tags:
        return "medium"
    return "weak"


def _evidence_id(object_id: str, matched_term: str) -> str:
    digest = hashlib.sha1(matched_term.lower().encode("utf-8")).hexdigest()[:10]
    return f"hackernews:{object_id}:{digest}"


def _snippet(value: str, limit: int = 280) -> str:
    text = " ".join(value.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Hacker News evidence")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Recent HN window to search",
    )
    args = parser.parse_args()

    result = run(lookback_days=args.lookback_days)
    print(
        "collect_hn_evidence complete: "
        f"inserted={result['inserted']} updated={result['updated']}"
    )


if __name__ == "__main__":
    main()
