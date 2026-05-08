from __future__ import annotations

from datetime import datetime, timezone

from community_metrics.jobs import collect_hn_evidence
from community_metrics.jobs.collect_hn_evidence import evidence_row_for_hn_hit
from community_metrics.sources.hn_client import HackerNewsClient, HackerNewsHit


def test_hn_client_parses_hit_and_strips_html() -> None:
    hit = HackerNewsClient._parse_hit(
        {
            "objectID": "123",
            "created_at_i": 1775000000,
            "story_title": "LanceDB thread",
            "comment_text": "Try <b>@lancedb/lancedb</b> for memory",
            "author": "dev",
            "points": 12,
            "num_comments": 6,
            "_tags": ["comment"],
        }
    )

    assert hit.object_id == "123"
    assert hit.created_at == datetime.fromtimestamp(1775000000, tz=timezone.utc)
    assert hit.title == "LanceDB thread"
    assert hit.text == "Try @lancedb/lancedb for memory"


def test_evidence_row_for_hn_hit_preserves_occurrence_date_and_snippet() -> None:
    occurred_at = datetime(2026, 4, 15, 12, tzinfo=timezone.utc)
    observed_at = datetime(2026, 4, 16, 12, tzinfo=timezone.utc)
    hit = HackerNewsHit(
        object_id="456",
        created_at=occurred_at,
        title="OpenClaw LanceDB memory plugin",
        url="",
        text="memory-lancedb looks useful for TypeScript agents",
        author="dev",
        points=55,
        num_comments=30,
        tags=("story",),
    )

    row = evidence_row_for_hn_hit(
        hit,
        matched_term="memory-lancedb",
        observed_at=observed_at,
    )

    assert row["source_type"] == "hackernews"
    assert row["occurred_at"] == occurred_at
    assert row["observed_at"] == observed_at
    assert row["snippet"] == "memory-lancedb looks useful for TypeScript agents"
    assert row["url"] == "https://news.ycombinator.com/item?id=456"
    assert row["related_metrics"] == ["downloads:lancedb:nodejs"]
    assert row["communities"] == ["typescript", "agent-memory"]
    assert row["evidence_strength"] == "strong"


def test_collect_hn_run_only_uses_derived_tables(monkeypatch) -> None:
    hit = HackerNewsHit(
        object_id="789",
        created_at=datetime(2026, 4, 15, 12, tzinfo=timezone.utc),
        title="LanceDB",
        url="",
        text="lancedb mention",
        author="dev",
        points=0,
        num_comments=0,
        tags=("comment",),
    )

    class _Store:
        def __init__(self) -> None:
            self.seed_called = False
            self.rows = []

        def ensure_derived_tables(self) -> None:
            pass

        def seed_metrics(self):
            self.seed_called = True

        def upsert_evidence_items(self, rows):
            self.rows = list(rows)
            return {"inserted": len(rows), "updated": 0}

    class _Client:
        def search_mentions(self, **_kwargs):
            return [hit]

    store = _Store()
    monkeypatch.setattr(collect_hn_evidence, "LanceDBStore", lambda: store)
    monkeypatch.setattr(collect_hn_evidence, "HackerNewsClient", _Client)

    result = collect_hn_evidence.run(lookback_days=1)

    unique_terms = {term.lower() for term in collect_hn_evidence.HN_SEARCH_TERMS}
    assert result["inserted"] == len(unique_terms)
    assert store.seed_called is False
    assert all(row["source_type"] == "hackernews" for row in store.rows)
