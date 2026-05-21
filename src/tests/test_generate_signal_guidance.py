from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from community_metrics.jobs import generate_signal_guidance
from community_metrics.jobs.generate_signal_guidance import (
    GUIDANCE_RESPONSE_SCHEMA,
    _evidence_for_guidance,
    guidance_prompt_payload,
    guidance_row,
    parse_guidance_response,
)


def _signal() -> dict[str, object]:
    return {
        "signal_id": "download_spike:2026-04-24:2026-04-30:lancedb-nodejs",
        "signal_type": "download_spike",
        "window_start": "2026-04-24",
        "window_end": "2026-04-30",
        "title": "LanceDB NodeJS downloads spiked",
        "summary": "NodeJS downloads are up 320% versus prior 7d.",
        "related_metrics": ["downloads:lancedb:nodejs"],
        "evidence_ids": ["hn-1"],
        "score": 100.0,
        "confidence": "high",
        "suggested_action": "community_outreach",
    }


def _rollup(window: str) -> dict[str, object]:
    return {
        "rollup_id": f"downloads:lancedb:nodejs:{window}:2026-04-30",
        "metric_id": "downloads:lancedb:nodejs",
        "metric_family": "downloads",
        "product": "lancedb",
        "sdk": "nodejs",
        "subject": "@lancedb/lancedb",
        "window": window,
        "window_start": "2026-04-24",
        "window_end": "2026-04-30",
        "current_value": 4200,
        "previous_value": 1000,
        "delta": 3200,
        "percent_change": 320.0,
        "sdk_share": 0.55,
        "sdk_share_delta": 0.25,
        "trend_slope": 10.0,
    }


def _evidence() -> dict[str, object]:
    return {
        "evidence_id": "hn-1",
        "source_type": "hackernews",
        "source_name": "HN Algolia",
        "occurred_at": datetime(2026, 4, 28, tzinfo=timezone.utc),
        "title": "LanceDB memory plugin",
        "url": "https://news.ycombinator.com/item?id=1",
        "snippet": "OpenClaw memory-lancedb mention",
        "matched_terms": ["memory-lancedb"],
        "related_metrics": ["downloads:lancedb:nodejs"],
        "related_packages": ["@lancedb/lancedb"],
        "communities": ["typescript", "agent-memory"],
        "evidence_strength": "medium",
    }


def test_guidance_prompt_payload_includes_7d_15d_30d_context() -> None:
    rollups = [_rollup("7d"), _rollup("15d"), _rollup("30d")]
    payload = guidance_prompt_payload(
        signal=_signal(),
        rollups=rollups,
        evidence=[_evidence()],
        evidence_context={
            "7d": {"count": 1, "matched_terms": {"memory-lancedb": 1}},
            "15d": {"count": 1},
            "30d": {"count": 1},
        },
        analysis_start=date(2026, 4, 24),
        analysis_end=date(2026, 4, 30),
    )

    assert payload["analysis_window"]["primary"]["window"] == "7d"
    assert payload["analysis_window"]["comparison_windows"] == ["15d", "30d"]
    assert {row["window"] for row in payload["rollups"]} == {"7d", "15d", "30d"}
    assert payload["evidence"][0]["evidence_id"] == "hn-1"
    assert (
        "downloads:lancedb:nodejs:15d:2026-04-30"
        in payload["output_requirements"]["citation_source_ids_must_come_from"]
    )
    assert payload["output_requirements"]["brevity"]["recommended_next_steps"] == (
        "3-4 short bullets maximum"
    )
    assert payload["output_requirements"]["brevity"]["number_format"] == (
        "use at most 1 decimal place"
    )
    assert (
        GUIDANCE_RESPONSE_SCHEMA["properties"]["recommended_next_steps"]["maxItems"]
        == 4
    )


def test_parse_guidance_response_maps_structured_output() -> None:
    raw = {
        "output_text": json.dumps(
            {
                "executive_summary": "NodeJS adoption accelerated.",
                "movement_assessment": "accelerating",
                "why_it_matters": "This may indicate TypeScript agent-memory pull.",
                "likely_community": "TypeScript agent-memory builders",
                "recommended_next_steps": [
                    "Audit TypeScript memory docs",
                    "Check the package changelog",
                    "Review linked HN context",
                    "Draft a short maintainer note",
                    "This fifth item should be dropped",
                ],
                "engineering_relevance": "watch",
                "confidence": "medium",
                "citations": [
                    {
                        "source_type": "rollup",
                        "source_id": "downloads:lancedb:nodejs:7d:2026-04-30",
                        "fact": "7d downloads increased 320%",
                        "used_for": "supports acceleration claim",
                    }
                ],
            }
        )
    }

    guidance = parse_guidance_response(raw)

    assert guidance["movement_assessment"] == "accelerating"
    assert guidance["recommended_next_steps"] == [
        "Audit TypeScript memory docs",
        "Check the package changelog",
        "Review linked HN context",
        "Draft a short maintainer note",
    ]
    assert (
        guidance["citations"][0]["source_id"]
        == "downloads:lancedb:nodejs:7d:2026-04-30"
    )


def test_guidance_row_serializes_citations() -> None:
    guidance = {
        "executive_summary": "NodeJS adoption accelerated.",
        "movement_assessment": "accelerating",
        "why_it_matters": "This may indicate TypeScript agent-memory pull.",
        "likely_community": "TypeScript agent-memory builders",
        "recommended_next_steps": ["Audit TypeScript memory docs"],
        "engineering_relevance": "watch",
        "confidence": "medium",
        "citations": [
            {
                "source_type": "signal",
                "source_id": str(_signal()["signal_id"]),
                "fact": "Signal exists",
                "used_for": "baseline",
            }
        ],
    }

    row = guidance_row(
        signal=_signal(),
        guidance=guidance,
        raw_response={"output_text": "{}"},
        analysis_start=date(2026, 4, 24),
        analysis_end=date(2026, 4, 30),
    )

    assert row["analysis_window_start"] == "2026-04-24"
    assert row["comparison_windows"] == ["15d", "30d"]
    assert json.loads(row["citations"])[0]["source_type"] == "signal"


def test_run_requires_openai_api_key(monkeypatch) -> None:
    monkeypatch.setattr(generate_signal_guidance, "OPENAI_API_KEY", "")

    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        generate_signal_guidance.run()


def test_evidence_for_guidance_filters_timestamps_and_hn_client_side() -> None:
    class _Store:
        def __init__(self) -> None:
            self.where = "not-called"

        def query_table(self, table_name, **kwargs):
            assert table_name == "evidence_items"
            self.where = kwargs.get("where")
            return [
                {
                    **_evidence(),
                    "evidence_id": "old",
                    "occurred_at": datetime(2026, 3, 1, tzinfo=timezone.utc),
                },
                _evidence(),
                {
                    **_evidence(),
                    "evidence_id": "non-hn",
                    "source_type": "dependency_manifest",
                },
            ]

    store = _Store()
    rows = _evidence_for_guidance(store, date(2026, 4, 30))

    assert store.where is None
    assert [row["evidence_id"] for row in rows] == ["non-hn"]


def test_run_upserts_guidance_without_mutating_source_tables(monkeypatch) -> None:
    class _Store:
        def __init__(self) -> None:
            self.seed_called = False
            self.guidance_rows = []

        def ensure_derived_tables(self) -> None:
            pass

        def seed_metrics(self):
            self.seed_called = True

        def query_table(self, table_name, **_kwargs):
            if table_name == "signal_candidates":
                return [
                    _signal(),
                    {
                        **_signal(),
                        "signal_id": "social_mention_burst:2026-04-24:2026-04-30:hn-manual-mention-burst",
                        "signal_type": "social_mention_burst",
                    },
                ]
            if table_name == "dashboard_metric_rollups":
                return [_rollup("7d"), _rollup("15d"), _rollup("30d")]
            if table_name == "evidence_items":
                return [_evidence()]
            raise AssertionError(f"unexpected table: {table_name}")

        def upsert_signal_guidance(self, rows):
            self.guidance_rows = list(rows)
            return {"inserted": len(rows), "updated": 0}

    class _Client:
        def __init__(self, *, api_key):
            assert api_key == "key"

        def create_structured_response(self, **kwargs):
            assert kwargs["model"] == "gpt-5.4"
            assert kwargs["reasoning_effort"] == "high"
            payload = kwargs["payload"]
            assert {row["window"] for row in payload["rollups"]} == {"7d", "15d", "30d"}
            assert payload["evidence"] == []
            return {
                "output_text": json.dumps(
                    {
                        "executive_summary": "NodeJS adoption accelerated.",
                        "movement_assessment": "accelerating",
                        "why_it_matters": "This may indicate TypeScript agent-memory pull.",
                        "likely_community": "TypeScript agent-memory builders",
                        "recommended_next_steps": ["Audit TypeScript memory docs"],
                        "engineering_relevance": "watch",
                        "confidence": "medium",
                        "citations": [
                            {
                                "source_type": "rollup",
                                "source_id": "downloads:lancedb:nodejs:7d:2026-04-30",
                                "fact": "7d downloads increased 320%",
                                "used_for": "supports acceleration claim",
                            },
                            {
                                "source_type": "rollup",
                                "source_id": "unknown",
                                "fact": "bad",
                                "used_for": "should be dropped",
                            },
                        ],
                    }
                )
            }

    store = _Store()
    monkeypatch.setattr(generate_signal_guidance, "OPENAI_API_KEY", "key")
    monkeypatch.setattr(generate_signal_guidance, "OPENAI_MODEL", "gpt-5.4")
    monkeypatch.setattr(generate_signal_guidance, "LanceDBStore", lambda: store)
    monkeypatch.setattr(generate_signal_guidance, "OpenAIResponsesClient", _Client)
    monkeypatch.setattr(
        generate_signal_guidance,
        "latest_completed_day",
        lambda: date(2026, 4, 30),
    )

    result = generate_signal_guidance.run(window_days=7)

    assert result == {"inserted": 1, "updated": 0}
    assert store.seed_called is False
    citations = json.loads(store.guidance_rows[0]["citations"])
    assert len(citations) == 1
    assert citations[0]["source_id"] == "downloads:lancedb:nodejs:7d:2026-04-30"
