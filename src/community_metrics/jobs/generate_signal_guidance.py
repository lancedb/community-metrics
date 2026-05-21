from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from community_metrics.config import (
    OPENAI_API_KEY,
    OPENAI_GUIDANCE_PROMPT_VERSION,
    OPENAI_MODEL,
    OPENAI_REASONING_EFFORT,
    OPENAI_TIMEOUT_SECONDS,
)
from community_metrics.sources.openai_client import OpenAIResponsesClient
from community_metrics.storage.lancedb_store import LanceDBStore
from community_metrics.utils.time import latest_completed_day, parse_iso_date

PRIMARY_WINDOW = "7d"
COMPARISON_WINDOWS = ("15d", "30d")
GUIDANCE_SCHEMA_VERSION = "v2"
ALLOWED_ENGINEERING_RELEVANCE = {"ignore", "watch", "investigate", "escalate"}
ALLOWED_CONFIDENCE = {"low", "medium", "high"}
ALLOWED_MOVEMENT = {"new", "sustained", "accelerating", "fading", "unclear"}


GUIDANCE_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "executive_summary",
        "movement_assessment",
        "why_it_matters",
        "likely_community",
        "recommended_next_steps",
        "engineering_relevance",
        "confidence",
        "citations",
    ],
    "properties": {
        "executive_summary": {"type": "string", "maxLength": 180},
        "movement_assessment": {
            "type": "string",
            "enum": sorted(ALLOWED_MOVEMENT),
        },
        "why_it_matters": {"type": "string", "maxLength": 220},
        "likely_community": {"type": "string", "maxLength": 80},
        "recommended_next_steps": {
            "type": "array",
            "items": {"type": "string", "maxLength": 140},
            "minItems": 1,
            "maxItems": 4,
        },
        "engineering_relevance": {
            "type": "string",
            "enum": sorted(ALLOWED_ENGINEERING_RELEVANCE),
        },
        "confidence": {"type": "string", "enum": sorted(ALLOWED_CONFIDENCE)},
        "citations": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["source_type", "source_id", "fact", "used_for"],
                "properties": {
                    "source_type": {
                        "type": "string",
                        "enum": ["signal", "rollup", "evidence"],
                    },
                    "source_id": {"type": "string"},
                    "fact": {"type": "string", "maxLength": 180},
                    "used_for": {"type": "string", "maxLength": 120},
                },
            },
            "minItems": 1,
            "maxItems": 8,
        },
    },
}


def run(*, window_days: int = 7) -> dict[str, int]:
    if window_days != 7:
        raise ValueError("Only --window-days 7 is supported for weekly guidance")
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY for generate_signal_guidance")

    store = LanceDBStore()
    store.ensure_derived_tables()
    latest_day = latest_completed_day()
    analysis_start = latest_day - timedelta(days=window_days - 1)

    signals = _weekly_signals(store, analysis_start, latest_day)
    rollups = _rollups_for_guidance(store, latest_day)
    evidence = _evidence_for_guidance(store, latest_day)
    evidence_context = _evidence_context(evidence, latest_day)
    client = OpenAIResponsesClient(api_key=OPENAI_API_KEY)

    print(
        "[generate_signal_guidance] "
        f"window={analysis_start.isoformat()}..{latest_day.isoformat()} "
        f"signals={len(signals)} rollups={len(rollups)} evidence={len(evidence)} "
        f"model={OPENAI_MODEL} reasoning={OPENAI_REASONING_EFFORT} "
        f"timeout={OPENAI_TIMEOUT_SECONDS}s",
        flush=True,
    )

    rows: list[dict[str, Any]] = []
    for index, signal in enumerate(signals, start=1):
        related_rollups = _related_rollups(signal, rollups)
        related_evidence = _related_evidence(
            signal, evidence, analysis_start, latest_day
        )
        print(
            "[generate_signal_guidance] "
            f"({index}/{len(signals)}) requesting guidance for "
            f"signal_id={signal.get('signal_id')} "
            f"type={signal.get('signal_type')} "
            f"related_rollups={len(related_rollups)} "
            f"related_evidence={len(related_evidence)}",
            flush=True,
        )
        payload = guidance_prompt_payload(
            signal=signal,
            rollups=related_rollups,
            evidence=related_evidence,
            evidence_context=evidence_context,
            analysis_start=analysis_start,
            analysis_end=latest_day,
        )
        raw = client.create_structured_response(
            model=OPENAI_MODEL,
            reasoning_effort=OPENAI_REASONING_EFFORT,
            instructions=GUIDANCE_INSTRUCTIONS,
            payload=payload,
            schema=GUIDANCE_RESPONSE_SCHEMA,
        )
        guidance = parse_guidance_response(raw)
        valid_ids = _valid_citation_ids(signal, related_rollups, related_evidence)
        guidance["citations"] = [
            citation
            for citation in guidance["citations"]
            if citation["source_id"] in valid_ids
        ]
        if not guidance["citations"]:
            guidance["citations"] = [_fallback_citation(signal)]
        print(
            "[generate_signal_guidance] "
            f"({index}/{len(signals)}) received guidance "
            f"confidence={guidance['confidence']} "
            f"engineering_relevance={guidance['engineering_relevance']} "
            f"citations={len(guidance['citations'])}",
            flush=True,
        )
        rows.append(
            guidance_row(
                signal=signal,
                guidance=guidance,
                raw_response=raw,
                analysis_start=analysis_start,
                analysis_end=latest_day,
            )
        )

    result = store.upsert_signal_guidance(rows)
    print(
        "[generate_signal_guidance] "
        f"upserted guidance rows inserted={result['inserted']} updated={result['updated']}",
        flush=True,
    )
    return {"inserted": result["inserted"], "updated": result["updated"]}


GUIDANCE_INSTRUCTIONS = """
You are generating DevRel guidance for the LanceDB community metrics dashboard.
Use only the supplied JSON payload. Do not invent evidence, repositories, packages,
communities, causes, customer names, or actions. If the data is insufficient, say so
and lower confidence. The 7d window is the primary assessment period. Use 15d and 30d
rollups as comparison context to decide whether a signal looks new, sustained,
accelerating, fading, or unclear. Every non-obvious conclusion must cite a supplied
signal_id, rollup_id, or evidence_id with the concrete fact used.

Keep the Insights tab readable. Write one concise sentence for executive_summary and
why_it_matters. Generate no more than 3-4 recommended_next_steps bullets, and keep
each bullet short, concrete, and directly useful to a DevRel reader. When writing
numbers, use at most 1 decimal place.
""".strip()


def guidance_prompt_payload(
    *,
    signal: dict[str, Any],
    rollups: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    evidence_context: dict[str, Any],
    analysis_start: date,
    analysis_end: date,
) -> dict[str, Any]:
    return {
        "analysis_window": {
            "primary": {
                "window": PRIMARY_WINDOW,
                "start": analysis_start.isoformat(),
                "end": analysis_end.isoformat(),
            },
            "comparison_windows": list(COMPARISON_WINDOWS),
        },
        "signal": _signal_payload(signal),
        "rollups": [_rollup_payload(row) for row in rollups],
        "evidence": [_evidence_payload(row) for row in evidence],
        "evidence_context": evidence_context,
        "output_requirements": {
            "engineering_relevance_values": sorted(ALLOWED_ENGINEERING_RELEVANCE),
            "confidence_values": sorted(ALLOWED_CONFIDENCE),
            "movement_assessment_values": sorted(ALLOWED_MOVEMENT),
            "citation_source_ids_must_come_from": _payload_ids(
                signal, rollups, evidence
            ),
            "brevity": {
                "executive_summary": "one concise sentence",
                "why_it_matters": "one concise sentence",
                "recommended_next_steps": "3-4 short bullets maximum",
                "number_format": "use at most 1 decimal place",
            },
        },
    }


def parse_guidance_response(raw_response: dict[str, Any]) -> dict[str, Any]:
    text = _response_text(raw_response)
    parsed = json.loads(text)
    citations = [
        {
            "source_type": str(citation.get("source_type") or ""),
            "source_id": str(citation.get("source_id") or ""),
            "fact": str(citation.get("fact") or ""),
            "used_for": str(citation.get("used_for") or ""),
        }
        for citation in parsed.get("citations", [])
        if citation.get("source_id")
    ]
    return {
        "executive_summary": str(parsed.get("executive_summary") or ""),
        "movement_assessment": _one_of(
            str(parsed.get("movement_assessment") or "unclear"),
            ALLOWED_MOVEMENT,
            "unclear",
        ),
        "why_it_matters": str(parsed.get("why_it_matters") or ""),
        "likely_community": str(parsed.get("likely_community") or ""),
        "recommended_next_steps": [
            str(step)
            for step in parsed.get("recommended_next_steps", [])
            if str(step).strip()
        ][:4],
        "engineering_relevance": _one_of(
            str(parsed.get("engineering_relevance") or "watch"),
            ALLOWED_ENGINEERING_RELEVANCE,
            "watch",
        ),
        "confidence": _one_of(
            str(parsed.get("confidence") or "low"),
            ALLOWED_CONFIDENCE,
            "low",
        ),
        "citations": citations,
    }


def guidance_row(
    *,
    signal: dict[str, Any],
    guidance: dict[str, Any],
    raw_response: dict[str, Any],
    analysis_start: date,
    analysis_end: date,
) -> dict[str, Any]:
    signal_id = str(signal["signal_id"])
    return {
        "guidance_id": (
            f"{signal_id}:{analysis_start.isoformat()}:{analysis_end.isoformat()}:"
            f"{OPENAI_GUIDANCE_PROMPT_VERSION}"
        ),
        "signal_id": signal_id,
        "generated_at": datetime.now(tz=timezone.utc),
        "model": OPENAI_MODEL,
        "reasoning_effort": OPENAI_REASONING_EFFORT,
        "prompt_version": OPENAI_GUIDANCE_PROMPT_VERSION,
        "analysis_window_start": analysis_start.isoformat(),
        "analysis_window_end": analysis_end.isoformat(),
        "comparison_windows": list(COMPARISON_WINDOWS),
        "executive_summary": guidance["executive_summary"],
        "movement_assessment": guidance["movement_assessment"],
        "why_it_matters": guidance["why_it_matters"],
        "likely_community": guidance["likely_community"],
        "recommended_next_steps": guidance["recommended_next_steps"],
        "engineering_relevance": guidance["engineering_relevance"],
        "confidence": guidance["confidence"],
        "citations": json.dumps(guidance["citations"], sort_keys=True),
        "raw_response": json.dumps(raw_response, sort_keys=True, default=str),
    }


def _weekly_signals(
    store: LanceDBStore, analysis_start: date, analysis_end: date
) -> list[dict[str, Any]]:
    rows = store.query_table("signal_candidates", limit=None)
    matches = [
        row
        for row in rows
        if _parse_day(row.get("window_end")) == analysis_end
        and str(row.get("signal_type")) != "social_mention_burst"
    ]
    return sorted(matches, key=lambda row: float(row.get("score") or 0), reverse=True)


def _rollups_for_guidance(
    store: LanceDBStore, latest_day: date
) -> list[dict[str, Any]]:
    windows = ", ".join(
        _sql_quote(window) for window in (PRIMARY_WINDOW, *COMPARISON_WINDOWS)
    )
    where = (
        f"window IN ({windows}) AND window_end = {_sql_quote(latest_day.isoformat())}"
    )
    return store.query_table("dashboard_metric_rollups", where=where, limit=500)


def _evidence_for_guidance(
    store: LanceDBStore, latest_day: date
) -> list[dict[str, Any]]:
    start = latest_day - timedelta(days=29)
    rows = store.query_table("evidence_items", limit=500)
    return [
        row
        for row in rows
        if str(row.get("source_type")) not in {"hackernews", "manual"}
        and start <= _parse_datetime(row.get("occurred_at")).date() <= latest_day
    ]


def _related_rollups(
    signal: dict[str, Any], rollups: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    related_metrics = set(str(metric) for metric in signal.get("related_metrics", []))
    if not related_metrics:
        return []
    return [
        row
        for row in rollups
        if str(row.get("metric_id")) in related_metrics
        and str(row.get("window")) in {PRIMARY_WINDOW, *COMPARISON_WINDOWS}
    ]


def _related_evidence(
    signal: dict[str, Any],
    evidence: list[dict[str, Any]],
    analysis_start: date,
    analysis_end: date,
) -> list[dict[str, Any]]:
    signal_evidence = set(str(item) for item in signal.get("evidence_ids", []))
    related_metrics = set(str(metric) for metric in signal.get("related_metrics", []))
    matches: list[dict[str, Any]] = []
    for row in evidence:
        occurred_day = _parse_datetime(row.get("occurred_at")).date()
        if not (analysis_start <= occurred_day <= analysis_end):
            continue
        evidence_id = str(row.get("evidence_id") or "")
        evidence_metrics = set(str(metric) for metric in row.get("related_metrics", []))
        if evidence_id in signal_evidence or evidence_metrics.intersection(
            related_metrics
        ):
            matches.append(row)
    return sorted(
        matches, key=lambda row: _parse_datetime(row.get("occurred_at")), reverse=True
    )[:8]


def _evidence_context(
    evidence: list[dict[str, Any]], latest_day: date
) -> dict[str, Any]:
    context: dict[str, Any] = {}
    for days in (7, 15, 30):
        start = latest_day - timedelta(days=days - 1)
        rows = [
            row
            for row in evidence
            if start <= _parse_datetime(row.get("occurred_at")).date() <= latest_day
        ]
        terms = Counter(
            str(term) for row in rows for term in row.get("matched_terms", []) if term
        )
        communities = Counter(
            str(community)
            for row in rows
            for community in row.get("communities", [])
            if community
        )
        context[f"{days}d"] = {
            "count": len(rows),
            "matched_terms": dict(terms.most_common(10)),
            "communities": dict(communities.most_common(10)),
        }
    return context


def _signal_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": str(row.get("signal_id") or ""),
        "signal_type": str(row.get("signal_type") or ""),
        "window_start": str(row.get("window_start") or ""),
        "window_end": str(row.get("window_end") or ""),
        "title": str(row.get("title") or ""),
        "summary": str(row.get("summary") or ""),
        "related_metrics": [str(metric) for metric in row.get("related_metrics", [])],
        "evidence_ids": [str(item) for item in row.get("evidence_ids", [])],
        "score": float(row.get("score") or 0),
        "confidence": str(row.get("confidence") or ""),
        "suggested_action": str(row.get("suggested_action") or ""),
    }


def _rollup_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rollup_id": str(row.get("rollup_id") or ""),
        "metric_id": str(row.get("metric_id") or ""),
        "metric_family": str(row.get("metric_family") or ""),
        "product": str(row.get("product") or ""),
        "sdk": str(row.get("sdk") or ""),
        "subject": str(row.get("subject") or ""),
        "window": str(row.get("window") or ""),
        "window_start": str(row.get("window_start") or ""),
        "window_end": str(row.get("window_end") or ""),
        "current_value": int(row.get("current_value") or 0),
        "previous_value": int(row.get("previous_value") or 0),
        "delta": int(row.get("delta") or 0),
        "percent_change": float(row.get("percent_change") or 0),
        "sdk_share": float(row.get("sdk_share") or 0),
        "sdk_share_delta": float(row.get("sdk_share_delta") or 0),
        "trend_slope": float(row.get("trend_slope") or 0),
    }


def _evidence_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "evidence_id": str(row.get("evidence_id") or ""),
        "source_type": str(row.get("source_type") or ""),
        "source_name": str(row.get("source_name") or ""),
        "occurred_at": _parse_datetime(row.get("occurred_at")).isoformat(),
        "title": str(row.get("title") or ""),
        "url": str(row.get("url") or ""),
        "snippet": str(row.get("snippet") or ""),
        "matched_terms": [str(term) for term in row.get("matched_terms", [])],
        "related_metrics": [str(metric) for metric in row.get("related_metrics", [])],
        "related_packages": [
            str(package) for package in row.get("related_packages", [])
        ],
        "communities": [str(community) for community in row.get("communities", [])],
        "evidence_strength": str(row.get("evidence_strength") or ""),
    }


def _payload_ids(
    signal: dict[str, Any],
    rollups: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
) -> list[str]:
    return [
        str(signal.get("signal_id") or ""),
        *[str(row.get("rollup_id") or "") for row in rollups],
        *[str(row.get("evidence_id") or "") for row in evidence],
    ]


def _valid_citation_ids(
    signal: dict[str, Any],
    rollups: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
) -> set[str]:
    return set(_payload_ids(signal, rollups, evidence))


def _fallback_citation(signal: dict[str, Any]) -> dict[str, str]:
    return {
        "source_type": "signal",
        "source_id": str(signal.get("signal_id") or ""),
        "fact": str(signal.get("summary") or signal.get("title") or ""),
        "used_for": "fallback citation for generated guidance",
    }


def _response_text(response: dict[str, Any]) -> str:
    if isinstance(response.get("output_text"), str):
        return str(response["output_text"])
    for item in response.get("output", []):
        for content in item.get("content", []):
            if isinstance(content.get("text"), str):
                return str(content["text"])
    raise ValueError("OpenAI response did not contain output text")


def _one_of(value: str, allowed: set[str], default: str) -> str:
    return value if value in allowed else default


def _parse_day(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return parse_iso_date(str(value)[:10])


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    raw = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate weekly OpenAI guidance for signal candidates"
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Primary LLM analysis window. Only 7 is supported.",
    )
    args = parser.parse_args()

    result = run(window_days=args.window_days)
    print(
        "generate_signal_guidance complete: "
        f"inserted={result['inserted']} updated={result['updated']}"
    )


if __name__ == "__main__":
    main()
