from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa


@dataclass(frozen=True)
class MetricDefinition:
    metric_id: str
    metric_family: str
    product: str
    subject: str
    sdk: str | None
    source: str
    value_kind: str
    unit: str
    is_active: bool
    display_name: str


@dataclass(frozen=True)
class MetricPoint:
    metric_id: str
    period_start: str
    period_end: str
    observed_at: str
    value: int
    provenance: str
    source_window: str
    ingestion_run_id: str
    source_ref: str


METRICS_SCHEMA = pa.schema(
    [
        pa.field("metric_id", pa.string()),
        pa.field("metric_family", pa.string()),
        pa.field("product", pa.string()),
        pa.field("subject", pa.string()),
        pa.field("sdk", pa.string()),
        pa.field("source", pa.string()),
        pa.field("value_kind", pa.string()),
        pa.field("unit", pa.string()),
        pa.field("is_active", pa.bool_()),
        pa.field("display_name", pa.string()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
    ]
)

STATS_SCHEMA = pa.schema(
    [
        pa.field("metric_id", pa.string()),
        pa.field("period_start", pa.string()),
        pa.field("period_end", pa.string()),
        pa.field("observed_at", pa.timestamp("us", tz="UTC")),
        pa.field("value", pa.int64()),
        pa.field("provenance", pa.string()),
        pa.field("source_window", pa.string()),
        pa.field("ingestion_run_id", pa.string()),
        pa.field("source_ref", pa.string()),
    ]
)

HISTORY_SCHEMA = pa.schema(
    [
        pa.field("ingestion_run_id", pa.string()),
        pa.field("job_name", pa.string()),
        pa.field("started_at", pa.timestamp("us", tz="UTC")),
        pa.field("finished_at", pa.timestamp("us", tz="UTC")),
        pa.field("status", pa.string()),
        pa.field("rows_inserted", pa.int64()),
        pa.field("rows_updated", pa.int64()),
        pa.field("error_summary", pa.string()),
    ]
)


METRIC_DEFINITIONS: list[MetricDefinition] = [
    MetricDefinition(
        metric_id="downloads:lance:python",
        metric_family="downloads",
        product="lance",
        subject="pylance",
        sdk="python",
        source="pypistats",
        value_kind="daily_downloads",
        unit="count",
        is_active=True,
        display_name="Python",
    ),
    MetricDefinition(
        metric_id="downloads:lance:rust",
        metric_family="downloads",
        product="lance",
        subject="lance",
        sdk="rust",
        source="cratesio",
        value_kind="daily_downloads",
        unit="count",
        is_active=True,
        display_name="Rust",
    ),
    MetricDefinition(
        metric_id="downloads:lancedb:python",
        metric_family="downloads",
        product="lancedb",
        subject="lancedb",
        sdk="python",
        source="pypistats",
        value_kind="daily_downloads",
        unit="count",
        is_active=True,
        display_name="Python",
    ),
    MetricDefinition(
        metric_id="downloads:lancedb:nodejs",
        metric_family="downloads",
        product="lancedb",
        subject="@lancedb/lancedb",
        sdk="nodejs",
        source="npm",
        value_kind="daily_downloads",
        unit="count",
        is_active=True,
        display_name="NodeJS",
    ),
    MetricDefinition(
        metric_id="downloads:lancedb:rust",
        metric_family="downloads",
        product="lancedb",
        subject="lancedb",
        sdk="rust",
        source="cratesio",
        value_kind="daily_downloads",
        unit="count",
        is_active=True,
        display_name="Rust",
    ),
    MetricDefinition(
        metric_id="stars:lance:github",
        metric_family="stars",
        product="lance",
        subject="lance-format/lance",
        sdk=None,
        source="github",
        value_kind="cumulative_stars",
        unit="count",
        is_active=True,
        display_name="GitHub Stars",
    ),
    MetricDefinition(
        metric_id="stars:lancedb:github",
        metric_family="stars",
        product="lancedb",
        subject="lancedb/lancedb",
        sdk=None,
        source="github",
        value_kind="cumulative_stars",
        unit="count",
        is_active=True,
        display_name="GitHub Stars",
    ),
]


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def metric_definition_rows() -> list[dict[str, Any]]:
    created_at = now_utc()
    rows: list[dict[str, Any]] = []
    for metric in METRIC_DEFINITIONS:
        rows.append(
            {
                "metric_id": metric.metric_id,
                "metric_family": metric.metric_family,
                "product": metric.product,
                "subject": metric.subject,
                "sdk": metric.sdk,
                "source": metric.source,
                "value_kind": metric.value_kind,
                "unit": metric.unit,
                "is_active": metric.is_active,
                "display_name": metric.display_name,
                "created_at": created_at,
            }
        )
    return rows
