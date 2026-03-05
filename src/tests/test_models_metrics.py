from __future__ import annotations

from community_metrics.models import METRIC_DEFINITIONS, metric_definition_rows


def test_metric_definitions_include_new_star_metrics() -> None:
    metrics = {metric.metric_id: metric for metric in METRIC_DEFINITIONS}

    assert "stars:lance-graph:github" in metrics
    assert metrics["stars:lance-graph:github"].subject == "lance-format/lance-graph"
    assert metrics["stars:lance-graph:github"].product == "lance"

    assert "stars:lance-context:github" in metrics
    assert metrics["stars:lance-context:github"].subject == "lance-format/lance-context"
    assert metrics["stars:lance-context:github"].product == "lance"


def test_metric_definition_rows_include_new_star_metrics() -> None:
    rows_by_id = {str(row["metric_id"]): row for row in metric_definition_rows()}

    assert (
        rows_by_id["stars:lance-graph:github"]["subject"] == "lance-format/lance-graph"
    )
    assert (
        rows_by_id["stars:lance-context:github"]["subject"]
        == "lance-format/lance-context"
    )
