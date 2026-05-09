from __future__ import annotations

from datetime import date, datetime, timezone

from community_metrics.jobs.derive_dashboard import (
    build_metric_rollups,
    build_signal_candidates,
)
from community_metrics.jobs import derive_dashboard


def test_build_metric_rollups_computes_windows_and_sdk_share() -> None:
    metrics = [
        {
            "metric_id": "downloads:lancedb:nodejs",
            "metric_family": "downloads",
            "product": "lancedb",
            "subject": "@lancedb/lancedb",
            "sdk": "nodejs",
        },
        {
            "metric_id": "downloads:lancedb:python",
            "metric_family": "downloads",
            "product": "lancedb",
            "subject": "lancedb",
            "sdk": "python",
        },
    ]
    stats = []
    for day in range(1, 31):
        stats.append(
            {
                "metric_id": "downloads:lancedb:nodejs",
                "period_start": f"2026-04-{day:02d}",
                "period_end": f"2026-04-{day:02d}",
                "value": 100,
            }
        )
        stats.append(
            {
                "metric_id": "downloads:lancedb:python",
                "period_start": f"2026-04-{day:02d}",
                "period_end": f"2026-04-{day:02d}",
                "value": 50,
            }
        )
    for day in range(2, 32):
        stats.append(
            {
                "metric_id": "downloads:lancedb:nodejs",
                "period_start": f"2026-03-{day:02d}",
                "period_end": f"2026-03-{day:02d}",
                "value": 25,
            }
        )

    rollups = build_metric_rollups(metrics, stats, latest_day=date(2026, 4, 30))
    node_30d = next(
        row
        for row in rollups
        if row["metric_id"] == "downloads:lancedb:nodejs" and row["window"] == "30d"
    )

    assert node_30d["current_value"] == 3000
    assert node_30d["previous_value"] == 750
    assert node_30d["delta"] == 2250
    assert node_30d["percent_change"] == 300
    assert round(node_30d["sdk_share"], 2) == 0.67
    assert any(row["window"] == "15d" for row in rollups)


def test_build_signal_candidates_detects_spike_and_social_burst() -> None:
    rollups = [
        {
            "metric_id": "downloads:lancedb:nodejs",
            "metric_family": "downloads",
            "product": "lancedb",
            "sdk": "nodejs",
            "window": "30d",
            "window_start": "2026-04-01",
            "window_end": "2026-04-30",
            "current_value": 3000,
            "previous_value": 750,
            "percent_change": 300.0,
            "trend_slope": 50.0,
            "sdk_share_delta": 0.2,
        }
    ]
    evidence = [
        {
            "evidence_id": f"e{i}",
            "source_type": "hackernews",
            "occurred_at": datetime(2026, 4, 28, tzinfo=timezone.utc),
            "related_metrics": ["downloads:lancedb:nodejs"],
        }
        for i in range(3)
    ]

    signals = build_signal_candidates(
        rollups,
        evidence,
        latest_day=date(2026, 4, 30),
    )
    signal_types = {signal["signal_type"] for signal in signals}

    assert "download_spike" in signal_types
    assert "sdk_share_shift" in signal_types
    assert "social_mention_burst" in signal_types


def test_run_uses_derived_tables_without_seeding_source_metrics(monkeypatch) -> None:
    class _Store:
        def __init__(self) -> None:
            self.seed_called = False
            self.rollups = []
            self.signals = []

        def ensure_derived_tables(self) -> None:
            pass

        def seed_metrics(self):
            self.seed_called = True

        def query_table(self, table_name, **_kwargs):
            if table_name == "metrics":
                return []
            if table_name in {"stats", "evidence_items"}:
                return []
            raise AssertionError(f"unexpected table: {table_name}")

        def replace_dashboard_rollups(self, rows):
            self.rollups = list(rows)
            return {"inserted": len(rows), "updated": 0}

        def replace_signal_candidates(self, rows):
            self.signals = list(rows)
            return {"inserted": len(rows), "updated": 0}

    store = _Store()
    monkeypatch.setattr(derive_dashboard, "LanceDBStore", lambda: store)
    monkeypatch.setattr(
        derive_dashboard,
        "latest_completed_day",
        lambda: date(2026, 4, 30),
    )

    result = derive_dashboard.run()

    assert result == {"rollups": 0, "signals": 0}
    assert store.seed_called is False
