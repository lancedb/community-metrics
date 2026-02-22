from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from community_metrics.api import main as api_main


class _FakeStore:
    def __init__(self) -> None:
        self._metrics = [
            {
                "metric_id": "downloads:lance:python",
                "metric_family": "downloads",
                "product": "lance",
                "subject": "pylance",
                "sdk": "python",
                "source": "pypistats",
                "value_kind": "daily_downloads",
                "unit": "count",
                "is_active": True,
                "display_name": "Python",
            }
        ]
        self._stats = [
            {
                "metric_id": "downloads:lance:python",
                "period_start": "2026-01-02",
                "period_end": "2026-01-02",
                "observed_at": datetime.now(tz=timezone.utc),
                "value": 80,
                "provenance": "api_daily",
                "source_window": "1d",
                "ingestion_run_id": "test-run",
                "source_ref": "unit-test",
            },
            {
                "metric_id": "downloads:lance:python",
                "period_start": "2026-01-03",
                "period_end": "2026-01-03",
                "observed_at": datetime.now(tz=timezone.utc),
                "value": 20,
                "provenance": "api_daily",
                "source_window": "1d",
                "ingestion_run_id": "test-run",
                "source_ref": "unit-test",
            },
            {
                "metric_id": "downloads:lance:python",
                "period_start": "2026-02-08",
                "period_end": "2026-02-08",
                "observed_at": datetime.now(tz=timezone.utc),
                "value": 100,
                "provenance": "api_daily",
                "source_window": "1d",
                "ingestion_run_id": "test-run",
                "source_ref": "unit-test",
            },
            {
                "metric_id": "downloads:lance:python",
                "period_start": "2026-02-10",
                "period_end": "2026-02-10",
                "observed_at": datetime.now(tz=timezone.utc),
                "value": 120,
                "provenance": "api_daily",
                "source_window": "1d",
                "ingestion_run_id": "test-run",
                "source_ref": "unit-test",
            },
        ]

    def get_metrics_df(self):
        return list(self._metrics)

    def get_stats_df(self):
        return list(self._stats)

    def list_refresh_errors(self, *, start_day, end_day, limit=500):
        assert start_day.isoformat() == "2026-02-01"
        assert end_day.isoformat() == "2026-02-05"
        assert limit == 50
        return [
            {
                "ingestion_run_id": "run-1",
                "job_name": "daily_refresh",
                "status": "partial",
                "started_at": "2026-02-01T09:00:00Z",
                "finished_at": "2026-02-01T09:01:00Z",
                "error_summary": "timeout",
            }
        ]


def test_dashboard_daily_shape(monkeypatch) -> None:
    monkeypatch.setattr(api_main, "_store", lambda: _FakeStore())
    monkeypatch.setattr(api_main, "latest_completed_day", lambda: date(2026, 2, 15))
    client = TestClient(api_main.app)

    response = client.get("/api/v1/dashboard/daily?days=90")
    assert response.status_code == 200

    payload = response.json()
    assert payload["days"] == 90
    assert "groups" in payload
    assert len(payload["groups"]) >= 1

    lance_group = next(
        group for group in payload["groups"] if group["product"] == "lance"
    )
    metric = next(
        item
        for item in lance_group["items"]
        if item["metric_id"] == "downloads:lance:python"
    )
    assert metric["latest_value"] == 100

    sparkline = metric["sparkline"]
    assert [point["period_end"] for point in sparkline] == ["2026-01-03", "2026-02-10"]
    assert [point["value"] for point in sparkline] == [100, 220]


def test_refresh_errors_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(api_main, "_store", lambda: _FakeStore())
    client = TestClient(api_main.app)

    response = client.get(
        "/api/v1/history/refresh-errors?start_date=2026-02-01&end_date=2026-02-05&limit=50"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["errors"][0]["error_summary"] == "timeout"


def test_monthly_download_sparkline_uses_seed_snapshots_then_daily_monthly_sums() -> None:
    rows = [
        {
            "period_start": "2025-10-30",
            "period_end": "2025-10-30",
            "value": 1_599_523,
            "source_window": "discrete_snapshot",
        },
        {
            "period_start": "2025-11-30",
            "period_end": "2025-11-30",
            "value": 2_100_000,
            "source_window": "discrete_snapshot",
        },
        {
            "period_start": "2025-12-01",
            "period_end": "2025-12-01",
            "value": 100,
            "source_window": "1d",
        },
        {
            "period_start": "2025-12-02",
            "period_end": "2025-12-02",
            "value": 200,
            "source_window": "1d",
        },
        {
            "period_start": "2026-01-02",
            "period_end": "2026-01-02",
            "value": 50,
            "source_window": "1d",
        },
    ]
    points = api_main._rows_to_monthly_download_sparkline(rows, days=365)

    assert points == [
        {
            "period_start": "2025-10-30",
            "period_end": "2025-10-30",
            "value": 1599523,
        },
        {
            "period_start": "2025-11-30",
            "period_end": "2025-11-30",
            "value": 2100000,
        },
        {
            "period_start": "2025-12-01",
            "period_end": "2025-12-02",
            "value": 300,
        },
        {
            "period_start": "2026-01-02",
            "period_end": "2026-01-02",
            "value": 50,
        },
    ]


def test_last_full_month_value_picks_latest_point_in_target_month() -> None:
    points = [
        {"period_end": "2026-01-02", "value": 10},
        {"period_end": "2026-01-31", "value": 20},
        {"period_end": "2026-02-10", "value": 30},
    ]
    value, period_end = api_main._last_full_month_value(points, date(2026, 2, 15))
    assert value == 20
    assert period_end == "2026-01-31"
