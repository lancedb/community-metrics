from __future__ import annotations

from datetime import date, datetime, timezone

from community_metrics.jobs.update_duckdb_extension_downloads import (
    build_monthly_rows,
)
from community_metrics.sources import duckdb_extensions_client as client_module
from community_metrics.sources.duckdb_extensions_client import (
    DuckDBExtensionsClient,
    WeeklyExtensionDownloads,
    weekly_download_url,
    weekly_snapshot_dates,
)


def test_weekly_snapshot_dates_start_at_january_2026_and_skip_week_53() -> None:
    days = weekly_snapshot_dates(
        start_day=date(2026, 1, 1),
        end_day=date(2026, 1, 22),
    )

    assert days == [
        date(2026, 1, 1),
        date(2026, 1, 8),
        date(2026, 1, 15),
        date(2026, 1, 22),
    ]
    assert (
        weekly_snapshot_dates(
            start_day=date(2026, 12, 31),
            end_day=date(2026, 12, 31),
        )
        == []
    )


def test_weekly_download_url_uses_expected_repo_and_week() -> None:
    assert weekly_download_url("core", date(2026, 1, 8)) == (
        "https://extensions.duckdb.org/download-stats-weekly/2026/2.json"
    )
    assert weekly_download_url("community", date(2026, 1, 8)) == (
        "https://community-extensions.duckdb.org/download-stats-weekly/2026/2.json"
    )


def test_client_reads_lance_and_treats_missing_lance_as_zero(monkeypatch) -> None:
    class _Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def raise_for_status(self) -> None:
            pass

        def json(self):
            return self._payload

    payloads = [
        {"_last_update": "2026-01-03T00:00:00Z", "lance": 11},
        {"_last_update": "2026-01-03T00:00:00Z"},
    ]

    def fake_get(_url, timeout):
        assert timeout == 7
        return _Response(payloads.pop(0))

    monkeypatch.setattr(client_module.requests, "get", fake_get)

    rows = DuckDBExtensionsClient(timeout_seconds=7).fetch_lance_weekly_downloads(
        start_day=date(2026, 1, 1),
        end_day=date(2026, 1, 1),
    )

    assert [row.repo for row in rows] == ["core", "community"]
    assert [row.downloads for row in rows] == [11, 0]


def test_build_monthly_rows_aggregates_split_and_marks_current_month() -> None:
    rows = [
        WeeklyExtensionDownloads(
            repo="core",
            week_date=date(2026, 1, 1),
            source_url="https://example.com/core/1.json",
            source_update_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            downloads=10,
        ),
        WeeklyExtensionDownloads(
            repo="community",
            week_date=date(2026, 1, 1),
            source_url="https://example.com/community/1.json",
            source_update_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
            downloads=20,
        ),
        WeeklyExtensionDownloads(
            repo="community",
            week_date=date(2026, 2, 5),
            source_url="https://example.com/community/6.json",
            source_update_at=datetime(2026, 2, 6, tzinfo=timezone.utc),
            downloads=7,
        ),
    ]

    monthly = build_monthly_rows(rows, today=date(2026, 2, 12))

    assert monthly == [
        {
            "month_start": date(2026, 1, 1),
            "month_label": "2026-01",
            "core_downloads": 10,
            "community_downloads": 20,
            "total_downloads": 30,
            "is_partial_month": False,
            "latest_source_update_at": datetime(2026, 1, 3, tzinfo=timezone.utc),
        },
        {
            "month_start": date(2026, 2, 1),
            "month_label": "2026-02",
            "core_downloads": 0,
            "community_downloads": 7,
            "total_downloads": 7,
            "is_partial_month": True,
            "latest_source_update_at": datetime(2026, 2, 6, tzinfo=timezone.utc),
        },
    ]
