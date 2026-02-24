from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from community_metrics.jobs import (
    update_all,
    update_daily_downloads,
    update_daily_stars,
)


@dataclass(frozen=True)
class _DailyRow:
    day: str
    downloads: int


@dataclass(frozen=True)
class _StarEvent:
    starred_at: datetime


class _FakeStore:
    def __init__(
        self, existing_by_metric: dict[str, list[dict[str, object]]] | None = None
    ):
        self._existing_by_metric = existing_by_metric or {}
        self.upserted_stats_rows: list[dict[str, object]] = []
        self.history_rows: list[dict[str, object]] = []

    def get_stats_for_metric(self, metric_id: str):
        return list(self._existing_by_metric.get(metric_id, []))

    def upsert_stats(self, rows):
        self.upserted_stats_rows = list(rows)
        return {"inserted": len(rows), "updated": 0}

    def upsert_history(self, row):
        self.history_rows.append(dict(row))
        return {"inserted": 1, "updated": 0}


def test_update_daily_downloads_writes_one_row_per_day(monkeypatch) -> None:
    fake_store = _FakeStore()
    monkeypatch.setattr(update_daily_downloads, "build_store", lambda: fake_store)
    monkeypatch.setattr(
        update_daily_downloads,
        "DOWNLOAD_METRIC_SOURCE_MAP",
        {
            "downloads:lance:python": ("pypi", "pylance"),
            "downloads:lancedb:nodejs": ("npm", "@lancedb/lancedb"),
            "downloads:lance:rust": ("crates", "lance"),
        },
    )
    monkeypatch.setattr(
        update_daily_downloads,
        "latest_completed_day",
        lambda: date(2026, 2, 12),
    )

    class _PyPI:
        def fetch_daily_downloads(self, package):
            assert package == "pylance"
            return [_DailyRow("2026-02-11", 10), _DailyRow("2026-02-12", 11)]

    class _Npm:
        def fetch_daily_downloads(self, package, start, end):
            assert package == "@lancedb/lancedb"
            assert start.isoformat() == "2026-02-11"
            assert end.isoformat() == "2026-02-12"
            return [_DailyRow("2026-02-11", 20), _DailyRow("2026-02-12", 21)]

    class _Crates:
        def fetch_daily_downloads(self, crate_name):
            assert crate_name == "lance"
            # crates may miss some days; only available days should be written.
            return [_DailyRow("2026-02-12", 30)]

    monkeypatch.setattr(update_daily_downloads, "PyPIStatsClient", _PyPI)
    monkeypatch.setattr(update_daily_downloads, "NpmDownloadsClient", _Npm)
    monkeypatch.setattr(update_daily_downloads, "CratesClient", _Crates)

    result = update_daily_downloads.run(lookback_days=2)

    assert result == {"inserted": 5, "updated": 0, "errors": 0}
    assert len(fake_store.upserted_stats_rows) == 5
    for row in fake_store.upserted_stats_rows:
        assert row["period_start"] == row["period_end"]
        assert row["provenance"] == "api_daily"
        assert row["source_window"] == "1d"

    assert fake_store.history_rows[-1]["status"] == "success"


def test_update_daily_stars_uses_snapshot_for_routine_run(monkeypatch) -> None:
    fake_store = _FakeStore(
        {
            "stars:lance:github": [
                {
                    "metric_id": "stars:lance:github",
                    "period_end": "2026-02-11",
                }
            ]
        }
    )
    monkeypatch.setattr(update_daily_stars, "build_store", lambda: fake_store)
    monkeypatch.setattr(
        update_daily_stars,
        "STARS_METRIC_SOURCE_MAP",
        {"stars:lance:github": "lance-format/lance"},
    )
    monkeypatch.setattr(
        update_daily_stars, "latest_completed_day", lambda: date(2026, 2, 12)
    )

    class _GitHub:
        def get_repo_stars(self, repo):
            assert repo == "lance-format/lance"
            return 123

        def iter_stargazer_events(self, repo):
            raise AssertionError(
                "stargazer events should not be used for routine snapshot"
            )

    monkeypatch.setattr(update_daily_stars, "GitHubClient", _GitHub)

    result = update_daily_stars.run(lookback_days=0)

    assert result == {"inserted": 1, "updated": 0, "errors": 0}
    row = fake_store.upserted_stats_rows[0]
    assert row["period_end"] == "2026-02-12"
    assert row["value"] == 123
    assert row["source_ref"] == "github:lance-format/lance"
    assert fake_store.history_rows[-1]["status"] == "success"


def test_update_daily_stars_uses_stargazer_events_for_lookback(monkeypatch) -> None:
    fake_store = _FakeStore(
        {
            "stars:lance:github": [
                {
                    "metric_id": "stars:lance:github",
                    "period_end": "2026-02-12",
                }
            ]
        }
    )
    monkeypatch.setattr(update_daily_stars, "build_store", lambda: fake_store)
    monkeypatch.setattr(
        update_daily_stars,
        "STARS_METRIC_SOURCE_MAP",
        {"stars:lance:github": "lance-format/lance"},
    )
    monkeypatch.setattr(
        update_daily_stars, "latest_completed_day", lambda: date(2026, 2, 12)
    )

    class _GitHub:
        def get_repo_stars(self, repo):
            return 999

        def iter_stargazer_events(self, repo):
            assert repo == "lance-format/lance"
            return iter(
                [
                    _StarEvent(datetime(2026, 2, 10, 2, tzinfo=timezone.utc)),
                    _StarEvent(datetime(2026, 2, 12, 16, tzinfo=timezone.utc)),
                ]
            )

    monkeypatch.setattr(update_daily_stars, "GitHubClient", _GitHub)

    result = update_daily_stars.run(lookback_days=3)

    assert result == {"inserted": 3, "updated": 0, "errors": 0}
    values = [row["value"] for row in fake_store.upserted_stats_rows]
    days = [row["period_end"] for row in fake_store.upserted_stats_rows]
    assert days == ["2026-02-10", "2026-02-11", "2026-02-12"]
    assert values == [1, 1, 2]
    assert all(
        row["source_ref"] == "github-stargazers:lance-format/lance"
        for row in fake_store.upserted_stats_rows
    )
    assert fake_store.history_rows[-1]["status"] == "success"


def test_update_daily_stars_fallback_marks_partial(monkeypatch) -> None:
    fake_store = _FakeStore(
        {
            "stars:lance:github": [
                {
                    "metric_id": "stars:lance:github",
                    "period_end": "2026-02-12",
                }
            ]
        }
    )
    monkeypatch.setattr(update_daily_stars, "build_store", lambda: fake_store)
    monkeypatch.setattr(
        update_daily_stars,
        "STARS_METRIC_SOURCE_MAP",
        {"stars:lance:github": "lance-format/lance"},
    )
    monkeypatch.setattr(
        update_daily_stars, "latest_completed_day", lambda: date(2026, 2, 12)
    )

    class _GitHub:
        def get_repo_stars(self, repo):
            return 77

        def iter_stargazer_events(self, repo):
            raise RuntimeError("rate limit")

    monkeypatch.setattr(update_daily_stars, "GitHubClient", _GitHub)

    result = update_daily_stars.run(lookback_days=2)

    assert result == {"inserted": 2, "updated": 0, "errors": 1}
    assert [row["value"] for row in fake_store.upserted_stats_rows] == [77, 77]
    assert fake_store.history_rows[-1]["status"] == "partial"
    assert "fell back to snapshot" in str(fake_store.history_rows[-1]["error_summary"])


def test_update_all_reset_tables_logs_and_writes(monkeypatch, capsys) -> None:
    class _BootstrapStore:
        def __init__(self) -> None:
            self.calls: list[object] = []

        def reset_tables(self) -> None:
            self.calls.append("reset_tables")

        def create_required_tables(
            self, on_table=None, *, recreate: bool = False
        ) -> None:
            self.calls.append("create_required_tables")
            self.calls.append(("create_required_tables.recreate", recreate))
            if on_table is not None:
                on_table("metrics")
                on_table("stats")
                on_table("history")

        def seed_metrics(self) -> dict[str, int]:
            self.calls.append("seed_metrics")
            return {"inserted": 7, "updated": 0}

        def append_stats(self, rows) -> dict[str, int]:
            self.calls.append(("append_stats", len(rows)))
            return {"inserted": len(rows), "updated": 0}

        def upsert_stats(self, rows) -> dict[str, int]:
            self.calls.append(("upsert_stats", len(rows)))
            return {"inserted": 0, "updated": 0}

        def upsert_history(self, _row) -> dict[str, int]:
            self.calls.append("upsert_history")
            return {"inserted": 1, "updated": 0}

    fake_store = _BootstrapStore()
    monkeypatch.setattr(update_all, "LanceDBStore", lambda: fake_store)
    monkeypatch.setattr(
        update_all,
        "latest_completed_day",
        lambda: date(2026, 2, 12),
    )
    monkeypatch.setattr(
        update_all,
        "_seed_rows_older_than",
        lambda _cutoff, run_id: [
            {
                "metric_id": "downloads:lance:python",
                "period_start": "2026-01-01",
                "period_end": "2026-01-01",
                "observed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "value": 1,
                "provenance": "csv_seed",
                "source_window": "discrete_snapshot",
                "ingestion_run_id": run_id,
                "source_ref": "seed_data/download_stats.csv",
            }
        ],
    )
    monkeypatch.setattr(
        update_all,
        "_api_rows_for_window",
        lambda **_kwargs: (
            [
                {
                    "metric_id": "downloads:lance:python",
                    "period_start": "2026-02-12",
                    "period_end": "2026-02-12",
                    "observed_at": datetime(2026, 2, 12, tzinfo=timezone.utc),
                    "value": 12,
                    "provenance": "recomputed",
                    "source_window": "1d",
                    "ingestion_run_id": "run-1",
                    "source_ref": "pypi:pylance",
                }
            ],
            [],
        ),
    )

    result = update_all.run(reset_tables=True, lookback_days=90)

    assert result["errors"] == 0
    assert result["inserted"] == 2
    assert fake_store.calls[0] == "reset_tables"
    assert fake_store.calls[1] == "create_required_tables"
    assert ("create_required_tables.recreate", True) in fake_store.calls
    assert "seed_metrics" in fake_store.calls
    assert ("append_stats", 2) in fake_store.calls
    assert "upsert_history" in fake_store.calls

    output = capsys.readouterr().out
    assert "[bootstrap] ensuring table: metrics" in output
    assert "[bootstrap] ensuring table: stats" in output
    assert "[bootstrap] ensuring table: history" in output


def test_update_all_defaults_to_assume_tables_exist(monkeypatch, capsys) -> None:
    class _Store:
        def __init__(self) -> None:
            self.calls: list[object] = []

        def reset_tables(self) -> None:
            self.calls.append("reset_tables")

        def create_required_tables(
            self, on_table=None, *, recreate: bool = False
        ) -> None:
            self.calls.append("create_required_tables")

        def seed_metrics(self) -> dict[str, int]:
            self.calls.append("seed_metrics")
            return {"inserted": 0, "updated": 0}

        def append_stats(self, rows) -> dict[str, int]:
            self.calls.append(("append_stats", len(rows)))
            return {"inserted": len(rows), "updated": 0}

        def upsert_stats(self, rows) -> dict[str, int]:
            self.calls.append(("upsert_stats", len(rows)))
            return {"inserted": len(rows), "updated": 0}

        def upsert_history(self, _row) -> dict[str, int]:
            self.calls.append("upsert_history")
            return {"inserted": 1, "updated": 0}

    fake_store = _Store()
    monkeypatch.setattr(update_all, "LanceDBStore", lambda: fake_store)
    monkeypatch.setattr(
        update_all,
        "latest_completed_day",
        lambda: date(2026, 2, 12),
    )
    monkeypatch.setattr(
        update_all,
        "_seed_rows_older_than",
        lambda _cutoff, _run_id: [],
    )
    monkeypatch.setattr(
        update_all,
        "_api_rows_for_window",
        lambda **_kwargs: ([], []),
    )

    result = update_all.run(lookback_days=7)

    assert result["errors"] == 0
    assert ("upsert_stats", 0) in fake_store.calls
    assert "upsert_history" in fake_store.calls
    assert "reset_tables" not in fake_store.calls
    assert "create_required_tables" not in fake_store.calls
    assert "seed_metrics" not in fake_store.calls
    assert "assuming tables exist" in capsys.readouterr().out
