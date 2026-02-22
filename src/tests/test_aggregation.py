from datetime import date, datetime, timezone

import pytest

from community_metrics.jobs.common import days_to_refresh, to_daily_stat_row


def test_days_to_refresh_incremental() -> None:
    result = days_to_refresh(
        latest_existing_period_end=date(2026, 2, 10),
        latest_completed_period_end=date(2026, 2, 12),
        lookback_days=0,
    )
    assert result == [date(2026, 2, 11), date(2026, 2, 12)]


def test_days_to_refresh_lookback_includes_recent_days() -> None:
    result = days_to_refresh(
        latest_existing_period_end=date(2026, 2, 15),
        latest_completed_period_end=date(2026, 2, 15),
        lookback_days=3,
    )
    assert result == [date(2026, 2, 13), date(2026, 2, 14), date(2026, 2, 15)]


def test_days_to_refresh_rejects_negative_lookback() -> None:
    with pytest.raises(ValueError, match="lookback_days"):
        days_to_refresh(
            latest_existing_period_end=None,
            latest_completed_period_end=date(2026, 2, 15),
            lookback_days=-1,
        )


def test_to_daily_stat_row_sets_day_period_bounds() -> None:
    row = to_daily_stat_row(
        metric_id="downloads:lance:python",
        day=date(2026, 2, 15),
        observed_at=datetime(2026, 2, 15, tzinfo=timezone.utc),
        value=12,
        provenance="api_daily",
        source_window="1d",
        ingestion_run_id="run-1",
        source_ref="pypi:pylance",
    )

    assert row["period_start"] == "2026-02-15"
    assert row["period_end"] == "2026-02-15"
    assert row["value"] == 12
