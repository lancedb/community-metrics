from datetime import date

from community_metrics.utils.time import (
    latest_completed_day,
    parse_iso_date,
    parse_seed_star_timestamp,
)


def test_parse_iso_date() -> None:
    result = parse_iso_date('"2026-02-02"')
    assert result.isoformat() == "2026-02-02"


def test_parse_seed_star_timestamp() -> None:
    ts = "Fri Feb 20 2026 14:34:58 GMT-0500 (Eastern Standard Time)"
    parsed = parse_seed_star_timestamp(ts)
    assert parsed.isoformat() == "2026-02-20T19:34:58+00:00"


def test_latest_completed_day() -> None:
    assert latest_completed_day(reference=date(2026, 2, 22)).isoformat() == "2026-02-21"
