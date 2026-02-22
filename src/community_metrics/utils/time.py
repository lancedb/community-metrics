from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def latest_completed_day(reference: date | None = None) -> date:
    today = reference or utc_now().date()
    return today - timedelta(days=1)


def parse_seed_star_timestamp(raw: str) -> datetime:
    # Example input:
    # Fri Feb 20 2026 14:34:58 GMT-0500 (Eastern Standard Time)
    cleaned = re.sub(r"\s+\(.*\)$", "", raw).strip()
    return datetime.strptime(cleaned, "%a %b %d %Y %H:%M:%S GMT%z").astimezone(
        timezone.utc
    )


def parse_iso_date(raw: str) -> date:
    return datetime.strptime(raw.strip().strip('"'), "%Y-%m-%d").date()
