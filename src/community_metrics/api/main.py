from __future__ import annotations

import logging
import math
from bisect import bisect_left
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from community_metrics.config import DEFAULT_DAYS, MAX_DAYS
from community_metrics.storage.lancedb_store import LanceDBStore
from community_metrics.utils.time import latest_completed_day, parse_iso_date

app = FastAPI(title="Community Metrics API", version="0.1.0")
logger = logging.getLogger("community_metrics.api")

STAR_METRIC_IDS = ["stars:lance:github", "stars:lancedb:github"]
DOWNLOAD_SNAPSHOT_CUTOFF = date(2025, 11, 30)
DOWNLOAD_DAILY_START = date(2025, 12, 1)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_request_start(request, call_next):
    logger.info(
        "request sent method=%s path=%s query=%s",
        request.method,
        request.url.path,
        request.url.query,
    )
    return await call_next(request)


def _store() -> LanceDBStore:
    return LanceDBStore()


def _normalize_days(days: int) -> int:
    return max(1, min(days, MAX_DAYS))


def _nullable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _coerce_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _interpolate_value(
    target: date, known_dates: list[date], known_map: dict[date, int]
) -> int:
    if target in known_map:
        return known_map[target]
    idx = bisect_left(known_dates, target)
    if idx <= 0:
        return known_map[known_dates[0]]
    if idx >= len(known_dates):
        return known_map[known_dates[-1]]
    left = known_dates[idx - 1]
    right = known_dates[idx]
    left_val = known_map[left]
    right_val = known_map[right]
    span_days = (right - left).days
    if span_days <= 0:
        return int(left_val)
    ratio = (target - left).days / span_days
    return int(round(left_val + (right_val - left_val) * ratio))


def _daily_interpolated(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    values_by_period_end: dict[date, int] = {}
    for row in sorted(rows, key=lambda item: _coerce_date(item.get("period_end"))):
        period_end = _coerce_date(row.get("period_end"))
        values_by_period_end[period_end] = int(row.get("value", 0))

    known_dates = sorted(values_by_period_end.keys())
    start = known_dates[0]
    end = known_dates[-1]
    all_days: list[date] = []
    cursor = start
    while cursor <= end:
        all_days.append(cursor)
        cursor += timedelta(days=1)

    return [
        {
            "period_start": day,
            "period_end": day,
            "value": _interpolate_value(day, known_dates, values_by_period_end),
        }
        for day in all_days
    ]


def _rows_to_sparkline(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if not rows:
        return []
    daily_rows = _daily_interpolated(rows)
    tail = daily_rows[-days:]
    return [
        {
            "period_start": row["period_start"].isoformat(),
            "period_end": row["period_end"].isoformat(),
            "value": int(row["value"]),
        }
        for row in tail
    ]


def _rows_to_monthly_download_sparkline(
    rows: list[dict[str, Any]], days: int
) -> list[dict[str, Any]]:
    if not rows:
        return []

    latest_day = max(_coerce_date(row.get("period_end")) for row in rows)
    window_start = latest_day - timedelta(days=days - 1)

    # Seed rows are sparse snapshots representing prior-month totals.
    snapshot_rows = [
        row
        for row in rows
        if window_start <= _coerce_date(row.get("period_end")) <= DOWNLOAD_SNAPSHOT_CUTOFF
        and str(row.get("source_window", "")) == "discrete_snapshot"
    ]
    snapshot_by_day: dict[date, int] = {}
    for row in sorted(snapshot_rows, key=lambda item: _coerce_date(item.get("period_end"))):
        snapshot_day = _coerce_date(row.get("period_end"))
        snapshot_by_day[snapshot_day] = int(row.get("value", 0))

    snapshot_points = [
        {
            "period_start": day.isoformat(),
            "period_end": day.isoformat(),
            "value": value,
        }
        for day, value in sorted(snapshot_by_day.items())
    ]

    # Daily rows are aggregated into monthly sums from Dec 1 onward.
    daily_rows = [
        row
        for row in rows
        if DOWNLOAD_DAILY_START <= _coerce_date(row.get("period_end")) <= latest_day
        and _coerce_date(row.get("period_end")) >= window_start
        and str(row.get("source_window", "")) == "1d"
    ]
    monthly: dict[tuple[int, int], dict[str, Any]] = {}
    for row in daily_rows:
        day = _coerce_date(row.get("period_end"))
        key = (day.year, day.month)
        bucket = monthly.get(key)
        if bucket is None:
            monthly[key] = {
                "period_start": day,
                "period_end": day,
                "value": int(row.get("value", 0)),
            }
            continue
        bucket["period_start"] = min(bucket["period_start"], day)
        bucket["period_end"] = max(bucket["period_end"], day)
        bucket["value"] += int(row.get("value", 0))

    monthly_points = [
        {
            "period_start": bucket["period_start"].isoformat(),
            "period_end": bucket["period_end"].isoformat(),
            "value": int(bucket["value"]),
        }
        for key, bucket in sorted(monthly.items())
    ]

    return sorted(
        [*snapshot_points, *monthly_points],
        key=lambda point: point["period_end"],
    )


def _last_full_month_key(reference_day: date) -> tuple[int, int]:
    if reference_day.month == 1:
        return (reference_day.year - 1, 12)
    return (reference_day.year, reference_day.month - 1)


def _last_full_month_value(
    points: list[dict[str, Any]], reference_day: date
) -> tuple[int | None, str | None]:
    target_key = _last_full_month_key(reference_day)
    candidate: tuple[int, str] | None = None
    for point in points:
        period_end = _coerce_date(point.get("period_end"))
        if (period_end.year, period_end.month) == target_key:
            value = int(point.get("value", 0))
            period_end_str = period_end.isoformat()
            if candidate is None or period_end_str > candidate[1]:
                candidate = (value, period_end_str)
    if candidate is None:
        return None, None
    return candidate


def _total_stars_aggregate(
    stats_rows: list[dict[str, Any]], days: int
) -> tuple[int | None, list[dict[str, Any]]]:
    if not stats_rows:
        return None, []

    per_metric_known: dict[str, dict[date, int]] = {}
    per_metric_dates: dict[str, list[date]] = {}
    all_dates: set[date] = set()

    for metric_id in STAR_METRIC_IDS:
        metric_rows = [row for row in stats_rows if row.get("metric_id") == metric_id]
        daily_rows = _daily_interpolated(metric_rows)
        if not daily_rows:
            continue
        known_map = {
            _coerce_date(row.get("period_end")): int(row.get("value", 0))
            for row in daily_rows
        }
        known_dates = sorted(known_map.keys())
        per_metric_known[metric_id] = known_map
        per_metric_dates[metric_id] = known_dates
        all_dates.update(known_dates)

    if not all_dates:
        return None, []

    totals: list[dict[str, Any]] = []
    for day in sorted(all_dates):
        total = 0
        for metric_id in per_metric_known:
            total += _interpolate_value(
                day, per_metric_dates[metric_id], per_metric_known[metric_id]
            )
        totals.append({"period_start": day, "period_end": day, "value": int(total)})

    tail = totals[-days:]
    sparkline = [
        {
            "period_start": row["period_start"].isoformat(),
            "period_end": row["period_end"].isoformat(),
            "value": int(row["value"]),
        }
        for row in tail
    ]
    latest_total = None if not tail else int(tail[-1]["value"])
    return latest_total, sparkline


def _overlap_days(a_start: date, a_end: date, b_start: date, b_end: date) -> int:
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    if end < start:
        return 0
    return (end - start).days + 1


def _last_30d_download_totals(stats_rows: list[dict[str, Any]]) -> dict[str, Any]:
    window_end = latest_completed_day()
    window_start = window_end - timedelta(days=29)

    if not stats_rows:
        return {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "lance": 0,
            "lancedb": 0,
        }

    totals = {"lance": 0.0, "lancedb": 0.0}

    for row in stats_rows:
        metric_id = str(row.get("metric_id", ""))
        if not metric_id.startswith("downloads:"):
            continue
        parts = metric_id.split(":")
        if len(parts) < 3:
            continue
        product = parts[1]
        if product not in totals:
            continue

        period_start = _coerce_date(row.get("period_start"))
        period_end = _coerce_date(row.get("period_end"))
        overlap = _overlap_days(period_start, period_end, window_start, window_end)
        if overlap <= 0:
            continue

        span_days = (period_end - period_start).days + 1
        if span_days <= 0:
            continue
        value = float(row.get("value", 0))
        totals[product] += value * (overlap / float(span_days))

    return {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "lance": int(round(totals["lance"])),
        "lancedb": int(round(totals["lancedb"])),
    }


@app.get("/api/v1/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/history/refresh-errors")
def refresh_errors(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD, UTC inclusive)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD, UTC inclusive)"),
    limit: int = Query(500, ge=1, le=5000),
) -> dict[str, Any]:
    try:
        start_day = parse_iso_date(start_date)
        end_day = parse_iso_date(end_date)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="start_date/end_date must be YYYY-MM-DD"
        ) from exc
    if end_day < start_day:
        raise HTTPException(
            status_code=400, detail="end_date must be on or after start_date"
        )
    store = _store()
    rows = store.list_refresh_errors(start_day=start_day, end_day=end_day, limit=limit)
    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "count": len(rows),
        "errors": rows,
    }


@app.get("/api/v1/definitions")
def definitions() -> list[dict[str, Any]]:
    store = _store()
    metrics = store.get_metrics_df()
    if not metrics:
        return []
    metrics = sorted(
        metrics,
        key=lambda row: (
            str(row.get("product", "")),
            str(row.get("metric_family", "")),
            str(row.get("display_name", "")),
            str(row.get("metric_id", "")),
        ),
    )
    rows: list[dict[str, Any]] = []
    for row in metrics:
        rows.append(
            {
                "metric_id": row["metric_id"],
                "metric_family": row["metric_family"],
                "product": row["product"],
                "subject": row["subject"],
                "sdk": _nullable(row["sdk"]),
                "source": row["source"],
                "value_kind": row["value_kind"],
                "unit": row["unit"],
                "is_active": bool(row["is_active"]),
                "display_name": row["display_name"],
            }
        )
    return rows


@app.get("/api/v1/series/{metric_id}")
def series(
    metric_id: str, days: int = Query(DEFAULT_DAYS, ge=1, le=MAX_DAYS)
) -> dict[str, Any]:
    store = _store()
    metric_rows = store.get_metrics_df()
    known_metric_ids = {str(row.get("metric_id")) for row in metric_rows}
    if not metric_rows or metric_id not in known_metric_ids:
        raise HTTPException(status_code=404, detail=f"Unknown metric_id: {metric_id}")

    stats_rows = store.get_stats_for_metric(metric_id)
    points = _rows_to_sparkline(stats_rows, _normalize_days(days))
    return {
        "metric_id": metric_id,
        "days": _normalize_days(days),
        "points": points,
    }


@app.get("/api/v1/dashboard/daily")
def dashboard_daily(
    days: int = Query(DEFAULT_DAYS, ge=1, le=MAX_DAYS),
) -> dict[str, Any]:
    days = _normalize_days(days)
    store = _store()
    latest_day = latest_completed_day()
    metrics_rows = store.get_metrics_df()
    stats_rows = store.get_stats_df()

    if not metrics_rows:
        return {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "days": days,
            "groups": [],
            "total_stars": None,
            "total_stars_sparkline": [],
            "last_30d_download_totals": _last_30d_download_totals(stats_rows),
        }

    stats_by_metric: dict[str, list[dict[str, Any]]] = {}
    for row in stats_rows:
        metric_id = str(row.get("metric_id", ""))
        stats_by_metric.setdefault(metric_id, []).append(row)

    groups: list[dict[str, Any]] = []
    for product in ["lance", "lancedb"]:
        subset_metrics = [
            row for row in metrics_rows if str(row.get("product", "")) == product
        ]
        if not subset_metrics:
            continue
        subset_metrics = sorted(
            subset_metrics,
            key=lambda row: (
                str(row.get("metric_family", "")),
                str(row.get("display_name", "")),
                str(row.get("metric_id", "")),
            ),
        )

        items: list[dict[str, Any]] = []
        for metric in subset_metrics:
            metric_id = metric["metric_id"]
            metric_stats = stats_by_metric.get(metric_id, [])
            if metric["metric_family"] == "downloads":
                sparkline = _rows_to_monthly_download_sparkline(metric_stats, days)
                latest_value, latest_period_end = _last_full_month_value(
                    sparkline, latest_day
                )
            else:
                sparkline = _rows_to_sparkline(metric_stats, days)
                latest_value = None
                latest_period_end = None
                if sparkline:
                    latest_value = sparkline[-1]["value"]
                    latest_period_end = sparkline[-1]["period_end"]

            latest_provenance = None
            if metric_stats:
                latest_row = max(
                    metric_stats, key=lambda row: _coerce_date(row.get("period_end"))
                )
                latest_provenance = latest_row.get("provenance")

            items.append(
                {
                    "metric_id": metric_id,
                    "display_name": metric["display_name"],
                    "metric_family": metric["metric_family"],
                    "sdk": _nullable(metric["sdk"]),
                    "subject": metric["subject"],
                    "latest_value": latest_value,
                    "latest_period_end": latest_period_end,
                    "latest_provenance": _nullable(latest_provenance),
                    "total_stars": None,
                    "sparkline": sparkline,
                }
            )

        groups.append(
            {
                "product": product,
                "title": "Lance" if product == "lance" else "LanceDB",
                "items": items,
            }
        )

    total_stars, total_stars_sparkline = _total_stars_aggregate(stats_rows, days)
    last_30d_download_totals = _last_30d_download_totals(stats_rows)
    for group in groups:
        for item in group["items"]:
            if item["metric_family"] == "stars":
                item["total_stars"] = total_stars

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "days": days,
        "groups": groups,
        "total_stars": total_stars,
        "total_stars_sparkline": total_stars_sparkline,
        "last_30d_download_totals": last_30d_download_totals,
    }
