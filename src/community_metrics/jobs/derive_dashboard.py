from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from community_metrics.storage.lancedb_store import LanceDBStore
from community_metrics.utils.time import latest_completed_day, parse_iso_date

ROLLUP_WINDOWS = (7, 15, 30, 90)
MONTHLY_TREND_MONTHS = 6
DOWNLOAD_SPIKE_PERCENT_THRESHOLD = 100.0
DOWNLOAD_SPIKE_MIN_VALUE = 1000
SUSTAINED_GROWTH_PERCENT_THRESHOLD = 20.0
SDK_SHARE_SHIFT_THRESHOLD = 0.15
SOCIAL_BURST_DAYS = 14
SOCIAL_BURST_MIN_ITEMS = 3


def run(*, days: int = 210) -> dict[str, int]:
    store = LanceDBStore()
    store.ensure_derived_tables()
    latest_day = latest_completed_day()
    earliest_day = latest_day - timedelta(days=max(days, 120) - 1)

    metrics = _active_metrics(store)
    stats = _stats_for_metrics(
        store,
        [str(row["metric_id"]) for row in metrics],
        earliest_day,
        latest_day,
    )
    evidence = store.query_table("evidence_items", limit=None)

    rollups = build_metric_rollups(metrics, stats, latest_day=latest_day)
    signals = build_signal_candidates(rollups, evidence, latest_day=latest_day)

    rollup_result = store.replace_dashboard_rollups(rollups)
    signal_result = store.replace_signal_candidates(signals)

    return {
        "rollups": rollup_result["inserted"],
        "signals": signal_result["inserted"],
    }


def _active_metrics(store: LanceDBStore) -> list[dict[str, Any]]:
    return store.query_table(
        "metrics",
        columns=[
            "metric_id",
            "metric_family",
            "product",
            "subject",
            "sdk",
            "is_active",
        ],
        where="is_active = true",
        limit=200,
    )


def _stats_for_metrics(
    store: LanceDBStore,
    metric_ids: list[str],
    earliest_day: date,
    latest_day: date,
) -> list[dict[str, Any]]:
    if not metric_ids:
        return []
    ids_clause = ", ".join(_sql_quote(metric_id) for metric_id in metric_ids)
    where = (
        f"metric_id IN ({ids_clause}) "
        f"AND period_end >= {_sql_quote(earliest_day.isoformat())} "
        f"AND period_end <= {_sql_quote(latest_day.isoformat())}"
    )
    return store.query_table(
        "stats",
        columns=["metric_id", "period_start", "period_end", "value", "source_window"],
        where=where,
        limit=max(5000, len(metric_ids) * 260),
    )


def build_metric_rollups(
    metrics: list[dict[str, Any]],
    stats: list[dict[str, Any]],
    *,
    latest_day: date,
) -> list[dict[str, Any]]:
    now = datetime.now(tz=timezone.utc)
    stats_by_metric: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stats:
        stats_by_metric[str(row.get("metric_id") or "")].append(row)

    rows: list[dict[str, Any]] = []
    product_totals: dict[tuple[str, str], int] = defaultdict(int)
    previous_product_totals: dict[tuple[str, str], int] = defaultdict(int)

    for metric in metrics:
        metric_id = str(metric["metric_id"])
        metric_stats = stats_by_metric.get(metric_id, [])
        for days in ROLLUP_WINDOWS:
            window = f"{days}d"
            window_start = latest_day - timedelta(days=days - 1)
            previous_end = window_start - timedelta(days=1)
            previous_start = previous_end - timedelta(days=days - 1)
            current_value = _window_value(
                metric, metric_stats, window_start, latest_day
            )
            previous_value = _window_value(
                metric, metric_stats, previous_start, previous_end
            )
            rows.append(
                _rollup_row(
                    metric,
                    window=window,
                    window_start=window_start,
                    window_end=latest_day,
                    current_value=current_value,
                    previous_value=previous_value,
                    trend_slope=_monthly_trend_slope(metric, metric_stats, latest_day),
                    updated_at=now,
                )
            )
            if str(metric.get("metric_family")) == "downloads":
                key = (str(metric.get("product")), window)
                product_totals[key] += current_value
                previous_product_totals[key] += previous_value

        month_start, month_end = _last_full_month(latest_day)
        previous_month_start, previous_month_end = _previous_month(month_start)
        current_value = _window_value(metric, metric_stats, month_start, month_end)
        previous_value = _window_value(
            metric,
            metric_stats,
            previous_month_start,
            previous_month_end,
        )
        rows.append(
            _rollup_row(
                metric,
                window="last_full_month",
                window_start=month_start,
                window_end=month_end,
                current_value=current_value,
                previous_value=previous_value,
                trend_slope=_monthly_trend_slope(metric, metric_stats, latest_day),
                updated_at=now,
            )
        )
        if str(metric.get("metric_family")) == "downloads":
            key = (str(metric.get("product")), "last_full_month")
            product_totals[key] += current_value
            previous_product_totals[key] += previous_value

    for row in rows:
        if row["metric_family"] != "downloads":
            continue
        key = (row["product"], row["window"])
        current_total = product_totals.get(key, 0)
        previous_total = previous_product_totals.get(key, 0)
        row["sdk_share"] = (
            row["current_value"] / current_total if current_total else 0.0
        )
        row["previous_sdk_share"] = (
            row["previous_value"] / previous_total if previous_total else 0.0
        )
        row["sdk_share_delta"] = row["sdk_share"] - row["previous_sdk_share"]

    return rows


def build_signal_candidates(
    rollups: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    *,
    latest_day: date,
) -> list[dict[str, Any]]:
    detected_at = datetime.now(tz=timezone.utc)
    signals: list[dict[str, Any]] = []

    for row in rollups:
        if row["metric_family"] != "downloads":
            continue
        if row["window"] == "30d" and _is_download_spike(row):
            signals.append(
                _signal(
                    signal_type="download_spike",
                    detected_at=detected_at,
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    title=f"{_metric_label(row)} downloads spiked",
                    summary=(
                        f"{_metric_label(row)} downloads are up "
                        f"{row['percent_change']:.0f}% versus the prior 30 days."
                    ),
                    related_metrics=[row["metric_id"]],
                    evidence_ids=[],
                    score=min(100.0, max(0.0, row["percent_change"])),
                    confidence="high" if row["percent_change"] >= 300 else "medium",
                    suggested_action="community_outreach",
                )
            )

        if row["window"] == "90d" and _is_sustained_growth(row):
            signals.append(
                _signal(
                    signal_type="sustained_growth",
                    detected_at=detected_at,
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    title=f"{_metric_label(row)} shows sustained growth",
                    summary=(
                        f"{_metric_label(row)} has positive recent trend and is up "
                        f"{row['percent_change']:.0f}% versus the prior 90 days."
                    ),
                    related_metrics=[row["metric_id"]],
                    evidence_ids=[],
                    score=min(100.0, max(0.0, row["percent_change"])),
                    confidence="medium",
                    suggested_action="docs_content",
                )
            )

        if (
            row["window"] == "30d"
            and abs(row["sdk_share_delta"]) >= SDK_SHARE_SHIFT_THRESHOLD
        ):
            direction = "gained" if row["sdk_share_delta"] > 0 else "lost"
            signals.append(
                _signal(
                    signal_type="sdk_share_shift",
                    detected_at=detected_at,
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    title=f"{_metric_label(row)} {direction} SDK share",
                    summary=(
                        f"{_metric_label(row)} {direction} "
                        f"{abs(row['sdk_share_delta']) * 100:.0f} percentage points "
                        "of product download share."
                    ),
                    related_metrics=[row["metric_id"]],
                    evidence_ids=[],
                    score=abs(row["sdk_share_delta"]) * 100,
                    confidence="medium",
                    suggested_action="sdk_investment",
                )
            )

    burst = _social_burst_signal(
        evidence, latest_day=latest_day, detected_at=detected_at
    )
    if burst:
        signals.append(burst)

    return signals


def _rollup_row(
    metric: dict[str, Any],
    *,
    window: str,
    window_start: date,
    window_end: date,
    current_value: int,
    previous_value: int,
    trend_slope: float,
    updated_at: datetime,
) -> dict[str, Any]:
    delta = current_value - previous_value
    return {
        "rollup_id": f"{metric['metric_id']}:{window}:{window_end.isoformat()}",
        "metric_id": str(metric["metric_id"]),
        "metric_family": str(metric["metric_family"]),
        "product": str(metric["product"]),
        "sdk": "" if metric.get("sdk") is None else str(metric.get("sdk")),
        "subject": str(metric["subject"]),
        "window": window,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "current_value": current_value,
        "previous_value": previous_value,
        "delta": delta,
        "percent_change": _percent_change(current_value, previous_value),
        "sdk_share": 0.0,
        "previous_sdk_share": 0.0,
        "sdk_share_delta": 0.0,
        "trend_slope": trend_slope,
        "updated_at": updated_at,
    }


def _window_value(
    metric: dict[str, Any],
    rows: list[dict[str, Any]],
    window_start: date,
    window_end: date,
) -> int:
    if str(metric.get("metric_family")) == "stars":
        return _star_delta(rows, window_start, window_end)
    total = 0.0
    for row in rows:
        period_start = _row_day(row.get("period_start"))
        period_end = _row_day(row.get("period_end"))
        overlap = _overlap_days(period_start, period_end, window_start, window_end)
        if overlap <= 0:
            continue
        span = max(1, _overlap_days(period_start, period_end, period_start, period_end))
        total += _row_value(row) * (overlap / span)
    return round(total)


def _star_delta(
    rows: list[dict[str, Any]], window_start: date, window_end: date
) -> int:
    before = _latest_value_on_or_before(rows, window_start - timedelta(days=1))
    end = _latest_value_on_or_before(rows, window_end)
    return max(0, end - before)


def _latest_value_on_or_before(rows: list[dict[str, Any]], day: date) -> int:
    value = 0
    latest: date | None = None
    for row in rows:
        period_end = _row_day(row.get("period_end"))
        if period_end > day:
            continue
        if latest is None or period_end > latest:
            latest = period_end
            value = _row_value(row)
    return value


def _monthly_trend_slope(
    metric: dict[str, Any],
    rows: list[dict[str, Any]],
    latest_day: date,
) -> float:
    month_start, _month_end = _last_full_month(latest_day)
    values: list[int] = []
    cursor_start = month_start
    for _ in range(MONTHLY_TREND_MONTHS):
        current_start = cursor_start
        current_end = _month_end_for_start(current_start)
        values.append(_window_value(metric, rows, current_start, current_end))
        cursor_start, _previous_end = _previous_month(current_start)
    values.reverse()
    if len(values) < 2:
        return 0.0
    x_mean = (len(values) - 1) / 2
    y_mean = sum(values) / len(values)
    numerator = sum(
        (idx - x_mean) * (value - y_mean) for idx, value in enumerate(values)
    )
    denominator = sum((idx - x_mean) ** 2 for idx in range(len(values)))
    return numerator / denominator if denominator else 0.0


def _is_download_spike(row: dict[str, Any]) -> bool:
    return (
        row["current_value"] >= DOWNLOAD_SPIKE_MIN_VALUE
        and row["percent_change"] >= DOWNLOAD_SPIKE_PERCENT_THRESHOLD
    )


def _is_sustained_growth(row: dict[str, Any]) -> bool:
    return (
        row["trend_slope"] > 0
        and row["percent_change"] >= SUSTAINED_GROWTH_PERCENT_THRESHOLD
    )


def _social_burst_signal(
    evidence: list[dict[str, Any]],
    *,
    latest_day: date,
    detected_at: datetime,
) -> dict[str, Any] | None:
    window_start = latest_day - timedelta(days=SOCIAL_BURST_DAYS - 1)
    recent = [
        row
        for row in evidence
        if str(row.get("source_type")) in {"hackernews", "manual"}
        and window_start <= _row_datetime(row.get("occurred_at")).date() <= latest_day
    ]
    if len(recent) < SOCIAL_BURST_MIN_ITEMS:
        return None
    evidence_ids = [
        str(row.get("evidence_id")) for row in recent if row.get("evidence_id")
    ]
    related_metrics = sorted(
        {
            str(metric)
            for row in recent
            for metric in row.get("related_metrics", [])
            if metric
        }
    )
    return _signal(
        signal_type="social_mention_burst",
        detected_at=detected_at,
        window_start=window_start.isoformat(),
        window_end=latest_day.isoformat(),
        title="HN/manual mention burst",
        summary=f"{len(recent)} relevant HN/manual evidence items appeared recently.",
        related_metrics=related_metrics,
        evidence_ids=evidence_ids,
        score=float(len(recent)),
        confidence="medium",
        suggested_action="community_outreach",
    )


def _signal(
    *,
    signal_type: str,
    detected_at: datetime,
    window_start: str,
    window_end: str,
    title: str,
    summary: str,
    related_metrics: list[str],
    evidence_ids: list[str],
    score: float,
    confidence: str,
    suggested_action: str,
) -> dict[str, Any]:
    signal_id = f"{signal_type}:{window_start}:{window_end}:{_slug(title)}"
    return {
        "signal_id": signal_id,
        "signal_type": signal_type,
        "detected_at": detected_at,
        "window_start": window_start,
        "window_end": window_end,
        "title": title,
        "summary": summary,
        "related_metrics": related_metrics,
        "evidence_ids": evidence_ids,
        "score": score,
        "confidence": confidence,
        "suggested_action": suggested_action,
    }


def _metric_label(row: dict[str, Any]) -> str:
    product = "LanceDB" if row["product"] == "lancedb" else "Lance"
    sdk = str(row.get("sdk") or "").title()
    return f"{product} {sdk}".strip()


def _last_full_month(latest_day: date) -> tuple[date, date]:
    this_month_start = latest_day.replace(day=1)
    month_end = this_month_start - timedelta(days=1)
    month_start = month_end.replace(day=1)
    return month_start, month_end


def _previous_month(month_start: date) -> tuple[date, date]:
    previous_end = month_start - timedelta(days=1)
    return previous_end.replace(day=1), previous_end


def _month_end_for_start(month_start: date) -> date:
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return next_month - timedelta(days=1)


def _percent_change(current_value: int, previous_value: int) -> float:
    if previous_value == 0:
        return 100.0 if current_value > 0 else 0.0
    return ((current_value - previous_value) / previous_value) * 100


def _overlap_days(a_start: date, a_end: date, b_start: date, b_end: date) -> int:
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    if end < start:
        return 0
    return (end - start).days + 1


def _row_day(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return parse_iso_date(str(value)[:10])


def _row_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    raw = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _row_value(row: dict[str, Any]) -> int:
    value = row.get("value", 0)
    if hasattr(value, "as_py"):
        value = value.as_py()
    return int(value or 0)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _slug(value: str) -> str:
    chars = [char.lower() if char.isalnum() else "-" for char in value]
    return "-".join(part for part in "".join(chars).split("-") if part)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Derive dashboard rollups and signal candidates"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=210,
        help="Source stats window used to compute dashboard rollups",
    )
    args = parser.parse_args()

    result = run(days=args.days)
    print(
        "derive_dashboard complete: "
        f"rollups={result['rollups']} signals={result['signals']}"
    )


if __name__ == "__main__":
    main()
