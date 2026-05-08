from __future__ import annotations

import argparse
import csv
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path

METRIC_COLUMN_MAP = {
    "downloads:lance:python": "lance_python_downloads",
    "downloads:lance:rust": "lance_rust_downloads",
    "downloads:lancedb:python": "lancedb_python_downloads",
    "downloads:lancedb:nodejs": "lancedb_nodejs_downloads",
    "downloads:lancedb:rust": "lancedb_rust_downloads",
}

OUTPUT_COLUMNS = ["date", *METRIC_COLUMN_MAP.values()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reshape a stats CSV dump into a date-indexed download table for the "
            "five tracked SDK download metrics."
        )
    )
    parser.add_argument(
        "--input",
        default="stats_dump_2026-04-06.csv",
        help="Path to the stats CSV dump exported from the LanceDB stats table.",
    )
    parser.add_argument(
        "--output",
        default="stats_downloads_by_date_2026-04-06.csv",
        help="Path for the reshaped wide CSV.",
    )
    return parser.parse_args()


def parse_iso_day(raw: str) -> date:
    return date.fromisoformat(raw[:10])


def iter_days(start_day: date, end_day: date) -> Iterable[date]:
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    rows_by_day: dict[str, dict[str, str]] = {}
    seen_pairs: set[tuple[str, str]] = set()
    min_day: date | None = None
    max_day: date | None = None

    with input_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            metric_id = row.get("metric_id", "")
            column_name = METRIC_COLUMN_MAP.get(metric_id)
            if column_name is None:
                continue

            day = row.get("period_end", "")[:10]
            if not day:
                continue

            pair = (day, metric_id)
            if pair in seen_pairs:
                raise ValueError(f"Duplicate metric/date row found for {metric_id} on {day}")
            seen_pairs.add(pair)

            parsed_day = parse_iso_day(day)
            min_day = parsed_day if min_day is None else min(min_day, parsed_day)
            max_day = parsed_day if max_day is None else max(max_day, parsed_day)

            day_row = rows_by_day.setdefault(day, {})
            day_row[column_name] = row.get("value", "")

    if min_day is None or max_day is None:
        raise ValueError("No matching download metrics were found in the input CSV.")

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for day in iter_days(min_day, max_day):
            day_key = day.isoformat()
            source_row = rows_by_day.get(day_key, {})
            output_row = {"date": day_key}
            for column in OUTPUT_COLUMNS[1:]:
                output_row[column] = source_row.get(column, "")
            writer.writerow(output_row)

    print(f"input={input_path.resolve()}")
    print(f"output={output_path.resolve()}")
    print(f"rows={sum(1 for _ in iter_days(min_day, max_day))}")
    print(f"columns={OUTPUT_COLUMNS}")


if __name__ == "__main__":
    main()
