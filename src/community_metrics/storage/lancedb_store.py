from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse

import lancedb

from community_metrics.config import (
    LANCEDB_API_KEY,
    LANCEDB_HOST_OVERRIDE,
    LANCEDB_REGION,
)
from community_metrics.models import (
    HISTORY_SCHEMA,
    METRICS_SCHEMA,
    STATS_SCHEMA,
    metric_definition_rows,
)

TABLE_SCHEMAS = {
    "metrics": METRICS_SCHEMA,
    "stats": STATS_SCHEMA,
    "history": HISTORY_SCHEMA,
}
EXPECTED_TABLES = ("metrics", "stats", "history")
TABLE_READY_TIMEOUT_SECONDS = 30.0
TABLE_READY_SLEEP_SECONDS = 0.5
TABLE_READY_MAX_ATTEMPTS = int(TABLE_READY_TIMEOUT_SECONDS / TABLE_READY_SLEEP_SECONDS)


# This store intentionally assumes a running LanceDB Enterprise cluster.
class LanceDBStore:
    enterprise_uri = "db://community-metrics"

    @staticmethod
    def _validate_host_override(host_override: str) -> str:
        assert host_override, (
            "Missing LANCEDB_HOST_OVERRIDE. Add LANCEDB_HOST_OVERRIDE=<enterprise host> "
            "to .env before running ingestion or API scripts."
        )
        parsed = urlparse(host_override)
        assert parsed.scheme in {"http", "https"} and parsed.netloc, (
            "Invalid LANCEDB_HOST_OVERRIDE. Expected an absolute URL such as "
            "https://<your-enterprise-host>"
        )
        return host_override

    def __init__(self):
        assert LANCEDB_API_KEY, (
            "Missing LANCEDB_API_KEY. Add LANCEDB_API_KEY=<enterprise api key> "
            "to .env before running ingestion or API scripts."
        )
        host_override = self._validate_host_override(LANCEDB_HOST_OVERRIDE)
        self.db = lancedb.connect(
            uri=self.enterprise_uri,
            api_key=LANCEDB_API_KEY,
            host_override=host_override,
            region=LANCEDB_REGION,
        )

    def list_tables(self) -> set[str]:
        return {str(name) for name in self.db.table_names(limit=1000)}

    def reset_tables(self) -> None:
        for table_name in EXPECTED_TABLES:
            try:
                self.db.drop_table(table_name)
            except Exception as exc:
                if self._is_table_not_found_error(exc):
                    continue
                if self._is_terminal_table_error(exc):
                    raise RuntimeError(
                        f"Terminal error while dropping table '{table_name}': {exc}"
                    ) from exc
                raise RuntimeError(
                    f"Failed to drop table '{table_name}': {exc}"
                ) from exc

    def create_required_tables(
        self,
        on_table: Callable[[str], None] | None = None,
        *,
        recreate: bool = False,
    ) -> None:
        create_mode = "overwrite" if recreate else "exist_ok"
        for table_name in EXPECTED_TABLES:
            if on_table is not None:
                on_table(table_name)
            self._create_table_ready(table_name, create_mode=create_mode)

    def ensure_tables(self) -> None:
        self.create_required_tables()

    def _create_table_ready(self, table_name: str, *, create_mode: str) -> None:
        schema = TABLE_SCHEMAS[table_name]
        last_error: Exception | None = None
        for _attempt in range(TABLE_READY_MAX_ATTEMPTS):
            try:
                self.db.create_table(table_name, schema=schema, mode=create_mode)
                return
            except Exception as exc:
                last_error = exc
                if self._is_terminal_table_error(exc):
                    raise RuntimeError(
                        f"Terminal error while ensuring table '{table_name}': {exc}"
                    ) from exc
                time.sleep(TABLE_READY_SLEEP_SECONDS)
        raise RuntimeError(
            f"Timed out ensuring table '{table_name}' after "
            f"{TABLE_READY_TIMEOUT_SECONDS:.0f}s. Last error: {last_error}"
        ) from last_error

    @staticmethod
    def _is_terminal_table_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        terminal_tokens = [
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "permission denied",
            "invalid api key",
            "invalid url",
            "relativeurlwithoutbase",
            "schema",
            "type mismatch",
            "invalid type",
        ]
        transient_tokens = [
            "404",
            "503",
            "table not found",
            "was not found",
            "_versions",
            "service unavailable",
            "temporarily unavailable",
            "retry limit",
            "timed out",
        ]
        if any(token in msg for token in transient_tokens):
            return False
        return any(token in msg for token in terminal_tokens)

    @staticmethod
    def _is_table_not_found_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "not found" in msg or "_versions" in msg

    def _open_table(self, table_name: str):
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unknown table: {table_name}")
        last_error: Exception | None = None
        for _attempt in range(TABLE_READY_MAX_ATTEMPTS):
            try:
                return self.db.open_table(table_name)
            except Exception as open_error:
                last_error = open_error
                if self._is_terminal_table_error(open_error):
                    raise RuntimeError(
                        f"Terminal error while opening table '{table_name}': {open_error}"
                    ) from open_error
                # Remote metadata can briefly lag behind create_table.
                try:
                    self.db.create_table(
                        table_name, schema=TABLE_SCHEMAS[table_name], mode="exist_ok"
                    )
                except Exception as create_error:
                    last_error = create_error
                    if self._is_terminal_table_error(create_error):
                        raise RuntimeError(
                            f"Terminal error while ensuring table '{table_name}' before open: "
                            f"{create_error}"
                        ) from create_error
                time.sleep(TABLE_READY_SLEEP_SECONDS)
        raise RuntimeError(
            f"Timed out opening table '{table_name}' after "
            f"{TABLE_READY_TIMEOUT_SECONDS:.0f}s. Last error: {last_error}"
        ) from last_error

    def seed_metrics(self) -> dict[str, int]:
        table = self._open_table("metrics")
        rows = metric_definition_rows()
        table.add(rows, mode="overwrite")
        return {"inserted": len(rows), "updated": 0}

    def append_stats(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not rows:
            return {"inserted": 0, "updated": 0}
        normalized_rows = [self._normalize_stat_row(row) for row in rows]
        table = self._open_table("stats")
        table.add(normalized_rows, mode="append")
        return {"inserted": len(normalized_rows), "updated": 0}

    def replace_stats(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not rows:
            return {"inserted": 0, "updated": 0}

        normalized_rows = [self._normalize_stat_row(row) for row in rows]
        table = self._open_table("stats")

        metric_windows: dict[str, tuple[str, str]] = {}
        for row in normalized_rows:
            metric_id = str(row["metric_id"])
            day = str(row["period_end"])
            existing = metric_windows.get(metric_id)
            if existing is None:
                metric_windows[metric_id] = (day, day)
                continue
            current_min, current_max = existing
            metric_windows[metric_id] = (min(current_min, day), max(current_max, day))

        for metric_id, (min_day, max_day) in metric_windows.items():
            metric_id_sql = metric_id.replace("'", "''")
            predicate = (
                f"metric_id = '{metric_id_sql}' "
                f"AND period_end >= '{min_day}' "
                f"AND period_end <= '{max_day}'"
            )
            table.delete(predicate)

        table.add(normalized_rows, mode="append")
        return {"inserted": len(normalized_rows), "updated": 0}

    def upsert_stats(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        return self.replace_stats(rows)

    def upsert_history(self, row: dict[str, Any]) -> dict[str, int]:
        normalized = self._normalize_history_row(row)
        table = self._open_table("history")
        ingestion_run_id = str(normalized["ingestion_run_id"]).replace("'", "''")
        table.delete(f"ingestion_run_id = '{ingestion_run_id}'")
        table.add([normalized], mode="append")
        return {"inserted": 1, "updated": 0}

    def query_table(
        self,
        table_name: str,
        *,
        where: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = 20,
    ) -> list[dict[str, Any]]:
        table = self._open_table(table_name)
        builder = table.query() if hasattr(table, "query") else table.search()
        if where:
            builder = builder.where(where)
        if columns:
            builder = builder.select(columns)
        if limit is not None:
            builder = builder.limit(limit)
        return builder.to_list()

    def get_metrics_df(self) -> list[dict[str, Any]]:
        return self.query_table("metrics", limit=None)

    def get_stats_df(self) -> list[dict[str, Any]]:
        return self.query_table("stats", limit=None)

    def get_history_df(self) -> list[dict[str, Any]]:
        return self.query_table("history", limit=None)

    def get_stats_for_metric(self, metric_id: str) -> list[dict[str, Any]]:
        metric_id_sql = str(metric_id).replace("'", "''")
        rows = self.query_table(
            "stats",
            where=f"metric_id = '{metric_id_sql}'",
            limit=None,
        )
        return sorted(rows, key=lambda row: self._coerce_date(row.get("period_end")))

    def count_table_rows(self, table_name: str) -> int:
        table = self._open_table(table_name)
        return int(table.count_rows())

    def list_refresh_errors(
        self,
        *,
        start_day: date,
        end_day: date,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        rows = self.query_table("history", limit=None)
        matches: list[dict[str, Any]] = []
        for row in rows:
            error_summary = str(row.get("error_summary") or "").strip()
            if not error_summary:
                continue
            finished_at = self._coerce_datetime(
                row.get("finished_at") or row.get("started_at")
            )
            finished_day = finished_at.date()
            if finished_day < start_day or finished_day > end_day:
                continue
            matches.append(
                {
                    "ingestion_run_id": row.get("ingestion_run_id"),
                    "job_name": row.get("job_name"),
                    "status": row.get("status"),
                    "started_at": row.get("started_at"),
                    "finished_at": row.get("finished_at"),
                    "error_summary": error_summary,
                }
            )
        matches.sort(
            key=lambda entry: self._coerce_datetime(
                entry.get("finished_at") or entry.get("started_at")
            ),
            reverse=True,
        )
        return matches[:limit]

    @staticmethod
    def _normalize_stat_row(row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        observed = normalized.get("observed_at")
        if isinstance(observed, str):
            normalized["observed_at"] = datetime.fromisoformat(
                observed.replace("Z", "+00:00")
            )
        elif observed is None:
            normalized["observed_at"] = datetime.now(tz=timezone.utc)

        normalized["value"] = int(normalized["value"])
        normalized["period_start"] = str(normalized["period_start"])
        normalized["period_end"] = str(normalized["period_end"])
        normalized["source_ref"] = str(normalized.get("source_ref", ""))
        normalized["provenance"] = str(normalized.get("provenance", "unknown"))
        normalized["source_window"] = str(normalized.get("source_window", "unknown"))
        normalized["ingestion_run_id"] = str(normalized.get("ingestion_run_id", ""))
        return normalized

    @staticmethod
    def _normalize_history_row(row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        for key in ("started_at", "finished_at"):
            value = normalized.get(key)
            if isinstance(value, str):
                normalized[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif value is None:
                normalized[key] = datetime.now(tz=timezone.utc)
        normalized["rows_inserted"] = int(normalized.get("rows_inserted", 0))
        normalized["rows_updated"] = int(normalized.get("rows_updated", 0))
        normalized["error_summary"] = normalized.get("error_summary")
        return normalized

    @staticmethod
    def _coerce_date(value: Any) -> date:
        return LanceDBStore._coerce_datetime(value).date()

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            parsed = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
