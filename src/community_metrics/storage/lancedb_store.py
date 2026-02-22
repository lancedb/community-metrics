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
CREATE_READY_TIMEOUT_SECONDS = 30.0
CREATE_READY_SLEEP_SECONDS = 0.5
CREATE_READY_MAX_ATTEMPTS = int(
    CREATE_READY_TIMEOUT_SECONDS / CREATE_READY_SLEEP_SECONDS
)


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
        existing = self.list_tables()
        for table_name in EXPECTED_TABLES:
            if table_name in existing:
                self.db.drop_table(table_name)

    def create_required_tables(
        self, on_table: Callable[[str], None] | None = None
    ) -> None:
        for table_name in EXPECTED_TABLES:
            if on_table is not None:
                on_table(table_name)
            self._create_or_open_ready(table_name)

    def ensure_tables(self) -> None:
        self.create_required_tables()

    def _create_or_open_ready(self, table_name: str) -> None:
        schema = TABLE_SCHEMAS[table_name]
        last_error: Exception | None = None
        for _attempt in range(CREATE_READY_MAX_ATTEMPTS):
            try:
                self.db.create_table(table_name, schema=schema, mode="exist_ok")
                table = self.db.open_table(table_name)
                table.count_rows()
                return
            except Exception as exc:
                last_error = exc
                if self._is_terminal_table_error(exc):
                    raise RuntimeError(
                        f"Terminal error while ensuring table '{table_name}': {exc}"
                    ) from exc
                time.sleep(CREATE_READY_SLEEP_SECONDS)
        raise RuntimeError(
            f"Timed out ensuring table '{table_name}' after "
            f"{CREATE_READY_TIMEOUT_SECONDS:.0f}s. Last error: {last_error}"
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
            "_versions",
            "service unavailable",
            "temporarily unavailable",
            "retry limit",
            "timed out",
        ]
        if any(token in msg for token in transient_tokens):
            return False
        return any(token in msg for token in terminal_tokens)

    def _open_table(self, table_name: str):
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unknown table: {table_name}")
        try:
            return self.db.open_table(table_name)
        except Exception as first_error:
            # Remote metadata can briefly lag behind create_table.
            self.db.create_table(
                table_name, schema=TABLE_SCHEMAS[table_name], mode="exist_ok"
            )
            time.sleep(CREATE_READY_SLEEP_SECONDS)
            try:
                return self.db.open_table(table_name)
            except Exception as second_error:
                raise RuntimeError(
                    f"Failed to open table '{table_name}' after fallback create. "
                    f"First error: {first_error}; second error: {second_error}"
                ) from second_error

    def seed_metrics(self) -> dict[str, int]:
        table = self._open_table("metrics")
        rows = metric_definition_rows()
        result = (
            table.merge_insert("metric_id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(rows)
        )
        return {
            "inserted": int(result.num_inserted_rows),
            "updated": int(result.num_updated_rows),
        }

    def append_stats(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not rows:
            return {"inserted": 0, "updated": 0}
        normalized_rows = [self._normalize_stat_row(row) for row in rows]
        table = self._open_table("stats")
        table.add(normalized_rows, mode="append")
        return {"inserted": len(normalized_rows), "updated": 0}

    def upsert_stats(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not rows:
            return {"inserted": 0, "updated": 0}

        normalized_rows = [self._normalize_stat_row(row) for row in rows]
        table = self._open_table("stats")
        try:
            result = (
                table.merge_insert(["metric_id", "period_end"])
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(normalized_rows)
            )
            return {
                "inserted": int(result.num_inserted_rows),
                "updated": int(result.num_updated_rows),
            }
        except NotImplementedError:
            inserted = 0
            updated = 0
            for row in normalized_rows:
                metric_id = str(row["metric_id"]).replace("'", "''")
                period_end = str(row["period_end"]).replace("'", "''")
                predicate = f"metric_id = '{metric_id}' AND period_end = '{period_end}'"
                exists = int(table.count_rows(filter=predicate)) > 0
                table.delete(predicate)
                table.add([row], mode="append")
                if exists:
                    updated += 1
                else:
                    inserted += 1
            return {"inserted": inserted, "updated": updated}

    def upsert_history(self, row: dict[str, Any]) -> dict[str, int]:
        normalized = self._normalize_history_row(row)
        table = self._open_table("history")
        result = (
            table.merge_insert("ingestion_run_id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute([normalized])
        )
        return {
            "inserted": int(result.num_inserted_rows),
            "updated": int(result.num_updated_rows),
        }

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
