from __future__ import annotations

from datetime import date

import pytest

from community_metrics.storage import lancedb_store as store_module
from community_metrics.storage.lancedb_store import LanceDBStore


def test_store_requires_api_key(monkeypatch) -> None:
    monkeypatch.setattr(store_module, "LANCEDB_API_KEY", "")
    monkeypatch.setattr(
        store_module,
        "LANCEDB_HOST_OVERRIDE",
        "https://enterprise.example.com",
    )

    with pytest.raises(AssertionError, match="Missing LANCEDB_API_KEY"):
        LanceDBStore()


def test_store_requires_host_override(monkeypatch) -> None:
    monkeypatch.setattr(store_module, "LANCEDB_API_KEY", "enterprise-key")
    monkeypatch.setattr(store_module, "LANCEDB_HOST_OVERRIDE", "")

    with pytest.raises(AssertionError, match="Missing LANCEDB_HOST_OVERRIDE"):
        LanceDBStore()


def test_store_rejects_non_url_host_override(monkeypatch) -> None:
    monkeypatch.setattr(store_module, "LANCEDB_API_KEY", "enterprise-key")
    monkeypatch.setattr(store_module, "LANCEDB_HOST_OVERRIDE", "enterprise.example.com")

    with pytest.raises(AssertionError, match="Invalid LANCEDB_HOST_OVERRIDE"):
        LanceDBStore()


def test_store_uses_enterprise_connection_kwargs(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(store_module, "LANCEDB_API_KEY", "enterprise-key")
    monkeypatch.setattr(
        store_module,
        "LANCEDB_HOST_OVERRIDE",
        "https://enterprise-host.example.com",
    )
    monkeypatch.setattr(store_module, "LANCEDB_REGION", "us-west-2")
    monkeypatch.setattr(store_module.lancedb, "connect", fake_connect)

    LanceDBStore()

    assert captured["uri"] == "db://community-metrics"
    assert captured["api_key"] == "enterprise-key"
    assert captured["host_override"] == "https://enterprise-host.example.com"
    assert captured["region"] == "us-west-2"


def test_reset_tables_drops_expected_tables_and_ignores_missing() -> None:
    class _DB:
        def __init__(self) -> None:
            self.dropped: list[str] = []

        def drop_table(self, name: str):
            if name == "stats":
                raise ValueError("Table 'stats' was not found")
            self.dropped.append(name)

    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()

    store.reset_tables()

    assert store.db.dropped == ["metrics", "history"]


def test_create_required_tables_creates_each_expected_table_once() -> None:
    class _DB:
        def __init__(self) -> None:
            self.create_calls: list[str] = []

        def create_table(self, name: str, **kwargs):
            self.create_calls.append(name)
            assert kwargs["mode"] == "exist_ok"
            return object()

    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()

    store.create_required_tables()

    assert store.db.create_calls == ["metrics", "stats", "history"]


def test_create_required_tables_recreate_uses_overwrite() -> None:
    class _DB:
        def __init__(self) -> None:
            self.modes: list[str] = []

        def create_table(self, _name: str, **kwargs):
            self.modes.append(str(kwargs["mode"]))
            return object()

    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()

    store.create_required_tables(recreate=True)

    assert store.db.modes == ["overwrite", "overwrite", "overwrite"]


def test_create_required_tables_retries_transient_create_errors(
    monkeypatch,
) -> None:
    class _DB:
        def __init__(self) -> None:
            self.create_calls: list[str] = []
            self.create_attempts = {"metrics": 0, "stats": 0, "history": 0}

        def create_table(self, name: str, **kwargs):
            self.create_calls.append(name)
            self.create_attempts[name] += 1
            assert kwargs["mode"] == "exist_ok"
            if name == "metrics" and self.create_attempts[name] < 3:
                raise RuntimeError("503 Service Temporarily Unavailable")
            return object()

    monkeypatch.setattr(store_module.time, "sleep", lambda *_args, **_kwargs: None)
    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()

    store.create_required_tables()

    # metrics required retries; others succeeded on first try.
    assert store.db.create_calls == [
        "metrics",
        "metrics",
        "metrics",
        "stats",
        "history",
    ]


def test_create_required_tables_timeout_includes_table_and_last_error(
    monkeypatch,
) -> None:
    class _DB:
        def create_table(self, _name: str, **_kwargs):
            raise RuntimeError("503 Service Temporarily Unavailable")

    monkeypatch.setattr(store_module.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(store_module, "TABLE_READY_MAX_ATTEMPTS", 3)
    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()

    with pytest.raises(RuntimeError) as exc_info:
        store.create_required_tables()
    message = str(exc_info.value)
    assert "Timed out ensuring table 'metrics'" in message
    assert "503 Service Temporarily Unavailable" in message


def test_open_table_fallback_create_then_open(monkeypatch) -> None:
    class _Table:
        pass

    class _DB:
        def __init__(self) -> None:
            self.open_calls = 0
            self.create_calls = 0

        def open_table(self, _name: str):
            self.open_calls += 1
            if self.open_calls == 1:
                raise ValueError("Table 'stats' was not found")
            return _Table()

        def create_table(self, _name: str, **kwargs):
            self.create_calls += 1
            assert kwargs["mode"] == "exist_ok"
            return object()

    monkeypatch.setattr(store_module.time, "sleep", lambda *_args, **_kwargs: None)
    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()

    table = store._open_table("stats")

    assert isinstance(table, _Table)
    assert store.db.open_calls == 2
    assert store.db.create_calls == 1


def test_query_table_uses_query_builder_when_available() -> None:
    class _Builder:
        def where(self, _clause):
            return self

        def select(self, _columns):
            return self

        def limit(self, _value):
            return self

        def to_list(self):
            return [{"ok": True}]

    class _Table:
        def query(self):
            return _Builder()

    class _DB:
        def open_table(self, _table_name: str):
            return _Table()

    store = LanceDBStore.__new__(LanceDBStore)
    store.db = _DB()
    rows = store.query_table(
        "metrics", where="metric_id = 'x'", columns=["metric_id"], limit=10
    )
    assert rows == [{"ok": True}]


def test_list_refresh_errors_filters_date_window() -> None:
    store = LanceDBStore.__new__(LanceDBStore)
    store.query_table = lambda _name, limit=None: [
        {
            "ingestion_run_id": "r1",
            "job_name": "daily_refresh",
            "status": "partial",
            "started_at": "2026-02-01T10:00:00Z",
            "finished_at": "2026-02-01T10:02:00Z",
            "error_summary": "timeout",
        },
        {
            "ingestion_run_id": "r2",
            "job_name": "daily_refresh",
            "status": "success",
            "started_at": "2026-02-02T10:00:00Z",
            "finished_at": "2026-02-02T10:01:00Z",
            "error_summary": None,
        },
        {
            "ingestion_run_id": "r3",
            "job_name": "daily_refresh",
            "status": "failed",
            "started_at": "2026-02-10T10:00:00Z",
            "finished_at": "2026-02-10T10:01:00Z",
            "error_summary": "503",
        },
    ]

    rows = store.list_refresh_errors(
        start_day=date(2026, 2, 1),
        end_day=date(2026, 2, 5),
    )

    assert len(rows) == 1
    assert rows[0]["ingestion_run_id"] == "r1"
