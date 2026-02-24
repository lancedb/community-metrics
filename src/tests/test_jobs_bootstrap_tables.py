from __future__ import annotations

from community_metrics.jobs import bootstrap_tables


def test_bootstrap_tables_resets_creates_and_seeds(monkeypatch, capsys) -> None:
    class _Store:
        def __init__(self) -> None:
            self.calls: list[str] = []

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
            return {"inserted": 3, "updated": 1}

    fake_store = _Store()
    monkeypatch.setattr(bootstrap_tables, "LanceDBStore", lambda: fake_store)

    result = bootstrap_tables.run()

    assert result == {"inserted": 3, "updated": 1}
    assert fake_store.calls == [
        "reset_tables",
        "create_required_tables",
        ("create_required_tables.recreate", True),
        "seed_metrics",
    ]
    output = capsys.readouterr().out
    assert "[bootstrap] ensuring table: metrics" in output
    assert "[bootstrap] ensuring table: stats" in output
    assert "[bootstrap] ensuring table: history" in output
