from __future__ import annotations

import argparse

from community_metrics.storage.lancedb_store import LanceDBStore


def run() -> dict[str, int]:
    store = LanceDBStore()

    print("[bootstrap] resetting tables: metrics, stats, history", flush=True)
    store.reset_tables()

    print("[bootstrap] creating required tables", flush=True)
    store.create_required_tables(
        on_table=lambda table_name: print(
            f"[bootstrap] ensuring table: {table_name}", flush=True
        ),
        recreate=True,
    )
    metrics_result = store.seed_metrics()
    print(
        f"[bootstrap] metrics table written inserted={metrics_result['inserted']} updated={metrics_result['updated']}",
        flush=True,
    )
    print("[bootstrap] history table ready", flush=True)
    return metrics_result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap required LanceDB tables for community metrics"
    )
    parser.parse_args()

    result = run()
    print(
        "bootstrap_tables complete: "
        f"metrics_inserted={result['inserted']} metrics_updated={result['updated']}"
    )


if __name__ == "__main__":
    main()
