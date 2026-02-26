# Community Metrics Dashboard

This repository tracks community metrics for Lance and LanceDB, stores them in LanceDB Enterprise, and renders a read-only dashboard frontend.

Architecture split:
- **Write path**: Python ingestion jobs run on a private host (for example EC2 + cron).
- **Read path**: Next.js dashboard app serves `/api/v1/dashboard/daily` and is deployed to Vercel.

## What This Tracks

- SDK downloads:
  - `pylance` (PyPI)
  - `lance` (crates.io)
  - `lancedb` (PyPI)
  - `@lancedb/lancedb` (npm)
  - `lancedb` (crates.io)
- GitHub stars:
  - `lance-format/lance`
  - `lancedb/lancedb`

## Prerequisites

- Python managed with `uv`
- Frontend managed with `npm`
- A running **LanceDB Enterprise** cluster

## Environment

Create `.env` in the repo root (or update existing):

```bash
LANCEDB_API_KEY=...
LANCEDB_HOST_OVERRIDE=https://<your-enterprise-host>
LANCEDB_REGION=us-east-1

# Strongly recommended for scheduled ingestion:
GITHUB_TOKEN=...
```

`GITHUB_TOKEN` should stay configured on the machine running scheduled updates.

## LanceDB Storage

Tables:
- `metrics`: metric definitions
- `stats`: daily observations keyed by `(metric_id, period_end)`
- `history`: ingestion run logs

Daily row semantics in `stats`:
- `period_start == period_end`
- routine provenance: `api_daily`
- recompute provenance: `recomputed`
- download `source_window`: `1d`
- star `source_window`: `cumulative_snapshot`

## Ingestion Jobs (EC2 / Private Host)

All writes happen directly through `LanceDBStore`.
No FastAPI/uvicorn runtime is required.

### Clean-Slate Bootstrap

```bash
uv run python -m community_metrics.jobs.bootstrap_tables
uv run python -m community_metrics.jobs.update_all --lookback-days 90
```

### Routine Refresh

```bash
uv run python -m community_metrics.jobs.daily_refresh
```

For ad-hoc correction windows:

```bash
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 7
```

### Suggested Cron (EC2)

Run daily at **09:00 UTC**:

```cron
0 9 * * * cd /path/to/community-metrics && /usr/bin/env -S bash -lc 'uv run python -m community_metrics.jobs.daily_refresh >> /var/log/community-metrics/daily_refresh.log 2>&1'
```

## Frontend (Next.js + Vercel)

The dashboard lives in `src/dashboard` and fetches:
- `GET /api/v1/dashboard/daily?days=180`

### Local frontend dev

```bash
cd src/dashboard
npm install
npm run dev
```

### Vercel env vars

Set these in the Vercel project:

```bash
LANCEDB_API_KEY=...
LANCEDB_HOST_OVERRIDE=https://<your-enterprise-host>
LANCEDB_REGION=us-east-1
```

The route is read-only by code path and only queries bounded dashboard windows.
If/when available, use a dedicated read-scoped key for Vercel.

### Frontend metric semantics

- Download chart points are monthly totals.
- Download card headline values are the last full-month totals.
- Through `2025-11-30`, download points come from seeded discrete snapshots.
- From `2025-12-01` onward, monthly download points are aggregated from daily rows.
- Star charts remain daily cumulative series.

## Which Job To Run

| Job | Use this for | Command |
| --- | --- | --- |
| `daily_refresh` | Normal daily updates (scheduled) | `uv run python -m community_metrics.jobs.daily_refresh` |
| `update_all` | Recompute/backfill a full lookback window | `uv run python -m community_metrics.jobs.update_all --lookback-days 90` |
| `bootstrap_tables` | Destructive reset/recreate before rebuild | `uv run python -m community_metrics.jobs.bootstrap_tables` |

## Debug helper

`debug.py` reads LanceDB Enterprise tables directly (no REST API required):

```bash
uv run debug.py metrics
uv run debug.py stats --metric-id downloads:lance:python --days 30
uv run debug.py history --start-date 2026-01-01 --end-date 2026-12-31 --limit 200
uv run debug.py all
```

## Development

Format and lint Python:

```bash
uv run ruff format .
uv run ruff check --fix --select I .
```

Run tests:

```bash
uv run pytest -q
```
