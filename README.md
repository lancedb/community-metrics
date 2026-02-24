# Community Metrics Dashboard

This repository tracks community metrics for Lance and LanceDB, stores them in LanceDB Enterprise, and serves a frontend dashboard.

The pipeline is now run **daily**, with the displayed download metrics summed over the last month.

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

### Environment

Create `.env` in the repo root (or update existing):

```bash
LANCEDB_API_KEY=...
LANCEDB_HOST_OVERRIDE=https://<your-enterprise-host>
LANCEDB_REGION=us-east-1

# Strongly recommended for maintainers/scheduled runs:
GITHUB_TOKEN=...
```

`GITHUB_TOKEN` should stay configured on the machine running scheduled updates. Without it, GitHub stargazer backfills are much more likely to rate-limit.
Use a personal access token (not a repo-level token). If fine-grained repo selection is restricted by org policy, use the org-approved token path available to maintainers.

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

## Clean-Slate Bootstrap

Before running bootstrap, start FastAPI so that the logging commands can pull data from the REST endpoints to display progress:

```bash
uv run uvicorn community_metrics.api.main:app --host 127.0.0.1 --port 8000 --reload
```

Run the first script `bootstrap_tables.py` to create the necessary Lance tables:

```bash
uv run python -m community_metrics.jobs.bootstrap_tables
```

This drops the `metrics`, `stats`, and `history` tables, recreates schemas, and seeds metric definitions.

## Routine Refresh

The pipeline is designed to run daily on a schedule, or on-demand to refresh data up to a certain date.

The following order of steps must be maintained:
1. Ensure that you first run the FastAPI server before running refresh jobs.
2. Run daily refresh script

```bash
uv run python -m community_metrics.jobs.daily_refresh
```

For ad-hoc correction of recent data, pick a set number of lookback days:

```bash
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 7
```

Default scheduling of refresh is set to **09:00 UTC daily**.

Notes:
- `update_all` now assumes tables exist by default.
- Bootstrap prints source request progress to stdout so maintainers can see exactly what is being requested.

## Which Job To Run

| Job | Use this for | Command |
| --- | --- | --- |
| `daily_refresh` | Normal daily updates (this runs on a schedule daily) | `uv run python -m community_metrics.jobs.daily_refresh` |
| `update_all` | Recompute/backfill a full lookback window across all sources | `uv run python -m community_metrics.jobs.update_all --lookback-days 90` |
| `bootstrap_tables` | Destructive reset/recreate of `metrics`/`stats`/`history` before a rebuild | `uv run python -m community_metrics.jobs.bootstrap_tables` |

Typical rebuild flow:
1. `uv run python -m community_metrics.jobs.bootstrap_tables`
2. `uv run python -m community_metrics.jobs.update_all --lookback-days 90`

## Individual Jobs

Ensure the FastAPI server is running first, then run these jobs.

Refresh downloads only:

```bash
uv run python -m community_metrics.jobs.update_daily_downloads
uv run python -m community_metrics.jobs.update_daily_downloads --lookback-days 7
```

Refresh stars only:

```bash
uv run python -m community_metrics.jobs.update_daily_stars
uv run python -m community_metrics.jobs.update_daily_stars --lookback-days 7
```

Star collection behavior:

- `lookback_days == 0` and one target day: snapshot stars (`/repos/{repo}`)
- lookback paths: stargazer events (`/repos/{repo}/stargazers`) aggregated by day
- if stargazer backfill fails: falls back to snapshot and records details in `history.error_summary`

## API

- `GET /api/v1/health`
- `GET /api/v1/definitions`
- `GET /api/v1/series/{metric_id}?days=180`
- `GET /api/v1/dashboard/daily?days=180`
- `GET /api/v1/history/refresh-errors?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

Daily response contract uses `days` (not `weeks`).

Defaults:

- `DEFAULT_DAYS = 180`
- `MAX_DAYS = 730`

## Frontend

Install and run dashboard:

```bash
cd src/dashboard
npm install
npm run dev
```

The frontend consumes `/api/v1/dashboard/daily`.

### Frontend Metric Semantics

- Download chart points are monthly totals.
- Download card headline values are the last full-month totals.
- Through `2025-11-30`, download points come from seeded discrete snapshots.
- From `2025-12-01` onward, monthly download points are aggregated from daily rows.
- Star charts remain daily cumulative series.

## API-Only Debug Posture

For maintainers and debug scripts, query FastAPI endpoints instead of directly reading the remote cluster.
This keeps access scoped through API contracts and avoids accidental full remote table materialization.

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
