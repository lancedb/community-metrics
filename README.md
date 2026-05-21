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
  - `lance-format/lance-graph`
  - `lance-format/lance-context`
- DuckDB `lance` extension downloads:
  - core repository: `extensions.duckdb.org`
  - community repository: `community-extensions.duckdb.org`

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

# Required only for weekly LLM guidance generation:
OPENAI_API_KEY=...
```

`GITHUB_TOKEN` should stay configured on the machine running scheduled updates.
`OPENAI_API_KEY` is only used by `generate_signal_guidance`; dashboard requests do not call OpenAI.

## LanceDB Storage

Tables:
- `metrics`: metric definitions
- `stats`: daily observations keyed by `(metric_id, period_end)`
- `history`: ingestion run logs
- `dashboard_metric_rollups`: derived dashboard windows and growth comparisons
- `evidence_items`: derived/manual community evidence such as Hacker News mentions
- `signal_candidates`: derived DevRel signal candidates generated from rollups and evidence
- `signal_guidance`: weekly cached LLM guidance generated from signals, rollups, and evidence
- `duckdb_lance_extension_downloads_monthly`: monthly DuckDB `lance` extension downloads split by core/community repository

`metrics`, `stats`, and `history` are the source-of-truth tables. The dashboard-derived
tables can be recreated from source data and external evidence collectors.
DuckDB extension download rows are isolated in their own monthly table and do not modify
`metrics`, `stats`, `history`, or the derived dashboard tables.

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

One-time star-history backfill for newly added GitHub repos:

```bash
uv run python -m community_metrics.jobs.update_daily_stars --lookback-days 180
```

One-time download snapshot backfill for older month-end history:

```bash
uv run python one_time_snapshot_backfill.py
uv run python one_time_snapshot_backfill.py --apply
```

Monthly DuckDB `lance` extension download refresh:

```bash
uv run python -m community_metrics.jobs.update_duckdb_extension_downloads
```

This recomputes monthly rows from January 2026 through the current month, marks the
current month as partial, and writes only `duckdb_lance_extension_downloads_monthly`.

### Suggested Cron (EC2)

Run daily at **09:00 UTC**:

```cron
0 9 * * * cd /path/to/community-metrics && /usr/bin/env -S bash -lc 'uv run python -m community_metrics.jobs.daily_refresh >> /var/log/community-metrics/daily_refresh.log 2>&1'
```

Run monthly DuckDB extension refresh at **10:00 UTC** on the first day of each month:

```cron
0 10 1 * * cd /path/to/community-metrics && /usr/bin/env -S bash -lc 'uv run python -m community_metrics.jobs.update_duckdb_extension_downloads >> /var/log/community-metrics/update_duckdb_extension_downloads.log 2>&1'
```

## Frontend (Next.js + Vercel)

The dashboard lives in `src/dashboard` and fetches:
- `GET /api/v1/dashboard/daily?days=180`
- Google SSO (restricted to `@lancedb.com` accounts)

### Local frontend dev

```bash
cd src/dashboard
npm install
npm run dev
```

Set these frontend env vars in `src/dashboard/.env.local` (local) or Vercel project settings (deployment):

```bash
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
NEXTAUTH_SECRET=...
NEXTAUTH_URL=http://127.0.0.1:3000
```

For local dashboard testing only, Google auth can be bypassed:

```bash
DISABLE_AUTH_LOCAL=1
```

This flag is only honored when Next.js runs with `NODE_ENV=development`; production builds still require Google SSO.
Restart `npm run dev` after changing `.env.local` because Next.js reads these values at server startup.

Google OAuth app setup must include this callback URI:

```bash
http://127.0.0.1:3000/api/auth/callback/google
```

### Vercel env vars

Set these in the Vercel project:

```bash
LANCEDB_API_KEY=...
LANCEDB_HOST_OVERRIDE=https://<your-enterprise-host>
LANCEDB_REGION=us-east-1
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
NEXTAUTH_SECRET=...
NEXTAUTH_URL=https://<your-dashboard-domain>
```

The route is read-only by code path and only queries bounded dashboard windows.
If/when available, use a dedicated read-scoped key for Vercel.

### Frontend metric semantics

- Download chart points are monthly totals.
- Download card headline values are the last full-month totals.
- The dashboard now fetches 730 days of history by default so seeded monthly snapshots remain visible.
- Through `2025-11-30`, download points come from seeded discrete snapshots.
- If the live table is missing a `2025-11-30` snapshot row, the dashboard synthesizes that month from the arithmetic mean of the October 2025 and December 2025 monthly values.
- From `2025-12-01` onward, monthly download points are aggregated from daily rows.
- Star charts remain daily cumulative series.
- Total stars combine all tracked GitHub star repos.
- DuckDB `lance` extension downloads start at January 2026 and are read from
  `duckdb_lance_extension_downloads_monthly`; the widget displays the latest
  community+core total with community and core monthly lines on one chart.
- The Action Cockpit reads precomputed `signal_candidates`, `dashboard_metric_rollups`, `evidence_items`, and `signal_guidance`.
- HN evidence uses `occurred_at` as the filter/order field for "most recent mentions" UI.
- LLM guidance is cached in `signal_guidance`; the dashboard never calls OpenAI during page load.

### Derived Dashboard Data

Derived jobs keep dashboard reads small and avoid recomputing expensive windows at request time:

```bash
uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 30
uv run python -m community_metrics.jobs.derive_dashboard
```

What is precomputed:
- `dashboard_metric_rollups`: 7d, 15d, 30d, 90d, and last-full-month values; prior-window values; deltas; percent changes; SDK share; SDK share deltas; recent trend slope.
- `signal_candidates`: deterministic `download_spike`, `sustained_growth`, `sdk_share_shift`, and `social_mention_burst` signals for DevRel review.
- `evidence_items`: HN/manual evidence with `occurred_at`, `snippet`, matched terms, related metrics/packages, communities, and strength.
- `signal_guidance`: weekly OpenAI-generated DevRel guidance with citations to concrete signal, rollup, and evidence IDs.

First-time setup or backfill for the Action Cockpit:

```bash
# 1. Ensure source tables are current.
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 30

# 2. Backfill recent HN evidence. Use a wider window if you want more initial context.
uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 365

# 3. Build derived rollups and deterministic signal candidates.
uv run python -m community_metrics.jobs.derive_dashboard

# 4. Generate cached LLM guidance for the latest weekly signal window.
uv run python -m community_metrics.jobs.generate_signal_guidance --window-days 7
```

Normal weekly LLM guidance cadence:

```bash
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 7
uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 30
uv run python -m community_metrics.jobs.derive_dashboard
uv run python -m community_metrics.jobs.generate_signal_guidance --window-days 7
```

`generate_signal_guidance` uses the latest 7d rollups as the primary assessment window, compares against 15d and 30d rollups, includes detailed 7d evidence, and sends bounded 15d/30d evidence summaries. Defaults:
- `COMMUNITY_METRICS_OPENAI_MODEL=gpt-5.5`
- `COMMUNITY_METRICS_OPENAI_REASONING_EFFORT=high`
- `COMMUNITY_METRICS_GUIDANCE_PROMPT_VERSION=v1`
- `COMMUNITY_METRICS_OPENAI_TIMEOUT_SECONDS=600`

The guidance job requires `OPENAI_API_KEY`. It writes to `signal_guidance` and may be safely rerun for the same weekly window; rows are upserted by guidance ID. The dashboard will show "guidance pending" for any signal that does not yet have a matching guidance row.

HN collector search terms:
- `LanceDB`
- `lancedb`
- `@lancedb/lancedb`
- `lance format`
- `lance file format`
- `memory-lancedb`
- `memory-lancedb-pro`
- `OpenClaw lancedb`

Reddit/F5bot and GitHub downstream dependency automation are intentionally deferred. Reddit/F5bot evidence can start as manual `evidence_items`; future GitHub dependency evidence should only count exact dependencies found in package manifests or lockfiles.

## Which Job To Run

| Job | Use this for | Command |
| --- | --- | --- |
| `daily_refresh` | Normal daily updates (scheduled) | `uv run python -m community_metrics.jobs.daily_refresh` |
| `update_all` | Recompute/backfill a full lookback window | `uv run python -m community_metrics.jobs.update_all --lookback-days 90` |
| `bootstrap_tables` | Destructive reset/recreate before rebuild | `uv run python -m community_metrics.jobs.bootstrap_tables` |
| `collect_hn_evidence` | Collect recent Hacker News evidence into derived evidence table | `uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 30` |
| `derive_dashboard` | Recompute dashboard rollups and signal candidates | `uv run python -m community_metrics.jobs.derive_dashboard` |
| `generate_signal_guidance` | Generate weekly cached LLM guidance for the Action Cockpit | `uv run python -m community_metrics.jobs.generate_signal_guidance --window-days 7` |
| `update_duckdb_extension_downloads` | Refresh monthly DuckDB `lance` extension downloads | `uv run python -m community_metrics.jobs.update_duckdb_extension_downloads` |

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
