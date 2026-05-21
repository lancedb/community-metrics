# Local Runbook (Remote LanceDB Enterprise)

This project always reads/writes a remote LanceDB Enterprise cluster.

Architecture split:
- Write jobs run from a private machine (for example EC2 + cron).
- Frontend runs as a Next.js app in `src/dashboard` and serves a read-only dashboard API route.

## 1. Install deps

```bash
cd /Users/prrao/code/community-metrics
uv sync --extra dev
cd src/dashboard && npm install && cd ../..
```

## 2. Configure `.env`

Required:

```bash
LANCEDB_API_KEY=...
LANCEDB_HOST_OVERRIDE=https://<your-enterprise-host>
LANCEDB_REGION=us-east-1
```

Recommended for GitHub rate limits:

```bash
GITHUB_TOKEN=...
```

Required only for weekly LLM guidance:

```bash
OPENAI_API_KEY=...
```

## 3. Bootstrap from clean slate (destructive)

```bash
uv run python -m community_metrics.jobs.bootstrap_tables
uv run python -m community_metrics.jobs.update_all --lookback-days 90
```

What this does:
- `bootstrap_tables`: drops `metrics`, `stats`, `history`; recreates all three tables; seeds `metrics`.
- `update_all`: assumes tables already exist; seeds older historical points from `seed_data/`, fetches recent API windows, writes `stats`, and writes one refresh-run record to `history`.
- Derived dashboard tables are separate and can be recreated after source data is loaded.

Single-command alternative:

```bash
uv run python -m community_metrics.jobs.update_all --reset-tables --lookback-days 90
```

## 4. Routine daily refresh

```bash
uv run python -m community_metrics.jobs.daily_refresh
```

Optional correction window:

```bash
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 7
```

Refresh derived dashboard data after source data changes:

```bash
uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 30
uv run python -m community_metrics.jobs.derive_dashboard
```

First-time Action Cockpit backfill:

```bash
# Refresh recent source stats.
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 30

# Collect a wider HN window for initial context.
uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 365

# Build rollups and deterministic signal candidates.
uv run python -m community_metrics.jobs.derive_dashboard

# Generate cached weekly LLM guidance.
uv run python -m community_metrics.jobs.generate_signal_guidance --window-days 7
```

Generate the weekly Action Cockpit guidance:

```bash
uv run python -m community_metrics.jobs.generate_signal_guidance --window-days 7
```

Recommended weekly sequence:

```bash
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 7
uv run python -m community_metrics.jobs.collect_hn_evidence --lookback-days 30
uv run python -m community_metrics.jobs.derive_dashboard
uv run python -m community_metrics.jobs.generate_signal_guidance --window-days 7
```

Derived tables:
- `dashboard_metric_rollups`: 7d, 15d, 30d, 90d, and last-full-month values; prior-window comparisons; percent changes; SDK share; trend slope.
- `evidence_items`: HN/manual evidence with filterable `occurred_at` and snippets for recent mention displays.
- `signal_candidates`: deterministic DevRel signals such as `download_spike`, `sustained_growth`, `sdk_share_shift`, and `social_mention_burst`.
- `signal_guidance`: weekly OpenAI-generated DevRel guidance with citations to concrete signal, rollup, and evidence IDs.

Isolated monthly metric table:
- `duckdb_lance_extension_downloads_monthly`: DuckDB `lance` extension downloads from January 2026 onward, split by core and community repositories.

The derived jobs do not modify source-of-truth tables (`metrics`, `stats`, `history`).
The dashboard reads cached guidance only; it does not call OpenAI at request time.
`generate_signal_guidance` requires `OPENAI_API_KEY` and can be rerun for the same week; guidance rows are upserted.
The OpenAI guidance request timeout defaults to 600 seconds via `COMMUNITY_METRICS_OPENAI_TIMEOUT_SECONDS`.

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

This writes only `duckdb_lance_extension_downloads_monthly`; it does not modify
`metrics`, `stats`, `history`, or derived dashboard tables.

## 5. Run frontend locally

```bash
cd /Users/prrao/code/community-metrics/src/dashboard
npm run dev
```

Add these frontend env vars in `src/dashboard/.env.local`:

```bash
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
NEXTAUTH_SECRET=...
NEXTAUTH_URL=http://127.0.0.1:3000
```

To bypass Google auth for local frontend testing, add:

```bash
DISABLE_AUTH_LOCAL=1
```

The bypass only works in Next.js development mode. Production deployments still require Google SSO.
Restart `npm run dev` after changing `.env.local` because Next.js reads these values at server startup.

In Google Cloud Console, configure this OAuth callback URL:

```bash
http://127.0.0.1:3000/api/auth/callback/google
```

Open `http://127.0.0.1:3000`.

## 6. Verify dashboard API route

```bash
curl "http://127.0.0.1:3000/api/v1/dashboard/daily?days=30"
```

The dashboard defaults to a 730-day history window so pre-`2025-12-01` monthly snapshot points remain visible.
The DuckDB extension widget reads `duckdb_lance_extension_downloads_monthly` and starts at January 2026.
The Action Cockpit reads from derived `signal_candidates`, `dashboard_metric_rollups`, `evidence_items`, and `signal_guidance`; it will show pending states until the derived jobs and weekly guidance job have run.

## 7. Debug script (direct LanceDB reads)

Usage:

```bash
# Metrics table view
uv run debug.py metrics

# Stats table view (for one metric)
uv run debug.py stats --metric-id downloads:lance:python --days 30

# History table errors view
uv run debug.py history --start-date 2026-01-01 --end-date 2026-12-31 --limit 200

# All three at once (sample stats metric + recent history errors)
uv run debug.py all
```
