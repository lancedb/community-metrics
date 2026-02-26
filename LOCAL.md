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

## 3. Bootstrap from clean slate (destructive)

```bash
uv run python -m community_metrics.jobs.bootstrap_tables
uv run python -m community_metrics.jobs.update_all --lookback-days 90
```

What this does:
- `bootstrap_tables`: drops `metrics`, `stats`, `history`; recreates all three tables; seeds `metrics`.
- `update_all`: assumes tables already exist; seeds older historical points from `seed_data/`, fetches recent API windows, writes `stats`, and writes one refresh-run record to `history`.

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

## 5. Run frontend locally

```bash
cd /Users/prrao/code/community-metrics/src/dashboard
npm run dev
```

Open `http://127.0.0.1:3000`.

## 6. Verify dashboard API route

```bash
curl "http://127.0.0.1:3000/api/v1/dashboard/daily?days=30"
```

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
