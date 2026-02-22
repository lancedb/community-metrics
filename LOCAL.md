# Local Runbook (Remote LanceDB Enterprise)

This project always reads/writes a remote LanceDB Enterprise cluster.

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

## 3. Start FastAPI locally (run this first)

Start this first and keep it running before bootstrap/refresh scripts.

```bash
cd /Users/prrao/code/community-metrics
uv run uvicorn community_metrics.api.main:app --host 127.0.0.1 --port 8000 --reload
```

## 4. Bootstrap from clean slate (destructive)

```bash
uv run python -m community_metrics.jobs.recompute_history --reset-tables --lookback-days 90
```

What this does:
- Drops `metrics`, `stats`, `history` if present.
- Recreates all three tables.
- Seeds `metrics`.
- Seeds older historical points from `seed_data/`.
- Fetches last 90 days from APIs and writes `stats`.
- Writes one refresh-run record to `history`.

If bootstrap fails with transient remote metadata/control-plane errors, rerun once. The script retries table readiness internally before failing.

## 5. Verify API

```bash
curl "http://127.0.0.1:8000/api/v1/health"
curl "http://127.0.0.1:8000/api/v1/dashboard/daily?days=30"
curl "http://127.0.0.1:8000/api/v1/history/refresh-errors?start_date=2026-01-01&end_date=2026-12-31"
```

`/api/v1/history/refresh-errors` returns refresh failures/partials in the date range.

## 6. Run frontend locally

```bash
cd /Users/prrao/code/community-metrics/src/dashboard
npm run dev
```

Open `http://127.0.0.1:5173`.

## 7. Routine daily refresh

Keep FastAPI running, then execute refresh:

```bash
uv run python -m community_metrics.jobs.daily_refresh
```

Optional correction window:

```bash
uv run python -m community_metrics.jobs.daily_refresh --lookback-days 7
```

## Debug script

A debug script is provided to run locally (assumes the FastAPI server endpoints are accessible and running).

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