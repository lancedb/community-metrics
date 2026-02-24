# AGENTS.md

## Project Summary

This repository is a community metrics dashboard that tracks usage & growth statistics for LanceDB,
a multimodal lakehouse and Lance, the lakehouse format that powers LanceDB.

The backend ingests daily metrics (SDK downloads and GitHub stars), stores them in LanceDB Enterprise tables (`metrics`, `stats`, `history`), serves chart data through FastAPI, and renders an interactive frontend built with Vite + React + Tailwind.

This project is remote-only for storage: always use the same LanceDB Enterprise cluster and do not add local LanceDB support.

The operational goal is to keep data fresh with incremental daily updates by default, while allowing controlled rolling backfills with `--lookback-days`.

## Package Management Preferences

For Python, use `uv` to manage dependencies and run scripts.

- Install/sync dependencies with `uv sync`.
- Run scripts with `uv run ...`.
- Do not switch to `pip`/`poetry` unless explicitly requested.

For frontend, use `npm`.

- Install packages with `npm install` in `src/dashboard`.
- Use `npm run dev` / `npm run build` for frontend workflows.

## Formatting and Linting Preferences

Use Ruff for Python formatting and linting.

- Format Python code with `uv run ruff format .`.
- Lint/fix Python code with `uv run ruff check . --fix`.
- Keep imports tidy and avoid style-only churn outside touched files.

If Ruff is missing from the environment, add it to dev dependencies before introducing alternative formatters.

## Usage Expectations for Future Agents

When making pipeline changes, preserve these behaviors:

1. Incremental-by-default updates.
2. `stats` key semantics remain `(metric_id, period_end)`.
3. Keep `--lookback-days` support on refresh/recompute jobs.
4. Preserve the provenance/source-window model:
   `csv_seed` + `discrete_snapshot` for legacy seeded snapshots,
   `recomputed` / `api_daily` + `1d` for daily downloads,
   and `cumulative_snapshot` for stars.
5. Keep `history` focused on refresh-run logging for maintainer troubleshooting (failures/partials), not generalized provenance modeling.
6. Avoid full-table materialization patterns on Enterprise; use bounded server-side query patterns.
7. Keep clean-start bootstrap compatible with the two-step flow:
   `bootstrap_tables` (reset/create/seed metrics) followed by
   `update_all` (which assumes tables already exist by default).

## Maintainer Guardrails

- Always treat LanceDB as using a remote database on an Enterprise cluster. Local LanceDB databse support isn't planned for this application.
- Keep docs in sync when pipeline contracts change (`README.md`, `LOCAL.md`, `AGENTS.md` in the same PR).
- Preserve API endpoints consumed by the frontend unless an explicit breaking change is intended and documented.

See the `README.md` file in this repo for up-to-date usage instructions.
