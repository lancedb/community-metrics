---
name: interpret-metrics
description: >-
  Interpret LanceDB community/download metrics and produce a slide deck summarizing
  how open-source adoption is trending. Use when asked to analyze the dashboard,
  read the download/stars numbers, explain what moved in a month, or build the
  monthly community-metrics deck. Pulls data from the dashboard
  API and the LanceDB stats table, runs trend + pre/post-event analysis, and
  generates a branded HTML deck from the bundled template.
---

# Interpret community metrics → monthly deck

This skill turns the `community-metrics` dashboard into an analysis and a presentation:
read the download/stars data, work out what changed and (cautiously) why, then build a
self-contained HTML slide deck a general internal audience can follow.

**Cadence: one report per month**, anchored on the most recent full month. Each report
sets that month against the **rest of the current quarter so far** (quarter-to-date
trends). In the **first month of a new quarter** there's no quarter-to-date yet, so lead
with **high-level trends from the quarter just ended**, then introduce the new month.

So the deck is monthly, but its comparison window is the quarter. Examples (calendar
quarters: Q1 Jan–Mar, Q2 Apr–Jun, Q3 Jul–Sep, Q4 Oct–Dec):
- **May 2026** report (the first one built): May vs April, within Q2.
- **June 2026** report: June vs April–May — full Q2-to-date.
- **July 2026** report (first month of Q3): recap high-level Q2 trends, then frame July as
  the start of Q3.

---

## 0. Two rules that always apply

These are not optional and they shape every slide:

### Human attribution
The data shows *what* happened; it almost never proves *why*. When the person invoking
this skill supplies a **seed fact** in their prompt — a real-world cause, an event date, a
piece of context the numbers can't contain (e.g. "the TypeScript spike is the OpenClaw
buzz", "the Lance-DuckDB blog posts went out after May 20") — you must:

- **Label it on the slide as coming from a human**, the owner of the Claude account that
  sent the prompt. Use their name. Derive it from `git config user.name` / the account
  email, or ask if unclear.
- Render it visually distinct from data-derived findings. The template uses a cool-toned
  badge (teal `#7fb5c4`, off the warm palette) reading e.g. `Observed by Prashanth` or
  `Blog timing from Prashanth`, plus a one-line plain-language note ("Prashanth attributes
  the April spike to OpenClaw buzz around the TS memory plugin. Plausible, but not
  verifiable from download data.").
- Never launder a human hypothesis into a data conclusion. Keep the two clearly separated.

### Writing style
The audience is a **general internal audience** who want to understand how our open-source
community engagement is going — not just engineers, not investors.

- **Matter-of-fact, not sales-y.** No "engine of growth", "explosive", "carries the
  quarter", "huge win". State the number and what it means.
- **Bullets over prose.** People scan. One idea per bullet.
- **Plain headers.** "Q2 2026 scorecard", "May downloads by package" — not slogans.
- **Define the jargon once.** Downloads are package-manager pulls, **not users** — they
  include CI, mirrors, and transitive installs. Say so.
- **Causation disclaimer is a single line, not a slide.** Put one short note where a causal
  read appears ("Correlation only. Downloads carry no referrer, so this can't confirm the
  posts caused the increase."). Do not dedicate a whole slide to caveats.

---

## 1. Where the data lives

- **App:** Next.js dashboard in `src/dashboard` (`npm run dev`, default port 3000).
- **Local Lance DB** (`data/community_metrics.lancedb`): a **stale seed** — only monthly
  rows, ends ~Feb 2026. Do **not** trust it for current numbers.
- **Live data:** the remote enterprise DB `db://community-metrics`. Credentials are in
  `src/dashboard/.env.local`:
  - `LANCEDB_API_KEY`, `LANCEDB_HOST_OVERRIDE`, `LANCEDB_REGION`
  - This DB has **daily (`source_window = '1d'`)** rows — finer than the dashboard shows.

**Tables:** `metrics` (definitions), `stats` (the time series), `history` (ingestion logs).

**`stats` schema:** `metric_id, period_start, period_end, observed_at, value, provenance,
source_window, ingestion_run_id, source_ref`. `source_window` is `1d`, `7d_sum`, etc.

**Metric IDs:**
- `downloads:lancedb:python` (PyPI `lancedb`), `downloads:lancedb:nodejs` (npm
  `@lancedb/lancedb`), `downloads:lancedb:rust` (crates.io `lancedb`)
- `downloads:lance:python` (PyPI `pylance`), `downloads:lance:rust` (crates.io `lance`)
- `stars:{lancedb,lance,lance-graph,lance-context}:github`

> Note the split: **LanceDB** = the database SDK; **Lance** = the underlying format
> (`pylance`, `lance` crate). A DuckDB + Lance workflow installs `pylance`, not `lancedb` —
> this distinction matters when attributing a move to a cause.

---

## 2. Pull the data

**Monthly per-metric series (fast, via the running app):**
```js
// in the dashboard page context (Claude Preview eval, or fetch from a script)
const r = await fetch('/api/v1/dashboard/daily?days=730');
const j = await r.json();              // groups[].items[].sparkline = monthly [{period_end, value}]
```
Other endpoint: `?response=download_window_totals&window_start=YYYY-MM-DD&window_end=YYYY-MM-DD`
returns the lance/lancedb **aggregate** for a window (no per-SDK split).

**Daily granularity (for within-month / pre-post-event analysis), straight from the DB:**
```bash
# from src/dashboard so .env.local is found
set -a && . ./.env.local && set +a && uv run python - <<'PY'
import os, lancedb
db = lancedb.connect('db://community-metrics',
    api_key=os.environ['LANCEDB_API_KEY'],
    host_override=os.environ['LANCEDB_HOST_OVERRIDE'],
    region=os.environ.get('LANCEDB_REGION','us-east-1'))
t = db.open_table('stats')
rows = t.search().where(
    "metric_id = 'downloads:lancedb:python' AND source_window = '1d'"
).limit(3000).to_list()
# bucket rows[period_end][:10] by week / pre-vs-post an event date as needed
PY
```
`pandas` is not installed in the env — aggregate with plain Python/dicts.

---

## 3. Analyze

Anchor the deck on the **most recent full month** (the latest `period_end`). Compare it
**against the rest of the current quarter so far** (quarter-to-date). In the **first month
of a new quarter**, there's no quarter-to-date yet — instead recap **high-level trends from
the quarter just ended**, then introduce the new month.

Do these every time:

1. **Scorecard** — for each download metric: previous-month value, month-over-month %
   change, and a 3–5 word plain read. (See the table in the example deck.)
2. **Composition** — share of total downloads by package for the latest month. One package
   usually dominates; say which and roughly what %.
3. **Quarter-to-date trend** — plot the monthly series for the headline package(s) across
   the current quarter so far (+ a couple of prior months for context; in the first month
   of a quarter, show the full prior quarter). Call out spikes/drops in absolute terms and
   as MoM %.
4. **Pre/post-event investigation** — when a human supplies an event + date (a launch, a
   blog post, a conference), pull **daily** data and compare avg/day **before vs after** the
   date. Check the *right* package (format vs SDK). Report the % change and whether it held.
5. **Sanity caveats** — flag anything that looks like a data artifact (e.g. a synchronized
   multi-registry dip almost certainly means a collection gap, not real demand) and note
   tail lag (last few days may be incomplete).

Honesty checks: a "decline" off an anomalous spike is **normalization**, not collapse —
compare to the pre-spike baseline. A late-month change must be checked on the package the
cause would actually touch.

---

## 4. Build the deck

Assets bundled in this skill:
- `lancedb-community-metrics-*.html` — the slide template (1280×720, dark warm theme,
  accent `#e97852`, self-contained: fonts + logo embedded as base64, speaker-notes,
  present mode, prints to PDF). **Do not** rewrite its CSS/JS.
- `build_deck.py` — the **Q2 2026 example generator**. It keeps the template's
  head/CSS/JS/logo, throws away the sample slides, and splices in findings slides with
  hand-built **inline-SVG** charts (SVG prints cleanly and keeps the file self-contained).
- `decks/` — generated output.

**To produce next month's deck:** copy `build_deck.py`, refresh the numbers from §2–§3,
update the slide copy, change `OUT` to the new month's filename (e.g.
`lancedb-community-metrics-2026-06.html`), and run it (`uv run python build_deck.py`).
Don't hand-edit 500 KB of HTML.

**Slide skeleton that worked (8 slides — adapt, don't pad):**

| # | Slide | Layout class | Content |
|---|-------|--------------|---------|
| 1 | Title | `slide-title` | "LanceDB community metrics — <Month> <Year>" (the report month), presenter, data-through date |
| 2 | Summary / TL;DR | `slide-content` | 3–4 bullets, one per SDK + the downloads≠users disclaimer |
| 3 | Scorecard | `slide-content` | the MoM table, green up / red down |
| 4 | Composition | `slide-stats`/`slide-content` | horizontal bar chart of latest-month volume + bullets |
| 5 | Headline trend | `slide-content` | monthly line chart + bullets + human badge if a cause was seeded |
| 6 | Investigation | `slide-content` | pre/post-event bars + bullets + human badge + 1-line correlation note |
| 7 | Recommendations | `slide-content` | plain bulleted next steps (hypotheses to test) |
| 8 | Closing | `slide-closing` | "Questions?", one-line recap, data-through date |

**Reusable template components:** `.eyebrow`, `.badge` (+ `.dot`), `.callout`,
`.bullet-list`, `.stat-grid`/`.stat`, `.gradient-text`. Charts the example builds, all as
functions in `build_deck.py` you can copy: `svg_ts_line` (monthly line), `svg_volume`
(horizontal volume bars), `svg_prepost` (two-panel before/after), `q2_table` (scorecard),
`human_badge` (the attribution chip).

**SVG chart gotchas:** scale to the package's own max; keep value labels inside the
viewBox (don't let the longest bar's label clip — the example caps bar width to leave room);
mono font for axis labels, sans for values; up = `#9ccb7a`, down = `#e2654a`, accent =
`#e97852`.

Put genuine human-seeded context into the per-slide **speaker notes** (`<aside
class="speaker-notes">`), so whoever presents knows what's data vs. human read.

---

## 5. Render & verify

The deck is one HTML file. To view it with the Claude Preview tools, copy it into a
folder under the project root (the preview server's cwd must be inside the repo) and serve
it statically, then screenshot each `.slide` by scrolling to its offset:

```jsonc
// .claude/launch.json — add a static-server config (relative cwd!)
{ "name": "deck", "runtimeExecutable": "python3",
  "runtimeArgs": ["-m", "http.server", "8099"],
  "cwd": ".deck-preview", "port": 8099, "autoPort": true }
```
```js
// after preview_start + navigate to /deck.html, resize to 1280x720:
document.querySelectorAll('.slide')[N].scrollIntoView();  // then preview_screenshot
```
Check: numbers match the data, labels aren't clipped, up/down colors are right, human
badges are present where a cause was seeded, and there's no sales language. **Clean up**
the temp preview dir and the `deck` launch entry afterward.

Export to PDF: open in Chrome → ⌘P → Save as PDF, **Background graphics ON**.
