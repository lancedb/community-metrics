#!/usr/bin/env python3
"""Build the Q2-2026 community-metrics slide deck from the LanceDB template.

Keeps the template's head/CSS/JS + embedded logo, discards the sample slides,
and injects findings slides with hand-built inline-SVG charts.
"""
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The bundled deck doubles as the template: this script strips its slides and
# re-injects fresh ones, so reading + writing the same file is idempotent.
# For a NEW quarter, copy an existing deck to a new filename and point both here.
OUT = HERE / "decks" / "lancedb-community-metrics-q2-2026.html"
TPL = OUT
OUT.parent.mkdir(exist_ok=True)

html = TPL.read_text()

# --- split: keep everything up to & including <div class="deck">, drop sample
#     slides, keep from the PRESENTATION-MODE chrome onward ---
open_marker = '<div class="deck">'
close_marker = '<!-- ────────── PRESENTATION MODE'
prefix = html[: html.index(open_marker) + len(open_marker)] + "\n"
suffix = "\n</div>\n\n" + html[html.index(close_marker):]

LOGO = re.search(r'class="slide-logo" src="([^"]+)"', html).group(1)
HERO = re.search(r'class="hero-image" src="([^"]+)"', html).group(1)

# ---------- palette ----------
ACCENT = "#e97852"
ACCENT_SOFT = "#f4a484"
UP = "#9ccb7a"
DOWN = "#e2654a"
FG = "#f0e7dc"
MUTED = "#a89a8b"
DIM = "#6e6357"
ELEV = "#221d18"
DEEP = "#14110e"
HUMAN = "#7fb5c4"   # cool tone => clearly "human-observed", not data

N_SLIDES = 8

def chrome(n):
    return (f'    <img class="slide-logo" src="{LOGO}" alt="LanceDB">\n'
            f'    <div class="slide-number">{n:02d}<span class="sep">/</span>{N_SLIDES:02d}</div>\n')

def notes(md):
    return ('    <aside class="speaker-notes"><script type="text/markdown">\n'
            + md.strip("\n") + "\n    </script></aside>\n")

def human_badge(text="Human-observed · Prashanth"):
    return (f'<span class="badge" style="border-color:rgba(127,181,196,.42);'
            f'background:rgba(127,181,196,.12);color:#a9d2dd">'
            f'<span class="dot" style="background:{HUMAN}"></span>{text}</span>')

# ============================================================ SVG: TS line
def svg_ts_line():
    pts = [("Jan", 600416), ("Feb", 1011327), ("Mar", 1417872),
           ("Apr", 4399751), ("May", 2765344)]
    x0, x1, y0, y1 = 70, 590, 50, 270
    maxv = 4_500_000
    xs = [x0 + i * ((x1 - x0) / (len(pts) - 1)) for i in range(len(pts))]
    ys = [y1 - (v / maxv) * (y1 - y0) for _, v in pts]
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    area = f"{xs[0]:.1f},{y1} " + line + f" {xs[-1]:.1f},{y1}"
    grid = ""
    for gv in (1_000_000, 2_000_000, 3_000_000, 4_000_000):
        gy = y1 - (gv / maxv) * (y1 - y0)
        grid += (f'<line x1="{x0}" y1="{gy:.1f}" x2="{x1}" y2="{gy:.1f}" stroke="{FG}" stroke-opacity="0.06"/>'
                 f'<text x="{x0-10:.0f}" y="{gy+4:.1f}" text-anchor="end" font-family="JetBrains Mono,monospace" '
                 f'font-size="11" fill="{DIM}">{gv//1_000_000}M</text>')
    dots = ""
    for i, ((label, v), x, y) in enumerate(zip(pts, xs, ys)):
        big = i in (3, 4)
        col = ACCENT if i != 3 else ACCENT_SOFT
        dots += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{6 if big else 4}" fill="{col}" stroke="{DEEP}" stroke-width="2"/>'
        dots += (f'<text x="{x:.1f}" y="{y1+22:.0f}" text-anchor="middle" '
                 f'font-family="JetBrains Mono,monospace" font-size="12" fill="{MUTED}">{label}</text>')
    # value callouts for Apr peak + May
    callouts = (
        f'<text x="{xs[3]:.1f}" y="{ys[3]-16:.1f}" text-anchor="middle" font-family="Hanken Grotesk,sans-serif" '
        f'font-size="15" font-weight="700" fill="{ACCENT_SOFT}">4.40M</text>'
        f'<text x="{xs[3]:.1f}" y="{ys[3]-34:.1f}" text-anchor="middle" font-family="JetBrains Mono,monospace" '
        f'font-size="10.5" fill="{HUMAN}">OpenClaw peak</text>'
        f'<text x="{xs[4]:.1f}" y="{ys[4]-14:.1f}" text-anchor="middle" font-family="Hanken Grotesk,sans-serif" '
        f'font-size="15" font-weight="700" fill="{FG}">2.77M</text>'
        f'<text x="{xs[4]:.1f}" y="{ys[4]-32:.1f}" text-anchor="middle" font-family="JetBrains Mono,monospace" '
        f'font-size="10.5" fill="{DOWN}">−37% MoM</text>')
    return f'''<svg viewBox="0 0 640 300" width="100%" style="max-width:640px">
  {grid}
  <polygon points="{area}" fill="{ACCENT}" fill-opacity="0.10"/>
  <polyline points="{line}" fill="none" stroke="{ACCENT}" stroke-width="2.5"/>
  {dots}
  {callouts}
</svg>'''

# ============================================================ SVG: two-panel pre/post
def svg_prepost():
    base_y, top_y = 250, 70
    span = base_y - top_y
    def panel(ox, title, pre, post, maxv, up):
        col = UP if up else DIM
        arrow = "▲" if up else "▼"
        dcol = UP if up else DOWN
        pct = (post - pre) / pre * 100
        hp = (pre / maxv) * span
        hq = (post / maxv) * span
        bx1, bx2, bw = ox + 36, ox + 150, 78
        s = (f'<text x="{ox+118}" y="42" text-anchor="middle" font-family="Hanken Grotesk,sans-serif" '
             f'font-size="16" font-weight="600" fill="{FG}">{title}</text>')
        # baseline
        s += f'<line x1="{ox+20}" y1="{base_y}" x2="{ox+216}" y2="{base_y}" stroke="{FG}" stroke-opacity="0.12"/>'
        # pre bar (muted) + post bar (colored)
        s += f'<rect x="{bx1}" y="{base_y-hp:.1f}" width="{bw}" height="{hp:.1f}" rx="4" fill="{MUTED}" fill-opacity="0.35"/>'
        s += f'<rect x="{bx2}" y="{base_y-hq:.1f}" width="{bw}" height="{hq:.1f}" rx="4" fill="{col}" fill-opacity="0.92"/>'
        # value labels
        s += f'<text x="{bx1+bw/2}" y="{base_y-hp-10:.1f}" text-anchor="middle" font-family="Hanken Grotesk,sans-serif" font-size="14" font-weight="700" fill="{MUTED}">{pre/1000:.0f}K</text>'
        s += f'<text x="{bx2+bw/2}" y="{base_y-hq-10:.1f}" text-anchor="middle" font-family="Hanken Grotesk,sans-serif" font-size="14" font-weight="700" fill="{FG}">{post/1000:.0f}K</text>'
        # axis labels
        s += f'<text x="{bx1+bw/2}" y="{base_y+20}" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="10.5" fill="{DIM}">≤ May 19</text>'
        s += f'<text x="{bx2+bw/2}" y="{base_y+20}" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="10.5" fill="{DIM}">May 20–31</text>'
        # delta
        s += f'<text x="{ox+118}" y="{base_y+44}" text-anchor="middle" font-family="Hanken Grotesk,sans-serif" font-size="17" font-weight="700" fill="{dcol}">{arrow} {pct:+.1f}%</text>'
        return s
    pa = panel(0, "Lance Python · pylance", 94888, 119199, 130000, True)
    pb = panel(300, "LanceDB Python · SDK", 271854, 245252, 290000, False)
    divider = f'<line x1="288" y1="60" x2="288" y2="300" stroke="{FG}" stroke-opacity="0.10" stroke-dasharray="4 5"/>'
    return f'''<svg viewBox="0 0 560 310" width="100%" style="max-width:600px">
  {pa}{divider}{pb}
</svg>'''

# ============================================================ SVG: May volume bars
def svg_volume():
    rows = [("LanceDB · Python", 8108278, ACCENT),
            ("Lance · Python (pylance)", 3233266, ACCENT_SOFT),
            ("LanceDB · NodeJS", 2765344, ACCENT_SOFT),
            ("Lance · Rust", 190919, MUTED),
            ("LanceDB · Rust", 98476, MUTED)]
    maxv = rows[0][1]
    bx, bxe = 210, 486
    bw = bxe - bx
    s = ""
    for i, (label, v, col) in enumerate(rows):
        y = 30 + i * 46
        w = (v / maxv) * bw
        vlabel = f"{v/1_000_000:.2f}M" if v >= 1_000_000 else f"{v/1000:.0f}K"
        s += f'<text x="{bx-12}" y="{y+18}" text-anchor="end" font-family="Hanken Grotesk,sans-serif" font-size="14" fill="{FG}">{label}</text>'
        s += f'<rect x="{bx}" y="{y}" width="{max(w,3):.1f}" height="26" rx="5" fill="{col}" fill-opacity="0.9"/>'
        s += f'<text x="{bx+max(w,3)+10:.1f}" y="{y+18}" font-family="JetBrains Mono,monospace" font-size="12.5" fill="{MUTED}">{vlabel}</text>'
    return f'''<svg viewBox="0 0 560 250" width="100%" style="max-width:600px">
  {s}
</svg>'''

# ============================================================ Q2 table
def q2_table():
    rows = [
        ("LanceDB Python", "<code>lancedb</code>", "6.80M", "8.11M", 19.2, "Largest SDK; still growing"),
        ("LanceDB NodeJS", "<code>@lancedb/lancedb</code>", "4.40M", "2.77M", -37.1, "Down off the April spike"),
        ("LanceDB Rust", "<code>lancedb</code>", "81.5K", "98.5K", 20.8, "Small base, growing fast"),
        ("Lance Python", "<code>pylance</code>", "3.01M", "3.23M", 7.6, "Up; late-May increase"),
        ("Lance Rust", "<code>lance</code>", "193.6K", "190.9K", -1.4, "Flat"),
    ]
    th = (f'font-family:JetBrains Mono,monospace;font-size:11px;letter-spacing:.12em;'
          f'text-transform:uppercase;color:{ACCENT};text-align:left;padding:0 18px 14px;font-weight:500;')
    body = ""
    for i, (name, pkg, apr, may, mom, note) in enumerate(rows):
        col = UP if mom > 0 else DOWN
        arrow = "▲" if mom > 0 else "▼"
        border = "" if i == len(rows) - 1 else f"border-bottom:1px solid rgba(240,231,220,0.07);"
        body += (
            f'<tr style="{border}">'
            f'<td style="padding:15px 18px;"><div style="font-size:17px;font-weight:600;color:{FG}">{name}</div>'
            f'<div style="font-family:JetBrains Mono,monospace;font-size:11.5px;color:{DIM};margin-top:3px">{pkg}</div></td>'
            f'<td style="padding:15px 18px;font-family:JetBrains Mono,monospace;font-size:16px;color:{MUTED};text-align:right;">{apr}</td>'
            f'<td style="padding:15px 18px;font-family:JetBrains Mono,monospace;font-size:17px;color:{FG};font-weight:500;text-align:right;">{may}</td>'
            f'<td style="padding:15px 18px;font-size:16px;font-weight:700;color:{col};text-align:right;white-space:nowrap;">{arrow} {mom:+.1f}%</td>'
            f'<td style="padding:15px 18px;font-size:13.5px;color:{MUTED};">{note}</td>'
            f'</tr>')
    return (
        f'<table style="width:100%;border-collapse:collapse;">'
        f'<thead><tr>'
        f'<th style="{th}">SDK · package</th>'
        f'<th style="{th}text-align:right">Apr</th>'
        f'<th style="{th}text-align:right">May</th>'
        f'<th style="{th}text-align:right">MoM</th>'
        f'<th style="{th}">Read</th>'
        f'</tr></thead><tbody>{body}</tbody></table>')

# ============================================================ SLIDES
slides = []

# 01 — TITLE
slides.append(f'''  <section class="slide slide-title">
{chrome(1)}{notes("""
## Open

- This is a read of the Q2 download numbers, not a roadmap.
- Everything here is package download data — directional, not ground truth.
- Data is through May 31, 2026.
""")}
    <div class="left">
      <span class="eyebrow">Community Metrics · Q2 2026</span>
      <h1>LanceDB community<br>metrics — <span class="gradient-text">Q2 2026.</span></h1>
      <p class="subtitle">Package download trends for the Python, TypeScript, and Rust SDKs through May 31, 2026. What moved, and what we can infer about why.</p>
      <div class="presenter">
        <div class="avatar"></div>
        <div class="meta">
          <span class="name">Prashanth Rao</span>
          <span class="role">DEVREL · LANCEDB</span>
        </div>
      </div>
    </div>
    <div class="right">
      <img class="hero-image" src="{HERO}" alt="LanceDB illustration">
    </div>
  </section>''')

# 02 — SUMMARY / TL;DR
slides.append(f'''  <section class="slide slide-content">
{chrome(2)}{notes("""
## Summary

Read the bullets. These are download counts, not user counts.
""")}
    <header>
      <span class="eyebrow">Summary</span>
      <h2>TL;DR</h2>
    </header>
    <ul class="bullet-list">
      <li><strong>Python (<code>lancedb</code>)</strong> is the largest SDK and still growing: +19% MoM, all-time high in May (8.11M).</li>
      <li><strong>TypeScript (<code>@lancedb/lancedb</code>)</strong> dropped 37% MoM to 2.77M, off an anomalous April peak (4.40M). Still ~2× March.</li>
      <li><strong>Rust (<code>lancedb</code>)</strong> is small but growing fast: +21% MoM, all-time high (98.5K).</li>
      <li><strong>Lance format</strong> (<code>pylance</code>, <code>lance</code> crate) is flat to slightly up.</li>
    </ul>
    <p style="font-size:14px;color:{DIM};line-height:1.5;max-width:860px;">All figures are package download counts (PyPI / npm / crates.io). Causes discussed later are hypotheses — downloads carry no referrer, so none of this proves causation.</p>
  </section>''')

# 03 — Q2 SCORECARD TABLE
slides.append(f'''  <section class="slide slide-content">
{chrome(3)}{notes("""
## Q2 scorecard

- Green = up MoM, red = down.
- NodeJS −37% is off an anomalous April; still ~2× its March baseline.
""")}
    <header>
      <span class="eyebrow">Downloads · last two full months</span>
      <h2>Q2 2026 scorecard</h2>
      <p class="lede">Monthly downloads for the two most recent full months. MoM = month-over-month; color shows direction.</p>
    </header>
    <div style="margin-top:8px;border:1px solid var(--border);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,0.012),transparent 80%);padding:14px 16px;">
      {q2_table()}
    </div>
  </section>''')

# 04 — VOLUME / WHERE DOWNLOADS COME FROM
slides.append(f'''  <section class="slide slide-stats">
{chrome(4)}{notes("""
## Composition

- LanceDB Python alone is larger than every other package combined.
- Rust numbers are small; judge it on growth rate, not volume.
""")}
    <header>
      <span class="eyebrow">Composition · May 2026</span>
      <h2>May downloads by package</h2>
      <p class="lede">Monthly downloads per package, May 2026.</p>
    </header>
    <div class="columns" style="display:grid;grid-template-columns:1.25fr 0.75fr;gap:48px;align-items:center;">
      <div>{svg_volume()}</div>
      <ul class="bullet-list" style="gap:16px;">
        <li><strong>Python is ~74%</strong> of LanceDB SDK downloads.</li>
        <li><strong>NodeJS is second</strong> (~25%) and Rust is ~1%.</li>
        <li><strong>Rust is small in absolute terms</strong> — judge it on growth rate, not volume.</li>
      </ul>
    </div>
  </section>''')

# 05 — TS / OPENCLAW STORY
slides.append(f'''  <section class="slide slide-content">
{chrome(5)}{notes("""
## TypeScript

- April was a ~3× single-month jump; May gave back 37%.
- OpenClaw attribution is Prashanth's read, not something the download data proves.
- Open question for June: does it settle above the old baseline?
""")}
    <header>
      <span class="eyebrow">TypeScript SDK</span>
      <h2>TypeScript: April spike, May drop</h2>
    </header>
    <div class="columns" style="display:grid;grid-template-columns:1.1fr 0.9fr;gap:44px;align-items:center;">
      <div>{svg_ts_line()}
        <div style="font-family:JetBrains Mono,monospace;font-size:10.5px;color:{DIM};margin-top:4px;text-align:center;">@lancedb/lancedb · monthly npm downloads · Jan–May 2026</div>
      </div>
      <div style="display:flex;flex-direction:column;gap:18px;">
        <ul class="bullet-list" style="gap:13px;">
          <li style="font-size:15px;"><strong>Apr 2026: 4.40M</strong> — ~3× the prior month.</li>
          <li style="font-size:15px;"><strong>May 2026: 2.77M</strong> — −37% MoM, still ~2× March.</li>
          <li style="font-size:15px;">Pattern is a spike normalizing, not a declining baseline.</li>
        </ul>
        {human_badge("Observed by Prashanth")}
        <p style="font-size:13.5px;color:{MUTED};line-height:1.5;">Prashanth attributes the April spike to OpenClaw buzz around the TS memory plugin. Plausible, but not verifiable from download data.</p>
      </div>
    </div>
  </section>''')

# 06 — INVESTIGATION pylance vs SDK
slides.append(f'''  <section class="slide slide-content">
{chrome(6)}{notes("""
## Investigation

- Question: did the Lance-DuckDB posts (after May 20) move downloads?
- pylance (Lance format, Python) rose ~26% post-May 20 and held ~2 weeks.
- The LanceDB Python SDK did not — it fell ~10% over the same window.
- Blog timing is a human-supplied fact (Prashanth); the link is correlation only.
""")}
    <header>
      <span class="eyebrow">Investigation</span>
      <h2>Did the Lance-DuckDB posts move downloads?</h2>
      <p class="lede">Avg downloads/day before vs after May 20 (post date). The increase shows up on the Lance <em>format</em> package, not the LanceDB SDK.</p>
    </header>
    <div class="columns" style="display:grid;grid-template-columns:1.05fr 0.95fr;gap:40px;align-items:center;">
      <div>{svg_prepost()}
        <div style="font-family:JetBrains Mono,monospace;font-size:10px;color:{DIM};margin-top:2px;text-align:center;">bars scaled per package · avg downloads/day</div>
      </div>
      <div style="display:flex;flex-direction:column;gap:16px;">
        <ul class="bullet-list" style="gap:13px;">
          <li style="font-size:15px;"><strong><code>pylance</code>: +25.6%</strong> after May 20, sustained ~2 weeks.</li>
          <li style="font-size:15px;"><strong>LanceDB Python SDK: −9.8%</strong> over the same window.</li>
          <li style="font-size:15px;"><code>pylance</code> is the package a DuckDB + Lance workflow installs.</li>
        </ul>
        {human_badge("Blog timing from Prashanth")}
        <p style="font-size:13.5px;color:{MUTED};line-height:1.5;">Correlation only. Downloads carry no referrer, so this can't confirm the posts caused the increase.</p>
      </div>
    </div>
  </section>''')

# 07 — RECOMMENDATIONS
slides.append(f'''  <section class="slide slide-content">
{chrome(7)}{notes("""
## Recommendations

- Scoped to the Q2 signals; each is a hypothesis to test.
""")}
    <header>
      <span class="eyebrow">Next steps</span>
      <h2>Recommendations</h2>
    </header>
    <ul class="bullet-list">
      <li><strong>TypeScript:</strong> capture trial installs from the April spike before they churn — docs beyond agent-memory, plus a production/deploy guide.</li>
      <li><strong>Lance-DuckDB:</strong> the <code>pylance</code> increase correlates with the posts. Ship more format-interop content.</li>
      <li><strong>June:</strong> check whether <code>@lancedb/lancedb</code> settles above its pre-April baseline or keeps dropping.</li>
      <li><strong>Attribution:</strong> pair download data with docs/referral analytics to test these hypotheses.</li>
    </ul>
  </section>''')

# 08 — CLOSING / CTA TO DASHBOARD
DASHBOARD_URL = "https://community-metrics-alpha.vercel.app/"
DASHBOARD_LABEL = "community-metrics-alpha.vercel.app"
slides.append(f'''  <section class="slide slide-closing">
{chrome(8)}{notes(f"""
## Close

- Recap the three findings; causes are hypotheses.
- Point people at the live dashboard: {DASHBOARD_URL}
""")}
    <span class="eyebrow">Live dashboard</span>
    <h2><span class="gradient-text">Explore the live dashboard</span></h2>
    <p class="lede">Python growing, TypeScript normalizing after the April spike, and a <code>pylance</code> increase that correlates with the Lance-DuckDB posts. The numbers update as new data lands — explore them yourself.</p>
    <a href="{DASHBOARD_URL}" style="display:inline-flex;align-items:center;gap:12px;margin-top:10px;padding:15px 30px;border:1px solid rgba(233,120,82,.42);background:rgba(233,120,82,.12);border-radius:999px;color:{ACCENT_SOFT};font-size:19px;font-weight:500;text-decoration:none;font-family:'JetBrains Mono',ui-monospace,monospace;letter-spacing:.01em;">→&nbsp;{DASHBOARD_LABEL}</a>
    <div class="links" style="margin-top:22px;">
      <div><span class="at">▦</span><span>Data through May 31, 2026</span></div>
    </div>
  </section>''')

OUT.write_text(prefix + "\n".join(slides) + suffix)
print("wrote", OUT, "·", OUT.stat().st_size, "bytes ·", len(slides), "slides")
