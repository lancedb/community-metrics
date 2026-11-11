/**
 * Fixed dashboard API payload for the UI tests. Synthetic numbers, shaped like the real
 * /api/v1/dashboard/daily response. The tests freeze the browser clock at FIXED_NOW, so
 * "last full month" is always October 2026.
 */
import type {
  DashboardMetric,
  DashboardMetricRollup,
  DashboardResponse,
  DownloadWindowTotals,
  SparkPoint,
} from '../../src/types'

export const FIXED_NOW = new Date('2026-11-11T12:00:00Z')

const MONTH_ENDS = [
  '2026-05-31',
  '2026-06-30',
  '2026-07-31',
  '2026-08-31',
  '2026-09-30',
  '2026-10-31',
]

function monthly(values: number[]): SparkPoint[] {
  return MONTH_ENDS.map((periodEnd, index) => ({
    period_start: `${periodEnd.slice(0, 8)}01`,
    period_end: periodEnd,
    value: values[index],
  }))
}

function daily(start: string, values: number[]): SparkPoint[] {
  const first = new Date(`${start}T00:00:00Z`)
  return values.map((value, index) => {
    const day = new Date(first.getTime() + index * 86_400_000).toISOString().slice(0, 10)
    return { period_start: day, period_end: day, value }
  })
}

function downloads(
  metricId: string,
  sdk: string,
  subject: string,
  values: number[],
  latestValue: number,
): DashboardMetric {
  return {
    metric_id: metricId,
    display_name: metricId,
    metric_family: 'downloads',
    sdk,
    subject,
    latest_value: latestValue,
    latest_period_end: '2026-11-10',
    latest_provenance: 'api_daily',
    total_stars: null,
    sparkline: monthly(values),
  }
}

function stars(metricId: string, subject: string, values: number[]): DashboardMetric {
  return {
    metric_id: metricId,
    display_name: metricId,
    metric_family: 'stars',
    sdk: null,
    subject,
    latest_value: values[values.length - 1],
    latest_period_end: '2026-11-10',
    latest_provenance: 'cumulative_snapshot',
    total_stars: null,
    sparkline: daily('2026-11-06', values),
  }
}

// Download cards show the October value ("last full month total"). Each series peaks in
// June so the October value is never also the chart's min or max label.
export const LANCEDB_DOWNLOADS = {
  python: downloads('downloads:lancedb:python', 'python', 'lancedb', [610_000, 9_640_500, 702_300, 755_120, 801_004, 8_123_456], 274_837),
  nodejs: downloads('downloads:lancedb:nodejs', 'nodejs', '@lancedb/lancedb', [90_100, 192_200, 95_300, 99_400, 101_500, 104_321], 3_210),
  rust: downloads('downloads:lancedb:rust', 'rust', 'lancedb', [210_000, 920_000, 230_000, 240_000, 250_000, 512_345], 17_850),
}

export const LANCE_DOWNLOADS = {
  python: downloads('downloads:lance:python', 'python', 'pylance', [1_100_000, 5_200_000, 1_300_000, 1_400_000, 1_500_000, 4_567_890], 160_331),
  rust: downloads('downloads:lance:rust', 'rust', 'lance', [300_000, 910_000, 320_000, 330_000, 340_000, 765_432], 26_725),
}

export const STARS = {
  lance: stars('stars:lance:github', 'lance-format/lance', [7_270, 7_272, 7_275, 7_279]),
  lancedb: stars('stars:lancedb:github', 'lancedb/lancedb', [11_860, 11_866, 11_870, 11_876]),
  graph: stars('stars:lance-graph:github', 'lance-format/lance-graph', [186, 187, 187, 188]),
  context: stars('stars:lance-context:github', 'lance-format/lance-context', [87, 88, 88, 89]),
}

export const TOTAL_STARS = 19_432
export const TOTAL_STARS_SPARKLINE = daily('2026-11-06', [19_403, 19_413, 19_420, 19_432])

export const INITIAL_WINDOW: DownloadWindowTotals = {
  window_start: '2026-10-12',
  window_end: '2026-11-10',
  lance: 1_234_567,
  lancedb: 2_345_678,
}

/** Totals the mocked API returns for any other window: 1,000/day Lance, 2,000/day LanceDB. */
export function totalsForWindow(windowStart: string, windowEnd: string): DownloadWindowTotals {
  if (windowStart === INITIAL_WINDOW.window_start && windowEnd === INITIAL_WINDOW.window_end) {
    return INITIAL_WINDOW
  }
  const days =
    (Date.parse(`${windowEnd}T00:00:00Z`) - Date.parse(`${windowStart}T00:00:00Z`)) / 86_400_000 + 1
  return { window_start: windowStart, window_end: windowEnd, lance: days * 1_000, lancedb: days * 2_000 }
}

function snapshot(periodEnd: string, lp: number, lr: number, dp: number, dn: number, dr: number) {
  return {
    period_end: periodEnd,
    lance_python: lp,
    lance_rust: lr,
    lancedb_python: dp,
    lancedb_nodejs: dn,
    lancedb_rust: dr,
    python: lp + dp,
    nodejs: dn,
    rust: lr + dr,
    lance: lp + lr,
    lancedb: dp + dn + dr,
    total: lp + lr + dp + dn + dr,
  }
}

export const SNAPSHOTS = [
  snapshot('2026-06-30', 1_000_000, 200_000, 3_000_000, 400_000, 100_000),
  snapshot('2026-07-31', 1_100_000, 210_000, 3_200_000, 410_000, 110_000),
  snapshot('2026-08-31', 1_200_000, 220_000, 3_400_000, 420_000, 120_000),
  snapshot('2026-09-30', 1_300_000, 230_000, 3_600_000, 430_000, 130_000),
  snapshot('2026-10-31', 1_400_000, 240_000, 3_800_000, 440_000, 140_000),
  snapshot('2026-11-10', 1_450_000, 245_000, 3_900_000, 445_000, 145_000),
]

export const DUCKDB = [
  {
    month_start: '2026-09-01',
    month_label: '2026-09',
    core_downloads: 53_473,
    community_downloads: 10_310,
    total_downloads: 63_783,
    is_partial_month: false,
    latest_source_update_at: '2026-09-27T04:00:16Z',
  },
  {
    month_start: '2026-10-01',
    month_label: '2026-10',
    core_downloads: 96_147,
    community_downloads: 6_441,
    total_downloads: 102_588,
    is_partial_month: false,
    latest_source_update_at: '2026-10-25T04:00:00Z',
  },
  {
    month_start: '2026-11-01',
    month_label: '2026-11',
    core_downloads: 64_176,
    community_downloads: 4_271,
    total_downloads: 68_447,
    is_partial_month: true,
    latest_source_update_at: '2026-11-08T04:00:23Z',
  },
]

function rollup(
  metricId: string,
  window: string,
  currentValue: number,
  percentChange: number,
  family = 'downloads',
): DashboardMetricRollup {
  const [, product, sdk] = metricId.split(':')
  return {
    rollup_id: `${metricId}:${window}`,
    metric_id: metricId,
    metric_family: family,
    product,
    sdk,
    subject: product,
    window,
    window_start: '2026-11-04',
    window_end: '2026-11-10',
    current_value: currentValue,
    previous_value: currentValue,
    delta: 0,
    percent_change: percentChange,
    sdk_share: 0,
    previous_sdk_share: 0,
    sdk_share_delta: 0,
    trend_slope: 0,
    updated_at: '2026-11-11T03:27:32Z',
  }
}

// Movers show the 6 largest |percent_change| among 7d download rollups.
export const ROLLUPS = [
  rollup('downloads:lancedb:python', '7d', 1_900_000, 12.4),
  rollup('downloads:lancedb:nodejs', '7d', 22_000, -31.6),
  rollup('downloads:lancedb:rust', '7d', 120_000, 4.2),
  rollup('downloads:lance:python', '7d', 1_100_000, 48.9),
  rollup('downloads:lance:rust', '7d', 180_000, -2.1),
  rollup('downloads:lance:nodejs', '7d', 5_000, 0.4), // 7th-largest: must be left out
  rollup('downloads:lancedb:java', '7d', 9_000, 7.7),
  rollup('downloads:lance:python', '30d', 4_000_000, 99.0), // not 7d: left out
  rollup('stars:lance:github', '7d', 7_279, 88.0, 'stars'), // not downloads: left out
]

export const SIGNALS = [
  {
    signal_id: 'sustained_growth:2026-08-13:2026-11-10:lance-python-shows-sustained-growth',
    signal_type: 'sustained_growth',
    detected_at: '2026-11-11T03:27:32Z',
    window_start: '2026-08-13',
    window_end: '2026-11-10',
    title: 'Lance Python shows sustained growth',
    summary: 'Lance Python downloads rose for 3 straight windows.',
    related_metrics: ['downloads:lance:python'],
    evidence_ids: ['hn:111'],
    score: 72,
    confidence: 'medium',
    suggested_action: 'docs_content',
  },
  {
    signal_id: 'sustained_growth:2026-08-13:2026-11-10:lancedb-rust-shows-sustained-growth',
    signal_type: 'sustained_growth',
    detected_at: '2026-11-11T03:27:32Z',
    window_start: '2026-08-13',
    window_end: '2026-11-10',
    title: 'LanceDB Rust shows sustained growth',
    summary: 'LanceDB Rust downloads are up versus the prior 30 days.',
    related_metrics: ['downloads:lancedb:rust'],
    evidence_ids: [],
    score: 55,
    confidence: 'low',
    suggested_action: 'watch',
  },
]

export const GUIDANCE = [
  {
    guidance_id: 'g-1',
    signal_id: SIGNALS[0].signal_id,
    generated_at: '2026-11-10T11:30:00Z',
    model: 'test-model',
    reasoning_effort: 'high',
    prompt_version: 'v2',
    analysis_window_start: '2026-11-04',
    analysis_window_end: '2026-11-10',
    comparison_windows: ['15d', '30d'],
    executive_summary: 'Lance Python growth is real and broad-based.',
    movement_assessment: 'sustained',
    why_it_matters: 'More Python users are adopting the Lance format directly.',
    likely_community: 'vector-database',
    recommended_next_steps: ['Publish a pylance quickstart', 'Track the HN thread'],
    engineering_relevance: 'watch',
    confidence: 'medium',
    citations: [
      { source_type: 'evidence', source_id: 'hn:222', fact: 'An HN thread compared Lance to Parquet.', used_for: 'community' },
    ],
  },
]

export const EVIDENCE = [
  {
    evidence_id: 'hn:111',
    source_type: 'hn',
    source_name: 'Hacker News',
    observed_at: '2026-11-09T10:00:00Z',
    occurred_at: '2026-11-09T09:00:00Z',
    title: 'Show HN: Lance v2 file format',
    url: 'https://news.ycombinator.com/item?id=111',
    snippet: 'We moved our training data to Lance.',
    matched_terms: ['lance'],
    related_metrics: [],
    related_packages: [],
    related_repos: [],
    communities: [],
    evidence_strength: 'strong',
  },
  {
    evidence_id: 'hn:222',
    source_type: 'hn',
    source_name: 'Hacker News',
    observed_at: '2026-11-08T10:00:00Z',
    occurred_at: '2026-11-08T09:00:00Z',
    title: 'Lance vs Parquet for ML',
    url: 'https://news.ycombinator.com/item?id=222',
    snippet: 'Random access is much faster with Lance.',
    matched_terms: ['lance'],
    related_metrics: [],
    related_packages: [],
    related_repos: [],
    communities: [],
    evidence_strength: 'medium',
  },
  {
    evidence_id: 'hn:333',
    source_type: 'hn',
    source_name: 'Hacker News',
    observed_at: '2026-11-07T10:00:00Z',
    occurred_at: '2026-11-07T09:00:00Z',
    title: 'Unrelated thread',
    url: 'https://news.ycombinator.com/item?id=333',
    snippet: 'Not linked to any signal.',
    matched_terms: ['lancedb'],
    related_metrics: [],
    related_packages: [],
    related_repos: [],
    communities: [],
    evidence_strength: 'weak',
  },
]

export function dashboardFixture(): DashboardResponse {
  return {
    generated_at: '2026-11-11T03:27:32Z',
    days: 730,
    // Deliberately out of display order: the UI sorts cards itself.
    groups: [
      {
        product: 'lance',
        title: 'Lance',
        items: [STARS.context, LANCE_DOWNLOADS.rust, STARS.graph, LANCE_DOWNLOADS.python, STARS.lance],
      },
      {
        product: 'lancedb',
        title: 'LanceDB',
        items: [LANCEDB_DOWNLOADS.rust, STARS.lancedb, LANCEDB_DOWNLOADS.python, LANCEDB_DOWNLOADS.nodejs],
      },
    ],
    total_stars: TOTAL_STARS,
    total_stars_sparkline: TOTAL_STARS_SPARKLINE,
    last_30d_download_totals: INITIAL_WINDOW,
    monthly_download_snapshots: SNAPSHOTS,
    duckdb_lance_extension_downloads: DUCKDB,
    metric_rollups: ROLLUPS,
    recent_evidence: EVIDENCE,
    signal_candidates: SIGNALS,
    signal_guidance: GUIDANCE,
  }
}
