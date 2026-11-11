/**
 * UI contract tests for the dashboard: which sections, metrics and graphs it shows, and
 * the numbers in them. They check content and behaviour, not pixels, so a redesign must
 * keep them passing. Conventions the tests rely on:
 *
 * - Each section is a <section> with a heading carrying its title.
 * - Each metric card is an <article>.
 * - Each chart has a hover surface marked data-testid="chart" and a screen-reader table
 *   (components/ChartDataTable.tsx) listing exactly the points it plots.
 */
import { expect, test, type Locator, type Page } from '@playwright/test'

import type { DashboardMetric, DashboardResponse, SparkPoint } from '../src/types'
import {
  DUCKDB,
  EVIDENCE,
  FIXED_NOW,
  GUIDANCE,
  INITIAL_WINDOW,
  LANCE_DOWNLOADS,
  LANCEDB_DOWNLOADS,
  SIGNALS,
  SNAPSHOTS,
  STARS,
  TOTAL_STARS,
  TOTAL_STARS_SPARKLINE,
  dashboardFixture,
  totalsForWindow,
} from './fixtures/dashboard'

const fmt = (value: number) => value.toLocaleString('en-US')

type MockOptions = {
  dashboard?: DashboardResponse
  failStatus?: number
}

async function mockApi(page: Page, options: MockOptions = {}) {
  const windowRequests: URLSearchParams[] = []
  await page.route('**/api/v1/dashboard/daily**', async (route) => {
    const params = new URL(route.request().url()).searchParams
    if (options.failStatus) {
      await route.fulfill({ status: options.failStatus, json: { detail: 'boom' } })
      return
    }
    if (params.get('response') === 'download_window_totals') {
      windowRequests.push(params)
      await route.fulfill({
        json: totalsForWindow(params.get('window_start') ?? '', params.get('window_end') ?? ''),
      })
      return
    }
    await route.fulfill({ json: options.dashboard ?? dashboardFixture() })
  })
  return { windowRequests }
}

async function openDashboard(page: Page, options: MockOptions = {}) {
  await page.clock.setFixedTime(FIXED_NOW)
  const mocks = await mockApi(page, options)
  await page.goto('/')
  return mocks
}

function section(page: Page, title: string): Locator {
  return page.locator('section').filter({ has: page.getByRole('heading', { name: title, exact: true }) })
}

async function chartTableRows(scope: Locator, caption: string): Promise<string[][]> {
  const table = scope.locator(`table[data-chart-table="${caption}"]`)
  await expect(table).toHaveCount(1)
  return table.locator('tbody tr').evaluateAll((rows) =>
    rows.map((row) => Array.from(row.querySelectorAll('td')).map((cell) => cell.textContent ?? '')),
  )
}

function sparkRows(points: SparkPoint[]): string[][] {
  return points.map((point) => [point.period_end, fmt(point.value)])
}

/** Visible text of a scope, ignoring the screen-reader chart tables. */
async function visibleText(scope: Locator): Promise<string> {
  return scope.evaluate((element) => {
    const clone = element.cloneNode(true) as HTMLElement
    clone.querySelectorAll('[data-chart-table]').forEach((table) => table.remove())
    return clone.textContent ?? ''
  })
}

function occurrences(text: string, needle: string): number {
  return text.split(needle).length - 1
}

/** Hovering the middle of a chart must show one of the values it plots. */
async function expectHoverShowsAPlottedValue(scope: Locator, values: number[]) {
  const before = await visibleText(scope)
  const surface = scope.getByTestId('chart').first()
  const box = await surface.boundingBox()
  expect(box).not.toBeNull()
  await surface.hover({ position: { x: box!.width / 2, y: box!.height / 2 } })
  await expect
    .poll(async () => {
      const after = await visibleText(scope)
      return values.some((value) => occurrences(after, fmt(value)) > occurrences(before, fmt(value)))
    })
    .toBe(true)
}

async function expectCards(
  scope: Locator,
  expected: Array<{ title: string; subtitle: string; value: number; metric: DashboardMetric | null; table: string }>,
) {
  const cards = scope.locator('article')
  await expect(cards).toHaveCount(expected.length)
  for (const [index, card] of expected.entries()) {
    const article = cards.nth(index)
    const text = await visibleText(article)
    expect(text, `card ${index + 1} title`).toContain(card.title)
    expect(text, `${card.title} subtitle`).toContain(card.subtitle)
    expect(text, `${card.title} value`).toContain(fmt(card.value))
    const points = card.metric ? card.metric.sparkline : TOTAL_STARS_SPARKLINE
    expect(await chartTableRows(article, card.table), `${card.title} chart points`).toEqual(sparkRows(points))
  }
}

test.describe('charts tab', () => {
  test('header, tabs and refresh time', async ({ page }) => {
    await openDashboard(page)
    await expect(page.getByRole('heading', { level: 1, name: 'LanceDB Community Metrics' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Charts & Downloads' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Insights' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Sign out' })).toBeVisible()
    await expect(page.getByText(/Last refreshed/)).toBeVisible()
  })

  test('page ends at the footer (hidden chart tables add no height)', async ({ page }) => {
    // Production star history is ~2 years of daily points; a hidden table that long
    // must not stretch the page.
    const dashboard = dashboardFixture()
    const start = Date.UTC(2024, 10, 10)
    dashboard.total_stars_sparkline = Array.from({ length: 730 }, (_, day) => {
      const date = new Date(start + day * 86_400_000).toISOString().slice(0, 10)
      return { period_start: date, period_end: date, value: 10_000 + day * 12 }
    })
    await openDashboard(page, { dashboard })
    await expect(section(page, 'Star Histories')).toBeVisible()
    const { footerBottom, pageHeight } = await page.evaluate(() => ({
      footerBottom: document.querySelector('footer')!.getBoundingClientRect().bottom + window.scrollY,
      pageHeight: document.documentElement.scrollHeight,
    }))
    expect(pageHeight).toBeLessThanOrEqual(Math.ceil(footerBottom) + 1)
  })

  test('sections appear in order', async ({ page }) => {
    await openDashboard(page)
    const titles = [
      'Downloads in Selected Window',
      'Monthly Download Snapshots',
      'LanceDB Download Stats',
      'Lance Download Stats',
      'Lance-DuckDB Extension Downloads',
      'Star Histories',
    ]
    await expect(section(page, titles[0])).toBeVisible()
    const headings = await page.locator('main section h2').allTextContents()
    expect(headings).toEqual(titles)
  })

  test('download window totals, date change and reset', async ({ page }) => {
    const { windowRequests } = await openDashboard(page)
    const panel = section(page, 'Downloads in Selected Window')
    const table = panel.locator('table:not([data-chart-table])')

    const readTotals = async () => ({
      headers: await table.locator('thead th').allTextContents(),
      values: await table.locator('tbody td').allTextContents(),
    })

    await expect(panel).toContainText('Window: Oct 12, 2026 to Nov 10, 2026')
    await expect.poll(readTotals).toEqual({
      headers: ['LanceDB', 'Lance', 'Total'],
      values: [fmt(INITIAL_WINDOW.lancedb), fmt(INITIAL_WINDOW.lance), fmt(INITIAL_WINDOW.lancedb + INITIAL_WINDOW.lance)],
    })

    await panel.getByLabel('Start date').fill('2026-11-01')
    await expect(panel).toContainText('Window: Nov 1, 2026 to Nov 10, 2026')
    await expect.poll(readTotals).toEqual({ headers: ['LanceDB', 'Lance', 'Total'], values: ['20,000', '10,000', '30,000'] })
    expect(windowRequests.at(-1)?.get('window_start')).toBe('2026-11-01')
    expect(windowRequests.at(-1)?.get('window_end')).toBe('2026-11-10')

    await panel.getByRole('button', { name: 'Reset to last 30 days' }).click()
    await expect(panel).toContainText('Window: Oct 12, 2026 to Nov 10, 2026')
    await expect.poll(readTotals).toEqual({
      headers: ['LanceDB', 'Lance', 'Total'],
      values: [fmt(INITIAL_WINDOW.lancedb), fmt(INITIAL_WINDOW.lance), fmt(INITIAL_WINDOW.lancedb + INITIAL_WINDOW.lance)],
    })
  })

  test('date inputs only allow the last 90 days', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'Downloads in Selected Window')
    for (const label of ['Start date', 'End date']) {
      await expect(panel.getByLabel(label)).toHaveAttribute('min', '2026-08-12')
      await expect(panel.getByLabel(label)).toHaveAttribute('max', '2026-11-10')
    }
  })

  test('monthly download snapshot chart', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'Monthly Download Snapshots')
    const latest = SNAPSHOTS[SNAPSHOTS.length - 1]

    await expect(panel).toContainText('through Nov 10')
    await expect(panel).toContainText('Latest total')
    await expect(panel).toContainText(fmt(latest.total))
    expect(await chartTableRows(panel, 'Monthly download snapshots')).toEqual(
      SNAPSHOTS.map((point) => [point.period_end, fmt(point.total)]),
    )
  })

  test('monthly snapshot CSV export has every column', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'Monthly Download Snapshots')
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      panel.getByRole('button', { name: 'Export snapshot' }).click(),
    ])
    expect(download.suggestedFilename()).toBe('monthly-download-snapshots.csv')
    const stream = await download.createReadStream()
    const chunks: Buffer[] = []
    for await (const chunk of stream) chunks.push(chunk as Buffer)
    const expected = [
      'period_end,lance_python,lance_rust,lancedb_python,lancedb_nodejs,lancedb_rust,python_total,nodejs_total,rust_total,lance_total,lancedb_total,total',
      ...SNAPSHOTS.map((p) =>
        [p.period_end, p.lance_python, p.lance_rust, p.lancedb_python, p.lancedb_nodejs, p.lancedb_rust, p.python, p.nodejs, p.rust, p.lance, p.lancedb, p.total].join(','),
      ),
    ].join('\n')
    expect(Buffer.concat(chunks).toString('utf8')).toBe(`${expected}\n`)
  })

  test('LanceDB download cards: Python, NodeJS, Rust with last full month totals', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'LanceDB Download Stats')
    await expectCards(panel, [
      { title: 'Python', subtitle: 'PyPI package lancedb', value: 8_123_456, metric: LANCEDB_DOWNLOADS.python, table: 'Python downloads by month' },
      { title: 'NodeJS', subtitle: 'npm package @lancedb/lancedb', value: 104_321, metric: LANCEDB_DOWNLOADS.nodejs, table: 'NodeJS downloads by month' },
      { title: 'Rust', subtitle: 'crates.io crate lancedb', value: 512_345, metric: LANCEDB_DOWNLOADS.rust, table: 'Rust downloads by month' },
    ])
    await expect(panel.locator('article').getByText('Last full month total', { exact: true })).toHaveCount(3)
  })

  test('Lance download cards: Python, Rust with last full month totals', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'Lance Download Stats')
    await expectCards(panel, [
      { title: 'Python', subtitle: 'PyPI package pylance', value: 4_567_890, metric: LANCE_DOWNLOADS.python, table: 'Python downloads by month' },
      { title: 'Rust', subtitle: 'crates.io crate lance', value: 765_432, metric: LANCE_DOWNLOADS.rust, table: 'Rust downloads by month' },
    ])
  })

  test('download card falls back to the latest value without a last-full-month point', async ({ page }) => {
    const dashboard = dashboardFixture()
    const lance = dashboard.groups.find((group) => group.product === 'lance')!
    lance.items = lance.items.map((item) =>
      item.metric_id === 'downloads:lance:rust'
        ? { ...item, sparkline: item.sparkline.filter((point) => point.period_end !== '2026-10-31') }
        : item,
    )
    await openDashboard(page, { dashboard })
    const card = section(page, 'Lance Download Stats').locator('article').nth(1)
    expect(await visibleText(card)).toContain(fmt(LANCE_DOWNLOADS.rust.latest_value!))
  })

  test('DuckDB extension widget', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'Lance-DuckDB Extension Downloads')
    const latest = DUCKDB[DUCKDB.length - 1]

    await expect(panel).toContainText(/this month/i)
    await expect(panel).toContainText(fmt(latest.total_downloads))
    // Legend names both series top-down, matching the stacked bars (outside the hidden data table).
    expect(await visibleText(panel)).toMatch(/Core[\s\S]*Community/)
    expect(await chartTableRows(panel, 'DuckDB lance extension downloads by month')).toEqual(
      DUCKDB.map((point) => [
        point.month_label,
        fmt(point.community_downloads),
        fmt(point.core_downloads),
        fmt(point.total_downloads),
      ]),
    )
  })

  test('DuckDB widget labels a complete latest month as the last full month', async ({ page }) => {
    const dashboard = dashboardFixture()
    dashboard.duckdb_lance_extension_downloads = DUCKDB.slice(0, 2)
    await openDashboard(page, { dashboard })
    const panel = section(page, 'Lance-DuckDB Extension Downloads')
    await expect(panel).toContainText(/last full month/i)
    await expect(panel).toContainText(fmt(DUCKDB[1].total_downloads))
  })

  test('DuckDB widget is hidden when there is no data', async ({ page }) => {
    const dashboard = dashboardFixture()
    dashboard.duckdb_lance_extension_downloads = []
    await openDashboard(page, { dashboard })
    await expect(section(page, 'Star Histories')).toBeVisible()
    await expect(page.getByRole('heading', { name: 'Lance-DuckDB Extension Downloads' })).toHaveCount(0)
  })

  test('star cards: four repos then the total', async ({ page }) => {
    await openDashboard(page)
    const panel = section(page, 'Star Histories')
    await expectCards(panel, [
      { title: 'Lance', subtitle: 'GitHub repo lance-format/lance', value: 7_279, metric: STARS.lance, table: 'Lance stars over time' },
      { title: 'LanceDB', subtitle: 'GitHub repo lancedb/lancedb', value: 11_876, metric: STARS.lancedb, table: 'LanceDB stars over time' },
      { title: 'Lance Graph', subtitle: 'GitHub repo lance-format/lance-graph', value: 188, metric: STARS.graph, table: 'Lance Graph stars over time' },
      { title: 'Lance Context', subtitle: 'GitHub repo lance-format/lance-context', value: 89, metric: STARS.context, table: 'Lance Context stars over time' },
      { title: 'Total Stars', subtitle: 'Lance and LanceDB family of projects', value: TOTAL_STARS, metric: null, table: 'Total Stars stars over time' },
    ])
  })

  test('hovering each chart shows a plotted value', async ({ page, isMobile }) => {
    test.skip(isMobile, 'hover is a pointer interaction')
    await openDashboard(page)

    await expectHoverShowsAPlottedValue(
      section(page, 'Monthly Download Snapshots'),
      SNAPSHOTS.map((point) => point.total),
    )
    await expectHoverShowsAPlottedValue(
      section(page, 'Lance-DuckDB Extension Downloads'),
      DUCKDB.flatMap((point) => [point.community_downloads, point.core_downloads, point.total_downloads]),
    )
    const cardCharts: Array<[string, number, DashboardMetric]> = [
      ['LanceDB Download Stats', 0, LANCEDB_DOWNLOADS.python],
      ['Lance Download Stats', 1, LANCE_DOWNLOADS.rust],
      ['Star Histories', 1, STARS.lancedb],
    ]
    for (const [title, index, metric] of cardCharts) {
      await expectHoverShowsAPlottedValue(
        section(page, title).locator('article').nth(index),
        metric.sparkline.map((point) => point.value),
      )
    }
  })
})

test.describe('insights tab', () => {
  test('7d download movers: six largest moves, downloads only', async ({ page }) => {
    await openDashboard(page)
    await page.getByRole('button', { name: 'Insights' }).click()
    const panel = section(page, '7d Download Movers')
    const text = await visibleText(panel)

    const expected = [
      ['Lance Python', '1,100,000 downloads', '+49%'],
      ['LanceDB NodeJS', '22,000 downloads', '-32%'],
      ['LanceDB Python', '1,900,000 downloads', '+12%'],
      ['LanceDB Java', '9,000 downloads', '+8%'],
      ['LanceDB Rust', '120,000 downloads', '+4%'],
      ['Lance Rust', '180,000 downloads', '-2%'],
    ]
    let cursor = 0
    for (const [name, value, change] of expected) {
      const at = text.indexOf(name, cursor)
      expect(at, `${name} listed in order`).toBeGreaterThanOrEqual(0)
      expect(text.slice(at)).toContain(value)
      expect(text.slice(at)).toContain(change)
      cursor = at + name.length
    }
    expect(text).not.toContain('Lance NodeJS')
    expect(text).not.toContain('4,000,000')
    expect(text).not.toContain('7,279')
  })

  test('signals with guidance, linked evidence and pending guidance', async ({ page }) => {
    await openDashboard(page)
    await page.getByRole('button', { name: 'Insights' }).click()
    const panel = section(page, 'Insights')
    await expect(panel).toContainText('Guidance generated Nov 10, 2026')

    const signals = panel.locator('article')
    await expect(signals).toHaveCount(2)

    const withGuidance = signals.nth(0)
    const guidance = GUIDANCE[0]
    await expect(withGuidance).toContainText(guidance.executive_summary)
    await expect(withGuidance).toContainText(guidance.why_it_matters)
    for (const step of guidance.recommended_next_steps) await expect(withGuidance).toContainText(step)
    await expect(withGuidance).toContainText(guidance.citations[0].fact)
    await expect(withGuidance).toContainText(`${SIGNALS[0].window_start} to ${SIGNALS[0].window_end}`)
    // Evidence: the signal's own (hn:111) and the guidance's cited (hn:222), never unrelated (hn:333).
    for (const item of EVIDENCE.slice(0, 2)) {
      await expect(withGuidance.getByRole('link', { name: new RegExp(item.title) })).toHaveAttribute('href', item.url)
    }
    await expect(withGuidance).not.toContainText(EVIDENCE[2].title)

    const pending = signals.nth(1)
    await expect(pending).toContainText(SIGNALS[1].title)
    await expect(pending).toContainText(SIGNALS[1].summary)
    await expect(pending).toContainText('LLM guidance has not been generated for this weekly signal yet.')
  })

  test('no signals shows the empty state', async ({ page }) => {
    const dashboard = dashboardFixture()
    dashboard.signal_candidates = []
    dashboard.signal_guidance = []
    await openDashboard(page, { dashboard })
    await page.getByRole('button', { name: 'Insights' }).click()
    const panel = section(page, 'Insights')
    await expect(panel).toContainText('Guidance pending')
    await expect(panel).toContainText('No current signal candidates were generated.')
  })
})

test.describe('page states and layout', () => {
  test('API failure shows the error', async ({ page }) => {
    await openDashboard(page, { failStatus: 500 })
    await expect(page.getByText('Failed to load dashboard: 500')).toBeVisible()
    await expect(page.locator('main section')).toHaveCount(0)
  })

  test('no horizontal page scroll on either tab', async ({ page }) => {
    await openDashboard(page)
    await expect(section(page, 'Star Histories')).toBeVisible()
    const overflow = () => page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)
    expect(await overflow()).toBeLessThanOrEqual(0)
    await page.getByRole('button', { name: 'Insights' }).click()
    await expect(section(page, 'Insights')).toBeVisible()
    expect(await overflow()).toBeLessThanOrEqual(0)
  })
})
