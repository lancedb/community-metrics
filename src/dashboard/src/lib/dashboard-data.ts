import * as lancedb from '@lancedb/lancedb'

import type { DashboardMetric, DashboardResponse, SparkPoint } from '@/types'

const ENTERPRISE_URI = 'db://community-metrics'
const DEFAULT_DAYS = 180
const MAX_DAYS = 730
const DOWNLOAD_SNAPSHOT_CUTOFF = '2025-11-30'
const DOWNLOAD_DAILY_START = '2025-12-01'
const STAR_METRIC_IDS = new Set(['stars:lance:github', 'stars:lancedb:github'])

type Row = Record<string, unknown>

type MetricDef = {
  metric_id: string
  metric_family: string
  product: string
  subject: string
  sdk: string | null
  display_name: string
}

function requireEnv(name: string): string {
  const value = (process.env[name] ?? '').trim()
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`)
  }
  return value
}

function normalizeDays(raw: number | null | undefined): number {
  if (!raw || Number.isNaN(raw)) return DEFAULT_DAYS
  return Math.max(1, Math.min(MAX_DAYS, Math.trunc(raw)))
}

function toIsoDay(value: Date): string {
  return value.toISOString().slice(0, 10)
}

function parseIsoDay(value: unknown): Date {
  const raw = String(value ?? '').slice(0, 10)
  return new Date(`${raw}T00:00:00Z`)
}

function dayKey(value: unknown): string {
  if (value instanceof Date) {
    return toIsoDay(value)
  }
  if (value !== null && typeof value === 'object') {
    const candidate = value as { valueOf?: () => unknown }
    if (typeof candidate.valueOf === 'function') {
      const primitive = candidate.valueOf()
      if (primitive !== value) {
        return dayKey(primitive)
      }
    }
  }
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
      return trimmed.slice(0, 10)
    }
    const parsed = new Date(trimmed)
    if (!Number.isNaN(parsed.getTime())) {
      return toIsoDay(parsed)
    }
    return trimmed.slice(0, 10)
  }
  if (typeof value === 'number') {
    const parsed = new Date(value)
    if (!Number.isNaN(parsed.getTime())) {
      return toIsoDay(parsed)
    }
  }
  const text = String(value ?? '').trim()
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
    return text.slice(0, 10)
  }
  const parsed = new Date(text)
  if (!Number.isNaN(parsed.getTime())) {
    return toIsoDay(parsed)
  }
  return text.slice(0, 10)
}

function latestCompletedDay(): Date {
  const now = new Date()
  const utcToday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()))
  utcToday.setUTCDate(utcToday.getUTCDate() - 1)
  return utcToday
}

function shiftDays(day: Date, amount: number): Date {
  const out = new Date(day)
  out.setUTCDate(out.getUTCDate() + amount)
  return out
}

function sqlQuote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`
}

function coerceNullableString(value: unknown): string | null {
  if (value === null || value === undefined) return null
  const text = String(value)
  return text.length > 0 ? text : null
}

function coerceNumber(value: unknown): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0
  if (typeof value === 'bigint') {
    return Number(value)
  }
  if (typeof value === 'string') {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : 0
  }
  if (value !== null && value !== undefined) {
    if (typeof value === 'object') {
      const record = value as Record<string, unknown>
      if (typeof record.toNumber === 'function') {
        const converted = (record.toNumber as () => unknown)()
        return coerceNumber(converted)
      }
      if (typeof record.valueOf === 'function') {
        const primitive = (record.valueOf as () => unknown)()
        if (primitive !== value) {
          return coerceNumber(primitive)
        }
      }
      if ('value' in record) {
        return coerceNumber(record.value)
      }
      if (typeof record.low === 'number' && typeof record.high === 'number') {
        const low = BigInt(record.low >>> 0)
        const high = BigInt(record.high >>> 0)
        const combined = high * BigInt(2 ** 32) + low
        return Number(combined)
      }
    }
    const parsed = Number(String(value))
    return Number.isFinite(parsed) ? parsed : 0
  }
  return 0
}

function rowsByMetric(rows: Row[]): Map<string, Row[]> {
  const grouped = new Map<string, Row[]>()
  for (const row of rows) {
    const metricId = String(row.metric_id ?? '')
    if (!metricId) continue
    const current = grouped.get(metricId)
    if (current) {
      current.push(row)
    } else {
      grouped.set(metricId, [row])
    }
  }
  return grouped
}

function toSparkline(rows: Row[], days: number): SparkPoint[] {
  if (rows.length === 0) return []
  const daily = interpolateDaily(rows)
  const tail = daily.slice(-days)
  return tail.map((row) => ({
    period_start: row.period_start,
    period_end: row.period_end,
    value: row.value,
  }))
}

function interpolateDaily(rows: Row[]): Array<{ period_start: string; period_end: string; value: number }> {
  if (rows.length === 0) return []

  const byDay = new Map<string, number>()
  for (const row of [...rows].sort((a, b) => String(a.period_end).localeCompare(String(b.period_end)))) {
    const day = dayKey(row.period_end)
    byDay.set(day, Math.trunc(coerceNumber(row.value)))
  }

  const knownDays = [...byDay.keys()].sort()
  if (knownDays.length === 0) return []

  const allDays: string[] = []
  let cursor = parseIsoDay(knownDays[0])
  const end = parseIsoDay(knownDays[knownDays.length - 1])
  while (cursor <= end) {
    allDays.push(toIsoDay(cursor))
    cursor = shiftDays(cursor, 1)
  }

  return allDays.map((day) => {
    const value = interpolateValue(day, knownDays, byDay)
    return { period_start: day, period_end: day, value }
  })
}

function interpolateValue(targetDay: string, knownDays: string[], knownMap: Map<string, number>): number {
  const exact = knownMap.get(targetDay)
  if (exact !== undefined) return exact

  let right = knownDays.findIndex((day) => day >= targetDay)
  if (right < 0) right = knownDays.length
  if (right <= 0) return knownMap.get(knownDays[0]) ?? 0
  if (right >= knownDays.length) return knownMap.get(knownDays[knownDays.length - 1]) ?? 0

  const leftDay = knownDays[right - 1]
  const rightDay = knownDays[right]
  const leftVal = knownMap.get(leftDay) ?? 0
  const rightVal = knownMap.get(rightDay) ?? leftVal

  const left = parseIsoDay(leftDay)
  const rightDate = parseIsoDay(rightDay)
  const target = parseIsoDay(targetDay)

  const span = Math.max(1, Math.round((rightDate.getTime() - left.getTime()) / 86_400_000))
  const offset = Math.round((target.getTime() - left.getTime()) / 86_400_000)
  const ratio = offset / span

  return Math.round(leftVal + (rightVal - leftVal) * ratio)
}

function monthlyDownloadSparkline(rows: Row[], days: number): SparkPoint[] {
  if (rows.length === 0) return []

  const latest = [...rows].reduce((acc, row) => {
    const day = dayKey(row.period_end)
    return day > acc ? day : acc
  }, '0000-00-00')

  const latestDay = parseIsoDay(latest)
  const windowStart = toIsoDay(shiftDays(latestDay, -(days - 1)))

  const snapshotByDay = new Map<string, number>()
  const monthlyBuckets = new Map<string, { period_start: string; period_end: string; value: number }>()

  for (const row of rows) {
    const periodEnd = dayKey(row.period_end)
    const sourceWindow = String(row.source_window ?? '')

    if (
      sourceWindow === 'discrete_snapshot' &&
      periodEnd >= windowStart &&
      periodEnd <= DOWNLOAD_SNAPSHOT_CUTOFF
    ) {
      snapshotByDay.set(periodEnd, Math.trunc(coerceNumber(row.value)))
      continue
    }

    if (
      sourceWindow === '1d' &&
      periodEnd >= DOWNLOAD_DAILY_START &&
      periodEnd >= windowStart &&
      periodEnd <= latest
    ) {
      const day = parseIsoDay(periodEnd)
      const key = `${day.getUTCFullYear()}-${String(day.getUTCMonth() + 1).padStart(2, '0')}`
      const current = monthlyBuckets.get(key)
      if (!current) {
        monthlyBuckets.set(key, {
          period_start: periodEnd,
          period_end: periodEnd,
          value: Math.trunc(coerceNumber(row.value)),
        })
      } else {
        current.period_start = periodEnd < current.period_start ? periodEnd : current.period_start
        current.period_end = periodEnd > current.period_end ? periodEnd : current.period_end
        current.value += Math.trunc(coerceNumber(row.value))
      }
    }
  }

  const snapshotPoints: SparkPoint[] = [...snapshotByDay.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([day, value]) => ({ period_start: day, period_end: day, value }))

  const monthlyPoints: SparkPoint[] = [...monthlyBuckets.values()]
    .sort((a, b) => a.period_end.localeCompare(b.period_end))
    .map((bucket) => ({
      period_start: bucket.period_start,
      period_end: bucket.period_end,
      value: bucket.value,
    }))

  return [...snapshotPoints, ...monthlyPoints].sort((a, b) => a.period_end.localeCompare(b.period_end))
}

function lastFullMonthValue(points: SparkPoint[], referenceDay: Date): { value: number | null; period_end: string | null } {
  const month = referenceDay.getUTCMonth()
  const year = referenceDay.getUTCFullYear()
  const target = month === 0 ? { year: year - 1, month: 12 } : { year, month }

  let candidate: SparkPoint | null = null
  for (const point of points) {
    const day = parseIsoDay(point.period_end)
    const pointMonth = day.getUTCMonth() + 1
    const pointYear = day.getUTCFullYear()
    if (pointYear !== target.year || pointMonth !== target.month) {
      continue
    }
    if (!candidate || point.period_end > candidate.period_end) {
      candidate = point
    }
  }

  return {
    value: candidate ? candidate.value : null,
    period_end: candidate ? candidate.period_end : null,
  }
}

function overlapDays(aStart: string, aEnd: string, bStart: string, bEnd: string): number {
  const start = Math.max(parseIsoDay(aStart).getTime(), parseIsoDay(bStart).getTime())
  const end = Math.min(parseIsoDay(aEnd).getTime(), parseIsoDay(bEnd).getTime())
  if (end < start) return 0
  return Math.floor((end - start) / 86_400_000) + 1
}

function last30dDownloadTotals(rows: Row[]): DashboardResponse['last_30d_download_totals'] {
  const windowEnd = latestCompletedDay()
  const windowEndIso = toIsoDay(windowEnd)
  const windowStartIso = toIsoDay(shiftDays(windowEnd, -29))

  const totals: Record<'lance' | 'lancedb', number> = { lance: 0, lancedb: 0 }

  for (const row of rows) {
    const metricId = String(row.metric_id ?? '')
    if (!metricId.startsWith('downloads:')) continue
    const parts = metricId.split(':')
    if (parts.length < 3) continue
    const product = parts[1]
    if (product !== 'lance' && product !== 'lancedb') continue

    const periodStart = String(row.period_start).slice(0, 10)
    const periodEnd = dayKey(row.period_end)
    const normalizedPeriodStart = dayKey(periodStart)
    const overlap = overlapDays(normalizedPeriodStart, periodEnd, windowStartIso, windowEndIso)
    if (overlap <= 0) continue
    const span = overlapDays(
      normalizedPeriodStart,
      periodEnd,
      normalizedPeriodStart,
      periodEnd,
    )
    if (span <= 0) continue

    totals[product] += coerceNumber(row.value) * (overlap / span)
  }

  return {
    window_start: windowStartIso,
    window_end: windowEndIso,
    lance: Math.round(totals.lance),
    lancedb: Math.round(totals.lancedb),
  }
}

function totalStars(statsRows: Row[], days: number): { total: number | null; sparkline: SparkPoint[] } {
  const perMetric = new Map<string, Map<string, number>>()

  for (const metricId of STAR_METRIC_IDS) {
    const rows = statsRows.filter((row) => String(row.metric_id ?? '') === metricId)
    const daily = interpolateDaily(rows)
    if (daily.length === 0) continue
    perMetric.set(
      metricId,
      new Map(daily.map((row) => [row.period_end, row.value])),
    )
  }

  const allDays = new Set<string>()
  for (const dailyMap of perMetric.values()) {
    for (const day of dailyMap.keys()) {
      allDays.add(day)
    }
  }
  if (allDays.size === 0) {
    return { total: null, sparkline: [] }
  }

  const sortedDays = [...allDays].sort()

  const totals: SparkPoint[] = sortedDays.map((day) => {
    let value = 0
    for (const [metricId, map] of perMetric.entries()) {
      const knownDays = [...map.keys()].sort()
      value += interpolateValue(day, knownDays, map)
      if (!STAR_METRIC_IDS.has(metricId)) {
        value += 0
      }
    }
    return { period_start: day, period_end: day, value }
  })

  const tail = totals.slice(-days)
  return {
    total: tail.length > 0 ? tail[tail.length - 1].value : null,
    sparkline: tail,
  }
}

async function queryRows(table: any, options: { where?: string; columns?: string[]; limit?: number }): Promise<Row[]> {
  let builder = typeof table.query === 'function' ? table.query() : table.search()
  if (options.where) {
    builder = builder.where(options.where)
  }
  if (options.columns) {
    builder = builder.select(options.columns)
  }
  if (options.limit) {
    builder = builder.limit(options.limit)
  }
  const rows = await builder.toArray()
  return rows as Row[]
}

async function fetchMetricsAndStats(days: number): Promise<{ metricsRows: MetricDef[]; statsRows: Row[]; latestDay: Date }> {
  const apiKey = requireEnv('LANCEDB_API_KEY')
  const hostOverride = requireEnv('LANCEDB_HOST_OVERRIDE')
  const region = (process.env.LANCEDB_REGION ?? 'us-east-1').trim() || 'us-east-1'

  const db = await lancedb.connect(ENTERPRISE_URI, {
    apiKey,
    hostOverride,
    region,
  })

  const latestDay = latestCompletedDay()
  const statsStartIso = toIsoDay(shiftDays(latestDay, -(days - 1)))
  const statsEndIso = toIsoDay(latestDay)

  const metricsTable = await db.openTable('metrics')
  const metricsRows = (await queryRows(metricsTable, {
    columns: [
      'metric_id',
      'metric_family',
      'product',
      'subject',
      'sdk',
      'display_name',
      'is_active',
    ],
    where: 'is_active = true',
    limit: 200,
  })) as MetricDef[]

  if (metricsRows.length === 0) {
    return { metricsRows: [], statsRows: [], latestDay }
  }

  const metricIds = metricsRows.map((row) => row.metric_id)
  const idsClause = metricIds.map(sqlQuote).join(', ')
  const where =
    `metric_id IN (${idsClause}) AND period_end >= ${sqlQuote(statsStartIso)} ` +
    `AND period_end <= ${sqlQuote(statsEndIso)}`

  const statsTable = await db.openTable('stats')
  const statsRows = await queryRows(statsTable, {
    columns: [
      'metric_id',
      'period_start',
      'period_end',
      'value',
      'provenance',
      'source_window',
    ],
    where,
    limit: Math.max(5000, metricIds.length * (days + 31)),
  })

  return { metricsRows, statsRows, latestDay }
}

export async function buildDashboardData(rawDays: number | null | undefined): Promise<DashboardResponse> {
  const days = normalizeDays(rawDays)
  const { metricsRows, statsRows, latestDay } = await fetchMetricsAndStats(days)

  if (metricsRows.length === 0) {
    return {
      generated_at: new Date().toISOString(),
      days,
      groups: [],
      total_stars: null,
      total_stars_sparkline: [],
      last_30d_download_totals: {
        window_start: toIsoDay(shiftDays(latestDay, -29)),
        window_end: toIsoDay(latestDay),
        lance: 0,
        lancedb: 0,
      },
    }
  }

  const groupedStats = rowsByMetric(statsRows)

  const groups = ['lance', 'lancedb']
    .map((product) => {
      const metricDefs = metricsRows
        .filter((row) => String(row.product) === product)
        .sort((a, b) => {
          return (
            String(a.metric_family).localeCompare(String(b.metric_family)) ||
            String(a.display_name).localeCompare(String(b.display_name)) ||
            String(a.metric_id).localeCompare(String(b.metric_id))
          )
        })

      if (metricDefs.length === 0) return null

      const items: DashboardMetric[] = metricDefs.map((metric) => {
        const metricStats = groupedStats.get(metric.metric_id) ?? []

        const sparkline =
          metric.metric_family === 'downloads'
            ? monthlyDownloadSparkline(metricStats, days)
            : toSparkline(metricStats, days)

        let latestValue: number | null = null
        let latestPeriodEnd: string | null = null

        if (metric.metric_family === 'downloads') {
          const monthly = lastFullMonthValue(sparkline, latestDay)
          latestValue = monthly.value
          latestPeriodEnd = monthly.period_end
        } else if (sparkline.length > 0) {
          const latest = sparkline[sparkline.length - 1]
          latestValue = latest.value
          latestPeriodEnd = latest.period_end
        }

        let latestProvenance: string | null = null
        if (metricStats.length > 0) {
          const latestRow = [...metricStats]
            .sort((a, b) => dayKey(a.period_end).localeCompare(dayKey(b.period_end)))
            .at(-1)
          latestProvenance = coerceNullableString(latestRow?.provenance ?? null)
        }

        return {
          metric_id: metric.metric_id,
          display_name: String(metric.display_name),
          metric_family: String(metric.metric_family),
          sdk: coerceNullableString(metric.sdk),
          subject: String(metric.subject),
          latest_value: latestValue,
          latest_period_end: latestPeriodEnd,
          latest_provenance: latestProvenance,
          total_stars: null,
          sparkline,
        }
      })

      return {
        product,
        title: product === 'lance' ? 'Lance' : 'LanceDB',
        items,
      }
    })
    .filter((group): group is NonNullable<typeof group> => group !== null)

  const stars = totalStars(statsRows, days)

  for (const group of groups) {
    for (const item of group.items) {
      if (item.metric_family === 'stars') {
        item.total_stars = stars.total
      }
    }
  }

  return {
    generated_at: new Date().toISOString(),
    days,
    groups,
    total_stars: stars.total,
    total_stars_sparkline: stars.sparkline,
    last_30d_download_totals: last30dDownloadTotals(statsRows),
  }
}
