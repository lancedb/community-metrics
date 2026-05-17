import * as lancedb from '@lancedb/lancedb'

import type {
  DashboardEvidenceItem,
  DashboardGuidanceCitation,
  DashboardMetric,
  DashboardMetricRollup,
  DashboardResponse,
  DashboardSignalCandidate,
  DashboardSignalGuidance,
  DownloadSnapshotPoint,
  DownloadWindowTotals,
  SparkPoint,
} from '@/types'

const ENTERPRISE_URI = 'db://community-metrics'
const DEFAULT_DAYS = 730
const MAX_DAYS = 730
const DOWNLOAD_SNAPSHOT_CUTOFF = '2025-11-30'
const DOWNLOAD_DAILY_START = '2025-12-01'
const SYNTHETIC_NOVEMBER_2025 = '2025-11-30'
const DOWNLOAD_SNAPSHOT_MONTH_ENDS = [
  '2024-09-30',
  '2024-10-31',
  '2024-11-30',
  '2024-12-31',
  '2025-01-31',
  '2025-02-28',
  '2025-03-31',
  '2025-04-30',
  '2025-05-31',
  '2025-06-30',
  '2025-07-31',
  '2025-08-31',
  '2025-09-30',
  '2025-10-31',
  '2025-11-30',
  '2025-12-31',
]
const STAR_METRIC_IDS = new Set([
  'stars:lance:github',
  'stars:lancedb:github',
  'stars:lance-graph:github',
  'stars:lance-context:github',
])

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

function monthStartIso(dayLike: unknown): string {
  const day = parseIsoDay(dayLike)
  return `${day.getUTCFullYear()}-${String(day.getUTCMonth() + 1).padStart(2, '0')}-01`
}

function monthEndIso(dayLike: unknown): string {
  const monthStart = parseIsoDay(monthStartIso(dayLike))
  const nextMonthStart = new Date(
    Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth() + 1, 1),
  )
  return toIsoDay(shiftDays(nextMonthStart, -1))
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

function coerceList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).filter(Boolean)
  }
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    if (Array.isArray(record.values)) {
      return record.values.map((item) => String(item)).filter(Boolean)
    }
  }
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return []
    if (trimmed.startsWith('[')) {
      try {
        const parsed = JSON.parse(trimmed)
        return Array.isArray(parsed) ? parsed.map((item) => String(item)).filter(Boolean) : []
      } catch {
        return [trimmed]
      }
    }
    return [trimmed]
  }
  return []
}

function coerceIsoDateTime(value: unknown): string {
  if (value instanceof Date) {
    return value.toISOString()
  }
  if (value !== null && typeof value === 'object') {
    const candidate = value as { valueOf?: () => unknown }
    if (typeof candidate.valueOf === 'function') {
      const primitive = candidate.valueOf()
      if (primitive !== value) {
        return coerceIsoDateTime(primitive)
      }
    }
  }
  const parsed = new Date(String(value ?? ''))
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString()
  }
  return String(value ?? '')
}

function coerceCitations(value: unknown): DashboardGuidanceCitation[] {
  let parsed: unknown
  try {
    parsed = typeof value === 'string' ? JSON.parse(value || '[]') : value
  } catch {
    return []
  }
  if (!Array.isArray(parsed)) return []
  return parsed
    .map((item) => {
      const record = item as Record<string, unknown>
      return {
        source_type: String(record.source_type ?? ''),
        source_id: String(record.source_id ?? ''),
        fact: String(record.fact ?? ''),
        used_for: String(record.used_for ?? ''),
      }
    })
    .filter((item) => item.source_id && item.fact)
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
  // Use today's date to determine the last completed month, not the latest data date
  const today = new Date()
  const thisMonthStart = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 1))
  const lastCompletedMonthEnd = toIsoDay(shiftDays(thisMonthStart, -1))

  const snapshotByMonth = new Map<string, { pointDate: string; sourcePeriodEnd: string; value: number }>()
  const monthlyBuckets = new Map<string, { pointDate: string; value: number }>()

  for (const row of rows) {
    const periodEnd = dayKey(row.period_end)
    const pointDate = monthEndIso(periodEnd)
    const sourceWindow = String(row.source_window ?? '')

    if (
      sourceWindow === 'discrete_snapshot' &&
      periodEnd >= windowStart &&
      periodEnd <= DOWNLOAD_SNAPSHOT_CUTOFF &&
      pointDate <= lastCompletedMonthEnd
    ) {
      const current = snapshotByMonth.get(pointDate)
      const nextValue = Math.trunc(coerceNumber(row.value))
      if (!current || periodEnd > current.sourcePeriodEnd) {
        snapshotByMonth.set(pointDate, { pointDate, sourcePeriodEnd: periodEnd, value: nextValue })
      }
      continue
    }

    if (
      sourceWindow === '1d' &&
      periodEnd >= DOWNLOAD_DAILY_START &&
      periodEnd >= windowStart &&
      periodEnd <= latest &&
      pointDate <= lastCompletedMonthEnd
    ) {
      const key = pointDate
      const current = monthlyBuckets.get(key)
      if (!current) {
        monthlyBuckets.set(key, {
          pointDate,
          value: Math.trunc(coerceNumber(row.value)),
        })
      } else {
        current.value += Math.trunc(coerceNumber(row.value))
      }
    }
  }

  const snapshotPoints: SparkPoint[] = [...snapshotByMonth.values()]
    .sort((a, b) => a.pointDate.localeCompare(b.pointDate))
    .map((bucket) => ({ period_start: bucket.pointDate, period_end: bucket.pointDate, value: bucket.value }))

  const monthlyPoints: SparkPoint[] = [...monthlyBuckets.values()]
    .sort((a, b) => a.pointDate.localeCompare(b.pointDate))
    .map((bucket) => ({
      period_start: bucket.pointDate,
      period_end: bucket.pointDate,
      value: bucket.value,
    }))

  const points = [...snapshotPoints, ...monthlyPoints].sort((a, b) => a.period_end.localeCompare(b.period_end))

  if (points.some((point) => point.period_end === SYNTHETIC_NOVEMBER_2025)) {
    return points
  }

  const october = points.find((point) => point.period_end === '2025-10-31')
  const december = points.find((point) => point.period_end === '2025-12-31')
  if (!october || !december) {
    return points
  }

  return [...points, {
    period_start: SYNTHETIC_NOVEMBER_2025,
    period_end: SYNTHETIC_NOVEMBER_2025,
    value: Math.round((october.value + december.value) / 2),
  }].sort((a, b) => a.period_end.localeCompare(b.period_end))
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

function validateIsoDay(value: string, field: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Invalid ${field}: expected YYYY-MM-DD`)
  }
  if (toIsoDay(parseIsoDay(value)) !== value) {
    throw new Error(`Invalid ${field}: ${value}`)
  }
  return value
}

function clampIsoDay(value: string, minIso: string, maxIso: string): string {
  if (value < minIso) return minIso
  if (value > maxIso) return maxIso
  return value
}

function downloadTotalsForWindow(rows: Row[], windowStartIso: string, windowEndIso: string): DownloadWindowTotals {
  const windowStart = windowStartIso <= windowEndIso ? windowStartIso : windowEndIso
  const windowEnd = windowStartIso <= windowEndIso ? windowEndIso : windowStartIso

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
    const overlap = overlapDays(normalizedPeriodStart, periodEnd, windowStart, windowEnd)
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
    window_start: windowStart,
    window_end: windowEnd,
    lance: Math.round(totals.lance),
    lancedb: Math.round(totals.lancedb),
  }
}

function last30dDownloadTotals(rows: Row[]): DashboardResponse['last_30d_download_totals'] {
  const windowEnd = latestCompletedDay()
  const windowEndIso = toIsoDay(windowEnd)
  const windowStartIso = toIsoDay(shiftDays(windowEnd, -29))
  return downloadTotalsForWindow(rows, windowStartIso, windowEndIso)
}

function completedMonthEndBefore(day: Date): string {
  const monthStart = new Date(Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), 1))
  return toIsoDay(shiftDays(monthStart, -1))
}

function downloadSnapshotTargetDates(latestDay: Date): string[] {
  const finalMonthEnd = completedMonthEndBefore(latestDay)
  const targetDates = DOWNLOAD_SNAPSHOT_MONTH_ENDS.filter((day) => day <= finalMonthEnd)

  let firstOfMonth = new Date(Date.UTC(2026, 1, 1))
  while (true) {
    const previousMonthEnd = toIsoDay(shiftDays(firstOfMonth, -1))
    if (previousMonthEnd > finalMonthEnd) break
    targetDates.push(previousMonthEnd)
    firstOfMonth = new Date(Date.UTC(firstOfMonth.getUTCFullYear(), firstOfMonth.getUTCMonth() + 1, 1))
  }

  return [...new Set(targetDates)].sort()
}

function buildMonthlyDownloadSnapshots(
  metricsRows: MetricDef[],
  statsRows: Row[],
  latestDay: Date,
): DownloadSnapshotPoint[] {
  const targetDates = downloadSnapshotTargetDates(latestDay)
  if (targetDates.length === 0) return []

  const targetSet = new Set(targetDates)
  const groupedStats = rowsByMetric(statsRows)
  const totalsByDate = new Map<string, Omit<DownloadSnapshotPoint, 'period_end' | 'total'>>()

  for (const metric of metricsRows) {
    if (metric.metric_family !== 'downloads') continue
    if (metric.product !== 'lance' && metric.product !== 'lancedb') continue
    if (metric.sdk !== 'python' && metric.sdk !== 'nodejs' && metric.sdk !== 'rust') continue

    const points = monthlyDownloadSparkline(groupedStats.get(metric.metric_id) ?? [], MAX_DAYS)
    for (const point of points) {
      const periodEnd = dayKey(point.period_end)
      if (!targetSet.has(periodEnd)) continue

      const current = totalsByDate.get(periodEnd) ?? {
        lance_python: 0,
        lance_rust: 0,
        lancedb_python: 0,
        lancedb_nodejs: 0,
        lancedb_rust: 0,
        python: 0,
        nodejs: 0,
        rust: 0,
        lance: 0,
        lancedb: 0,
      }
      const value = Math.trunc(coerceNumber(point.value))
      const metricKey = `${metric.product}_${metric.sdk}` as keyof Pick<
        DownloadSnapshotPoint,
        'lance_python' | 'lance_rust' | 'lancedb_python' | 'lancedb_nodejs' | 'lancedb_rust'
      >
      current[metricKey] += value
      current[metric.sdk] += value
      current[metric.product] += value
      totalsByDate.set(periodEnd, current)
    }
  }

  return targetDates
    .map((periodEnd) => {
      const totals = totalsByDate.get(periodEnd)
      if (!totals) return null
      const lance = Math.round(totals.lance)
      const lancedb = Math.round(totals.lancedb)
      return {
        period_end: periodEnd,
        lance_python: Math.round(totals.lance_python),
        lance_rust: Math.round(totals.lance_rust),
        lancedb_python: Math.round(totals.lancedb_python),
        lancedb_nodejs: Math.round(totals.lancedb_nodejs),
        lancedb_rust: Math.round(totals.lancedb_rust),
        python: Math.round(totals.python),
        nodejs: Math.round(totals.nodejs),
        rust: Math.round(totals.rust),
        lance,
        lancedb,
        total: lance + lancedb,
      }
    })
    .filter((point): point is DownloadSnapshotPoint => point !== null)
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

async function queryOptionalTable(
  db: any,
  tableName: string,
  options: { where?: string; columns?: string[]; limit?: number },
): Promise<Row[]> {
  try {
    const table = await db.openTable(tableName)
    return await queryRows(table, options)
  } catch {
    return []
  }
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

async function fetchDerivedDashboardData(): Promise<{
  metricRollups: DashboardMetricRollup[]
  recentEvidence: DashboardEvidenceItem[]
  signalCandidates: DashboardSignalCandidate[]
  signalGuidance: DashboardSignalGuidance[]
}> {
  const apiKey = requireEnv('LANCEDB_API_KEY')
  const hostOverride = requireEnv('LANCEDB_HOST_OVERRIDE')
  const region = (process.env.LANCEDB_REGION ?? 'us-east-1').trim() || 'us-east-1'

  const db = await lancedb.connect(ENTERPRISE_URI, {
    apiKey,
    hostOverride,
    region,
  })

  const rollupRows = await queryOptionalTable(db, 'dashboard_metric_rollups', {
    columns: [
      'rollup_id',
      'metric_id',
      'metric_family',
      'product',
      'sdk',
      'subject',
      'window',
      'window_start',
      'window_end',
      'current_value',
      'previous_value',
      'delta',
      'percent_change',
      'sdk_share',
      'previous_sdk_share',
      'sdk_share_delta',
      'trend_slope',
      'updated_at',
    ],
    limit: 500,
  })

  const evidenceRows = await queryOptionalTable(db, 'evidence_items', {
    columns: [
      'evidence_id',
      'source_type',
      'source_name',
      'observed_at',
      'occurred_at',
      'title',
      'url',
      'snippet',
      'matched_terms',
      'related_metrics',
      'related_packages',
      'related_repos',
      'communities',
      'evidence_strength',
    ],
    limit: 200,
  })

  const signalRows = await queryOptionalTable(db, 'signal_candidates', {
    columns: [
      'signal_id',
      'signal_type',
      'detected_at',
      'window_start',
      'window_end',
      'title',
      'summary',
      'related_metrics',
      'evidence_ids',
      'score',
      'confidence',
      'suggested_action',
    ],
    limit: 100,
  })

  const guidanceRows = await queryOptionalTable(db, 'signal_guidance', {
    columns: [
      'guidance_id',
      'signal_id',
      'generated_at',
      'model',
      'reasoning_effort',
      'prompt_version',
      'analysis_window_start',
      'analysis_window_end',
      'comparison_windows',
      'executive_summary',
      'movement_assessment',
      'why_it_matters',
      'likely_community',
      'recommended_next_steps',
      'engineering_relevance',
      'confidence',
      'citations',
    ],
    limit: 100,
  })

  const windowPriority: Record<string, number> = {
    '7d': 0,
    '15d': 1,
    '30d': 2,
    '90d': 3,
    last_full_month: 4,
  }

  const metricRollups = rollupRows
    .map((row) => ({
      rollup_id: String(row.rollup_id ?? ''),
      metric_id: String(row.metric_id ?? ''),
      metric_family: String(row.metric_family ?? ''),
      product: String(row.product ?? ''),
      sdk: String(row.sdk ?? ''),
      subject: String(row.subject ?? ''),
      window: String(row.window ?? ''),
      window_start: String(row.window_start ?? ''),
      window_end: String(row.window_end ?? ''),
      current_value: coerceNumber(row.current_value),
      previous_value: coerceNumber(row.previous_value),
      delta: coerceNumber(row.delta),
      percent_change: coerceNumber(row.percent_change),
      sdk_share: coerceNumber(row.sdk_share),
      previous_sdk_share: coerceNumber(row.previous_sdk_share),
      sdk_share_delta: coerceNumber(row.sdk_share_delta),
      trend_slope: coerceNumber(row.trend_slope),
      updated_at: coerceIsoDateTime(row.updated_at),
    }))
    .sort((a, b) => {
      const windowDiff = (windowPriority[a.window] ?? 99) - (windowPriority[b.window] ?? 99)
      if (windowDiff !== 0) return windowDiff
      return Math.abs(b.percent_change) - Math.abs(a.percent_change)
    })
    .slice(0, 80)

  const recentEvidence = evidenceRows
    .map((row) => ({
      evidence_id: String(row.evidence_id ?? ''),
      source_type: String(row.source_type ?? ''),
      source_name: String(row.source_name ?? ''),
      observed_at: coerceIsoDateTime(row.observed_at),
      occurred_at: coerceIsoDateTime(row.occurred_at),
      title: String(row.title ?? ''),
      url: String(row.url ?? ''),
      snippet: String(row.snippet ?? ''),
      matched_terms: coerceList(row.matched_terms),
      related_metrics: coerceList(row.related_metrics),
      related_packages: coerceList(row.related_packages),
      related_repos: coerceList(row.related_repos),
      communities: coerceList(row.communities),
      evidence_strength: String(row.evidence_strength ?? ''),
    }))
    .sort((a, b) => b.occurred_at.localeCompare(a.occurred_at))
    .slice(0, 8)

  const signalCandidates = signalRows
    .map((row) => ({
      signal_id: String(row.signal_id ?? ''),
      signal_type: String(row.signal_type ?? ''),
      detected_at: coerceIsoDateTime(row.detected_at),
      window_start: String(row.window_start ?? ''),
      window_end: String(row.window_end ?? ''),
      title: String(row.title ?? ''),
      summary: String(row.summary ?? ''),
      related_metrics: coerceList(row.related_metrics),
      evidence_ids: coerceList(row.evidence_ids),
      score: coerceNumber(row.score),
      confidence: String(row.confidence ?? ''),
      suggested_action: String(row.suggested_action ?? ''),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 8)

  const signalGuidance = guidanceRows
    .map((row) => ({
      guidance_id: String(row.guidance_id ?? ''),
      signal_id: String(row.signal_id ?? ''),
      generated_at: coerceIsoDateTime(row.generated_at),
      model: String(row.model ?? ''),
      reasoning_effort: String(row.reasoning_effort ?? ''),
      prompt_version: String(row.prompt_version ?? ''),
      analysis_window_start: String(row.analysis_window_start ?? ''),
      analysis_window_end: String(row.analysis_window_end ?? ''),
      comparison_windows: coerceList(row.comparison_windows),
      executive_summary: String(row.executive_summary ?? ''),
      movement_assessment: String(row.movement_assessment ?? ''),
      why_it_matters: String(row.why_it_matters ?? ''),
      likely_community: String(row.likely_community ?? ''),
      recommended_next_steps: coerceList(row.recommended_next_steps),
      engineering_relevance: String(row.engineering_relevance ?? ''),
      confidence: String(row.confidence ?? ''),
      citations: coerceCitations(row.citations),
    }))
    .sort((a, b) => b.generated_at.localeCompare(a.generated_at))
    .slice(0, 20)

  return { metricRollups, recentEvidence, signalCandidates, signalGuidance }
}

export async function fetchDownloadTotalsForWindow(
  rawWindowStart: string,
  rawWindowEnd: string,
): Promise<DownloadWindowTotals> {
  const requestedStart = validateIsoDay(String(rawWindowStart ?? '').trim(), 'window_start')
  const requestedEnd = validateIsoDay(String(rawWindowEnd ?? '').trim(), 'window_end')

  const latestDay = latestCompletedDay()
  const latestIso = toIsoDay(latestDay)
  const earliestIso = toIsoDay(shiftDays(latestDay, -(MAX_DAYS - 1)))

  const windowStartIso = clampIsoDay(requestedStart, earliestIso, latestIso)
  const windowEndIso = clampIsoDay(requestedEnd, earliestIso, latestIso)
  const normalizedStart = windowStartIso <= windowEndIso ? windowStartIso : windowEndIso
  const normalizedEnd = windowStartIso <= windowEndIso ? windowEndIso : windowStartIso

  const apiKey = requireEnv('LANCEDB_API_KEY')
  const hostOverride = requireEnv('LANCEDB_HOST_OVERRIDE')
  const region = (process.env.LANCEDB_REGION ?? 'us-east-1').trim() || 'us-east-1'

  const db = await lancedb.connect(ENTERPRISE_URI, {
    apiKey,
    hostOverride,
    region,
  })

  const metricsTable = await db.openTable('metrics')
  const metricRows = await queryRows(metricsTable, {
    columns: ['metric_id'],
    where: "is_active = true AND metric_family = 'downloads'",
    limit: 100,
  })
  const metricIds = metricRows.map((row) => String(row.metric_id ?? '')).filter(Boolean)
  if (metricIds.length === 0) {
    return {
      window_start: normalizedStart,
      window_end: normalizedEnd,
      lance: 0,
      lancedb: 0,
    }
  }

  const idsClause = metricIds.map(sqlQuote).join(', ')
  const where =
    `metric_id IN (${idsClause}) AND period_end >= ${sqlQuote(normalizedStart)} ` +
    `AND period_start <= ${sqlQuote(normalizedEnd)}`

  const statsTable = await db.openTable('stats')
  const rangeDays = overlapDays(normalizedStart, normalizedEnd, normalizedStart, normalizedEnd)
  const statsRows = await queryRows(statsTable, {
    columns: ['metric_id', 'period_start', 'period_end', 'value'],
    where,
    limit: Math.max(5000, metricIds.length * (rangeDays + 62)),
  })

  return downloadTotalsForWindow(statsRows, normalizedStart, normalizedEnd)
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
      monthly_download_snapshots: [],
      metric_rollups: [],
      recent_evidence: [],
      signal_candidates: [],
      signal_guidance: [],
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

  const derived = await fetchDerivedDashboardData()

  return {
    generated_at: new Date().toISOString(),
    days,
    groups,
    total_stars: stars.total,
    total_stars_sparkline: stars.sparkline,
    last_30d_download_totals: last30dDownloadTotals(statsRows),
    monthly_download_snapshots: buildMonthlyDownloadSnapshots(metricsRows, statsRows, latestDay),
    metric_rollups: derived.metricRollups,
    recent_evidence: derived.recentEvidence,
    signal_candidates: derived.signalCandidates,
    signal_guidance: derived.signalGuidance,
  }
}
