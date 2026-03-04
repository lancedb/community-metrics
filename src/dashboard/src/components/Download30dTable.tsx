import { useMemo, useState } from 'react'
import type { DashboardMetric } from '../types'

type Download30dTableProps = {
  lanceMetrics: DashboardMetric[]
  lancedbMetrics: DashboardMetric[]
  maxDaysBack?: number
}

function formatInt(value: number): string {
  return Intl.NumberFormat('en-US').format(value)
}

function formatWindow(date: Date): string {
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

function dateOnlyLocal(input: Date): Date {
  return new Date(input.getFullYear(), input.getMonth(), input.getDate())
}

function minusDays(base: Date, days: number): Date {
  const out = new Date(base.getTime())
  out.setDate(out.getDate() - days)
  return dateOnlyLocal(out)
}

function toInputDate(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function parseInputDate(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null
  const [yearRaw, monthRaw, dayRaw] = value.split('-')
  const year = Number(yearRaw)
  const month = Number(monthRaw)
  const day = Number(dayRaw)
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null
  const parsed = new Date(year, month - 1, day)
  if (
    parsed.getFullYear() !== year ||
    parsed.getMonth() !== month - 1 ||
    parsed.getDate() !== day
  ) {
    return null
  }
  return dateOnlyLocal(parsed)
}

function clampDate(value: Date, min: Date, max: Date): Date {
  if (value < min) return min
  if (value > max) return max
  return value
}

function overlapDays(aStart: Date, aEnd: Date, bStart: Date, bEnd: Date): number {
  const start = Math.max(aStart.getTime(), bStart.getTime())
  const end = Math.min(aEnd.getTime(), bEnd.getTime())
  if (end < start) return 0
  return Math.floor((end - start) / 86400000) + 1
}

function totalForWindow(metrics: DashboardMetric[], windowStart: Date, windowEnd: Date): number {
  let total = 0
  for (const metric of metrics) {
    for (const point of metric.sparkline) {
      const periodStart = new Date(`${point.period_start}T00:00:00`)
      const periodEnd = new Date(`${point.period_end}T00:00:00`)
      const days = overlapDays(periodStart, periodEnd, windowStart, windowEnd)
      if (days <= 0) continue
      const spanDays = overlapDays(periodStart, periodEnd, periodStart, periodEnd)
      if (spanDays <= 0) continue
      total += (point.value * days) / spanDays
    }
  }
  return Math.round(total)
}

export function Download30dTable({ lanceMetrics, lancedbMetrics, maxDaysBack = 90 }: Download30dTableProps) {
  const today = useMemo(() => dateOnlyLocal(new Date()), [])
  const earliestAllowed = useMemo(() => minusDays(today, maxDaysBack), [today, maxDaysBack])
  const defaultStart = useMemo(() => minusDays(today, 29), [today])
  const minInputDate = useMemo(() => toInputDate(earliestAllowed), [earliestAllowed])
  const maxInputDate = useMemo(() => toInputDate(today), [today])

  const [startDate, setStartDate] = useState<Date>(defaultStart)
  const [endDate, setEndDate] = useState<Date>(today)

  const onStartDateChange = (value: string) => {
    const parsed = parseInputDate(value)
    if (!parsed) return
    const nextStart = clampDate(parsed, earliestAllowed, today)
    setStartDate(nextStart)
    setEndDate((currentEnd) => (nextStart > currentEnd ? nextStart : currentEnd))
  }

  const onEndDateChange = (value: string) => {
    const parsed = parseInputDate(value)
    if (!parsed) return
    const nextEnd = clampDate(parsed, earliestAllowed, today)
    setEndDate(nextEnd)
    setStartDate((currentStart) => (nextEnd < currentStart ? nextEnd : currentStart))
  }

  const windowStart = useMemo(() => {
    return startDate <= endDate ? startDate : endDate
  }, [endDate, startDate])

  const windowEnd = useMemo(() => {
    return startDate <= endDate ? endDate : startDate
  }, [endDate, startDate])

  const lance = useMemo(() => totalForWindow(lanceMetrics, windowStart, windowEnd), [lanceMetrics, windowStart, windowEnd])
  const lancedb = useMemo(
    () => totalForWindow(lancedbMetrics, windowStart, windowEnd),
    [lancedbMetrics, windowStart, windowEnd],
  )
  const total = lance + lancedb

  return (
    <section className="relative z-20 space-y-4 rounded-2xl border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b border-edge pb-3">
        <div>
          <h2 className="text-2xl font-bold text-ink">Downloads in Selected Window</h2>
          <p className="text-sm text-muted">
            Window: {formatWindow(windowStart)} to {formatWindow(windowEnd)}
          </p>
        </div>
        <button
          type="button"
          onClick={() => {
            setStartDate(defaultStart)
            setEndDate(today)
          }}
          className="rounded-md border border-edge bg-panel px-3 py-2 text-sm font-semibold text-ink hover:bg-brand-soft"
        >
          Reset to last 30 days
        </button>
      </header>

      <div className="flex flex-wrap gap-3">
        <label className="w-full sm:w-[48%] lg:w-72">
          <span className="mb-1 block text-xs font-semibold uppercase tracking-[0.14em] text-muted">Start date</span>
          <input
            type="date"
            value={toInputDate(windowStart)}
            min={minInputDate}
            max={maxInputDate}
            onChange={(event) => onStartDateChange(event.target.value)}
            className="block w-full rounded-lg border border-edge bg-panel px-3 py-2 text-sm text-ink focus:border-brand-strong focus:outline-none"
          />
        </label>
        <label className="w-full sm:w-[48%] lg:w-72">
          <span className="mb-1 block text-xs font-semibold uppercase tracking-[0.14em] text-muted">End date</span>
          <input
            type="date"
            value={toInputDate(windowEnd)}
            min={minInputDate}
            max={maxInputDate}
            onChange={(event) => onEndDateChange(event.target.value)}
            className="block w-full rounded-lg border border-edge bg-panel px-3 py-2 text-sm text-ink focus:border-brand-strong focus:outline-none"
          />
        </label>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-left">
          <thead>
            <tr className="border-b border-edge text-xs uppercase tracking-[0.14em] text-muted">
              <th className="px-2 py-3 font-semibold">LanceDB</th>
              <th className="px-2 py-3 font-semibold">Lance</th>
              <th className="px-2 py-3 font-semibold">Total</th>
            </tr>
          </thead>
          <tbody>
            <tr className="text-lg font-bold text-ink">
              <td className="px-2 py-3">{formatInt(lancedb)}</td>
              <td className="px-2 py-3">{formatInt(lance)}</td>
              <td className="px-2 py-3">{formatInt(total)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  )
}
