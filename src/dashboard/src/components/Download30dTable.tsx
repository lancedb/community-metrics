import { useMemo, useState } from 'react'
import { DayPicker, type DateRange } from 'react-day-picker'
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
  const defaultRange = useMemo<DateRange>(() => ({ from: minusDays(today, 30), to: today }), [today])

  const [range, setRange] = useState<DateRange | undefined>(defaultRange)
  const [pickerOpen, setPickerOpen] = useState(false)

  const onSelectRange = (nextRange: DateRange | undefined) => {
    setRange(nextRange)
    if (nextRange?.from && nextRange?.to) {
      setPickerOpen(false)
    }
  }

  const windowStart = useMemo(() => {
    const from = range?.from ?? defaultRange.from ?? today
    const to = range?.to ?? range?.from ?? defaultRange.to ?? today
    return from <= to ? dateOnlyLocal(from) : dateOnlyLocal(to)
  }, [defaultRange.from, defaultRange.to, range, today])

  const windowEnd = useMemo(() => {
    const from = range?.from ?? defaultRange.from ?? today
    const to = range?.to ?? range?.from ?? defaultRange.to ?? today
    return from <= to ? dateOnlyLocal(to) : dateOnlyLocal(from)
  }, [defaultRange.from, defaultRange.to, range, today])

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
            setRange(defaultRange)
            setPickerOpen(false)
          }}
          className="rounded-md border border-edge bg-panel px-3 py-2 text-sm font-semibold text-ink hover:bg-brand-soft"
        >
          Reset to last 30 days
        </button>
      </header>

      <div className="relative z-30">
        <button
          type="button"
          onClick={() => setPickerOpen((open) => !open)}
          className="w-full rounded-lg border border-edge bg-brand-soft/35 px-4 py-3 text-left text-sm font-medium text-ink hover:bg-brand-soft"
        >
          Date range: {formatWindow(windowStart)} to {formatWindow(windowEnd)}
        </button>
        {pickerOpen && (
          <div className="compact-range-picker absolute left-0 z-[120] mt-2 rounded-lg border border-edge bg-white p-3 shadow-xl">
            <DayPicker
              mode="range"
              selected={range}
              onSelect={onSelectRange}
              showOutsideDays
              startMonth={earliestAllowed}
              endMonth={today}
              disabled={{ before: earliestAllowed, after: today }}
              className="text-xs"
            />
            <div className="mt-2 flex justify-end">
              <button
                type="button"
                className="rounded-md border border-edge bg-panel px-2 py-1 text-xs font-semibold text-muted hover:bg-brand-soft"
                onClick={() => setPickerOpen(false)}
              >
                Close
              </button>
            </div>
          </div>
        )}
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
