'use client'

import { CalendarRangeIcon, Loader2Icon, RotateCcwIcon } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

import { Button } from '@/components/ui/button'
import { formatInt } from '@/lib/format'
import { cn } from '@/lib/utils'

import { fetchDownloadWindowTotals } from '../api'
import type { DownloadWindowTotals } from '../types'
import { SectionPanel } from './SectionPanel'

type Download30dTableProps = {
  initialTotals: DownloadWindowTotals
  maxDaysBack?: number
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
  const [year, month, day] = value.split('-').map(Number)
  const parsed = new Date(year, month - 1, day)
  if (parsed.getFullYear() !== year || parsed.getMonth() !== month - 1 || parsed.getDate() !== day) return null
  return dateOnlyLocal(parsed)
}

function parseIsoDate(value: string): Date | null {
  return parseInputDate(value.slice(0, 10))
}

function clampDate(value: Date, min: Date, max: Date): Date {
  if (value < min) return min
  if (value > max) return max
  return value
}

const PRESETS = [7, 30, 90] as const

const inputClass =
  'block h-10 w-full rounded-lg border border-input bg-background/70 px-3 text-sm text-foreground tabular shadow-xs transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/40 focus-visible:outline-none'

export function Download30dTable({ initialTotals, maxDaysBack = 90 }: Download30dTableProps) {
  const maxDate = useMemo(
    () => parseIsoDate(initialTotals.window_end) ?? dateOnlyLocal(new Date()),
    [initialTotals.window_end],
  )
  const earliestAllowed = useMemo(() => minusDays(maxDate, maxDaysBack), [maxDate, maxDaysBack])
  const defaultStart = useMemo(() => {
    const parsed = parseIsoDate(initialTotals.window_start)
    return clampDate(parsed ?? minusDays(maxDate, 29), earliestAllowed, maxDate)
  }, [earliestAllowed, initialTotals.window_start, maxDate])
  const minInputDate = toInputDate(earliestAllowed)
  const maxInputDate = toInputDate(maxDate)

  const [startDate, setStartDate] = useState<Date>(defaultStart)
  const [endDate, setEndDate] = useState<Date>(maxDate)
  const [totals, setTotals] = useState(() => ({ lance: initialTotals.lance, lancedb: initialTotals.lancedb }))
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const onStartDateChange = (value: string) => {
    const parsed = parseInputDate(value)
    if (!parsed) return
    const nextStart = clampDate(parsed, earliestAllowed, maxDate)
    setStartDate(nextStart)
    setEndDate((currentEnd) => (nextStart > currentEnd ? nextStart : currentEnd))
  }

  const onEndDateChange = (value: string) => {
    const parsed = parseInputDate(value)
    if (!parsed) return
    const nextEnd = clampDate(parsed, earliestAllowed, maxDate)
    setEndDate(nextEnd)
    setStartDate((currentStart) => (nextEnd < currentStart ? nextEnd : currentStart))
  }

  const applyPreset = (days: number) => {
    setEndDate(maxDate)
    setStartDate(clampDate(minusDays(maxDate, days - 1), earliestAllowed, maxDate))
  }

  const windowStart = startDate <= endDate ? startDate : endDate
  const windowEnd = startDate <= endDate ? endDate : startDate
  const windowStartIso = toInputDate(windowStart)
  const windowEndIso = toInputDate(windowEnd)
  const windowDays = Math.round((windowEnd.getTime() - windowStart.getTime()) / 86_400_000) + 1

  useEffect(() => {
    let cancelled = false
    setIsRefreshing(true)
    setError(null)
    fetchDownloadWindowTotals(windowStartIso, windowEndIso)
      .then((next) => {
        if (!cancelled) setTotals({ lance: next.lance, lancedb: next.lancedb })
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Could not refresh download totals')
      })
      .finally(() => {
        if (!cancelled) setIsRefreshing(false)
      })
    return () => {
      cancelled = true
    }
  }, [windowEndIso, windowStartIso])

  const total = totals.lance + totals.lancedb
  const lancedbShare = total > 0 ? (totals.lancedb / total) * 100 : 0

  return (
    <SectionPanel
      title="Downloads in Selected Window"
      icon={<CalendarRangeIcon />}
      description={
        <>
          Window: {formatWindow(windowStart)} to {formatWindow(windowEnd)}
          <span className="text-muted-foreground/70"> · {windowDays} days</span>
        </>
      }
      actions={
        <>
          <div className="flex rounded-lg border bg-background/60 p-0.5" role="group" aria-label="Quick windows">
            {PRESETS.map((days) => {
              const active = windowEndIso === maxInputDate && windowDays === days
              return (
                <button
                  key={days}
                  type="button"
                  aria-pressed={active}
                  onClick={() => applyPreset(days)}
                  className={cn(
                    'rounded-md px-2.5 py-1 text-xs font-medium transition-colors',
                    active ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:text-foreground',
                  )}
                >
                  {days}d
                </button>
              )
            })}
          </div>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => {
              setStartDate(defaultStart)
              setEndDate(maxDate)
              setTotals({ lance: initialTotals.lance, lancedb: initialTotals.lancedb })
              setError(null)
            }}
          >
            <RotateCcwIcon aria-hidden />
            Reset to last 30 days
          </Button>
        </>
      }
    >
      <div className="grid gap-3 sm:grid-cols-2 lg:max-w-xl">
        <label className="block">
          <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Start date
          </span>
          <input
            type="date"
            value={windowStartIso}
            min={minInputDate}
            max={maxInputDate}
            onChange={(event) => onStartDateChange(event.target.value)}
            className={inputClass}
          />
        </label>
        <label className="block">
          <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
            End date
          </span>
          <input
            type="date"
            value={windowEndIso}
            min={minInputDate}
            max={maxInputDate}
            onChange={(event) => onEndDateChange(event.target.value)}
            className={inputClass}
          />
        </label>
      </div>

      <div className={cn('mt-6 transition-opacity', isRefreshing && 'opacity-60')} aria-busy={isRefreshing}>
        <table className="w-full table-fixed border-separate border-spacing-0 text-left">
          <thead>
            <tr className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              {[
                { label: 'LanceDB', color: 'var(--chart-1)' },
                { label: 'Lance', color: 'var(--chart-2)' },
                { label: 'Total', color: null },
              ].map((column) => (
                <th key={column.label} scope="col" className="pb-1.5 pr-3 font-medium">
                  <span className="inline-flex items-center gap-1.5">
                    {column.color && (
                      <span aria-hidden className="size-2 rounded-full" style={{ background: column.color }} />
                    )}
                    {column.label}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr className="text-xl font-semibold tracking-tight text-foreground tabular sm:text-3xl">
              <td className="pr-3">{formatInt(totals.lancedb)}</td>
              <td className="pr-3">{formatInt(totals.lance)}</td>
              <td className="pr-3 text-primary">{formatInt(total)}</td>
            </tr>
          </tbody>
        </table>

        <div
          className="mt-5 flex h-2 overflow-hidden rounded-full bg-muted"
          role="img"
          aria-label={`LanceDB ${lancedbShare.toFixed(0)}% and Lance ${(100 - lancedbShare).toFixed(0)}% of downloads in this window`}
        >
          <div className="h-full bg-chart-1 transition-[width] duration-500" style={{ width: `${lancedbShare}%` }} />
          <div className="h-full flex-1 bg-chart-2" />
        </div>
        <div className="mt-2 flex justify-between text-xs text-muted-foreground tabular">
          <span>LanceDB {lancedbShare.toFixed(0)}%</span>
          <span>Lance {(100 - lancedbShare).toFixed(0)}%</span>
        </div>
      </div>

      <div className="mt-3 min-h-5 text-xs" aria-live="polite">
        {isRefreshing && (
          <span className="inline-flex items-center gap-1.5 text-muted-foreground">
            <Loader2Icon aria-hidden className="size-3 animate-spin" />
            Refreshing totals...
          </span>
        )}
        {error && <span className="text-down">{error}</span>}
      </div>
    </SectionPanel>
  )
}
