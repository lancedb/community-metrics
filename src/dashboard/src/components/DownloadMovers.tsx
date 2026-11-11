import { ArrowDownRightIcon, ArrowUpRightIcon, TrendingUpIcon } from 'lucide-react'

import { formatInt, formatPercent } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { DashboardMetricRollup } from '@/types'

import { SectionPanel } from './SectionPanel'

type DownloadMoversProps = {
  rollups: DashboardMetricRollup[]
}

export function moverName(metricId: string): string {
  const parts = metricId.split(':')
  if (parts.length < 3) return metricId
  const product = parts[1] === 'lancedb' ? 'LanceDB' : 'Lance'
  const sdk = parts[2] === 'nodejs' ? 'NodeJS' : `${parts[2].charAt(0).toUpperCase()}${parts[2].slice(1)}`
  return `${product} ${sdk}`
}

/** The 6 largest 7d download moves by absolute percent change. */
export function topMovers(rollups: DashboardMetricRollup[]): DashboardMetricRollup[] {
  return rollups
    .filter((rollup) => rollup.window === '7d' && rollup.metric_family === 'downloads')
    .sort((a, b) => Math.abs(b.percent_change) - Math.abs(a.percent_change))
    .slice(0, 6)
}

export function DownloadMovers({ rollups }: DownloadMoversProps) {
  const movers = topMovers(rollups)
  const maxMove = Math.max(1, ...movers.map((rollup) => Math.abs(rollup.percent_change)))

  return (
    <SectionPanel
      title="7d Download Movers"
      icon={<TrendingUpIcon />}
      description="Largest 7-day download changes versus the previous 7 days, from the derived dashboard rollups."
    >
      {movers.length === 0 ? (
        <p className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">No 7d rollups are available yet.</p>
      ) : (
        <ul className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          {movers.map((rollup) => {
            const up = rollup.percent_change >= 0
            const Icon = up ? ArrowUpRightIcon : ArrowDownRightIcon
            return (
              <li key={rollup.rollup_id} className="rounded-xl border bg-background/60 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="font-semibold text-foreground">{moverName(rollup.metric_id)}</p>
                    <p className="mt-0.5 font-mono text-xs text-muted-foreground tabular">
                      {formatInt(rollup.current_value)} downloads
                    </p>
                  </div>
                  <p
                    className={cn(
                      'inline-flex items-center gap-0.5 rounded-md px-1.5 py-0.5 font-mono text-sm font-semibold tabular',
                      up ? 'bg-up-soft text-up' : 'bg-down-soft text-down',
                    )}
                  >
                    <Icon aria-hidden className="size-3.5" />
                    {formatPercent(rollup.percent_change)}
                  </p>
                </div>
                <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-muted" aria-hidden>
                  <div
                    className={cn('h-full rounded-full', up ? 'bg-up' : 'bg-down')}
                    style={{ width: `${(Math.abs(rollup.percent_change) / maxMove) * 100}%` }}
                  />
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </SectionPanel>
  )
}
