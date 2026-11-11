import { ArrowDownRightIcon, ArrowUpRightIcon, MinusIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { formatInt, formatMonth, formatNullableInt, formatPercent, formatShortDay } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { DashboardMetric, SparkPoint } from '@/types'

import { TrendChart } from './TrendChart'

type MetricCardProps = {
  metric: DashboardMetric
  /** A --chart-N color variable for this card's trend line. */
  color?: string
  highlight?: boolean
}

function starRepoTitle(subject: string): string {
  const repo = (subject.split('/').at(-1) ?? '').trim().toLowerCase()
  if (repo === 'lance') return 'Lance'
  if (repo === 'lancedb') return 'LanceDB'
  if (repo === 'lance-graph') return 'Lance Graph'
  if (repo === 'lance-context') return 'Lance Context'
  if (!repo) return 'GitHub Stars'
  return repo
    .split('-')
    .filter((part) => part.length > 0)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(' ')
}

export function titleForMetric(metric: DashboardMetric): string {
  if (metric.metric_family === 'downloads') {
    const sdk = (metric.sdk || '').toLowerCase()
    if (sdk === 'nodejs') return 'NodeJS'
    if (sdk === 'python') return 'Python'
    if (sdk === 'rust') return 'Rust'
    return metric.display_name
  }
  if (metric.metric_family === 'stars') {
    if (metric.metric_id === 'stars:total:github') return 'Total Stars'
    return starRepoTitle(metric.subject)
  }
  return metric.display_name
}

function subtitleForMetric(metric: DashboardMetric): string {
  if (metric.metric_family === 'downloads') {
    if (metric.sdk === 'nodejs') return `npm package ${metric.subject}`
    if (metric.sdk === 'python') return `PyPI package ${metric.subject}`
    if (metric.sdk === 'rust') return `crates.io crate ${metric.subject}`
  }
  if (metric.metric_family === 'stars') {
    if (metric.metric_id === 'stars:total:github') return metric.subject
    return `GitHub repo ${metric.subject}`
  }
  return metric.subject
}

/** The point for the last full calendar month (UTC), if the series has one. */
function lastFullMonthIndex(points: SparkPoint[]): number {
  const today = new Date()
  const targetMonth = today.getUTCMonth() === 0 ? 11 : today.getUTCMonth() - 1
  const targetYear = today.getUTCMonth() === 0 ? today.getUTCFullYear() - 1 : today.getUTCFullYear()

  let best = -1
  points.forEach((point, index) => {
    const periodEnd = new Date(`${point.period_end}T00:00:00Z`)
    if (periodEnd.getUTCMonth() !== targetMonth || periodEnd.getUTCFullYear() !== targetYear) return
    if (best === -1 || point.period_end > points[best].period_end) best = index
  })
  return best
}

type Change = { text: string; context: string; direction: 'up' | 'down' | 'flat' }

function downloadChange(points: SparkPoint[], index: number): Change | null {
  if (index < 1) return null
  const current = points[index].value
  const previous = points[index - 1].value
  if (previous <= 0) return null
  const percent = ((current - previous) / previous) * 100
  const text = formatPercent(percent)
  return {
    text,
    context: `vs ${formatMonth(points[index - 1].period_end)}`,
    direction: text === '0%' ? 'flat' : percent > 0 ? 'up' : 'down',
  }
}

function starChange(points: SparkPoint[]): Change | null {
  if (points.length < 2) return null
  const delta = points[points.length - 1].value - points[0].value
  return {
    text: `${delta > 0 ? '+' : ''}${formatInt(delta)}`,
    context: `since ${formatShortDay(points[0].period_end)}`,
    direction: delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat',
  }
}

function ChangeBadge({ change }: { change: Change }) {
  const Icon = change.direction === 'up' ? ArrowUpRightIcon : change.direction === 'down' ? ArrowDownRightIcon : MinusIcon
  return (
    <div className="flex flex-col items-end gap-1">
      <Badge
        variant="outline"
        className={cn(
          'gap-0.5 border-transparent tabular',
          change.direction === 'up' && 'bg-up-soft text-up',
          change.direction === 'down' && 'bg-down-soft text-down',
          change.direction === 'flat' && 'bg-muted text-muted-foreground',
        )}
      >
        <Icon aria-hidden className="size-3" />
        {change.text}
      </Badge>
      <span className="text-[11px] text-muted-foreground">{change.context}</span>
    </div>
  )
}

export function MetricCard({ metric, color, highlight = false }: MetricCardProps) {
  const isDownloads = metric.metric_family === 'downloads'
  const monthIndex = isDownloads ? lastFullMonthIndex(metric.sparkline) : -1
  const displayedValue = isDownloads
    ? monthIndex >= 0
      ? metric.sparkline[monthIndex].value
      : metric.latest_value
    : metric.latest_value
  const change = isDownloads ? downloadChange(metric.sparkline, monthIndex) : starChange(metric.sparkline)
  const title = titleForMetric(metric)

  return (
    <article
      className={cn(
        'flex flex-col gap-4 rounded-xl border bg-background/60 p-4 transition-colors hover:border-primary/40',
        highlight && 'border-primary/40 bg-primary/[0.06]',
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-semibold text-foreground">{title}</p>
          <p className="truncate text-xs text-muted-foreground">{subtitleForMetric(metric)}</p>
        </div>
        {change && <ChangeBadge change={change} />}
      </div>

      <div>
        <p className="text-3xl font-semibold tracking-tight text-foreground tabular">{formatNullableInt(displayedValue)}</p>
        {isDownloads && <p className="mt-1 text-xs text-muted-foreground">Last full month total</p>}
      </div>

      <TrendChart
        points={metric.sparkline}
        label={`${title} ${isDownloads ? 'downloads by month' : 'stars over time'}`}
        granularity={isDownloads ? 'month' : 'day'}
        color={color}
        seriesName={isDownloads ? 'Downloads' : 'Stars'}
      />
    </article>
  )
}
