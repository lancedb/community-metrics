import { TrendChart } from './TrendChart'
import type { DashboardMetric, SparkPoint } from '../types'

type MetricCardProps = {
  metric: DashboardMetric
}

function formatNumber(value: number | null): string {
  if (value === null) return 'N/A'
  return Intl.NumberFormat('en-US').format(value)
}

function titleForMetric(metric: DashboardMetric): string {
  if (metric.metric_family === 'downloads') {
    const sdk = (metric.sdk || '').toLowerCase()
    if (sdk === 'nodejs') return 'NodeJS'
    if (sdk === 'python') return 'Python'
    if (sdk === 'rust') return 'Rust'
    return metric.display_name
  }
  if (metric.metric_family === 'stars') {
    if (metric.metric_id === 'stars:total:github') return 'Total Stars'
    return 'GitHub Stars'
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
    if (metric.metric_id === 'stars:total:github') return 'Combined: lance-format/lance + lancedb/lancedb'
    return `GitHub repo ${metric.subject}`
  }
  return metric.subject
}

function lastFullMonthDownloadValue(points: SparkPoint[]): number | null {
  if (points.length === 0) return null

  const today = new Date()
  const targetMonth = today.getUTCMonth() === 0 ? 11 : today.getUTCMonth() - 1
  const targetYear = today.getUTCMonth() === 0 ? today.getUTCFullYear() - 1 : today.getUTCFullYear()

  let candidate: SparkPoint | null = null
  for (const point of points) {
    const periodEnd = new Date(`${point.period_end}T00:00:00Z`)
    if (periodEnd.getUTCMonth() !== targetMonth || periodEnd.getUTCFullYear() !== targetYear) {
      continue
    }
    if (candidate === null || point.period_end > candidate.period_end) {
      candidate = point
    }
  }

  return candidate?.value ?? null
}

export function MetricCard({ metric }: MetricCardProps) {
  const displayedValue =
    metric.metric_family === 'downloads'
      ? (lastFullMonthDownloadValue(metric.sparkline) ?? metric.latest_value)
      : metric.latest_value

  return (
    <article className="rounded-xl border border-edge bg-panel p-4 shadow-card">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-semibold text-ink">{titleForMetric(metric)}</p>
          <p className="text-xs text-muted">{subtitleForMetric(metric)}</p>
        </div>
      </div>

      <p className="mb-2 text-3xl font-bold leading-none text-ink">{formatNumber(displayedValue)}</p>
      {metric.metric_family === 'downloads' && (
        <p className="mb-2 text-[11px] text-muted">Last full month total</p>
      )}
      <TrendChart points={metric.sparkline} showMarkers={metric.metric_family === 'downloads'} />
    </article>
  )
}
