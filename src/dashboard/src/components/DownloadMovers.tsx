import type { DashboardMetricRollup } from '../types'

type DownloadMoversProps = {
  rollups: DashboardMetricRollup[]
}

function formatInt(value: number): string {
  return Intl.NumberFormat('en-US').format(value)
}

function formatPercent(value: number): string {
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toFixed(0)}%`
}

function metricName(metricId: string): string {
  const parts = metricId.split(':')
  if (parts.length < 3) return metricId
  const product = parts[1] === 'lancedb' ? 'LanceDB' : 'Lance'
  const sdk = parts[2] === 'nodejs' ? 'NodeJS' : `${parts[2].charAt(0).toUpperCase()}${parts[2].slice(1)}`
  return `${product} ${sdk}`
}

function topRollups(rollups: DashboardMetricRollup[]) {
  return rollups
    .filter((rollup) => rollup.window === '7d' && rollup.metric_family === 'downloads')
    .sort((a, b) => Math.abs(b.percent_change) - Math.abs(a.percent_change))
    .slice(0, 6)
}

export function DownloadMovers({ rollups }: DownloadMoversProps) {
  const movers = topRollups(rollups)

  return (
    <section className="space-y-4 rounded-2xl border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="space-y-1 border-b border-edge pb-3">
        <h2 className="text-2xl font-bold text-ink">7d Download Movers</h2>
        <p className="text-sm text-muted">Largest 7d download shifts from the derived dashboard rollups.</p>
      </header>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {movers.length === 0 ? (
          <p className="text-sm text-muted">No 7d rollups are available yet.</p>
        ) : (
          movers.map((rollup) => (
            <div key={rollup.rollup_id} className="rounded-lg border border-edge bg-panel p-3">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-ink">{metricName(rollup.metric_id)}</p>
                  <p className="text-xs text-muted">{formatInt(rollup.current_value)} downloads</p>
                </div>
                <p className="text-sm font-bold text-ink">{formatPercent(rollup.percent_change)}</p>
              </div>
            </div>
          ))
        )}
      </div>
    </section>
  )
}
