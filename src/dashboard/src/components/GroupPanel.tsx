import type { DashboardGroup } from '../types'
import { MetricCard } from './MetricCard'

type GroupPanelProps = {
  group: DashboardGroup
}

export function GroupPanel({ group }: GroupPanelProps) {
  const downloadItems = group.items.filter((item) => item.metric_family === 'downloads')
  const starItems = group.items.filter((item) => item.metric_family === 'stars')

  return (
    <section className="space-y-5 rounded-2xl border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="flex items-center justify-between border-b border-edge pb-3">
        <h2 className="text-2xl font-bold text-ink">{group.title}</h2>
        <span className="rounded-full bg-brand-soft px-3 py-1 text-xs font-semibold uppercase tracking-wide text-muted">
          Daily Trends
        </span>
      </header>

      {downloadItems.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-xs font-bold uppercase tracking-[0.18em] text-muted">Download Stats</h3>
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {downloadItems.map((item) => (
              <MetricCard key={item.metric_id} metric={item} />
            ))}
          </div>
        </div>
      )}

      {starItems.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-xs font-bold uppercase tracking-[0.18em] text-muted">Stars</h3>
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {starItems.map((item) => (
              <MetricCard key={item.metric_id} metric={item} />
            ))}
          </div>
        </div>
      )}
    </section>
  )
}
