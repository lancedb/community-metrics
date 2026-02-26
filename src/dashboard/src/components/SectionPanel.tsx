import type { DashboardMetric } from '../types'
import { MetricCard } from './MetricCard'

type SectionPanelProps = {
  title: string
  subtitle: string
  items: DashboardMetric[]
  emphasized?: boolean
}

export function SectionPanel({ title, subtitle, items, emphasized = false }: SectionPanelProps) {
  const sectionClass = emphasized
    ? 'space-y-4 rounded-2xl border-2 border-brand bg-white p-5 shadow-card'
    : 'space-y-4 rounded-2xl border border-edge bg-white/90 p-5 backdrop-blur'

  return (
    <section className={sectionClass}>
      <header className="space-y-1 border-b border-edge pb-3">
        <h2 className="text-2xl font-bold text-ink">{title}</h2>
        <p className="text-sm text-muted">{subtitle}</p>
      </header>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {items.map((item) => (
          <MetricCard key={item.metric_id} metric={item} />
        ))}
      </div>
    </section>
  )
}
