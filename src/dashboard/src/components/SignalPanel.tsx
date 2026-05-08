import type { DashboardSignalCandidate } from '../types'

type SignalPanelProps = {
  items: DashboardSignalCandidate[]
}

function actionLabel(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(' ')
}

function formatWindow(start: string, end: string): string {
  if (!start || !end) return ''
  return `${start} to ${end}`
}

export function SignalPanel({ items }: SignalPanelProps) {
  return (
    <section className="space-y-4 rounded-2xl border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="space-y-1 border-b border-edge pb-3">
        <h2 className="text-2xl font-bold text-ink">Community Signals</h2>
        <p className="text-sm text-muted">
          Precomputed candidate signals for DevRel review and follow-up.
        </p>
      </header>

      {items.length === 0 ? (
        <p className="rounded-lg border border-edge bg-panel p-4 text-sm text-muted">
          No signal candidates have been generated yet.
        </p>
      ) : (
        <div className="grid gap-3 lg:grid-cols-2">
          {items.map((item) => (
            <article key={item.signal_id} className="rounded-xl border border-edge bg-panel p-4 shadow-card">
              <div className="mb-2 flex flex-wrap items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-muted">
                <span>{actionLabel(item.signal_type)}</span>
                <span className="rounded-md bg-brand-soft px-2 py-1 text-ink">{item.confidence}</span>
                <span>{formatWindow(item.window_start, item.window_end)}</span>
              </div>
              <h3 className="text-base font-bold text-ink">{item.title}</h3>
              <p className="mt-1 text-sm text-muted">{item.summary}</p>
              <div className="mt-3 flex flex-wrap gap-2 text-xs">
                <span className="rounded-md border border-edge px-2 py-1 text-muted">
                  {actionLabel(item.suggested_action)}
                </span>
                <span className="rounded-md border border-edge px-2 py-1 text-muted">
                  Score {Math.round(item.score)}
                </span>
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  )
}
