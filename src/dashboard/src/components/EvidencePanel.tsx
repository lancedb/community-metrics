import type { DashboardEvidenceItem } from '../types'

type EvidencePanelProps = {
  items: DashboardEvidenceItem[]
}

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

export function EvidencePanel({ items }: EvidencePanelProps) {
  return (
    <section className="space-y-4 rounded-2xl border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="space-y-1 border-b border-edge pb-3">
        <h2 className="text-2xl font-bold text-ink">Recent HN Mentions</h2>
        <p className="text-sm text-muted">
          Most recent Hacker News evidence ordered by occurrence date.
        </p>
      </header>

      {items.length === 0 ? (
        <p className="rounded-lg border border-edge bg-panel p-4 text-sm text-muted">
          No Hacker News evidence has been collected yet.
        </p>
      ) : (
        <div className="divide-y divide-edge rounded-xl border border-edge bg-panel">
          {items.map((item) => (
            <article key={item.evidence_id} className="p-4">
              <div className="mb-1 flex flex-wrap items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-muted">
                <span>{formatDate(item.occurred_at)}</span>
                <span>{item.evidence_strength}</span>
                {item.matched_terms.slice(0, 2).map((term) => (
                  <span key={term} className="rounded-md bg-brand-soft px-2 py-1 text-ink">
                    {term}
                  </span>
                ))}
              </div>
              <h3 className="text-sm font-bold text-ink">
                {item.url ? (
                  <a href={item.url} target="_blank" rel="noreferrer" className="hover:text-brand-strong">
                    {item.title}
                  </a>
                ) : (
                  item.title
                )}
              </h3>
              <p className="mt-1 text-sm text-muted">{item.snippet}</p>
            </article>
          ))}
        </div>
      )}
    </section>
  )
}
