import type {
  DashboardEvidenceItem,
  DashboardMetricRollup,
  DashboardSignalCandidate,
  DashboardSignalGuidance,
} from '../types'

type ActionCockpitProps = {
  signals: DashboardSignalCandidate[]
  guidance: DashboardSignalGuidance[]
  rollups: DashboardMetricRollup[]
  evidence: DashboardEvidenceItem[]
}

function label(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(' ')
}

function formatInt(value: number): string {
  return Intl.NumberFormat('en-US').format(value)
}

function formatPercent(value: number): string {
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toFixed(0)}%`
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

function metricName(metricId: string): string {
  const parts = metricId.split(':')
  if (parts.length < 3) return metricId
  const product = parts[1] === 'lancedb' ? 'LanceDB' : 'Lance'
  const sdk = parts[2] === 'nodejs' ? 'NodeJS' : `${parts[2].charAt(0).toUpperCase()}${parts[2].slice(1)}`
  return `${product} ${sdk}`
}

function guidanceForSignal(signal: DashboardSignalCandidate, guidance: DashboardSignalGuidance[]) {
  return guidance
    .filter((item) => item.signal_id === signal.signal_id)
    .sort((a, b) => b.generated_at.localeCompare(a.generated_at))[0]
}

function rollupsForSignal(signal: DashboardSignalCandidate, rollups: DashboardMetricRollup[]) {
  const related = new Set(signal.related_metrics)
  return rollups.filter((rollup) => related.has(rollup.metric_id) && ['7d', '15d', '30d'].includes(rollup.window))
}

function evidenceForSignal(
  signal: DashboardSignalCandidate,
  evidence: DashboardEvidenceItem[],
  guidance?: DashboardSignalGuidance,
) {
  const ids = new Set([
    ...signal.evidence_ids,
    ...(guidance?.citations.filter((citation) => citation.source_type === 'evidence').map((citation) => citation.source_id) ?? []),
  ])
  return evidence.filter((item) => ids.has(item.evidence_id)).slice(0, 3)
}

function topRollups(rollups: DashboardMetricRollup[]) {
  return rollups
    .filter((rollup) => rollup.window === '7d' && rollup.metric_family === 'downloads')
    .sort((a, b) => Math.abs(b.percent_change) - Math.abs(a.percent_change))
    .slice(0, 6)
}

export function ActionCockpit({ signals, guidance, rollups, evidence }: ActionCockpitProps) {
  const visibleSignals = signals.slice(0, 5)
  const movers = topRollups(rollups)

  return (
    <section className="space-y-4 rounded-2xl border-2 border-brand bg-white p-5 shadow-card">
      <header className="flex flex-wrap items-end justify-between gap-3 border-b border-edge pb-3">
        <div>
          <h2 className="text-2xl font-bold text-ink">Action Cockpit</h2>
          <p className="text-sm text-muted">
            Weekly DevRel guidance generated from 7d signals, 15d/30d comparisons, and cited evidence.
          </p>
        </div>
        <div className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">
          {guidance.length > 0 ? `Guidance generated ${formatDate(guidance[0].generated_at)}` : 'Guidance pending'}
        </div>
      </header>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(320px,0.8fr)]">
        <div className="space-y-3">
          {visibleSignals.length === 0 ? (
            <div className="rounded-xl border border-edge bg-panel p-4 text-sm text-muted">
              No current signal candidates were generated. Run the derived jobs to refresh rollups, evidence, and guidance.
            </div>
          ) : (
            visibleSignals.map((signal) => {
              const signalGuidance = guidanceForSignal(signal, guidance)
              const signalRollups = rollupsForSignal(signal, rollups)
              const signalEvidence = evidenceForSignal(signal, evidence, signalGuidance)
              const nextSteps = signalGuidance?.recommended_next_steps.filter((step) => step.trim().length > 0) ?? []
              const citedFacts = signalGuidance?.citations
                .filter((citation) => citation.fact.trim().length > 0)
                .slice(0, 3) ?? []
              return (
                <article key={signal.signal_id} className="rounded-xl border border-edge bg-panel p-4 shadow-card">
                  <div className="mb-2 flex flex-wrap items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-muted">
                    <span>{label(signal.signal_type)}</span>
                    <span className="rounded-md bg-brand-soft px-2 py-1 text-ink">
                      {signalGuidance ? label(signalGuidance.movement_assessment) : 'Guidance pending'}
                    </span>
                    <span>{signal.window_start} to {signal.window_end}</span>
                  </div>

                  <h3 className="text-lg font-bold text-ink">{signalGuidance?.executive_summary || signal.title}</h3>
                  <p className="mt-1 text-sm text-muted">{signalGuidance?.why_it_matters || signal.summary}</p>

                  <div className="mt-3 grid gap-2 sm:grid-cols-3">
                    {signalRollups.slice(0, 3).map((rollup) => (
                      <div key={rollup.rollup_id} className="rounded-lg border border-edge bg-white/70 p-3">
                        <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted">
                          {metricName(rollup.metric_id)} · {rollup.window}
                        </p>
                        <p className="mt-1 text-base font-bold text-ink">{formatInt(rollup.current_value)}</p>
                        <p className="text-xs text-muted">
                          {formatPercent(rollup.percent_change)} · share {(rollup.sdk_share * 100).toFixed(0)}%
                        </p>
                      </div>
                    ))}
                  </div>

                  {signalGuidance && (nextSteps.length > 0 || citedFacts.length > 0) ? (
                    <div className="mt-3 grid gap-3 lg:grid-cols-2">
                      {nextSteps.length > 0 && (
                        <div>
                          <p className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">Next Steps</p>
                          <ul className="mt-1 space-y-1 text-sm text-ink">
                            {nextSteps.map((step) => (
                              <li key={step}>{step}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {citedFacts.length > 0 && (
                        <div>
                          <p className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">Cited Facts</p>
                          <ul className="mt-1 space-y-1 text-sm text-muted">
                            {citedFacts.map((citation) => (
                              <li key={`${citation.source_type}:${citation.source_id}:${citation.fact}`}>
                                {citation.fact}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  ) : !signalGuidance ? (
                    <p className="mt-3 rounded-lg border border-edge bg-white/70 p-3 text-sm text-muted">
                      LLM guidance has not been generated for this weekly signal yet.
                    </p>
                  ) : null}

                  {signalEvidence.length > 0 && (
                    <div className="mt-3 border-t border-edge pt-3">
                      <p className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">Linked Evidence</p>
                      <div className="mt-2 space-y-2">
                        {signalEvidence.map((item) => (
                          <a
                            key={item.evidence_id}
                            href={item.url}
                            target="_blank"
                            rel="noreferrer"
                            className="block rounded-lg border border-edge bg-white/70 p-3 text-sm hover:border-brand"
                          >
                            <span className="font-semibold text-ink">{item.title}</span>
                            <span className="ml-2 text-xs text-muted">{formatDate(item.occurred_at)}</span>
                            <span className="mt-1 block text-muted">{item.snippet}</span>
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                </article>
              )
            })
          )}
        </div>

        <aside className="space-y-4">
          <div className="rounded-xl border border-edge bg-panel p-4">
            <h3 className="text-base font-bold text-ink">7d Movers</h3>
            <div className="mt-3 space-y-2">
              {movers.length === 0 ? (
                <p className="text-sm text-muted">No 7d rollups are available yet.</p>
              ) : (
                movers.map((rollup) => (
                  <div key={rollup.rollup_id} className="flex items-center justify-between gap-3 rounded-lg bg-white/70 p-3">
                    <div>
                      <p className="text-sm font-semibold text-ink">{metricName(rollup.metric_id)}</p>
                      <p className="text-xs text-muted">{formatInt(rollup.current_value)} downloads</p>
                    </div>
                    <p className="text-sm font-bold text-ink">{formatPercent(rollup.percent_change)}</p>
                  </div>
                ))
              )}
            </div>
          </div>

        </aside>
      </div>
    </section>
  )
}
