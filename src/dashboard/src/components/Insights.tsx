import type {
  DashboardEvidenceItem,
  DashboardSignalCandidate,
  DashboardSignalGuidance,
} from '../types'

type InsightsProps = {
  signals: DashboardSignalCandidate[]
  guidance: DashboardSignalGuidance[]
  evidence: DashboardEvidenceItem[]
}

function label(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(' ')
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

function signalIdentityKey(signalId: string): string {
  const parts = signalId.split(':')
  if (parts.length < 4) return signalId
  return `${parts[0]}:${parts.slice(3).join(':')}`
}

function guidanceHistoryForSignal(signal: DashboardSignalCandidate, guidance: DashboardSignalGuidance[]) {
  const key = signalIdentityKey(signal.signal_id)
  return guidance
    .filter((item) => item.signal_id === signal.signal_id || signalIdentityKey(item.signal_id) === key)
    .sort((a, b) => b.generated_at.localeCompare(a.generated_at))
    .slice(0, 20)
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

function RecentInsightsTable({ insights }: { insights: DashboardSignalGuidance[] }) {
  if (insights.length === 0) return null

  return (
    <div className="mt-3 border-t border-edge pt-3">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">Recent Insights</p>
        <p className="text-xs text-muted">Top {insights.length} most recent</p>
      </div>
      <div className="overflow-x-auto rounded-lg border border-edge bg-white/70">
        <table className="min-w-[760px] w-full border-collapse text-left text-sm">
          <thead>
            <tr className="border-b border-edge text-[11px] uppercase tracking-[0.14em] text-muted">
              <th className="px-3 py-2 font-semibold">Generated</th>
              <th className="px-3 py-2 font-semibold">Movement</th>
              <th className="px-3 py-2 font-semibold">Relevance</th>
              <th className="px-3 py-2 font-semibold">Confidence</th>
              <th className="px-3 py-2 font-semibold">Summary</th>
            </tr>
          </thead>
          <tbody>
            {insights.map((insight) => (
              <tr key={insight.guidance_id} className="border-b border-edge/70 last:border-0">
                <td className="whitespace-nowrap px-3 py-2 text-xs text-muted">{formatDate(insight.generated_at)}</td>
                <td className="whitespace-nowrap px-3 py-2 text-xs font-semibold text-ink">
                  {label(insight.movement_assessment)}
                </td>
                <td className="whitespace-nowrap px-3 py-2 text-xs text-muted">{label(insight.engineering_relevance)}</td>
                <td className="whitespace-nowrap px-3 py-2 text-xs text-muted">{label(insight.confidence)}</td>
                <td className="px-3 py-2 text-sm text-ink">{insight.executive_summary}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function Insights({ signals, guidance, evidence }: InsightsProps) {
  const visibleSignals = signals.slice(0, 5)

  return (
    <section className="space-y-4 rounded-2xl border-2 border-brand bg-white p-5 shadow-card">
      <header className="flex flex-wrap items-end justify-between gap-3 border-b border-edge pb-3">
        <div>
          <h2 className="text-2xl font-bold text-ink">Insights</h2>
          <p className="text-sm text-muted">
            Weekly DevRel guidance generated from 7d signals, 15d/30d comparisons, and cited evidence.
          </p>
        </div>
        <div className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">
          {guidance.length > 0 ? `Guidance generated ${formatDate(guidance[0].generated_at)}` : 'Guidance pending'}
        </div>
      </header>

      <div>
        <div className="space-y-3">
          {visibleSignals.length === 0 ? (
            <div className="rounded-xl border border-edge bg-panel p-4 text-sm text-muted">
              No current signal candidates were generated. Run the derived jobs to refresh rollups, evidence, and guidance.
            </div>
          ) : (
            visibleSignals.map((signal) => {
              const signalInsights = guidanceHistoryForSignal(signal, guidance)
              const signalGuidance = signalInsights[0]
              const signalEvidence = evidenceForSignal(signal, evidence, signalGuidance)
              const nextSteps = signalGuidance?.recommended_next_steps.filter((step) => step.trim().length > 0).slice(0, 4) ?? []
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

                  <RecentInsightsTable insights={signalInsights} />
                </article>
              )
            })
          )}
        </div>
      </div>
    </section>
  )
}
