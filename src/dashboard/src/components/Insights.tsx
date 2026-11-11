import { CheckIcon, ExternalLinkIcon, QuoteIcon, SparklesIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatDate, humanize } from '@/lib/format'
import type { DashboardEvidenceItem, DashboardSignalCandidate, DashboardSignalGuidance } from '@/types'

import { SectionPanel } from './SectionPanel'

type InsightsProps = {
  signals: DashboardSignalCandidate[]
  guidance: DashboardSignalGuidance[]
  evidence: DashboardEvidenceItem[]
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
    ...(guidance?.citations
      .filter((citation) => citation.source_type === 'evidence')
      .map((citation) => citation.source_id) ?? []),
  ])
  return evidence.filter((item) => ids.has(item.evidence_id)).slice(0, 3)
}

function SubHeading({ children }: { children: React.ReactNode }) {
  return <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">{children}</p>
}

function RecentInsightsTable({ insights }: { insights: DashboardSignalGuidance[] }) {
  if (insights.length === 0) return null

  return (
    <div className="mt-5 border-t pt-4">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <SubHeading>Recent insights</SubHeading>
        <p className="text-xs text-muted-foreground">Top {insights.length} most recent</p>
      </div>
      <div className="rounded-lg border">
        <Table className="min-w-[640px]">
          <TableHeader>
            <TableRow>
              <TableHead>Generated</TableHead>
              <TableHead>Movement</TableHead>
              <TableHead>Relevance</TableHead>
              <TableHead>Confidence</TableHead>
              <TableHead>Summary</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {insights.map((insight) => (
              <TableRow key={insight.guidance_id}>
                <TableCell className="font-mono text-xs text-muted-foreground">{formatDate(insight.generated_at)}</TableCell>
                <TableCell className="text-xs font-medium">{humanize(insight.movement_assessment)}</TableCell>
                <TableCell className="text-xs text-muted-foreground">{humanize(insight.engineering_relevance)}</TableCell>
                <TableCell className="text-xs text-muted-foreground">{humanize(insight.confidence)}</TableCell>
                <TableCell className="min-w-72 whitespace-normal text-sm">{insight.executive_summary}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}

function SignalCard({
  signal,
  guidance,
  evidence,
}: {
  signal: DashboardSignalCandidate
  guidance: DashboardSignalGuidance[]
  evidence: DashboardEvidenceItem[]
}) {
  const history = guidanceHistoryForSignal(signal, guidance)
  const latest = history[0]
  const linkedEvidence = evidenceForSignal(signal, evidence, latest)
  const nextSteps = latest?.recommended_next_steps.filter((step) => step.trim().length > 0).slice(0, 4) ?? []
  const citedFacts = latest?.citations.filter((citation) => citation.fact.trim().length > 0).slice(0, 3) ?? []

  return (
    <article className="rounded-xl border bg-background/60 p-4 sm:p-5">
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{humanize(signal.signal_type)}</Badge>
        <Badge
          variant="outline"
          className={latest ? 'border-primary/40 text-primary' : 'border-dashed text-muted-foreground'}
        >
          {latest ? humanize(latest.movement_assessment) : 'Guidance pending'}
        </Badge>
        <span className="font-mono text-xs text-muted-foreground">
          {signal.window_start} to {signal.window_end}
        </span>
      </div>

      <h3 className="text-base font-semibold leading-snug text-foreground sm:text-lg">
        {latest?.executive_summary || signal.title}
      </h3>
      <p className="mt-1.5 text-sm leading-relaxed text-muted-foreground">{latest?.why_it_matters || signal.summary}</p>

      {latest && (nextSteps.length > 0 || citedFacts.length > 0) ? (
        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          {nextSteps.length > 0 && (
            <div>
              <SubHeading>Next steps</SubHeading>
              <ul className="space-y-1.5 text-sm text-foreground">
                {nextSteps.map((step) => (
                  <li key={step} className="flex gap-2">
                    <CheckIcon aria-hidden className="mt-0.5 size-4 shrink-0 text-up" />
                    <span>{step}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
          {citedFacts.length > 0 && (
            <div>
              <SubHeading>Cited facts</SubHeading>
              <ul className="space-y-1.5 text-sm text-muted-foreground">
                {citedFacts.map((citation) => (
                  <li key={`${citation.source_type}:${citation.source_id}:${citation.fact}`} className="flex gap-2">
                    <QuoteIcon aria-hidden className="mt-0.5 size-3.5 shrink-0" />
                    <span>{citation.fact}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      ) : !latest ? (
        <p className="mt-4 rounded-lg border border-dashed p-3 text-sm text-muted-foreground">
          LLM guidance has not been generated for this weekly signal yet.
        </p>
      ) : null}

      {linkedEvidence.length > 0 && (
        <div className="mt-5 border-t pt-4">
          <SubHeading>Linked evidence</SubHeading>
          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
            {linkedEvidence.map((item) => (
              <a
                key={item.evidence_id}
                href={item.url}
                target="_blank"
                rel="noreferrer"
                className="group block rounded-lg border bg-card p-3 text-sm transition-colors hover:border-primary/50"
              >
                <span className="flex items-start justify-between gap-2">
                  <span className="font-medium text-foreground group-hover:text-primary">{item.title}</span>
                  <ExternalLinkIcon aria-hidden className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
                </span>
                <span className="mt-0.5 block font-mono text-[11px] text-muted-foreground">
                  {item.source_name} · {formatDate(item.occurred_at)}
                </span>
                <span className="mt-1.5 line-clamp-2 block text-muted-foreground">{item.snippet}</span>
              </a>
            ))}
          </div>
        </div>
      )}

      <RecentInsightsTable insights={history} />
    </article>
  )
}

export function Insights({ signals, guidance, evidence }: InsightsProps) {
  const visibleSignals = signals.slice(0, 5)

  return (
    <SectionPanel
      title="Insights"
      icon={<SparklesIcon />}
      description="Weekly DevRel guidance generated from 7d signals, 15d/30d comparisons, and cited evidence."
      actions={
        <Badge variant="outline" className="font-mono text-[11px] font-normal text-muted-foreground">
          {guidance.length > 0 ? `Guidance generated ${formatDate(guidance[0].generated_at)}` : 'Guidance pending'}
        </Badge>
      }
    >
      {visibleSignals.length === 0 ? (
        <p className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">
          No current signal candidates were generated. Run the derived jobs to refresh rollups, evidence, and guidance.
        </p>
      ) : (
        <div className="space-y-4">
          {visibleSignals.map((signal) => (
            <SignalCard key={signal.signal_id} signal={signal} guidance={guidance} evidence={evidence} />
          ))}
        </div>
      )}
    </SectionPanel>
  )
}
