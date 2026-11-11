'use client'

import {
  AlertTriangleIcon,
  ArrowDownRightIcon,
  ArrowUpRightIcon,
  BarChart3Icon,
  DatabaseIcon,
  DownloadIcon,
  LightbulbIcon,
  LogOutIcon,
  PackageIcon,
  StarIcon,
  TrendingUpIcon,
} from 'lucide-react'
import { signOut } from 'next-auth/react'
import { useEffect, useMemo, useState, type ReactNode } from 'react'

import { ThemeToggle } from '@/components/theme'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { formatCompact, formatDateTime, formatInt, formatMonth, formatPercent } from '@/lib/format'
import { cn } from '@/lib/utils'

import { fetchDashboard } from './api'
import { Download30dTable } from './components/Download30dTable'
import { DownloadMovers, moverName, topMovers } from './components/DownloadMovers'
import { DownloadSnapshotChart } from './components/DownloadSnapshotChart'
import { DuckDBExtensionDownloadsWidget } from './components/DuckDBExtensionDownloadsWidget'
import { Insights } from './components/Insights'
import { MetricCard } from './components/MetricCard'
import { SectionPanel } from './components/SectionPanel'
import type { DashboardMetric, DashboardResponse } from './types'

type DashboardTab = 'charts' | 'insights'

const TABS: Array<{ id: DashboardTab; label: string; icon: ReactNode }> = [
  { id: 'charts', label: 'Charts & Downloads', icon: <BarChart3Icon aria-hidden /> },
  { id: 'insights', label: 'Insights', icon: <LightbulbIcon aria-hidden /> },
]

// One color per SDK / repo, used consistently across cards.
const SDK_COLORS: Record<string, string> = {
  python: 'var(--chart-1)',
  nodejs: 'var(--chart-3)',
  rust: 'var(--chart-2)',
}
const STAR_COLORS: Record<string, string> = {
  'stars:lance:github': 'var(--chart-2)',
  'stars:lancedb:github': 'var(--chart-1)',
  'stars:lance-graph:github': 'var(--chart-4)',
  'stars:lance-context:github': 'var(--chart-5)',
  'stars:total:github': 'var(--chart-3)',
}

function useDashboardSections(data: DashboardResponse | null) {
  return useMemo(() => {
    if (!data) {
      return {
        starItems: [] as DashboardMetric[],
        lancedbDownloads: [] as DashboardMetric[],
        lanceDownloads: [] as DashboardMetric[],
      }
    }

    const lanceGroup = data.groups.find((group) => group.product === 'lance')
    const lancedbGroup = data.groups.find((group) => group.product === 'lancedb')
    const starOrder: Record<string, number> = {
      'stars:lance:github': 0,
      'stars:lancedb:github': 1,
      'stars:lance-graph:github': 2,
      'stars:lance-context:github': 3,
    }
    const starItems = data.groups
      .flatMap((group) => group.items.filter((item) => item.metric_family === 'stars'))
      .sort((a, b) => (starOrder[a.metric_id] ?? 99) - (starOrder[b.metric_id] ?? 99) || a.subject.localeCompare(b.subject))

    if (data.total_stars !== null) {
      starItems.push({
        metric_id: 'stars:total:github',
        display_name: 'Total Stars',
        metric_family: 'stars',
        sdk: null,
        subject: 'Lance and LanceDB family of projects',
        latest_value: data.total_stars,
        latest_period_end: null,
        latest_provenance: 'computed_total',
        total_stars: data.total_stars,
        sparkline: data.total_stars_sparkline,
      })
    }

    const sdkOrder: Record<string, number> = { python: 0, nodejs: 1, rust: 2 }
    const sortDownloads = (items: DashboardMetric[]) =>
      [...items].sort(
        (a, b) => (sdkOrder[(a.sdk ?? '').toLowerCase()] ?? 99) - (sdkOrder[(b.sdk ?? '').toLowerCase()] ?? 99),
      )

    return {
      starItems,
      lancedbDownloads: sortDownloads((lancedbGroup?.items ?? []).filter((item) => item.metric_family === 'downloads')),
      lanceDownloads: sortDownloads((lanceGroup?.items ?? []).filter((item) => item.metric_family === 'downloads')),
    }
  }, [data])
}

function KpiTile({
  icon,
  label,
  value,
  detail,
  tone,
}: {
  icon: ReactNode
  label: string
  value: string
  detail?: ReactNode
  tone?: 'up' | 'down'
}) {
  return (
    <div className="relative overflow-hidden rounded-xl border bg-card/70 p-4 backdrop-blur-sm">
      <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wider text-muted-foreground [&_svg]:size-3.5">
        {icon}
        {label}
      </div>
      <p
        className={cn(
          'mt-2 truncate text-2xl font-semibold tracking-tight tabular sm:text-[28px]',
          tone === 'up' ? 'text-up' : tone === 'down' ? 'text-down' : 'text-foreground',
        )}
      >
        {value}
      </p>
      {detail && <p className="mt-1 truncate text-xs text-muted-foreground">{detail}</p>}
    </div>
  )
}

function KpiStrip({ data }: { data: DashboardResponse }) {
  const window = data.last_30d_download_totals
  const duckdb = data.duckdb_lance_extension_downloads.at(-1)
  const stars = data.total_stars_sparkline
  const starDelta = stars.length >= 2 ? stars[stars.length - 1].value - stars[0].value : null
  const mover = topMovers(data.metric_rollups)[0]

  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <KpiTile
        icon={<DownloadIcon aria-hidden />}
        label="Downloads · 30d"
        value={formatCompact(window.lance + window.lancedb)}
        detail={`LanceDB ${formatCompact(window.lancedb)} · Lance ${formatCompact(window.lance)}`}
      />
      <KpiTile
        icon={<StarIcon aria-hidden />}
        label="GitHub stars"
        value={data.total_stars === null ? 'N/A' : formatInt(data.total_stars)}
        detail={starDelta === null ? 'All four repos' : `${starDelta >= 0 ? '+' : ''}${formatInt(starDelta)} in the chart window`}
      />
      <KpiTile
        icon={<DatabaseIcon aria-hidden />}
        label="DuckDB extension"
        value={duckdb ? formatCompact(duckdb.total_downloads) : 'N/A'}
        detail={duckdb ? `${formatMonth(duckdb.month_start)}${duckdb.is_partial_month ? ' so far' : ''}` : 'No data yet'}
      />
      <KpiTile
        icon={mover && mover.percent_change < 0 ? <ArrowDownRightIcon aria-hidden /> : <ArrowUpRightIcon aria-hidden />}
        label="Top 7d mover"
        value={mover ? formatPercent(mover.percent_change) : 'N/A'}
        tone={mover ? (mover.percent_change >= 0 ? 'up' : 'down') : undefined}
        detail={mover ? moverName(mover.metric_id) : 'No rollups yet'}
      />
    </div>
  )
}

function MetricGrid({ items, colorFor, columns = 3 }: { items: DashboardMetric[]; colorFor: (item: DashboardMetric) => string; columns?: 2 | 3 }) {
  return (
    <div className={cn('grid gap-4 sm:grid-cols-2', columns === 3 ? 'xl:grid-cols-3' : '')}>
      {items.map((item) => (
        <MetricCard
          key={item.metric_id}
          metric={item}
          color={colorFor(item)}
          highlight={item.metric_id === 'stars:total:github'}
        />
      ))}
    </div>
  )
}

function LoadingState() {
  return (
    <div className="space-y-6" aria-busy="true" aria-label="Loading metrics">
      <p className="sr-only">Loading metrics...</p>
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {Array.from({ length: 4 }, (_, index) => (
          <Skeleton key={index} className="h-[104px] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-10 w-72 rounded-lg" />
      <Skeleton className="h-64 rounded-2xl" />
      <Skeleton className="h-96 rounded-2xl" />
    </div>
  )
}

export default function App() {
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<DashboardTab>('charts')
  const sections = useDashboardSections(data)

  useEffect(() => {
    let cancelled = false
    fetchDashboard()
      .then((result) => {
        if (!cancelled) setData(result)
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Unknown error')
      })
    return () => {
      cancelled = true
    }
  }, [])

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-30 border-b bg-background/75 backdrop-blur-md">
        <div className="mx-auto flex h-14 w-full max-w-7xl items-center justify-between gap-3 px-4 sm:px-6 lg:px-8">
          <div className="flex min-w-0 items-center gap-3">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/lancedb-logo.png"
              alt="LanceDB"
              width={390}
              height={91}
              className="h-6 w-auto invert dark:invert-0"
            />
            <span className="hidden h-5 w-px bg-border sm:block" aria-hidden />
            <span className="hidden font-mono text-xs uppercase tracking-widest text-muted-foreground sm:block">
              DevRel
            </span>
          </div>
          <div className="flex items-center gap-1">
            <ThemeToggle />
            <Button type="button" variant="ghost" size="sm" onClick={() => signOut({ callbackUrl: '/signin' })}>
              <LogOutIcon aria-hidden className="hidden sm:block" />
              Sign out
            </Button>
          </div>
        </div>
      </header>

      <main className="mx-auto w-full max-w-7xl px-4 pb-16 pt-8 sm:px-6 lg:px-8">
        <div className="mb-8 flex flex-wrap items-end justify-between gap-4">
          <div className="min-w-0">
            <p className="mb-2 inline-flex items-center gap-2 rounded-full border bg-card/60 px-3 py-1 font-mono text-[11px] uppercase tracking-widest text-muted-foreground">
              <span className="size-1.5 rounded-full bg-primary" aria-hidden />
              Community metrics
            </p>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground sm:text-4xl">LanceDB Community Metrics</h1>
            <p className="mt-2 max-w-2xl text-sm text-muted-foreground sm:text-base">
              Downloads, usage and growth tracking for Lance format and LanceDB SDK adoption.
            </p>
          </div>
          {data && (
            <p className="font-mono text-xs text-muted-foreground">
              Last refreshed: {formatDateTime(data.generated_at)}
            </p>
          )}
        </div>

        {error && (
          <div
            role="alert"
            className="flex items-start gap-3 rounded-xl border border-down/40 bg-down-soft p-4 text-sm text-down"
          >
            <AlertTriangleIcon aria-hidden className="mt-0.5 size-4 shrink-0" />
            <div>
              <p className="font-medium">Could not load the dashboard</p>
              <p className="mt-0.5 opacity-90">{error}</p>
            </div>
          </div>
        )}

        {!data && !error && <LoadingState />}

        {data && (
          <div className="space-y-6">
            <KpiStrip data={data} />

            <div
              className="inline-flex w-full rounded-xl border bg-card/60 p-1 sm:w-auto"
              role="group"
              aria-label="Dashboard view"
            >
              {TABS.map((tab) => {
                const isActive = activeTab === tab.id
                return (
                  <button
                    key={tab.id}
                    type="button"
                    aria-pressed={isActive}
                    onClick={() => setActiveTab(tab.id)}
                    className={cn(
                      'inline-flex flex-1 items-center justify-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors sm:flex-none [&_svg]:size-4',
                      isActive
                        ? 'bg-primary text-primary-foreground shadow-sm'
                        : 'text-muted-foreground hover:bg-accent hover:text-foreground',
                    )}
                  >
                    {tab.icon}
                    {tab.label}
                  </button>
                )
              })}
            </div>

            {activeTab === 'charts' && (
              <>
                <Download30dTable initialTotals={data.last_30d_download_totals} maxDaysBack={90} />
                <DownloadSnapshotChart points={data.monthly_download_snapshots} />
                <SectionPanel
                  title="LanceDB Download Stats"
                  icon={<PackageIcon />}
                  description="Monthly SDK downloads for LanceDB across Python, NodeJS and Rust. Card value = last full month total."
                >
                  <MetricGrid
                    items={sections.lancedbDownloads}
                    colorFor={(item) => SDK_COLORS[(item.sdk ?? '').toLowerCase()] ?? 'var(--chart-1)'}
                  />
                </SectionPanel>
                <SectionPanel
                  title="Lance Download Stats"
                  icon={<PackageIcon />}
                  description="Monthly SDK downloads for the Lance format packages. Card value = last full month total."
                >
                  <MetricGrid
                    items={sections.lanceDownloads}
                    columns={2}
                    colorFor={(item) => SDK_COLORS[(item.sdk ?? '').toLowerCase()] ?? 'var(--chart-1)'}
                  />
                </SectionPanel>
                <DuckDBExtensionDownloadsWidget points={data.duckdb_lance_extension_downloads} />
                <SectionPanel
                  title="Star Histories"
                  icon={<StarIcon />}
                  description="GitHub stars for Lance, LanceDB, Lance Graph, Lance Context, and the combined total."
                >
                  <MetricGrid items={sections.starItems} colorFor={(item) => STAR_COLORS[item.metric_id] ?? 'var(--chart-1)'} />
                </SectionPanel>
              </>
            )}

            {activeTab === 'insights' && (
              <>
                <DownloadMovers rollups={data.metric_rollups} />
                <Insights
                  signals={data.signal_candidates}
                  guidance={data.signal_guidance}
                  evidence={data.recent_evidence}
                />
              </>
            )}
          </div>
        )}
      </main>

      <footer className="border-t">
        <div className="mx-auto flex w-full max-w-7xl flex-wrap items-center justify-between gap-2 px-4 py-6 font-mono text-[11px] text-muted-foreground sm:px-6 lg:px-8">
          <span>Sources: PyPI, npm, crates.io, GitHub, DuckDB, Hacker News</span>
          <span className="inline-flex items-center gap-1.5">
            <TrendingUpIcon aria-hidden className="size-3" />
            Data refreshes daily at 11:00 UTC
          </span>
        </div>
      </footer>
    </div>
  )
}
