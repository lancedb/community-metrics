'use client'

import { useEffect, useMemo, useState } from 'react'
import { fetchDashboard } from './api'
import { Download30dTable } from './components/Download30dTable'
import { SectionPanel } from './components/SectionPanel'
import type { DashboardMetric, DashboardResponse } from './types'

export default function App() {
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    fetchDashboard(180)
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

  const generatedAt = useMemo(() => {
    if (!data) return null
    return new Date(data.generated_at).toLocaleString()
  }, [data])

  const sections = useMemo(() => {
    if (!data) {
      return {
        starItems: [] as DashboardMetric[],
        lancedbDownloads: [] as DashboardMetric[],
        lanceDownloads: [] as DashboardMetric[],
      }
    }

    const lanceGroup = data.groups.find((group) => group.product === 'lance')
    const lancedbGroup = data.groups.find((group) => group.product === 'lancedb')

    const lanceStar = lanceGroup?.items.find((item) => item.metric_id === 'stars:lance:github') ?? null
    const lancedbStar = lancedbGroup?.items.find((item) => item.metric_id === 'stars:lancedb:github') ?? null

    const totalStarsItem: DashboardMetric | null =
      data.total_stars !== null
        ? {
            metric_id: 'stars:total:github',
            display_name: 'Total Stars',
            metric_family: 'stars',
            sdk: null,
            subject: 'lance-format/lance + lancedb/lancedb',
            latest_value: data.total_stars,
            latest_period_end: null,
            latest_provenance: 'computed_total',
            total_stars: data.total_stars,
            sparkline: data.total_stars_sparkline,
          }
        : null

    const starItems = [lancedbStar, lanceStar, totalStarsItem].filter((item): item is DashboardMetric => item !== null)

    const sdkOrder: Record<string, number> = {
      python: 0,
      nodejs: 1,
      rust: 2,
    }

    const sortDownloads = (items: DashboardMetric[]) =>
      items.sort((a, b) => {
        const aKey = (a.sdk ?? '').toLowerCase()
        const bKey = (b.sdk ?? '').toLowerCase()
        return (sdkOrder[aKey] ?? 99) - (sdkOrder[bKey] ?? 99)
      })

    const lancedbDownloads = sortDownloads(
      (lancedbGroup?.items ?? []).filter((item) => item.metric_family === 'downloads'),
    )
    const lanceDownloads = sortDownloads((lanceGroup?.items ?? []).filter((item) => item.metric_family === 'downloads'))

    return { starItems, lancedbDownloads, lanceDownloads }
  }, [data])

  return (
    <div className="min-h-screen bg-canvas text-ink">
      <header
        className="w-full border-b border-edge"
        style={{ background: 'linear-gradient(90deg, #e4d8f8 0%, #F0B7C1 45%, #E55A2B 100%)' }}
      >
        <div className="mx-auto w-full max-w-7xl px-4 py-8 sm:px-8">
          <p className="text-sm font-semibold uppercase tracking-[0.2em] text-[#4f3042]">DevRel Dashboard</p>
          <h1 className="mt-1 text-3xl font-extrabold tracking-tight text-[#2d1a14] sm:text-4xl">
            LanceDB Community Metrics
          </h1>
          <p className="mt-2 text-sm text-[#53342b]">
            Downloads, usage & growth tracking for Lance format and LanceDB SDK adoption.
          </p>
          {generatedAt && <p className="mt-4 text-xs text-[#5f3d33]">Last refreshed: {generatedAt}</p>}
        </div>
      </header>

      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-8 sm:px-8">

        {error && (
          <div className="rounded-lg border border-down bg-down-bg p-4 text-sm text-down">{error}</div>
        )}

        {!data && !error && (
          <div className="rounded-lg border border-edge bg-panel p-4 text-sm text-muted">Loading metrics...</div>
        )}

        {data && (
          <>
            <Download30dTable
              lanceMetrics={sections.lanceDownloads}
              lancedbMetrics={sections.lancedbDownloads}
              maxDaysBack={90}
            />
            <SectionPanel
              title="LanceDB Download Stats"
              subtitle="Monthly total SDK download trends for LanceDB across NodeJS, Python, and Rust. Card value = last full month total."
              items={sections.lancedbDownloads}
              emphasized
            />
            <SectionPanel
              title="Lance Download Stats"
              subtitle="Monthly total SDK download trends for Lance format packages. Card value = last full month total."
              items={sections.lanceDownloads}
              emphasized
            />
            <SectionPanel
              title="Star Histories"
              subtitle="GitHub stars for Lance, LanceDB, and the combined total."
              items={sections.starItems}
            />
          </>
        )}
      </main>
    </div>
  )
}
