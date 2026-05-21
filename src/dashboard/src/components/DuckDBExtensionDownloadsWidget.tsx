import { useMemo, useState } from 'react'
import type { DuckDBLanceExtensionDownloadPoint } from '../types'

type DuckDBExtensionDownloadsWidgetProps = {
  points: DuckDBLanceExtensionDownloadPoint[]
}

type ChartPoint = {
  x: number
  communityY: number
  coreY: number
  communityValue: number
  coreValue: number
  monthLabel: string
  total: number
}

function formatInt(value: number): string {
  return Intl.NumberFormat('en-US').format(value)
}

function formatCompact(value: number): string {
  return Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(value)
}

function CombinedChart({ points }: { points: DuckDBLanceExtensionDownloadPoint[] }) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null)

  const width = 640
  const height = 220
  const margin = { top: 20, right: 22, bottom: 40, left: 52 }
  const plotWidth = width - margin.left - margin.right
  const plotHeight = height - margin.top - margin.bottom

  const prepared = useMemo(() => {
    if (points.length < 2) return null
    const values = points.flatMap((point) => [point.community_downloads, point.core_downloads])
    const max = Math.max(...values, 1)
    const step = plotWidth / (points.length - 1)
    const chartPoints: ChartPoint[] = points.map((point, index) => {
      return {
        x: margin.left + index * step,
        communityY: margin.top + (1 - point.community_downloads / max) * plotHeight,
        coreY: margin.top + (1 - point.core_downloads / max) * plotHeight,
        communityValue: point.community_downloads,
        coreValue: point.core_downloads,
        monthLabel: point.month_label,
        total: point.total_downloads,
      }
    })
    return {
      max,
      step,
      chartPoints,
      communityLine: chartPoints.map((point) => `${point.x},${point.communityY}`).join(' '),
      coreLine: chartPoints.map((point) => `${point.x},${point.coreY}`).join(' '),
      startLabel: points[0].month_label,
      endLabel: points[points.length - 1].month_label,
    }
  }, [plotHeight, plotWidth, points])

  if (!prepared) {
    return (
      <div className="rounded-lg border border-edge bg-panel p-4 text-sm text-muted">
        Need at least 2 monthly points for the DuckDB extension trend.
      </div>
    )
  }

  const hovered = hoverIndex === null ? null : prepared.chartPoints[hoverIndex]

  return (
    <div className="rounded-lg border border-edge bg-panel p-4">
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-sm font-bold text-ink">Core vs. Community</p>
          <p className="text-xs text-muted">
            {hovered
              ? `${hovered.monthLabel} · Community ${formatInt(hovered.communityValue)} · Core ${formatInt(hovered.coreValue)} · Total ${formatInt(hovered.total)}`
              : `max ${formatCompact(prepared.max)}`}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-3 text-xs font-semibold text-muted">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-2.5 rounded-full bg-brand-strong" />
            Community
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-2.5 rounded-full bg-up" />
            Core
          </span>
        </div>
      </div>

      <svg
        className="h-56 w-full"
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label="Community and core monthly DuckDB lance extension downloads"
        onMouseLeave={() => setHoverIndex(null)}
        onMouseMove={(event) => {
          const rect = event.currentTarget.getBoundingClientRect()
          const xPx = event.clientX - rect.left
          const x = (xPx / rect.width) * width
          const index = Math.round((x - margin.left) / prepared.step)
          setHoverIndex(Math.max(0, Math.min(prepared.chartPoints.length - 1, index)))
        }}
      >
        {[0, 0.5, 1].map((ratio) => {
          const y = margin.top + (1 - ratio) * plotHeight
          return (
            <g key={ratio}>
              <line x1={margin.left} x2={width - margin.right} y1={y} y2={y} stroke="var(--color-edge)" />
              <text x={margin.left - 10} y={y + 4} textAnchor="end" fontSize="11" className="fill-muted">
                {formatCompact(Math.round(prepared.max * ratio))}
              </text>
            </g>
          )
        })}
        <line
          x1={margin.left}
          x2={width - margin.right}
          y1={height - margin.bottom}
          y2={height - margin.bottom}
          stroke="var(--color-edge)"
        />
        <polyline
          fill="none"
          stroke="var(--color-brand-strong)"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3"
          points={prepared.communityLine}
        />
        <polyline
          fill="none"
          stroke="var(--color-up)"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3"
          points={prepared.coreLine}
        />
        {prepared.chartPoints.map((point, index) => (
          <g key={point.monthLabel}>
            <circle
              cx={point.x}
              cy={point.communityY}
              r={hoverIndex === index ? 4 : 2.5}
              fill="#fffdfb"
              stroke="var(--color-brand-strong)"
              strokeWidth="1.75"
            />
            <circle
              cx={point.x}
              cy={point.coreY}
              r={hoverIndex === index ? 4 : 2.5}
              fill="#fffdfb"
              stroke="var(--color-up)"
              strokeWidth="1.75"
            />
          </g>
        ))}
        {hovered && (
          <>
            <line
              x1={hovered.x}
              x2={hovered.x}
              y1={margin.top}
              y2={height - margin.bottom}
              stroke="var(--color-muted)"
              strokeDasharray="3 4"
            />
            <circle cx={hovered.x} cy={hovered.communityY} r="4" fill="var(--color-brand-strong)" />
            <circle cx={hovered.x} cy={hovered.coreY} r="4" fill="var(--color-up)" />
          </>
        )}
        <text x={margin.left} y={height - 8} textAnchor="start" fontSize="11" className="fill-muted">
          {prepared.startLabel}
        </text>
        <text x={width - margin.right} y={height - 8} textAnchor="end" fontSize="11" className="fill-muted">
          {prepared.endLabel}
        </text>
      </svg>
    </div>
  )
}

export function DuckDBExtensionDownloadsWidget({ points }: DuckDBExtensionDownloadsWidgetProps) {
  if (points.length === 0) return null

  const latest = points[points.length - 1]
  const periodLabel = latest.is_partial_month ? 'this month' : 'last full month'

  return (
    <section className="space-y-4 rounded-lg border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="border-b border-edge pb-3">
        <h2 className="text-2xl font-bold text-ink">Lance-DuckDB Extension Downloads</h2>
        <p className="text-sm text-muted">Monthly downloads for the DuckDB lance extension, starting January 2026.</p>
      </header>

      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">{periodLabel}</p>
          <p className="text-3xl font-bold leading-none text-ink">{formatInt(latest.total_downloads)}</p>
        </div>
        <p className="text-sm font-semibold text-brand-strong">
          Core: {formatCompact(latest.core_downloads)} · Community: {formatCompact(latest.community_downloads)}
        </p>
      </div>

      <CombinedChart points={points} />
    </section>
  )
}
