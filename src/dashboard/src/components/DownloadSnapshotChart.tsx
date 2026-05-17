import { useMemo, useState } from 'react'
import type { DownloadSnapshotPoint } from '../types'

type DownloadSnapshotChartProps = {
  points: DownloadSnapshotPoint[]
}

type ChartPoint = {
  x: number
  y: number
  value: number
  periodEnd: string
}

function formatInt(value: number): string {
  return Intl.NumberFormat('en-US').format(value)
}

function formatShortDay(value: string): string {
  const date = new Date(`${value}T00:00:00Z`)
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  })
}

function formatAxisDay(value: string): string {
  const date = new Date(`${value}T00:00:00Z`)
  return `${String(date.getUTCMonth() + 1).padStart(2, '0')}-${date.getUTCFullYear()}`
}

function csvEscape(value: string | number): string {
  const text = String(value)
  if (!/[",\n]/.test(text)) return text
  return `"${text.replace(/"/g, '""')}"`
}

function exportSnapshotCsv(points: DownloadSnapshotPoint[]) {
  const rows = [
    [
      'period_end',
      'lance_python',
      'lance_rust',
      'lancedb_python',
      'lancedb_nodejs',
      'lancedb_rust',
      'python_total',
      'nodejs_total',
      'rust_total',
      'lance_total',
      'lancedb_total',
      'total',
    ],
    ...points.map((point) => [
      point.period_end,
      point.lance_python,
      point.lance_rust,
      point.lancedb_python,
      point.lancedb_nodejs,
      point.lancedb_rust,
      point.python,
      point.nodejs,
      point.rust,
      point.lance,
      point.lancedb,
      point.total,
    ]),
  ]
  const csv = rows.map((row) => row.map(csvEscape).join(',')).join('\n')
  const blob = new Blob([`${csv}\n`], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = 'monthly-download-snapshots.csv'
  document.body.appendChild(link)
  link.click()
  link.remove()
  URL.revokeObjectURL(url)
}

export function DownloadSnapshotChart({ points }: DownloadSnapshotChartProps) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null)

  const width = 760
  const height = 320
  const margin = { top: 24, right: 28, bottom: 76, left: 86 }
  const plotWidth = width - margin.left - margin.right
  const plotHeight = height - margin.top - margin.bottom
  const baseline = margin.top + plotHeight

  const prepared = useMemo(() => {
    if (points.length < 2) return null

    const values = points.map((point) => point.total)
    const maxValue = Math.max(...values)
    const yMax = Math.max(1, Math.ceil(maxValue / 1_000_000) * 1_000_000)
    const step = plotWidth / (points.length - 1)
    const chartPoints: ChartPoint[] = points.map((point, index) => {
      const x = margin.left + index * step
      const y = margin.top + (1 - point.total / yMax) * plotHeight
      return { x, y, value: point.total, periodEnd: point.period_end }
    })
    const linePath = chartPoints
      .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`)
      .join(' ')
    const areaPath = `${linePath} L ${chartPoints[chartPoints.length - 1].x} ${baseline} L ${chartPoints[0].x} ${baseline} Z`
    const ticks = Array.from({ length: 5 }, (_, index) => Math.round((yMax * index) / 4))
    const labelEvery = Math.max(1, Math.ceil(points.length / 10))

    return {
      yMax,
      chartPoints,
      linePath,
      areaPath,
      ticks,
      step,
      labelEvery,
      latest: points[points.length - 1],
    }
  }, [baseline, plotHeight, plotWidth, points])

  if (!prepared) {
    return (
      <section className="rounded-lg border border-edge bg-white/90 p-5 text-sm text-muted backdrop-blur">
        Not enough snapshot points to render the download chart.
      </section>
    )
  }

  const hovered = hoverIndex === null ? null : prepared.chartPoints[hoverIndex]

  return (
    <section className="space-y-4 rounded-lg border border-edge bg-white/90 p-5 backdrop-blur">
      <header className="flex flex-wrap items-start justify-between gap-3 border-b border-edge pb-3">
        <div>
          <h2 className="text-2xl font-bold text-ink">Monthly Download Snapshots</h2>
          <p className="text-sm text-muted">
            Total downloads across Lance and LanceDB packages, through {formatShortDay(prepared.latest.period_end)}.
          </p>
        </div>
        <button
          type="button"
          onClick={() => exportSnapshotCsv(points)}
          className="rounded-md border border-edge bg-panel px-3 py-2 text-sm font-semibold text-ink hover:bg-brand-soft"
        >
          Export snapshot
        </button>
      </header>

      <div className="flex flex-wrap items-center justify-between gap-3 text-sm">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.14em] text-muted">Latest total</p>
          <p className="text-3xl font-bold leading-none text-ink">{formatInt(prepared.latest.total)}</p>
        </div>
        <p className="text-sm font-semibold text-brand-strong">
          {hovered ? `${hovered.periodEnd} · ${formatInt(hovered.value)}` : 'Hover for point value'}
        </p>
      </div>

      <div className="overflow-x-auto">
        <svg
          className="min-w-[640px] rounded-lg bg-panel"
          viewBox={`0 0 ${width} ${height}`}
          role="img"
          aria-label="Area chart of monthly total download snapshots"
          onMouseLeave={() => setHoverIndex(null)}
          onMouseMove={(event) => {
            const rect = event.currentTarget.getBoundingClientRect()
            const xPx = event.clientX - rect.left
            const x = (xPx / rect.width) * width
            const nextIndex = Math.round((x - margin.left) / prepared.step)
            setHoverIndex(Math.max(0, Math.min(prepared.chartPoints.length - 1, nextIndex)))
          }}
        >
          {prepared.ticks.map((tick) => {
            const y = margin.top + (1 - tick / prepared.yMax) * plotHeight
            return (
              <g key={tick}>
                <line x1={margin.left} x2={width - margin.right} y1={y} y2={y} stroke="#f2c6b9" />
                <text x={margin.left - 12} y={y + 4} textAnchor="end" fontSize="12" className="fill-muted">
                  {formatInt(tick)}
                </text>
              </g>
            )
          })}

          <path d={prepared.areaPath} fill="rgba(244, 111, 82, 0.22)" />
          <path
            d={prepared.linePath}
            fill="none"
            stroke="var(--color-brand-strong)"
            strokeWidth="3"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {prepared.chartPoints.map((point, index) => (
            <g key={point.periodEnd}>
              <circle
                cx={point.x}
                cy={point.y}
                r={hoverIndex === index ? 5 : 3}
                fill="#fffdfb"
                stroke="var(--color-brand-strong)"
                strokeWidth="2"
              />
              {index % prepared.labelEvery === 0 || index === prepared.chartPoints.length - 1 ? (
                <text
                  x={point.x}
                  y={height - 34}
                  textAnchor="end"
                  fontSize="10"
                  transform={`rotate(-45 ${point.x} ${height - 34})`}
                  className="fill-muted"
                >
                  {formatAxisDay(point.periodEnd)}
                </text>
              ) : null}
            </g>
          ))}

          {hovered && (
            <>
              <line
                x1={hovered.x}
                x2={hovered.x}
                y1={margin.top}
                y2={baseline}
                stroke="var(--color-brand-strong)"
                strokeDasharray="4 5"
              />
              <circle cx={hovered.x} cy={hovered.y} r="5" fill="var(--color-brand-strong)" />
            </>
          )}
        </svg>
      </div>
    </section>
  )
}
