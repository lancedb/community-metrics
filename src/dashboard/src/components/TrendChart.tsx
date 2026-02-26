import { useMemo, useState } from 'react'
import type { SparkPoint } from '../types'

type TrendChartProps = {
  points: SparkPoint[]
  showMarkers?: boolean
}

type ChartPoint = {
  x: number
  y: number
  value: number
  periodEnd: string
}

function formatValue(value: number): string {
  return Intl.NumberFormat('en-US').format(value)
}

function formatDay(input: string): string {
  const date = new Date(`${input}T00:00:00Z`)
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  })
}

export function TrendChart({ points, showMarkers = true }: TrendChartProps) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null)

  const width = 220
  const height = 92
  const margin = { top: 8, right: 16, bottom: 16, left: 2 }
  const plotWidth = width - margin.left - margin.right
  const plotHeight = height - margin.top - margin.bottom

  const prepared = useMemo(() => {
    if (points.length < 2) return null
    const values = points.map((p) => p.value)
    const min = Math.min(...values)
    const max = Math.max(...values)
    const spread = Math.max(1, max - min)
    const step = plotWidth / (points.length - 1)

    const chartPoints: ChartPoint[] = points.map((point, index) => {
      const x = margin.left + index * step
      const y = margin.top + (1 - (point.value - min) / spread) * plotHeight
      return { x, y, value: point.value, periodEnd: point.period_end }
    })

    return {
      min,
      max,
      chartPoints,
      startLabel: formatDay(points[0].period_end),
      endLabel: formatDay(points[points.length - 1].period_end),
      line: chartPoints.map((p) => `${p.x},${p.y}`).join(' '),
      step,
    }
  }, [points, plotHeight, plotWidth])

  if (!prepared) {
    return (
      <div className="space-y-2">
        <div className="rounded-md bg-brand-soft p-3 text-[11px] text-muted">Need at least 2 points for trend</div>
      </div>
    )
  }

  const hovered = hoverIndex === null ? null : prepared.chartPoints[hoverIndex]
  const stroke = 'var(--color-brand-strong)'

  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[11px] text-muted">
        <span>min {formatValue(prepared.min)} · max {formatValue(prepared.max)}</span>
        <span>
          {hovered ? `${formatDay(hovered.periodEnd)} · ${formatValue(hovered.value)}` : 'Hover for value'}
        </span>
      </div>

      <svg
        className="h-20 w-full rounded-md bg-brand-soft/40"
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        onMouseLeave={() => setHoverIndex(null)}
        onMouseMove={(event) => {
          const rect = event.currentTarget.getBoundingClientRect()
          const xPx = event.clientX - rect.left
          const x = (xPx / rect.width) * width
          const idx = Math.round((x - margin.left) / prepared.step)
          const clamped = Math.max(0, Math.min(prepared.chartPoints.length - 1, idx))
          setHoverIndex(clamped)
        }}
        aria-label="Interactive trend chart"
      >
        <polyline
          fill="none"
          stroke={stroke}
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          points={prepared.line}
        />
        {showMarkers &&
          prepared.chartPoints.map((point, index) => (
            <circle
              key={`${point.periodEnd}-${index}`}
              cx={point.x}
              cy={point.y}
              r="2.25"
              fill="#ffffff"
              stroke={stroke}
              strokeWidth="1.5"
            />
          ))}
        {hovered && (
          <>
            <line x1={hovered.x} y1={margin.top} x2={hovered.x} y2={height - margin.bottom} stroke={stroke} strokeDasharray="2 3" />
            <circle cx={hovered.x} cy={hovered.y} r="3.5" fill={stroke} />
          </>
        )}
      </svg>

      <div className="flex items-center justify-between text-[11px] text-muted">
        <span>{prepared.startLabel}</span>
        <span>{prepared.endLabel}</span>
      </div>
    </div>
  )
}
