import type { SparkPoint } from '../types'

type SparklineProps = {
  points: SparkPoint[]
  positive?: boolean
}

export function Sparkline({ points, positive = true }: SparklineProps) {
  if (points.length < 2) {
    return <div className="h-12 rounded-md bg-brand-soft" />
  }

  const width = 180
  const height = 48
  const values = points.map((p) => p.value)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const spread = Math.max(1, max - min)

  const coords = points.map((point, index) => {
    const x = (index / (points.length - 1)) * width
    const y = height - ((point.value - min) / spread) * height
    return `${x},${y}`
  })

  const stroke = positive ? 'var(--color-brand-strong)' : 'var(--color-down)'

  return (
    <svg
      className="h-12 w-full"
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      aria-label="Trend sparkline"
    >
      <polyline
        fill="none"
        stroke={stroke}
        strokeWidth="2.25"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={coords.join(' ')}
      />
    </svg>
  )
}
