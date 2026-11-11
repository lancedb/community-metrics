'use client'

import { useId } from 'react'
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from 'recharts'

import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from '@/components/ui/chart'
import { formatCompact, formatDay, formatInt, formatMonth, formatMonthTick, formatShortDay } from '@/lib/format'
import type { SparkPoint } from '@/types'

import { ChartDataTable } from './ChartDataTable'

type TrendChartProps = {
  points: SparkPoint[]
  /** Names the chart's data table, e.g. "Python downloads by month". */
  label: string
  /** Monthly points get month labels; daily points get day labels. */
  granularity: 'month' | 'day'
  /** A --chart-N color variable. */
  color?: string
  seriesName: string
}

export function TrendChart({ points, label, granularity, color = 'var(--chart-1)', seriesName }: TrendChartProps) {
  const gradientId = `trend-${useId().replace(/:/g, '')}`
  const config = { value: { label: seriesName, color } } satisfies ChartConfig
  const tickLabel = granularity === 'month' ? formatMonthTick : formatShortDay
  // Compact labels ("7.3K") collapse a narrow range like 7,270-7,279 into one repeated
  // label, so use full numbers unless the series spans a wide range.
  const values = points.map((point) => point.value)
  const spread = values.length ? Math.max(...values) - Math.min(...values) : 0
  const yTick = (value: number) => (spread < 5_000 ? formatInt(value) : formatCompact(value))
  const tooltipLabel = granularity === 'month' ? formatMonth : formatDay

  const dataTable = (
    <ChartDataTable
      caption={label}
      columns={[
        { label: 'Period end', value: (point: SparkPoint) => point.period_end },
        { label: 'Value', value: (point: SparkPoint) => point.value },
      ]}
      rows={points}
    />
  )

  if (points.length < 2) {
    return (
      <div className="flex h-36 items-center justify-center rounded-lg border border-dashed text-xs text-muted-foreground">
        Not enough data for a trend yet
        {dataTable}
      </div>
    )
  }

  return (
    <div>
      <ChartContainer data-testid="chart" config={config} className="aspect-auto h-36 w-full">
        <AreaChart data={points} margin={{ top: 6, right: 6, bottom: 0, left: 0 }} accessibilityLayer>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--color-value)" stopOpacity={0.35} />
              <stop offset="100%" stopColor="var(--color-value)" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid vertical={false} strokeDasharray="3 4" />
          <XAxis
            dataKey="period_end"
            tickLine={false}
            axisLine={false}
            tickMargin={8}
            minTickGap={24}
            tickFormatter={tickLabel}
            className="text-[10px]"
          />
          <YAxis
            width={48}
            tickLine={false}
            axisLine={false}
            tickCount={3}
            allowDecimals={false}
            domain={granularity === 'day' ? ['dataMin', 'dataMax'] : [0, 'auto']}
            tickFormatter={yTick}
            className="text-[10px]"
          />
          <ChartTooltip
            cursor={{ strokeDasharray: '3 3' }}
            content={
              <ChartTooltipContent
                indicator="line"
                labelFormatter={(_, payload) => tooltipLabel(String(payload?.[0]?.payload?.period_end ?? ''))}
                formatter={(value) => (
                  <div className="flex w-full items-center justify-between gap-4">
                    <span className="text-muted-foreground">{seriesName}</span>
                    <span className="font-medium text-foreground tabular">{formatInt(Number(value))}</span>
                  </div>
                )}
              />
            }
          />
          <Area
            dataKey="value"
            type="monotone"
            stroke="var(--color-value)"
            strokeWidth={2}
            fill={`url(#${gradientId})`}
            animationDuration={500}
            dot={granularity === 'month' ? { r: 2.5, strokeWidth: 0, fill: 'var(--color-value)' } : false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: 'var(--card)' }}
          />
        </AreaChart>
      </ChartContainer>
      {dataTable}
    </div>
  )
}
