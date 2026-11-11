'use client'

import { DatabaseIcon } from 'lucide-react'
import { Bar, BarChart, CartesianGrid, Cell, XAxis, YAxis } from 'recharts'

import { ChartContainer, ChartTooltip, type ChartConfig } from '@/components/ui/chart'
import { formatCompact, formatInt, formatMonth, formatMonthTick } from '@/lib/format'
import type { DuckDBLanceExtensionDownloadPoint } from '@/types'

import { ChartDataTable } from './ChartDataTable'
import { SectionPanel } from './SectionPanel'

type DuckDBExtensionDownloadsWidgetProps = {
  points: DuckDBLanceExtensionDownloadPoint[]
}

const chartConfig = {
  community_downloads: { label: 'Community', color: 'var(--chart-2)' },
  core_downloads: { label: 'Core', color: 'var(--chart-1)' },
} satisfies ChartConfig

const SERIES = [
  { key: 'community_downloads', label: 'Community' },
  { key: 'core_downloads', label: 'Core' },
] as const

// Bars stack bottom-up in SERIES order, so the tooltip and legend list them top-down.
const SERIES_TOP_DOWN = [...SERIES].reverse()

function TooltipBody({ point }: { point: DuckDBLanceExtensionDownloadPoint }) {
  return (
    <div className="grid min-w-44 gap-1.5 rounded-lg border bg-popover px-3 py-2 text-xs shadow-xl">
      <p className="font-medium text-foreground">
        {formatMonth(point.month_start)}
        {point.is_partial_month && <span className="ml-1.5 text-muted-foreground">(partial)</span>}
      </p>
      {SERIES_TOP_DOWN.map((series) => (
        <div key={series.key} className="flex items-center justify-between gap-6">
          <span className="flex items-center gap-1.5 text-muted-foreground">
            <span className="size-2 rounded-[2px]" style={{ background: `var(--color-${series.key})` }} />
            {series.label}
          </span>
          <span className="text-foreground tabular">{formatInt(point[series.key])}</span>
        </div>
      ))}
      <div className="flex items-center justify-between gap-6 border-t pt-1.5">
        <span className="text-muted-foreground">Total</span>
        <span className="font-medium text-foreground tabular">{formatInt(point.total_downloads)}</span>
      </div>
    </div>
  )
}

export function DuckDBExtensionDownloadsWidget({ points }: DuckDBExtensionDownloadsWidgetProps) {
  if (points.length === 0) return null

  const latest = points[points.length - 1]
  const periodLabel = latest.is_partial_month ? 'This month' : 'Last full month'

  return (
    <SectionPanel
      title="Lance-DuckDB Extension Downloads"
      icon={<DatabaseIcon />}
      description="Monthly downloads of the DuckDB lance extension from the core and community repositories, starting January 2026."
    >
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            {periodLabel} · {formatMonth(latest.month_start)}
          </p>
          <p className="mt-1 text-4xl font-semibold tracking-tight text-foreground tabular">
            {formatInt(latest.total_downloads)}
          </p>
        </div>
        <div className="flex items-center gap-5 text-sm" aria-label="Legend">
          {SERIES_TOP_DOWN.map((series) => (
            <div key={series.key} className="flex items-center gap-2">
              <span className="size-2.5 rounded-[3px]" style={{ background: chartConfig[series.key].color }} />
              <span className="text-muted-foreground">{series.label}</span>
              <span className="font-medium text-foreground tabular">{formatCompact(latest[series.key])}</span>
            </div>
          ))}
        </div>
      </div>

      {points.length < 2 ? (
        <p className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">
          Need at least 2 monthly points for the DuckDB extension trend.
        </p>
      ) : (
        <ChartContainer data-testid="chart" config={chartConfig} className="aspect-auto h-64 w-full sm:h-72">
          <BarChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 4 }} accessibilityLayer>
            <CartesianGrid vertical={false} strokeDasharray="3 4" />
            <XAxis
              dataKey="month_start"
              tickLine={false}
              axisLine={false}
              tickMargin={10}
              minTickGap={8}
              tickFormatter={formatMonthTick}
              className="text-[11px]"
            />
            <YAxis
              width={44}
              tickLine={false}
              axisLine={false}
              tickCount={5}
              tickFormatter={(value: number) => formatCompact(value)}
              className="text-[11px]"
            />
            <ChartTooltip
              cursor={{ fill: 'var(--muted)', opacity: 0.5 }}
              content={({ active, payload }) =>
                active && payload?.length ? (
                  <TooltipBody point={payload[0].payload as DuckDBLanceExtensionDownloadPoint} />
                ) : null
              }
            />
            {SERIES.map((series, index) => (
              <Bar
                key={series.key}
                dataKey={series.key}
                stackId="downloads"
                fill={`var(--color-${series.key})`}
                radius={index === SERIES.length - 1 ? [6, 6, 0, 0] : [0, 0, 0, 0]}
                maxBarSize={56}
                animationDuration={500}
              >
                {points.map((point) => (
                  <Cell key={point.month_start} fillOpacity={point.is_partial_month ? 0.45 : 1} />
                ))}
              </Bar>
            ))}
          </BarChart>
        </ChartContainer>
      )}
      {latest.is_partial_month && (
        <p className="mt-3 text-xs text-muted-foreground">
          Faded bar: {formatMonth(latest.month_start)} so far. DuckDB publishes weekly counts; this updates on the 1st of
          each month.
        </p>
      )}

      <ChartDataTable
        caption="DuckDB lance extension downloads by month"
        columns={[
          { label: 'Month', value: (point: DuckDBLanceExtensionDownloadPoint) => point.month_label },
          { label: 'Community', value: (point: DuckDBLanceExtensionDownloadPoint) => point.community_downloads },
          { label: 'Core', value: (point: DuckDBLanceExtensionDownloadPoint) => point.core_downloads },
          { label: 'Total', value: (point: DuckDBLanceExtensionDownloadPoint) => point.total_downloads },
        ]}
        rows={points}
      />
    </SectionPanel>
  )
}
