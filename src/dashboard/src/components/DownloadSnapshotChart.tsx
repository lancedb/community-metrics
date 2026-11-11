'use client'

import { DownloadIcon, LineChartIcon } from 'lucide-react'
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from 'recharts'

import { Button } from '@/components/ui/button'
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from '@/components/ui/chart'
import { formatCompact, formatDay, formatInt, formatMonthTick, formatPercent, formatShortDay } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { DownloadSnapshotPoint } from '@/types'

import { ChartDataTable } from './ChartDataTable'
import { SectionPanel } from './SectionPanel'

type DownloadSnapshotChartProps = {
  points: DownloadSnapshotPoint[]
}

const chartConfig = { total: { label: 'Total downloads', color: 'var(--chart-1)' } } satisfies ChartConfig

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

/** Month-end points read as months; a mid-month (partial) last point reads as its day. */
function tickLabel(periodEnd: string): string {
  const date = new Date(`${periodEnd}T00:00:00Z`)
  const nextDay = new Date(date.getTime() + 86_400_000)
  return nextDay.getUTCDate() === 1 ? formatMonthTick(periodEnd) : formatShortDay(periodEnd)
}

function Stat({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <div className={cn('min-w-0', className)}>
      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold text-foreground tabular">{value}</p>
    </div>
  )
}

export function DownloadSnapshotChart({ points }: DownloadSnapshotChartProps) {
  if (points.length < 2) {
    return (
      <SectionPanel title="Monthly Download Snapshots" icon={<LineChartIcon />}>
        <p className="text-sm text-muted-foreground">Not enough snapshot points to render the download chart.</p>
      </SectionPanel>
    )
  }

  const latest = points[points.length - 1]
  const previous = points[points.length - 2]
  const change = previous.total > 0 ? ((latest.total - previous.total) / previous.total) * 100 : null

  return (
    <SectionPanel
      title="Monthly Download Snapshots"
      icon={<LineChartIcon />}
      description={`Total downloads across Lance and LanceDB packages, through ${formatShortDay(latest.period_end)}.`}
      actions={
        <Button type="button" variant="outline" size="sm" onClick={() => exportSnapshotCsv(points)}>
          <DownloadIcon aria-hidden />
          Export snapshot
        </Button>
      }
    >
      <div className="mb-6 grid grid-cols-2 gap-x-6 gap-y-4 sm:flex sm:flex-wrap sm:items-end">
        <div className="col-span-2 sm:mr-6">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Latest total</p>
          <div className="mt-1 flex items-baseline gap-3">
            <p className="text-4xl font-semibold tracking-tight text-foreground tabular">{formatInt(latest.total)}</p>
            {change !== null && (
              <span
                className={cn(
                  ' text-sm tabular',
                  change >= 0 ? 'text-up' : 'text-down',
                )}
              >
                {formatPercent(change)} vs {formatShortDay(previous.period_end)}
              </span>
            )}
          </div>
        </div>
        <Stat label="LanceDB" value={formatCompact(latest.lancedb)} />
        <Stat label="Lance" value={formatCompact(latest.lance)} />
      </div>

      <ChartContainer data-testid="chart" config={chartConfig} className="aspect-auto h-64 w-full sm:h-80">
        <AreaChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 4 }} accessibilityLayer>
          <defs>
            <linearGradient id="snapshot-fill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--color-total)" stopOpacity={0.4} />
              <stop offset="100%" stopColor="var(--color-total)" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid vertical={false} strokeDasharray="3 4" />
          <XAxis
            dataKey="period_end"
            tickLine={false}
            axisLine={false}
            tickMargin={10}
            minTickGap={28}
            tickFormatter={tickLabel}
            className="text-[11px]"
          />
          <YAxis
            width={52}
            tickLine={false}
            axisLine={false}
            tickCount={5}
            tickFormatter={(value: number) => formatCompact(value)}
            className="text-[11px]"
          />
          <ChartTooltip
            cursor={{ strokeDasharray: '3 3' }}
            content={
              <ChartTooltipContent
                indicator="line"
                labelFormatter={(_, payload) => formatDay(String(payload?.[0]?.payload?.period_end ?? ''))}
                formatter={(value, _name, item) => {
                  const point = item.payload as DownloadSnapshotPoint
                  return (
                    <div className="grid w-full gap-1">
                      <div className="flex items-center justify-between gap-6">
                        <span className="text-muted-foreground">Total</span>
                        <span className="font-medium text-foreground tabular">{formatInt(Number(value))}</span>
                      </div>
                      <div className="flex items-center justify-between gap-6 text-muted-foreground">
                        <span>LanceDB</span>
                        <span className="tabular">{formatCompact(point.lancedb)}</span>
                      </div>
                      <div className="flex items-center justify-between gap-6 text-muted-foreground">
                        <span>Lance</span>
                        <span className="tabular">{formatCompact(point.lance)}</span>
                      </div>
                    </div>
                  )
                }}
              />
            }
          />
          <Area
            dataKey="total"
            type="monotone"
            stroke="var(--color-total)"
            strokeWidth={2.5}
            fill="url(#snapshot-fill)"
            animationDuration={600}
            dot={{ r: 3, strokeWidth: 0, fill: 'var(--color-total)' }}
            activeDot={{ r: 5, strokeWidth: 2, stroke: 'var(--card)' }}
          />
        </AreaChart>
      </ChartContainer>

      <ChartDataTable
        caption="Monthly download snapshots"
        columns={[
          { label: 'Period end', value: (point: DownloadSnapshotPoint) => point.period_end },
          { label: 'Total', value: (point: DownloadSnapshotPoint) => point.total },
        ]}
        rows={points}
      />
    </SectionPanel>
  )
}
