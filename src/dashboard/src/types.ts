export type SparkPoint = {
  period_start: string
  period_end: string
  value: number
}

export type DashboardMetric = {
  metric_id: string
  display_name: string
  metric_family: string
  sdk: string | null
  subject: string
  latest_value: number | null
  latest_period_end: string | null
  latest_provenance: string | null
  total_stars: number | null
  sparkline: SparkPoint[]
}

export type DashboardGroup = {
  product: string
  title: string
  items: DashboardMetric[]
}

export type DashboardResponse = {
  generated_at: string
  days: number
  groups: DashboardGroup[]
  total_stars: number | null
  total_stars_sparkline: SparkPoint[]
  last_30d_download_totals: {
    window_start: string
    window_end: string
    lance: number
    lancedb: number
  }
}
