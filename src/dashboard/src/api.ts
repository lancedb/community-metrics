import type { DashboardResponse, DownloadWindowTotals } from './types'

export async function fetchDashboard(days = 180): Promise<DashboardResponse> {
  const response = await fetch(`/api/v1/dashboard/daily?days=${days}`)
  if (!response.ok) {
    throw new Error(`Failed to load dashboard: ${response.status}`)
  }
  return (await response.json()) as DashboardResponse
}

export async function fetchDownloadWindowTotals(
  windowStart: string,
  windowEnd: string,
): Promise<DownloadWindowTotals> {
  const params = new URLSearchParams({
    response: 'download_window_totals',
    window_start: windowStart,
    window_end: windowEnd,
  })
  const response = await fetch(`/api/v1/dashboard/daily?${params.toString()}`)
  if (!response.ok) {
    throw new Error(`Failed to load download totals: ${response.status}`)
  }
  return (await response.json()) as DownloadWindowTotals
}
