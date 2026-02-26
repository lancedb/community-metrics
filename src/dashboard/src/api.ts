import type { DashboardResponse } from './types'

export async function fetchDashboard(days = 180): Promise<DashboardResponse> {
  const response = await fetch(`/api/v1/dashboard/daily?days=${days}`)
  if (!response.ok) {
    throw new Error(`Failed to load dashboard: ${response.status}`)
  }
  return (await response.json()) as DashboardResponse
}
