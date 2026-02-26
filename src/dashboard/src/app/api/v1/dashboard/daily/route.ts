import { NextRequest, NextResponse } from 'next/server'

import { buildDashboardData } from '@/lib/dashboard-data'

export const runtime = 'nodejs'

export async function GET(request: NextRequest) {
  try {
    const daysRaw = request.nextUrl.searchParams.get('days')
    const days = daysRaw === null ? null : Number(daysRaw)
    const payload = await buildDashboardData(days)
    return NextResponse.json(payload)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to load dashboard data'
    return NextResponse.json({ detail: message }, { status: 500 })
  }
}
