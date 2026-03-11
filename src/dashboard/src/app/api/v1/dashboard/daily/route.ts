import { getServerSession } from 'next-auth'
import { NextRequest, NextResponse } from 'next/server'

import { authOptions, isAllowedLanceDbEmail } from '@/lib/auth'
import { buildDashboardData, fetchDownloadTotalsForWindow } from '@/lib/dashboard-data'

export const runtime = 'nodejs'

export async function GET(request: NextRequest) {
  try {
    const session = await getServerSession(authOptions)
    if (!isAllowedLanceDbEmail(session?.user?.email)) {
      return NextResponse.json({ detail: 'Unauthorized' }, { status: 401 })
    }

    const responseMode = request.nextUrl.searchParams.get('response')
    if (responseMode === 'download_window_totals') {
      const windowStart = request.nextUrl.searchParams.get('window_start')
      const windowEnd = request.nextUrl.searchParams.get('window_end')
      if (!windowStart || !windowEnd) {
        return NextResponse.json(
          { detail: 'Missing required query params: window_start and window_end' },
          { status: 400 },
        )
      }
      const payload = await fetchDownloadTotalsForWindow(windowStart, windowEnd)
      return NextResponse.json(payload)
    }

    const daysRaw = request.nextUrl.searchParams.get('days')
    const days = daysRaw === null ? null : Number(daysRaw)
    const payload = await buildDashboardData(days)
    return NextResponse.json(payload)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to load dashboard data'
    const status = message.startsWith('Invalid ') ? 400 : 500
    return NextResponse.json({ detail: message }, { status })
  }
}
