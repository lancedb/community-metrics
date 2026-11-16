import { NextResponse, type NextRequest } from 'next/server'
import { withAuth } from 'next-auth/middleware'

import {
  getAuthMode,
  isAllowedLanceDbEmail,
  isLocalAuthDisabled,
  TAILSCALE_USER_LOGIN_HEADER,
} from '@/lib/auth-policy'

const authMiddleware = withAuth({
  pages: {
    signIn: '/signin',
  },
  callbacks: {
    authorized: ({ token }) => isAllowedLanceDbEmail(token?.email),
  },
})

export default function middleware(...args: Parameters<typeof authMiddleware>) {
  if (isLocalAuthDisabled()) {
    return NextResponse.next()
  }

  if (getAuthMode() === 'tailscale') {
    return tailscaleMiddleware(args[0])
  }

  return authMiddleware(...args)
}

function tailscaleMiddleware(request: NextRequest) {
  if (isAllowedLanceDbEmail(request.headers.get(TAILSCALE_USER_LOGIN_HEADER))) {
    return NextResponse.next()
  }
  if (request.nextUrl.pathname.startsWith('/api/')) {
    return NextResponse.json({ detail: 'Unauthorized' }, { status: 401 })
  }
  // Rewrite rather than redirect: behind `tailscale serve` the request URL carries the
  // local address, not the tailnet hostname the visitor used.
  return NextResponse.rewrite(new URL('/signin', request.url))
}

export const config = {
  matcher: ['/((?!api/auth|_next/static|_next/image|favicon.ico|signin).*)'],
}
