import { NextResponse } from 'next/server'
import { withAuth } from 'next-auth/middleware'

import { isAllowedLanceDbEmail, isLocalAuthDisabled } from '@/lib/auth-policy'

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

  return authMiddleware(...args)
}

export const config = {
  matcher: ['/((?!api/auth|_next/static|_next/image|favicon.ico|signin).*)'],
}
