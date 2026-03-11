import { withAuth } from 'next-auth/middleware'

import { isAllowedLanceDbEmail } from '@/lib/auth'

export default withAuth({
  pages: {
    signIn: '/signin',
  },
  callbacks: {
    authorized: ({ token }) => isAllowedLanceDbEmail(token?.email),
  },
})

export const config = {
  matcher: ['/((?!api/auth|_next/static|_next/image|favicon.ico|signin).*)'],
}
