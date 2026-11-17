import NextAuth from 'next-auth'

import { authOptions, getAuthMode } from '@/lib/auth'

const nextAuthHandler = NextAuth(authOptions)

// In tailscale mode there is no Google sign-in, so NextAuth has nothing to serve and,
// without its Google and NEXTAUTH_* settings, fails with a 500. Answer 404 instead.
function handler(...args: Parameters<typeof nextAuthHandler>) {
  if (getAuthMode() === 'tailscale') {
    return new Response(null, { status: 404 })
  }
  return nextAuthHandler(...args)
}

export { handler as GET, handler as POST }
