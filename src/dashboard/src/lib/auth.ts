import type { NextAuthOptions } from 'next-auth'
import { getServerSession } from 'next-auth'
import GoogleProvider from 'next-auth/providers/google'
import { headers } from 'next/headers'

import {
  ALLOWED_EMAIL_DOMAIN,
  getAuthMode,
  isAllowedLanceDbEmail,
  isLocalAuthDisabled,
  TAILSCALE_USER_LOGIN_HEADER,
} from './auth-policy'

export { ALLOWED_EMAIL_DOMAIN, getAuthMode, isAllowedLanceDbEmail, isLocalAuthDisabled }

type GoogleProfile = {
  email?: string
  email_verified?: boolean
  hd?: string
}

export const authOptions: NextAuthOptions = {
  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_CLIENT_ID ?? '',
      clientSecret: process.env.GOOGLE_CLIENT_SECRET ?? '',
      authorization: {
        params: {
          hd: ALLOWED_EMAIL_DOMAIN,
          prompt: 'select_account',
        },
      },
    }),
  ],
  pages: {
    signIn: '/signin',
  },
  callbacks: {
    async signIn({ account, profile }) {
      if (account?.provider !== 'google') {
        return false
      }

      const googleProfile = profile as GoogleProfile | undefined
      const email = googleProfile?.email
      const emailVerified = googleProfile?.email_verified === true
      const hostedDomain = googleProfile?.hd?.toLowerCase()

      return emailVerified && hostedDomain === ALLOWED_EMAIL_DOMAIN && isAllowedLanceDbEmail(email)
    },
  },
}

/** True when the current request comes from an allowed @lancedb.com visitor. */
export async function isRequestAllowed(): Promise<boolean> {
  if (isLocalAuthDisabled()) {
    return true
  }

  if (getAuthMode() === 'tailscale') {
    const login = (await headers()).get(TAILSCALE_USER_LOGIN_HEADER)
    return isAllowedLanceDbEmail(login)
  }

  const session = await getServerSession(authOptions)
  return isAllowedLanceDbEmail(session?.user?.email)
}
