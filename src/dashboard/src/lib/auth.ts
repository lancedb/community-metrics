import type { NextAuthOptions } from 'next-auth'
import GoogleProvider from 'next-auth/providers/google'

import { ALLOWED_EMAIL_DOMAIN, isAllowedLanceDbEmail, isLocalAuthDisabled } from './auth-policy'

export { ALLOWED_EMAIL_DOMAIN, isAllowedLanceDbEmail, isLocalAuthDisabled }

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
