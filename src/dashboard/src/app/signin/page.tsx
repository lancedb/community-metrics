import type { Metadata } from 'next'

import { ALLOWED_EMAIL_DOMAIN } from '@/lib/auth'

import { SignInButton } from './SignInButton'

type SignInPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>
}

const ERROR_MESSAGES: Record<string, string> = {
  AccessDenied: `Only @${ALLOWED_EMAIL_DOMAIN} Google accounts can access this dashboard.`,
  Configuration: 'SSO is not configured yet. Set the required Google OAuth and NextAuth environment variables.',
  OAuthSignin: 'Google sign-in failed. Please try again.',
  OAuthCallback: 'Google sign-in callback failed. Please try again.',
  Callback: 'Sign-in callback failed. Please try again.',
}

export const metadata: Metadata = {
  title: 'Sign In | LanceDB Community Metrics',
}

export default async function SignInPage({ searchParams }: SignInPageProps) {
  const params = (await searchParams) ?? {}
  const callbackUrlValue = params.callbackUrl
  const callbackUrl = typeof callbackUrlValue === 'string' ? callbackUrlValue : '/'
  const errorValue = params.error
  const errorCode = typeof errorValue === 'string' ? errorValue : null
  const errorMessage = errorCode ? ERROR_MESSAGES[errorCode] ?? 'Sign-in failed. Please try again.' : null

  return (
    <main className="flex min-h-screen items-center justify-center bg-canvas px-4">
      <section className="w-full max-w-md rounded-xl border border-edge bg-panel p-6 shadow-sm">
        <h1 className="text-2xl font-bold tracking-tight text-ink">LanceDB Community Metrics</h1>
        <p className="mt-2 text-sm text-muted">
          Sign in with your Google workspace account. Only users with @{ALLOWED_EMAIL_DOMAIN} addresses are allowed.
        </p>

        {errorMessage && <p className="mt-4 rounded-md border border-down bg-down-bg p-3 text-sm text-down">{errorMessage}</p>}

        <div className="mt-6">
          <SignInButton callbackUrl={callbackUrl} />
        </div>
      </section>
    </main>
  )
}
