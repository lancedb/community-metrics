import type { Metadata } from 'next'
import { redirect } from 'next/navigation'

import { ALLOWED_EMAIL_DOMAIN, isLocalAuthDisabled } from '@/lib/auth-policy'

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
  if (isLocalAuthDisabled()) {
    redirect('/')
  }

  const params = (await searchParams) ?? {}
  const callbackUrlValue = params.callbackUrl
  const callbackUrl = typeof callbackUrlValue === 'string' ? callbackUrlValue : '/'
  const errorValue = params.error
  const errorCode = typeof errorValue === 'string' ? errorValue : null
  const errorMessage = errorCode ? ERROR_MESSAGES[errorCode] ?? 'Sign-in failed. Please try again.' : null

  return (
    <main className="flex min-h-screen items-center justify-center px-4">
      <div className="w-full max-w-sm rounded-2xl border bg-card/80 p-8 shadow-2xl backdrop-blur-sm">
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src="/lancedb-logo.png" alt="LanceDB" width={390} height={91} className="h-7 w-auto invert dark:invert-0" />
        <h1 className="mt-8 text-2xl font-semibold tracking-tight text-foreground">LanceDB Community Metrics</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Sign in with your Google workspace account. Only @{ALLOWED_EMAIL_DOMAIN} addresses are allowed.
        </p>

        {errorMessage && (
          <p role="alert" className="mt-5 rounded-lg border border-down/40 bg-down-soft p-3 text-sm text-down">
            {errorMessage}
          </p>
        )}

        <div className="mt-8">
          <SignInButton callbackUrl={callbackUrl} />
        </div>
      </div>
    </main>
  )
}
