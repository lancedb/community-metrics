export const ALLOWED_EMAIL_DOMAIN = 'lancedb.com'

export function isAllowedLanceDbEmail(email: string | null | undefined): boolean {
  return typeof email === 'string' && email.toLowerCase().endsWith(`@${ALLOWED_EMAIL_DOMAIN}`)
}

export function isLocalAuthDisabled(): boolean {
  return process.env.NODE_ENV === 'development' && process.env.DISABLE_AUTH_LOCAL === '1'
}

/**
 * How visitors are identified.
 * - `google` (default): Google SSO through NextAuth.
 * - `tailscale`: the app sits behind `tailscale serve`, which adds the visitor's
 *   tailnet login in the Tailscale-User-Login header. Only use this when the app is
 *   reachable through `tailscale serve` alone (bind it to 127.0.0.1), because anything
 *   that can reach the app directly could send that header itself.
 */
export type AuthMode = 'google' | 'tailscale'

export const TAILSCALE_USER_LOGIN_HEADER = 'tailscale-user-login'

export function getAuthMode(): AuthMode {
  return process.env.AUTH_MODE === 'tailscale' ? 'tailscale' : 'google'
}
