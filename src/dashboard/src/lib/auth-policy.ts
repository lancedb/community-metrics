export const ALLOWED_EMAIL_DOMAIN = 'lancedb.com'

export function isAllowedLanceDbEmail(email: string | null | undefined): boolean {
  return typeof email === 'string' && email.toLowerCase().endsWith(`@${ALLOWED_EMAIL_DOMAIN}`)
}

export function isLocalAuthDisabled(): boolean {
  return process.env.NODE_ENV === 'development' && process.env.DISABLE_AUTH_LOCAL === '1'
}
