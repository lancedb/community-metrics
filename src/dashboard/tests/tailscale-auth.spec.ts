/**
 * Auth in tailscale mode (AUTH_MODE=tailscale), the mode the tailnet host runs.
 *
 * `tailscale serve` is the only way in, and it adds the visitor's tailnet login in the
 * Tailscale-User-Login header. These tests send requests the way it would, with and
 * without that header, against a dev server started in tailscale mode
 * (playwright.config.ts, project `tailscale-auth`).
 */
import { expect, test } from '@playwright/test'

const LANCEDB_USER = { 'Tailscale-User-Login': 'someone@lancedb.com' }
const OUTSIDE_USER = { 'Tailscale-User-Login': 'someone@example.com' }

test.describe('tailscale mode', () => {
  test('a @lancedb.com tailnet user gets the dashboard', async ({ request }) => {
    const response = await request.get('/', { headers: LANCEDB_USER, maxRedirects: 0 })
    expect(response.status()).toBe(200)
  })

  test('a request without a tailnet login gets the tailnet message, not the dashboard', async ({
    request,
  }) => {
    const response = await request.get('/', { maxRedirects: 0 })
    expect(response.status()).toBe(200)
    expect(await response.text()).toContain('LanceDB tailnet')
  })

  test('the data API refuses requests without an allowed tailnet login', async ({ request }) => {
    for (const headers of [undefined, OUTSIDE_USER]) {
      const response = await request.get('/api/v1/dashboard/daily?days=30', { headers })
      expect(response.status()).toBe(401)
    }
  })

  // Audit finding 4: NextAuth's routes answered with a 500 in tailscale mode.
  for (const path of ['/api/auth/session', '/api/auth/providers', '/api/auth/csrf', '/api/auth/signin']) {
    test(`${path} is not served`, async ({ request }) => {
      const response = await request.get(path, { headers: LANCEDB_USER, maxRedirects: 0 })
      expect(response.status()).toBe(404)
    })
  }

  test('POST to the NextAuth routes is not served either', async ({ request }) => {
    const response = await request.post('/api/auth/signout', { headers: LANCEDB_USER, maxRedirects: 0 })
    expect(response.status()).toBe(404)
  })

  // Audit finding 4: responses advertised the framework.
  test('responses do not carry an X-Powered-By header', async ({ request }) => {
    // The data API is checked without a login, so the test never needs LanceDB.
    const requests = [
      { path: '/', headers: LANCEDB_USER },
      { path: '/signin', headers: LANCEDB_USER },
      { path: '/api/auth/session', headers: LANCEDB_USER },
      { path: '/api/v1/dashboard/daily?days=30', headers: undefined },
    ]
    for (const { path, headers } of requests) {
      const response = await request.get(path, { headers, maxRedirects: 0 })
      expect(response.headers()['x-powered-by'], path).toBeUndefined()
    }
  })
})
