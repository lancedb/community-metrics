import { defineConfig, devices } from '@playwright/test'

const PORT = 3100

// UI tests run the real Next.js app with auth off. The dashboard API is mocked in the
// browser (tests/fixtures/dashboard.ts), so no LanceDB or Google credentials are needed.
export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['github'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    timezoneId: 'UTC',
    locale: 'en-US',
    trace: 'retain-on-failure',
  },
  projects: [
    { name: 'desktop', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 1000 } } },
    { name: 'mobile', use: { ...devices['Pixel 7'] } },
  ],
  webServer: {
    command: `npx next dev --port ${PORT} --hostname 127.0.0.1`,
    url: `http://127.0.0.1:${PORT}/signin`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      DISABLE_AUTH_LOCAL: '1',
      NEXTAUTH_SECRET: 'ui-tests-only',
      NEXTAUTH_URL: `http://127.0.0.1:${PORT}`,
    },
  },
})
