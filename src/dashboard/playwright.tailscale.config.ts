import { defineConfig } from '@playwright/test'

const PORT = 3101

// Auth tests for tailscale mode (tests/tailscale-auth.spec.ts). They need a server started
// with AUTH_MODE=tailscale and no local auth bypass, so they run after the UI tests
// (`npm run test:ui`) rather than alongside them: two `next dev` servers can't share `.next`.
export default defineConfig({
  testDir: './tests',
  testMatch: /tailscale-auth\.spec\.ts/,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['github'], ['html', { open: 'never', outputFolder: 'playwright-report-tailscale' }]] : 'list',
  outputDir: 'test-results-tailscale',
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
  },
  webServer: {
    command: `npx next dev --port ${PORT} --hostname 127.0.0.1`,
    url: `http://127.0.0.1:${PORT}/signin`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      AUTH_MODE: 'tailscale',
    },
  },
})
