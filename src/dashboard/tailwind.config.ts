import type { Config } from 'tailwindcss'

const config: Config = {
  content: ['./src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        canvas: 'var(--color-canvas)',
        panel: 'var(--color-panel)',
        ink: 'var(--color-ink)',
        muted: 'var(--color-muted)',
        edge: 'var(--color-edge)',
        brand: 'var(--color-brand)',
        'brand-strong': 'var(--color-brand-strong)',
        'brand-soft': 'var(--color-brand-soft)',
        up: 'var(--color-up)',
        down: 'var(--color-down)',
        'up-bg': 'var(--color-up-bg)',
        'down-bg': 'var(--color-down-bg)',
      },
      fontFamily: {
        sans: [
          'IBMPlexSans',
          'Satoshi',
          'Manrope',
          'ui-sans-serif',
          'system-ui',
          '-apple-system',
          'BlinkMacSystemFont',
          'Segoe UI',
          'Roboto',
          'Helvetica Neue',
          'Arial',
          'Noto Sans',
          'sans-serif',
        ],
        mono: ['IBMPlexMono', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', 'monospace'],
      },
      boxShadow: {
        card: '0 18px 40px -24px rgba(129, 67, 48, 0.42)',
      },
    },
  },
  plugins: [],
}

export default config
