import '@fontsource-variable/hanken-grotesk'
import '@fontsource-variable/jetbrains-mono'
import './globals.css'

import type { Metadata, Viewport } from 'next'

import { ThemeProvider } from '@/components/theme'
import { TooltipProvider } from '@/components/ui/tooltip'

export const metadata: Metadata = {
  title: 'LanceDB Community Metrics',
  description: 'Downloads, usage and growth tracking for Lance format and LanceDB SDK adoption.',
}

export const viewport: Viewport = {
  themeColor: [
    { media: '(prefers-color-scheme: dark)', color: '#14110e' },
    { media: '(prefers-color-scheme: light)', color: '#faf6f1' },
  ],
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        {/* Dark by default (LanceDB brand); the header toggle switches and remembers. */}
        <ThemeProvider attribute="class" defaultTheme="dark" enableSystem={false} disableTransitionOnChange>
          <TooltipProvider delayDuration={150}>{children}</TooltipProvider>
        </ThemeProvider>
      </body>
    </html>
  )
}
