import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'LanceDB Community Metrics',
  description:
    'Downloads, usage and growth tracking for Lance format and LanceDB SDK adoption.',
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
