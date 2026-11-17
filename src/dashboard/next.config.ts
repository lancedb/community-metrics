import path from 'node:path'
import { fileURLToPath } from 'node:url'

import type { NextConfig } from 'next'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // Don't advertise the framework in an `X-Powered-By` header.
  poweredByHeader: false,
  // The Docker image (Dockerfile) builds a self-contained server. Other builds
  // (Vercel, CI tests, local dev) are unchanged.
  output: process.env.NEXT_OUTPUT_STANDALONE === '1' ? 'standalone' : undefined,
  allowedDevOrigins: ['127.0.0.1', 'localhost'],
  outputFileTracingRoot: path.join(__dirname, '../..'),
  serverExternalPackages: ['@lancedb/lancedb'],
}

export default nextConfig
