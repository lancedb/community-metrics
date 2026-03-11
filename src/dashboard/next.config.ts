import path from 'node:path'
import { fileURLToPath } from 'node:url'

import type { NextConfig } from 'next'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const nextConfig: NextConfig = {
  reactStrictMode: true,
  allowedDevOrigins: ['127.0.0.1', 'localhost'],
  outputFileTracingRoot: path.join(__dirname, '../..'),
  serverExternalPackages: ['@lancedb/lancedb'],
}

export default nextConfig
