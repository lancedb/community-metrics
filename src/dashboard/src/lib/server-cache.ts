/**
 * Small in-memory cache for server-side data loads.
 *
 * The dashboard's source data changes once a day (the scheduled refresh job), so
 * repeated page loads within a few minutes can reuse one LanceDB read instead of
 * paying the network round trips again. Concurrent requests for the same key share
 * one in-flight load. Failed loads are dropped, never cached.
 *
 * The cache lives in the server process: on Vercel each warm instance has its own,
 * and a cold start begins empty.
 */

type Entry<T> = { expiresAt: number; value: Promise<T> }

const MAX_ENTRIES = 200

const entries = new Map<string, Entry<unknown>>()

export function cached<T>(key: string, ttlMs: number, load: () => Promise<T>): Promise<T> {
  const now = Date.now()
  const hit = entries.get(key) as Entry<T> | undefined
  if (hit && hit.expiresAt > now) {
    return hit.value
  }

  const value = load()
  entries.delete(key) // re-insert so eviction order stays oldest-first
  entries.set(key, { expiresAt: now + ttlMs, value })
  value.catch(() => {
    if (entries.get(key)?.value === value) {
      entries.delete(key)
    }
  })

  // Keys come from request parameters, so bound the map. Map iteration is insertion
  // order, so the first keys are the oldest.
  while (entries.size > MAX_ENTRIES) {
    const oldest = entries.keys().next().value
    if (oldest === undefined) break
    entries.delete(oldest)
  }

  return value
}
