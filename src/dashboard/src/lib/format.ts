// Display formatting shared by the dashboard. Always en-US so numbers read the same for
// every viewer (and in the UI tests).

const integer = new Intl.NumberFormat('en-US')
const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })

export function formatInt(value: number): string {
  return integer.format(value)
}

export function formatCompact(value: number): string {
  return compact.format(value)
}

export function formatNullableInt(value: number | null): string {
  return value === null ? 'N/A' : formatInt(value)
}

/** Signed whole percent, e.g. "+49%" / "-32%" / "0%". */
export function formatPercent(value: number): string {
  const rounded = Number(value.toFixed(0))
  const sign = rounded > 0 ? '+' : ''
  return `${sign}${rounded}%`
}

function utcDate(isoDay: string): Date {
  return new Date(`${isoDay.slice(0, 10)}T00:00:00Z`)
}

/** "Oct 31, 2026" */
export function formatDay(isoDay: string): string {
  return utcDate(isoDay).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  })
}

/** "Nov 10" */
export function formatShortDay(isoDay: string): string {
  return utcDate(isoDay).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })
}

/** "Oct 2026" */
export function formatMonth(isoDay: string): string {
  return utcDate(isoDay).toLocaleDateString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' })
}

/** "Oct '26" for tight axes. */
export function formatMonthTick(isoDay: string): string {
  const date = utcDate(isoDay)
  const month = date.toLocaleDateString('en-US', { month: 'short', timeZone: 'UTC' })
  return `${month} '${String(date.getUTCFullYear()).slice(2)}`
}

/** Date-time in the viewer's locale, e.g. for "last refreshed". */
export function formatDateTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  })
}

/** "Nov 10, 2026" from a full timestamp, in the viewer's time zone. */
export function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

/** snake_case -> "Title Case" */
export function humanize(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(' ')
}
