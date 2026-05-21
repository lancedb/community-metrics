export type SparkPoint = {
  period_start: string
  period_end: string
  value: number
}

export type DashboardMetric = {
  metric_id: string
  display_name: string
  metric_family: string
  sdk: string | null
  subject: string
  latest_value: number | null
  latest_period_end: string | null
  latest_provenance: string | null
  total_stars: number | null
  sparkline: SparkPoint[]
}

export type DashboardGroup = {
  product: string
  title: string
  items: DashboardMetric[]
}

export type DownloadWindowTotals = {
  window_start: string
  window_end: string
  lance: number
  lancedb: number
}

export type DownloadSnapshotPoint = {
  period_end: string
  lance_python: number
  lance_rust: number
  lancedb_python: number
  lancedb_nodejs: number
  lancedb_rust: number
  python: number
  nodejs: number
  rust: number
  lance: number
  lancedb: number
  total: number
}

export type DuckDBLanceExtensionDownloadPoint = {
  month_start: string
  month_label: string
  core_downloads: number
  community_downloads: number
  total_downloads: number
  is_partial_month: boolean
  latest_source_update_at: string
}

export type DashboardEvidenceItem = {
  evidence_id: string
  source_type: string
  source_name: string
  observed_at: string
  occurred_at: string
  title: string
  url: string
  snippet: string
  matched_terms: string[]
  related_metrics: string[]
  related_packages: string[]
  related_repos: string[]
  communities: string[]
  evidence_strength: string
}

export type DashboardSignalCandidate = {
  signal_id: string
  signal_type: string
  detected_at: string
  window_start: string
  window_end: string
  title: string
  summary: string
  related_metrics: string[]
  evidence_ids: string[]
  score: number
  confidence: string
  suggested_action: string
}

export type DashboardMetricRollup = {
  rollup_id: string
  metric_id: string
  metric_family: string
  product: string
  sdk: string
  subject: string
  window: string
  window_start: string
  window_end: string
  current_value: number
  previous_value: number
  delta: number
  percent_change: number
  sdk_share: number
  previous_sdk_share: number
  sdk_share_delta: number
  trend_slope: number
  updated_at: string
}

export type DashboardGuidanceCitation = {
  source_type: string
  source_id: string
  fact: string
  used_for: string
}

export type DashboardSignalGuidance = {
  guidance_id: string
  signal_id: string
  generated_at: string
  model: string
  reasoning_effort: string
  prompt_version: string
  analysis_window_start: string
  analysis_window_end: string
  comparison_windows: string[]
  executive_summary: string
  movement_assessment: string
  why_it_matters: string
  likely_community: string
  recommended_next_steps: string[]
  engineering_relevance: string
  confidence: string
  citations: DashboardGuidanceCitation[]
}

export type DashboardResponse = {
  generated_at: string
  days: number
  groups: DashboardGroup[]
  total_stars: number | null
  total_stars_sparkline: SparkPoint[]
  last_30d_download_totals: DownloadWindowTotals
  monthly_download_snapshots: DownloadSnapshotPoint[]
  duckdb_lance_extension_downloads: DuckDBLanceExtensionDownloadPoint[]
  metric_rollups: DashboardMetricRollup[]
  recent_evidence: DashboardEvidenceItem[]
  signal_candidates: DashboardSignalCandidate[]
  signal_guidance: DashboardSignalGuidance[]
}
