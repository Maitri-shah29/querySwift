export type SourceKind = "csv" | "postgres";
export type QueryMode = "exact" | "approx" | "compare";
export type SamplingMethod = "uniform" | "stratified";

export interface SourceConfig {
  id: string;
  name: string;
  kind: SourceKind;
  table_name: string;
  file_path?: string;
  postgres_dsn?: string;
  postgres_schema?: string;
  postgres_table?: string;
  primary_key?: string;
  watermark_column?: string;
  last_watermark?: string;
  poll_interval_seconds?: number;
  stratify_columns?: string[];
  sampling_method?: SamplingMethod;
  sample_rate?: number;
  status?: string;
  last_sync_at?: string;
  raw_table: string;
  sample_table: string;
  raw_row_count: number;
  sample_row_count: number;
  streaming: boolean;
}

export interface QueryMetric {
  execution_millis: number;
  row_count: number;
  sample_rate?: number;
  estimated_error?: number;
  confidence?: number;
  speedup?: number;
  actual_error?: number;
}

export interface QueryResult {
  schema: string[];
  rows: Array<Record<string, unknown>>;
  metric: QueryMetric;
}

export interface RunQueryResponse {
  mode: QueryMode;
  exact?: QueryResult;
  approx?: QueryResult;
}

export interface BenchmarkQueryResult {
  query: string;
  exact_millis: number;
  approx_millis: number;
  speedup: number;
  estimated_error: number;
  actual_error: number;
  approx_confidence: number;
}

export interface BenchmarkReport {
  id: string;
  name: string;
  created_at: string;
  iterations: number;
  accuracy_target: number;
  results: BenchmarkQueryResult[];
}

export interface QueryHistoryEntry {
  id: string;
  sql: string;
  mode: string;
  status: string;
  source_name: string;
  exact_millis: number;
  approx_millis: number;
  row_count: number;
  speedup: number;
  error_pct: number;
  created_at: string;
}

export interface DashboardStats {
  total_queries: number;
  avg_runtime_ms: number;
  connected_sources: number;
  success_count: number;
  error_count: number;
  daily_counts: Array<{
    day: string;
    success_count: number;
    error_count: number;
  }>;
}
