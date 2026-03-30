import type { BenchmarkReport, DashboardStats, QueryHistoryEntry, QueryMode, RunQueryResponse, SourceConfig } from "./types";

const API_BASE = import.meta.env.VITE_AQE_API_BASE ?? "http://127.0.0.1:8088";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    },
    ...init
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error ?? `Request failed with ${response.status}`);
  }

  return response.json();
}

export const fetchSources = () => request<SourceConfig[]>("/sources");
export const fetchBenchmarks = () => request<BenchmarkReport[]>("/benchmarks");
export const fetchHealth = () => request<{ ok: boolean; source_count: number }>("/health");
export const fetchStats = () => request<DashboardStats>("/stats");
export const fetchQueryHistory = () => request<QueryHistoryEntry[]>("/queries/history");

export const importCSV = (payload: Partial<SourceConfig>) =>
  request<SourceConfig>("/sources/csv/import", { method: "POST", body: JSON.stringify(payload) });

export const registerPostgres = (payload: Partial<SourceConfig>) =>
  request<SourceConfig>("/sources/postgres/register", { method: "POST", body: JSON.stringify(payload) });

export const syncSource = (id: string) => request<SourceConfig>(`/sources/${id}/sync`, { method: "POST" });
export const startStream = (id: string) => request<SourceConfig>(`/sources/${id}/stream/start`, { method: "POST" });
export const stopStream = (id: string) => request<SourceConfig>(`/sources/${id}/stream/stop`, { method: "POST" });

export const runQuery = (
  sql: string,
  mode: QueryMode,
  accuracyTarget: number,
  sourceId?: string
) =>
  request<RunQueryResponse>("/queries/run", {
    method: "POST",
    body: JSON.stringify({
      sql,
      mode,
      accuracy_target: accuracyTarget,
      ...(sourceId ? { source_id: sourceId } : {})
    })
  });

export const runBenchmark = (payload: {
  name: string;
  queries: string[];
  iterations: number;
  accuracy_target: number;
}) => request<BenchmarkReport>("/benchmarks/run", { method: "POST", body: JSON.stringify(payload) });
