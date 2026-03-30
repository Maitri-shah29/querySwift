import { useCallback, useEffect, useMemo, useState } from "react";
import {
  fetchHealth,
  fetchQueryHistory,
  fetchSources,
  fetchStats,
  importCSV,
  registerPostgres,
  runBenchmark,
  runQuery,
  startStream,
  stopStream,
  syncSource,
} from "./api";
import type { BenchmarkReport, DashboardStats, QueryHistoryEntry, QueryMode, QueryResult, RunQueryResponse, SourceConfig } from "./types";

type Screen = "dashboard" | "sources" | "workspace" | "history" | "settings";

type ConnectionMode = "csv" | "postgres";
type CardStatus = "Healthy" | "Auth Failed" | "Syncing";

type SourceCard = {
  id: string;
  name: string;
  engine: string;
  owner: string;
  ownerInitials: string;
  status: CardStatus;
  syncText: string;
  tables: number;
  syncProgress?: number;
  source: SourceConfig;
};

const defaultSql = `-- Get top users by order volume in last 30 days WITH user_stats AS (
SELECT u.id, u.full_name, u.email,
COUNT(o.id) AS total_orders, SUM(o.total_amount) AS total_spent
FROM users u LEFT JOIN orders o ON u.id = o.user_id
WHERE o.created_at > NOW() - INTERVAL '30 days'
GROUP BY u.id, u.full_name, u.email )
SELECT * FROM user_stats WHERE total_spent > 1000 ORDER BY total_spent DESC;`;

export default function App() {
  const [screen, setScreen] = useState<Screen>("dashboard");
  const [sql, setSql] = useState(defaultSql);
  const [sources, setSources] = useState<SourceConfig[]>([]);
  const [health, setHealth] = useState("Checking backend...");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [activeCardId, setActiveCardId] = useState("");
  const [searchText, setSearchText] = useState("");
  const [typeFilter, setTypeFilter] = useState<"all" | "csv" | "postgres">("all");
  const [statusFilter, setStatusFilter] = useState<"all" | "healthy" | "auth-failed" | "syncing">("all");
  const [connectionOpen, setConnectionOpen] = useState(false);
  const [connectionMode, setConnectionMode] = useState<ConnectionMode>("csv");
  const [queryMode, setQueryMode] = useState<QueryMode>("compare");
  const [accuracyTarget, setAccuracyTarget] = useState(0.9);
  const [queryResult, setQueryResult] = useState<RunQueryResponse | null>(null);
  const [resultView, setResultView] = useState<"approx" | "exact">("approx");
  const [isRunningQuery, setIsRunningQuery] = useState(false);
  const [streamActionId, setStreamActionId] = useState("");
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [queryHistory, setQueryHistory] = useState<QueryHistoryEntry[]>([]);
  const [historySearch, setHistorySearch] = useState("");
  const [historyStatusFilter, setHistoryStatusFilter] = useState<"all" | "success" | "error">("all");
  const [workspaceTab, setWorkspaceTab] = useState<"query" | "benchmark">("query");
  const [samplingMethod, setSamplingMethod] = useState<"random" | "stratified">("random");
  const [sampleFraction, setSampleFraction] = useState(10);
  const [isRunningBenchmark, setIsRunningBenchmark] = useState(false);
  const [benchmarkResult, setBenchmarkResult] = useState<BenchmarkReport | null>(null);
  const [csvForm, setCSVForm] = useState({
    name: "CSV Dataset",
    file_path: "",
    table_name: "csv_dataset",
    stratify_columns: "region",
    sample_rate: 0.1,
  });
  const [pgForm, setPGForm] = useState({
    name: "Postgres Database",
    postgres_dsn: "",
    table_name: "postgres_dataset",
    postgres_schema: "public",
    postgres_table: "",
    primary_key: "id",
    watermark_column: "updated_at",
    poll_interval_seconds: 15,
    stratify_columns: "region",
    sample_rate: 0.1,
  });

  // Settings state (persisted via localStorage)
  const [settingsApiBase, setSettingsApiBase] = useState(
    () => localStorage.getItem("qs_api_base") || "http://127.0.0.1:8088"
  );
  const [settingsDefaultMode, setSettingsDefaultMode] = useState<QueryMode>(
    () => (localStorage.getItem("qs_default_mode") as QueryMode) || "compare"
  );
  const [settingsDefaultAccuracy, setSettingsDefaultAccuracy] = useState(
    () => Number(localStorage.getItem("qs_default_accuracy")) || 0.9
  );
  const [settingsRowLimit, setSettingsRowLimit] = useState(
    () => Number(localStorage.getItem("qs_row_limit")) || 1000
  );

  const loadData = useCallback(async () => {
    try {
      setIsLoading(true);
      setError("");
      const [healthPayload, sourcePayload] = await Promise.all([fetchHealth(), fetchSources()]);
      setHealth(
        healthPayload.ok
          ? `Backend ready – ${healthPayload.source_count} tables`
          : "Backend unavailable"
      );
      setSources(sourcePayload);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setHealth("Backend unavailable");
      setError(message);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const loadStats = useCallback(async () => {
    try {
      const data = await fetchStats();
      setStats(data);
    } catch {
      // Silently ignore stats errors
    }
  }, []);

  const loadHistory = useCallback(async () => {
    try {
      const data = await fetchQueryHistory();
      setQueryHistory(data);
    } catch {
      // Silently ignore history errors
    }
  }, []);

  useEffect(() => {
    loadData();
    loadStats();
    loadHistory();
  }, [loadData, loadStats, loadHistory]);

  const sourceCards = useMemo(() => {
    return sources.map((source) => {
      const status = normalizeStatus(source);
      return {
        id: source.id,
        name: source.name,
        engine: source.kind === "postgres" ? "PostgreSQL" : "CSV",
        owner: source.kind === "postgres" ? "DB Team" : "Data Team",
        ownerInitials: source.kind === "postgres" ? "DB" : "DT",
        status,
        syncText: source.last_sync_at ? formatRelativeTime(source.last_sync_at) : "Never synced",
        tables: Math.max(source.raw_row_count, source.sample_row_count),
        syncProgress: status === "Syncing" ? 45 : undefined,
        source,
      } satisfies SourceCard;
    });
  }, [sources]);

  const filteredCards = useMemo(() => {
    const query = searchText.trim().toLowerCase();
    return sourceCards.filter((card) => {
      const matchesText =
        query.length === 0 ||
        card.name.toLowerCase().includes(query) ||
        card.source.table_name.toLowerCase().includes(query);
      const matchesType = typeFilter === "all" || card.source.kind === typeFilter;
      const statusKey = card.status.toLowerCase().replace(" ", "-") as
        | "healthy"
        | "auth-failed"
        | "syncing";
      const matchesStatus = statusFilter === "all" || statusKey === statusFilter;
      return matchesText && matchesType && matchesStatus;
    });
  }, [searchText, sourceCards, statusFilter, typeFilter]);

  useEffect(() => {
    if (sourceCards.length === 0) {
      setActiveCardId("");
      return;
    }
    if (!activeCardId || !sourceCards.some((card) => card.id === activeCardId)) {
      setActiveCardId(sourceCards[0].id);
    }
  }, [activeCardId, sourceCards]);

  const themeClass = screen === "sources" ? "theme-sources" : "theme-workspace";
  const activeCard = useMemo(
    () => sourceCards.find((card) => card.id === activeCardId) ?? sourceCards[0],
    [activeCardId, sourceCards]
  );

  useEffect(() => {
    if (!activeCard?.source.table_name) {
      return;
    }
    setSql(`SELECT COUNT(*) AS total_rows FROM ${activeCard.source.table_name}`);
  }, [activeCard?.source.id]);

  const activeResult = useMemo<QueryResult | null>(() => {
    if (!queryResult) {
      return null;
    }
    if (resultView === "exact" && queryResult.exact) {
      return queryResult.exact;
    }
    if (resultView === "approx" && queryResult.approx) {
      return queryResult.approx;
    }
    return queryResult.approx ?? queryResult.exact ?? null;
  }, [queryResult, resultView]);

  const hasActiveStream = useMemo(
    () => sources.some((source) => source.streaming),
    [sources]
  );

  useEffect(() => {
    if (!hasActiveStream) {
      return;
    }
    const timer = window.setInterval(() => {
      loadData();
    }, 2000);
    return () => window.clearInterval(timer);
  }, [hasActiveStream, loadData]);

  const filteredHistory = useMemo(() => {
    return queryHistory.filter((entry) => {
      const matchSearch =
        historySearch.trim() === "" ||
        entry.sql.toLowerCase().includes(historySearch.toLowerCase()) ||
        entry.source_name.toLowerCase().includes(historySearch.toLowerCase());
      const matchStatus = historyStatusFilter === "all" || entry.status === historyStatusFilter;
      return matchSearch && matchStatus;
    });
  }, [queryHistory, historySearch, historyStatusFilter]);

  async function submitQuery() {
    if (!sql.trim()) {
      setError("Please enter a SQL query.");
      return;
    }

    try {
      setIsRunningQuery(true);
      setError("");
      const result = await runQuery(sql, queryMode, accuracyTarget, activeCard?.source.id);
      setQueryResult(result);
      if (result.approx) {
        setResultView("approx");
      } else if (result.exact) {
        setResultView("exact");
      }
      // Refresh history & stats after query
      loadHistory();
      loadStats();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed");
      loadHistory();
      loadStats();
    } finally {
      setIsRunningQuery(false);
    }
  }

  async function submitBenchmark() {
    if (!sql.trim()) {
      setError("Please enter a SQL query.");
      return;
    }
    try {
      setIsRunningBenchmark(true);
      setError("");
      const result = await runBenchmark({
        name: "Workspace Benchmark",
        queries: [sql],
        iterations: 3,
        accuracy_target: accuracyTarget,
      });
      setBenchmarkResult(result);
      loadHistory();
      loadStats();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Benchmark failed");
    } finally {
      setIsRunningBenchmark(false);
    }
  }

  async function submitCSV() {
    const filePath = stripWrappingQuotes(csvForm.file_path.trim());
    const tableName =
      csvForm.table_name.trim() || inferTableName(csvForm.name, filePath);
    if (!filePath || !tableName) {
      setError("file_path and table_name are required");
      return;
    }

    try {
      setError("");
      const created = await importCSV({
        ...csvForm,
        file_path: filePath,
        table_name: tableName,
        stratify_columns: splitColumns(csvForm.stratify_columns),
      });
      setConnectionOpen(false);
      await loadData();
      setActiveCardId(created.id);
      setScreen("workspace");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to import CSV");
    }
  }

  async function submitPostgres() {
    const postgresDSN = stripWrappingQuotes(pgForm.postgres_dsn.trim());
    const postgresTable = pgForm.postgres_table.trim();
    const tableName =
      pgForm.table_name.trim() || inferTableName(pgForm.name, postgresTable);
    if (!postgresDSN || !postgresTable || !tableName) {
      setError("postgres_dsn, postgres_table, and table_name are required");
      return;
    }

    try {
      setError("");
      const payload = {
        ...pgForm,
        postgres_dsn: postgresDSN,
        postgres_table: postgresTable,
        table_name: tableName,
        stratify_columns: splitColumns(pgForm.stratify_columns),
      };
      const createdUnknown = (await registerPostgres(payload)) as unknown;
      const createdList = Array.isArray(createdUnknown)
        ? (createdUnknown as SourceConfig[])
        : ([createdUnknown as SourceConfig]);
      setConnectionOpen(false);
      await loadData();
      if (createdList.length > 0) {
        setActiveCardId(createdList[0].id);
      }
      setScreen("workspace");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to register Postgres source");
    }
  }

  async function handleSyncSource(id: string) {
    try {
      setError("");
      await syncSource(id);
      await loadData();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to sync source");
    }
  }

  async function handleToggleStream(source: SourceConfig) {
    try {
      setStreamActionId(source.id);
      setError("");
      if (source.streaming) {
        await stopStream(source.id);
      } else {
        await startStream(source.id);
      }
      await loadData();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to update streaming");
    } finally {
      setStreamActionId("");
    }
  }

  function saveSettings() {
    localStorage.setItem("qs_api_base", settingsApiBase);
    localStorage.setItem("qs_default_mode", settingsDefaultMode);
    localStorage.setItem("qs_default_accuracy", String(settingsDefaultAccuracy));
    localStorage.setItem("qs_row_limit", String(settingsRowLimit));
    setQueryMode(settingsDefaultMode);
    setAccuracyTarget(settingsDefaultAccuracy);
  }

  function renderScreen() {
    switch (screen) {
      case "dashboard":
        return (
          <DashboardView
            stats={stats}
            sources={sourceCards}
            history={queryHistory}
            onNavigateWorkspace={() => setScreen("workspace")}
            onNavigateSources={() => setScreen("sources")}
            onNavigateHistory={() => setScreen("history")}
            onAddSource={() => setConnectionOpen(true)}
          />
        );
      case "sources":
        return (
          <SourcesView
            cards={filteredCards}
            activeCardId={activeCard?.id ?? ""}
            typeFilter={typeFilter}
            statusFilter={statusFilter}
            onTypeFilter={setTypeFilter}
            onStatusFilter={setStatusFilter}
            onAddSource={() => setConnectionOpen(true)}
            onSelectCard={(id) => {
              setActiveCardId(id);
              setScreen("workspace");
            }}
            onSyncSource={handleSyncSource}
            onToggleStream={handleToggleStream}
            streamActionId={streamActionId}
            loading={isLoading}
          />
        );
      case "workspace":
        return (
          <WorkspaceView
            sql={sql}
            onSqlChange={setSql}
            activeCard={activeCard}
            sourceCards={sourceCards}
            onSelectSource={setActiveCardId}
            health={health}
            hasActiveStream={hasActiveStream}
            queryMode={queryMode}
            onQueryMode={setQueryMode}
            accuracyTarget={accuracyTarget}
            onAccuracyTarget={setAccuracyTarget}
            resultView={resultView}
            onResultView={setResultView}
            queryResult={queryResult}
            activeResult={activeResult}
            streamActionId={streamActionId}
            onToggleStream={handleToggleStream}
            workspaceTab={workspaceTab}
            onWorkspaceTab={setWorkspaceTab}
            samplingMethod={samplingMethod}
            onSamplingMethod={setSamplingMethod}
            sampleFraction={sampleFraction}
            onSampleFraction={setSampleFraction}
            onRunQuery={submitQuery}
            isRunningQuery={isRunningQuery}
            onRunBenchmark={submitBenchmark}
            isRunningBenchmark={isRunningBenchmark}
            benchmarkResult={benchmarkResult}
          />
        );
      case "history":
        return (
          <HistoryView
            entries={filteredHistory}
            searchText={historySearch}
            onSearchChange={setHistorySearch}
            statusFilter={historyStatusFilter}
            onStatusFilter={setHistoryStatusFilter}
            onRerunQuery={(sql) => {
              setSql(sql);
              setScreen("workspace");
            }}
          />
        );
      case "settings":
        return (
          <SettingsView
            apiBase={settingsApiBase}
            onApiBaseChange={setSettingsApiBase}
            defaultMode={settingsDefaultMode}
            onDefaultModeChange={setSettingsDefaultMode}
            defaultAccuracy={settingsDefaultAccuracy}
            onDefaultAccuracyChange={setSettingsDefaultAccuracy}
            rowLimit={settingsRowLimit}
            onRowLimitChange={setSettingsRowLimit}
            onSave={saveSettings}
          />
        );
    }
  }

  return (
    <div className={`qs-root ${themeClass}`}>
      <div className="qs-shell">
        <aside className="qs-sidebar sidebar-light">
          <div>
            <div className="brand-row">
              <span className="brand-name-aqp"><em>AQP</em> ENGINE</span>
            </div>

            <button className="account-pill account-pill-light" type="button">
              <span>Acme Corp</span>
              <span className="caret">{"\u25BE"}</span>
            </button>

            <nav className="nav-sections" aria-label="Navigation">
              <NavButton
                label="Project and Workspace"
                icon=""
                active={screen === "dashboard"}
                onClick={() => setScreen("dashboard")}
              />
              <NavButton
                label="Query Workspace"
                icon=""
                active={screen === "workspace"}
                onClick={() => setScreen("workspace")}
              />
              <NavButton
                label="Saved Queries & History"
                icon=""
                active={screen === "history"}
                onClick={() => setScreen("history")}
              />
              <NavButton
                label="Data Sources"
                icon=""
                active={screen === "sources"}
                onClick={() => setScreen("sources")}
              />

              <div className="nav-heading nav-heading-light">CONFIGURATION</div>
              <NavButton
                label="Settings"
                icon=""
                active={screen === "settings"}
                onClick={() => setScreen("settings")}
              />
            </nav>
          </div>
        </aside>

        <main className="qs-main">
          <header className="qs-topbar">
            {screen === "sources" ? (
              <div className="top-search-wrap">
                <span className="search-icon">⌕</span>
                <input
                  aria-label="Search"
                  value={searchText}
                  onChange={(event) => setSearchText(event.target.value)}
                  placeholder="Search data sources..."
                />
              </div>
            ) : screen === "dashboard" ? (
              <div className="top-search-wrap">
                <span className="search-icon">⌕</span>
                <input
                  aria-label="Search"
                  placeholder="Search queries, data sources, or history... (Cmd+K)"
                  readOnly
                />
                <span className="search-shortcut">K</span>
              </div>
            ) : (
              <h2 className="top-title">
                {screen === "workspace"
                  ? "Query Workspace"
                  : screen === "history"
                  ? "Query History"
                  : "Settings"}
              </h2>
            )}

            <div className="topbar-actions">
              {screen === "sources" ? (
                <button type="button" className="cta-button warm" onClick={() => setConnectionOpen(true)}>
                  + Add Data Source
                </button>
              ) : screen === "workspace" ? (
                null
              ) : screen === "dashboard" ? (
                <>
                  <button type="button" className="cta-button outline" onClick={() => setConnectionOpen(true)}>
                    Add Data Source
                  </button>
                  <button
                    type="button"
                    className="cta-button cool"
                    onClick={() => setScreen("workspace")}
                  >
                    + New Query
                  </button>
                </>
              ) : null}
            </div>
          </header>

          {error ? <div className="inline-error">{error}</div> : null}

          {renderScreen()}
        </main>
      </div>

      {connectionOpen ? (
        <div className="connection-overlay" onClick={() => setConnectionOpen(false)}>
          <aside className="connection-drawer" onClick={(event) => event.stopPropagation()}>
            <div className="drawer-header">
              <h2>Add Data Source</h2>
              <button className="drawer-close" type="button" onClick={() => setConnectionOpen(false)}>
                ×
              </button>
            </div>

            <div className="drawer-tabs">
              <button
                type="button"
                className={connectionMode === "csv" ? "active" : ""}
                onClick={() => setConnectionMode("csv")}
              >
                CSV
              </button>
              <button
                type="button"
                className={connectionMode === "postgres" ? "active" : ""}
                onClick={() => setConnectionMode("postgres")}
              >
                Postgres
              </button>
            </div>

            {connectionMode === "csv" ? (
              <div className="drawer-body">
                <Field
                  label="Display name"
                  value={csvForm.name}
                  onChange={(value) => setCSVForm({ ...csvForm, name: value })}
                />
                <Field
                  label="CSV file path"
                  value={csvForm.file_path}
                  onChange={(value) => setCSVForm({ ...csvForm, file_path: value })}
                  placeholder="C:\\data\\sales.csv"
                />
                <Field
                  label="Internal table name"
                  value={csvForm.table_name}
                  onChange={(value) => setCSVForm({ ...csvForm, table_name: value })}
                  placeholder="sales_data"
                />
                <Field
                  label="Stratify columns"
                  value={csvForm.stratify_columns}
                  onChange={(value) => setCSVForm({ ...csvForm, stratify_columns: value })}
                />
                <Field
                  label="Sample rate"
                  type="number"
                  value={String(csvForm.sample_rate)}
                  onChange={(value) => setCSVForm({ ...csvForm, sample_rate: Number(value) })}
                />
                <button type="button" className="cta-button warm block" onClick={submitCSV}>
                  Import CSV
                </button>
              </div>
            ) : (
              <div className="drawer-body">
                <Field
                  label="Display name"
                  value={pgForm.name}
                  onChange={(value) => setPGForm({ ...pgForm, name: value })}
                />
                <Field
                  label="Postgres DSN"
                  value={pgForm.postgres_dsn}
                  onChange={(value) => setPGForm({ ...pgForm, postgres_dsn: value })}
                  placeholder="postgres://user:pass@localhost:5432/dbname"
                />
                <Field
                  label="Postgres schema"
                  value={pgForm.postgres_schema}
                  onChange={(value) => setPGForm({ ...pgForm, postgres_schema: value })}
                  placeholder="public"
                />
                <Field
                  label="Postgres table"
                  value={pgForm.postgres_table}
                  onChange={(value) => setPGForm({ ...pgForm, postgres_table: value })}
                  placeholder="orders"
                />
                <Field
                  label="Internal table name"
                  value={pgForm.table_name}
                  onChange={(value) => setPGForm({ ...pgForm, table_name: value })}
                  placeholder="orders_snapshot"
                />
                <Field
                  label="Primary key"
                  value={pgForm.primary_key}
                  onChange={(value) => setPGForm({ ...pgForm, primary_key: value })}
                />
                <Field
                  label="Watermark column"
                  value={pgForm.watermark_column}
                  onChange={(value) => setPGForm({ ...pgForm, watermark_column: value })}
                />
                <Field
                  label="Poll interval (seconds)"
                  type="number"
                  value={String(pgForm.poll_interval_seconds)}
                  onChange={(value) =>
                    setPGForm({ ...pgForm, poll_interval_seconds: Number(value) })
                  }
                />
                <Field
                  label="Stratify columns"
                  value={pgForm.stratify_columns}
                  onChange={(value) => setPGForm({ ...pgForm, stratify_columns: value })}
                />
                <Field
                  label="Sample rate"
                  type="number"
                  value={String(pgForm.sample_rate)}
                  onChange={(value) => setPGForm({ ...pgForm, sample_rate: Number(value) })}
                />
                <button type="button" className="cta-button cool block" onClick={submitPostgres}>
                  Register Postgres
                </button>
              </div>
            )}
          </aside>
        </div>
      ) : null}
    </div>
  );
}

/* ═══════════════════════════════════════════════════
   DASHBOARD VIEW
   ═══════════════════════════════════════════════════ */

function DashboardView({
  stats,
  sources,
  history,
  onNavigateWorkspace,
  onNavigateSources,
  onNavigateHistory,
  onAddSource,
}: {
  stats: DashboardStats | null;
  sources: SourceCard[];
  history: QueryHistoryEntry[];
  onNavigateWorkspace: () => void;
  onNavigateSources: () => void;
  onNavigateHistory: () => void;
  onAddSource: () => void;
}) {
  const recentHistory = history.slice(0, 5);

  const csvCount = sources.filter((s) => s.source.kind === "csv").length;
  const pgCount = sources.filter((s) => s.source.kind === "postgres").length;
  const sourceSubtext = [
    pgCount > 0 ? `${pgCount} PostgreSQL` : null,
    csvCount > 0 ? `${csvCount} CSV` : null,
  ]
    .filter(Boolean)
    .join(", ") || "No sources";

  return (
    <section className="screen-body dashboard-body">
      <div className="section-head">
        <div>
          <h1>Dashboard Overview</h1>
          <p>Welcome back, here's what's happening in your workspace today</p>
        </div>
        <div className="topbar-actions">
          <button type="button" className="cta-button outline" onClick={onAddSource}>
            Add Data Source
          </button>
          <button type="button" className="cta-button outline" onClick={onNavigateHistory}>
            Browse Saved
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="stats-grid">
        <div className="stat-card" onClick={onNavigateHistory}>
          <div className="stat-header">Total Queries Run</div>
          <div className="stat-value">{stats ? stats.total_queries.toLocaleString() : "—"}</div>
          <div className="stat-subtext success-text">
            {stats && stats.success_count > 0
              ? `${stats.success_count} successful`
              : "Run your first query"}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-header">Average Runtime</div>
          <div className="stat-value">{stats ? `${stats.avg_runtime_ms}ms` : "—"}</div>
          <div className="stat-subtext info-text">
            {stats && stats.avg_runtime_ms > 0 ? "Across all queries" : "No data yet"}
          </div>
        </div>
        <div className="stat-card" onClick={onNavigateSources}>
          <div className="stat-header">Connected Sources</div>
          <div className="stat-value">{stats ? stats.connected_sources : "—"}</div>
          <div className="stat-subtext">{sourceSubtext}</div>
        </div>
      </div>

      <div className="dashboard-columns">
        {/* Query Volume Chart */}
        <div className="dashboard-panel chart-panel">
          <div className="panel-header">
            <h3>Query Volume</h3>
            <span className="panel-badge">Last 7 Days</span>
          </div>
          <div className="chart-area">
            {stats && stats.daily_counts.length > 0 ? (
              <MiniBarChart data={stats.daily_counts} />
            ) : (
              <div className="chart-placeholder">
                <p>No query data yet. Run some queries to see volume trends.</p>
              </div>
            )}
          </div>
          <div className="chart-legend">
            <span className="legend-item">
              <span className="legend-dot success" /> Successful Queries
            </span>
            <span className="legend-item">
              <span className="legend-dot error" /> Errors
            </span>
          </div>
        </div>

        {/* Recent Activity */}
        <div className="dashboard-panel activity-panel">
          <div className="panel-header">
            <h3>Recent Activity</h3>
          </div>
          <div className="activity-list">
            {recentHistory.length === 0 ? (
              <p className="empty-message">No recent activity. Run a query to get started.</p>
            ) : (
              recentHistory.map((entry) => (
                <div key={entry.id} className="activity-item">
                  <span className={`activity-dot ${entry.status}`} />
                  <div className="activity-content">
                    <strong>
                      {entry.status === "success" ? "Query executed" : "Query failed"}
                    </strong>
                    <div className="activity-query-preview">
                      <code>{truncateSQL(entry.sql, 50)}</code>
                      <span className={`activity-status-badge ${entry.status}`}>
                        {entry.status === "success" ? "Success" : "Error"}
                      </span>
                    </div>
                    <small>{formatRelativeTime(entry.created_at)}</small>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      {/* Active Data Sources Table */}
      <div className="dashboard-panel">
        <div className="panel-header">
          <h3>Active Data Sources</h3>
          <button type="button" className="ghost-action" onClick={onNavigateSources}>
            View All
          </button>
        </div>
        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Source Name</th>
                <th>Type</th>
                <th>Status</th>
                <th>Rows</th>
                <th>Last Sync</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {sources.length === 0 ? (
                <tr>
                  <td colSpan={6} style={{ textAlign: "center", padding: 24 }}>
                    No data sources connected. Add one to get started.
                  </td>
                </tr>
              ) : (
                sources.map((card) => (
                  <tr key={card.id}>
                    <td>
                      <strong>{card.name}</strong>
                    </td>
                    <td>{card.engine}</td>
                    <td>
                      <StatusPill status={card.status} />
                    </td>
                    <td>{card.tables.toLocaleString()}</td>
                    <td>{card.syncText}</td>
                    <td>
                      <button
                        type="button"
                        className="ghost-action"
                        onClick={onNavigateWorkspace}
                      >
                        Query
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════════════════
   MINI BAR CHART (pure CSS/HTML)
   ═══════════════════════════════════════════════════ */

function MiniBarChart({
  data,
}: {
  data: Array<{ day: string; success_count: number; error_count: number }>;
}) {
  const maxVal = Math.max(...data.map((d) => d.success_count + d.error_count), 1);
  const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

  return (
    <div className="mini-chart">
      <div className="chart-bars">
        {data.map((d, i) => {
          const total = d.success_count + d.error_count;
          const successPct = (d.success_count / maxVal) * 100;
          const errorPct = (d.error_count / maxVal) * 100;
          const dayDate = new Date(d.day + "T00:00:00");
          const dayLabel = dayNames[dayDate.getDay()] || d.day;
          return (
            <div key={i} className="chart-bar-group" title={`${dayLabel}: ${total} queries`}>
              <div className="bar-stack" style={{ height: "120px" }}>
                {errorPct > 0 && (
                  <div className="bar-segment error" style={{ height: `${errorPct}%` }} />
                )}
                <div className="bar-segment success" style={{ height: `${successPct}%` }} />
              </div>
              <span className="bar-label">{dayLabel}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════
   HISTORY VIEW
   ═══════════════════════════════════════════════════ */

function HistoryView({
  entries,
  searchText,
  onSearchChange,
  statusFilter,
  onStatusFilter,
  onRerunQuery,
}: {
  entries: QueryHistoryEntry[];
  searchText: string;
  onSearchChange: (value: string) => void;
  statusFilter: "all" | "success" | "error";
  onStatusFilter: (value: "all" | "success" | "error") => void;
  onRerunQuery: (sql: string) => void;
}) {
  return (
    <section className="screen-body history-body">
      <div className="section-head">
        <div>
          <h1>Query History</h1>
          <p>View and re-run your previously executed queries.</p>
        </div>
        <div className="filters">
          <div className="top-search-wrap" style={{ width: 260 }}>
            <span className="search-icon">⌕</span>
            <input
              aria-label="Search history"
              value={searchText}
              onChange={(e) => onSearchChange(e.target.value)}
              placeholder="Search queries..."
            />
          </div>
          <select
            aria-label="Status filter"
            value={statusFilter}
            onChange={(e) => onStatusFilter(e.target.value as "all" | "success" | "error")}
          >
            <option value="all">All</option>
            <option value="success">Success</option>
            <option value="error">Error</option>
          </select>
        </div>
      </div>

      <div className="history-table-wrap">
        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Query</th>
                <th>Source</th>
                <th>Mode</th>
                <th>Status</th>
                <th>Runtime</th>
                <th>Speedup</th>
                <th>Rows</th>
                <th>Time</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {entries.length === 0 ? (
                <tr>
                  <td colSpan={9} style={{ textAlign: "center", padding: 32 }}>
                    {searchText || statusFilter !== "all"
                      ? "No queries match your filters."
                      : "No queries have been run yet. Head to the workspace to execute your first query."}
                  </td>
                </tr>
              ) : (
                entries.map((entry) => {
                  const runtime =
                    entry.exact_millis > 0
                      ? `${entry.exact_millis.toFixed(2)}ms`
                      : entry.approx_millis > 0
                      ? `${entry.approx_millis.toFixed(2)}ms`
                      : "—";
                  return (
                    <tr key={entry.id}>
                      <td>
                        <code className="history-sql">{truncateSQL(entry.sql, 60)}</code>
                      </td>
                      <td>{entry.source_name || "—"}</td>
                      <td>
                        <span className="mode-badge">{entry.mode}</span>
                      </td>
                      <td>
                        <span className={`status-pill ${entry.status}`}>
                          {entry.status === "success" ? "Success" : "Error"}
                        </span>
                      </td>
                      <td>{runtime}</td>
                      <td>
                        {entry.speedup > 0 ? (
                          <span className="speedup-badge">{entry.speedup.toFixed(1)}×</span>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td>{entry.row_count > 0 ? entry.row_count.toLocaleString() : "—"}</td>
                      <td>{formatRelativeTime(entry.created_at)}</td>
                      <td>
                        <button
                          type="button"
                          className="ghost-action"
                          onClick={() => onRerunQuery(entry.sql)}
                        >
                          Re-run
                        </button>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════════════════
   SETTINGS VIEW
   ═══════════════════════════════════════════════════ */

function SettingsView({
  apiBase,
  onApiBaseChange,
  defaultMode,
  onDefaultModeChange,
  defaultAccuracy,
  onDefaultAccuracyChange,
  rowLimit,
  onRowLimitChange,
  onSave,
}: {
  apiBase: string;
  onApiBaseChange: (v: string) => void;
  defaultMode: QueryMode;
  onDefaultModeChange: (v: QueryMode) => void;
  defaultAccuracy: number;
  onDefaultAccuracyChange: (v: number) => void;
  rowLimit: number;
  onRowLimitChange: (v: number) => void;
  onSave: () => void;
}) {
  const [saved, setSaved] = useState(false);

  function handleSave() {
    onSave();
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  }

  return (
    <section className="screen-body settings-body">
      <div className="section-head">
        <div>
          <h1>Settings</h1>
          <p>Configure your QuerySwift workspace preferences.</p>
        </div>
      </div>

      <div className="settings-grid">
        <div className="settings-section">
          <h3>Connection</h3>
          <div className="settings-card">
            <label>
              <span className="settings-label">API Base URL</span>
              <span className="settings-hint">The backend server address</span>
              <input
                type="text"
                value={apiBase}
                onChange={(e) => onApiBaseChange(e.target.value)}
              />
            </label>
          </div>
        </div>

        <div className="settings-section">
          <h3>Query Defaults</h3>
          <div className="settings-card">
            <label>
              <span className="settings-label">Default Query Mode</span>
              <span className="settings-hint">
                Compare runs both exact & approximate; Exact runs full dataset only
              </span>
              <select
                value={defaultMode}
                onChange={(e) => onDefaultModeChange(e.target.value as QueryMode)}
              >
                <option value="compare">Compare</option>
                <option value="exact">Exact</option>
                <option value="approx">Approx</option>
              </select>
            </label>

            <label>
              <span className="settings-label">Default Accuracy Target</span>
              <span className="settings-hint">{Math.round(defaultAccuracy * 100)}% confidence</span>
              <input
                type="range"
                min="0.5"
                max="0.99"
                step="0.01"
                value={defaultAccuracy}
                onChange={(e) => onDefaultAccuracyChange(Number(e.target.value))}
              />
            </label>

            <label>
              <span className="settings-label">Row Limit</span>
              <span className="settings-hint">Maximum rows returned per query</span>
              <input
                type="number"
                value={rowLimit}
                onChange={(e) => onRowLimitChange(Number(e.target.value))}
                min={1}
                max={100000}
              />
            </label>
          </div>
        </div>

        <div className="settings-actions">
          <button type="button" className="cta-button cool" onClick={handleSave}>
            {saved ? "✓ Saved!" : "Save Settings"}
          </button>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════════════════
   NAV BUTTON (unchanged)
   ═══════════════════════════════════════════════════ */

function NavButton({
  icon,
  label,
  active,
  onClick,
}: {
  icon: string;
  label: string;
  active?: boolean;
  onClick?: () => void;
}) {
  return (
    <button type="button" className={`nav-button ${active ? "active" : ""}`} onClick={onClick}>
      <span className="nav-icon">{icon}</span>
      <span>{label}</span>
    </button>
  );
}

/* ═══════════════════════════════════════════════════
   SOURCES VIEW (unchanged logic)
   ═══════════════════════════════════════════════════ */

function SourcesView({
  cards,
  activeCardId,
  typeFilter,
  statusFilter,
  loading,
  onTypeFilter,
  onStatusFilter,
  onAddSource,
  onSelectCard,
  onSyncSource,
  onToggleStream,
  streamActionId,
}: {
  cards: SourceCard[];
  activeCardId: string;
  typeFilter: "all" | "csv" | "postgres";
  statusFilter: "all" | "healthy" | "auth-failed" | "syncing";
  loading: boolean;
  onTypeFilter: (value: "all" | "csv" | "postgres") => void;
  onStatusFilter: (value: "all" | "healthy" | "auth-failed" | "syncing") => void;
  onAddSource: () => void;
  onSelectCard: (id: string) => void;
  onSyncSource: (id: string) => void;
  onToggleStream: (source: SourceConfig) => void;
  streamActionId: string;
}) {
  return (
    <section className="screen-body sources-body">
      <div className="section-head">
        <div>
          <h1>Data Sources</h1>
          <p>Manage your database connections and sync settings.</p>
        </div>

        <div className="filters">
          <select
            aria-label="Type filter"
            value={typeFilter}
            onChange={(event) =>
              onTypeFilter(event.target.value as "all" | "csv" | "postgres")
            }
          >
            <option value="all">All Types</option>
            <option value="postgres">Postgres</option>
            <option value="csv">CSV</option>
          </select>
          <select
            aria-label="Status filter"
            value={statusFilter}
            onChange={(event) =>
              onStatusFilter(
                event.target.value as "all" | "healthy" | "auth-failed" | "syncing"
              )
            }
          >
            <option value="all">All Statuses</option>
            <option value="healthy">Healthy</option>
            <option value="syncing">Syncing</option>
            <option value="auth-failed">Auth Failed</option>
          </select>
        </div>
      </div>

      <div className="cards-grid">
        {cards.map((card) => (
          <article
            key={card.id}
            className={`source-card ${activeCardId === card.id ? "active" : ""}`}
            onClick={() => onSelectCard(card.id)}
          >
            <header>
              <div className="source-name-wrap">
                <span className={`db-badge ${card.status === "Syncing" ? "mysql" : "postgres"}`}>⛁</span>
                <div>
                  <h3>{card.name}</h3>
                  <p>{displayTableName(card.source)}</p>
                </div>
              </div>
              <span className="menu-dot">⋮</span>
            </header>

            <div className="source-metadata-row">
              <div>
                <small>Status</small>
                <StatusPill status={card.status} />
              </div>
              <div>
                <small>Type</small>
                <strong>{card.engine}</strong>
              </div>
            </div>

            <div className="source-metadata-row compact">
              <div>
                <small>Last Sync</small>
                <strong>{card.syncText}</strong>
              </div>
              <div>
                <small>Owner</small>
                <div className="owner-tag">
                  <span>{card.ownerInitials}</span>
                  <strong>{card.owner}</strong>
                </div>
              </div>
            </div>

            {card.syncProgress ? (
              <div className="progress-wrap">
                <small>Syncing tables...</small>
                <div className="progress-track">
                  <div style={{ width: `${card.syncProgress}%` }} />
                </div>
                <span>{card.syncProgress}%</span>
              </div>
            ) : null}

            <footer>
              <span>◫ {card.tables.toLocaleString()} rows</span>
              <div className="card-actions">
                <button
                  type="button"
                  className="ghost-action"
                  onClick={(event) => {
                    event.stopPropagation();
                    onSyncSource(card.id);
                  }}
                >
                  Sync now
                </button>
                {card.source.kind === "postgres" ? (
                  <button
                    type="button"
                    className="ghost-action"
                    onClick={(event) => {
                      event.stopPropagation();
                      onToggleStream(card.source);
                    }}
                    disabled={streamActionId === card.id}
                  >
                    {streamActionId === card.id
                      ? "Working..."
                      : (card.source.streaming ? "Stop stream" : "Start stream")}
                  </button>
                ) : null}
              </div>
            </footer>
          </article>
        ))}

        <button type="button" className="add-card" onClick={onAddSource}>
          <span>＋</span>
          <strong>Add Data Source</strong>
          <p>Connect a new database to start querying.</p>
        </button>
      </div>

      {!loading && cards.length === 0 ? (
        <p className="hint-text">No sources match your filters. Add a source to begin.</p>
      ) : null}
      {loading ? <p className="hint-text">Loading sources...</p> : null}
    </section>
  );
}

/* ═══════════════════════════════════════════════════
   WORKSPACE VIEW — Redesigned
   ═══════════════════════════════════════════════════ */

function WorkspaceView({
  sql,
  onSqlChange,
  activeCard,
  sourceCards,
  onSelectSource,
  health,
  hasActiveStream,
  queryMode,
  onQueryMode,
  accuracyTarget,
  onAccuracyTarget,
  resultView,
  onResultView,
  queryResult,
  activeResult,
  streamActionId,
  onToggleStream,
  workspaceTab,
  onWorkspaceTab,
  samplingMethod,
  onSamplingMethod,
  sampleFraction,
  onSampleFraction,
  onRunQuery,
  isRunningQuery,
  onRunBenchmark,
  isRunningBenchmark,
  benchmarkResult,
}: {
  sql: string;
  onSqlChange: (value: string) => void;
  activeCard?: SourceCard;
  sourceCards: SourceCard[];
  onSelectSource: (id: string) => void;
  health: string;
  hasActiveStream: boolean;
  queryMode: QueryMode;
  onQueryMode: (value: QueryMode) => void;
  accuracyTarget: number;
  onAccuracyTarget: (value: number) => void;
  resultView: "approx" | "exact";
  onResultView: (value: "approx" | "exact") => void;
  queryResult: RunQueryResponse | null;
  activeResult: QueryResult | null;
  streamActionId: string;
  onToggleStream: (source: SourceConfig) => void;
  workspaceTab: "query" | "benchmark";
  onWorkspaceTab: (tab: "query" | "benchmark") => void;
  samplingMethod: "random" | "stratified";
  onSamplingMethod: (m: "random" | "stratified") => void;
  sampleFraction: number;
  onSampleFraction: (v: number) => void;
  onRunQuery: () => void;
  isRunningQuery: boolean;
  onRunBenchmark: () => void;
  isRunningBenchmark: boolean;
  benchmarkResult: BenchmarkReport | null;
}) {
  const approxResult = queryResult?.approx ?? null;
  const exactResult = queryResult?.exact ?? null;

  const speedup = approxResult?.metric.speedup ?? 0;
  const errorPct = approxResult?.metric.actual_error ?? approxResult?.metric.estimated_error ?? 0;
  const sampleRate = approxResult?.metric.sample_rate ?? sampleFraction / 100;
  const methodLabel = samplingMethod === "stratified" ? "STRATIFIED" : "RANDOM";

  const formatNum = (v: unknown): string => {
    if (typeof v === "number") return v.toLocaleString();
    if (typeof v === "string") {
      const n = Number(v);
      return Number.isNaN(n) ? v : n.toLocaleString();
    }
    return String(v ?? "—");
  };

  const getResultDisplay = (result: QueryResult | null) => {
    if (!result || result.rows.length === 0) return { value: "—", time: "—", rows: "—" };
    const firstRow = result.rows[0];
    const firstKey = result.schema[0];
    const val = firstRow[firstKey];
    return {
      value: formatNum(val),
      time: `${result.metric.execution_millis.toFixed(3)} ms`,
      rows: `${(activeCard?.source.raw_row_count ?? result.metric.row_count).toLocaleString()} rows scanned`,
    };
  };

  const approxDisplay = getResultDisplay(approxResult);
  const exactDisplay = getResultDisplay(exactResult);

  return (
    <section className="screen-body workspace-body ws-new">
      {/* ─── Workspace Tabs ─── */}
      <div className="ws-tabs">
        <button
          type="button"
          className={workspaceTab === "query" ? "active" : ""}
          onClick={() => onWorkspaceTab("query")}
        >
          Query runner
        </button>
        <button
          type="button"
          className={workspaceTab === "benchmark" ? "active" : ""}
          onClick={() => onWorkspaceTab("benchmark")}
        >
          Benchmark
        </button>
        <div className="ws-tabs-line" />
      </div>

      {/* ─── SQL Query Section ─── */}
      <div className="ws-section">
        <label className="ws-label">SQL QUERY</label>
        <div className="ws-sql-box">
          <textarea
            value={sql}
            onChange={(e) => onSqlChange(e.target.value)}
            spellCheck={false}
            rows={3}
            placeholder="SELECT COUNT(*) FROM data"
          />
        </div>
      </div>

      {/* ─── Sampling Method ─── */}
      <div className="ws-section">
        <label className="ws-label">SAMPLING METHOD</label>
        <div className="ws-method-toggle">
          <button
            type="button"
            className={samplingMethod === "random" ? "active" : ""}
            onClick={() => onSamplingMethod("random")}
          >
            Random Sampling
          </button>
          <button
            type="button"
            className={samplingMethod === "stratified" ? "active" : ""}
            onClick={() => onSamplingMethod("stratified")}
          >
            Stratified Sampling (need GROUP BY)
          </button>
        </div>
      </div>

      {/* ─── Sample Fraction ─── */}
      <div className="ws-section">
        <label className="ws-label">
          SAMPLE FRACTION — <span className="ws-pct-highlight">{sampleFraction}%</span>{" "}
          <span className="ws-pct-detail">(scans {sampleFraction}% of data)</span>
        </label>
        <div className="ws-slider-row">
          <span className="ws-slider-bound">1%</span>
          <input
            type="range"
            min="1"
            max="100"
            value={sampleFraction}
            onChange={(e) => onSampleFraction(Number(e.target.value))}
            className="ws-fraction-slider"
          />
          <span className="ws-slider-bound">100%</span>
        </div>
      </div>

      {/* ─── Action Buttons ─── */}
      <div className="ws-actions">
        <button
          type="button"
          className="ws-btn-run"
          onClick={onRunQuery}
          disabled={isRunningQuery}
        >
          {isRunningQuery ? "Running..." : "Run Query"}
        </button>
        <button
          type="button"
          className="ws-btn-benchmark"
          onClick={onRunBenchmark}
          disabled={isRunningBenchmark}
        >
          {isRunningBenchmark ? "Running..." : "Run Benchmark"}
        </button>
      </div>

      {/* ─── Sampling Method Used ─── */}
      {queryResult && (
        <div className="ws-method-used">
          Sampling method used:{" "}
          <span className="ws-method-link">{samplingMethod === "stratified" ? "Stratified" : "Random"}</span>
        </div>
      )}

      {/* ─── Side-by-Side Results ─── */}
      <div className="ws-results-grid">
        <div className={`ws-result-card ${resultView === "approx" ? "active" : ""}`} onClick={() => onResultView("approx")}>
          <span className="ws-result-tag approx">APPROXIMATE</span>
          <div className="ws-result-value">{approxDisplay.value}</div>
          <div className="ws-result-meta">{approxDisplay.time} | {approxDisplay.rows}</div>
        </div>
        <div className={`ws-result-card ${resultView === "exact" ? "active" : ""}`} onClick={() => onResultView("exact")}>
          <span className="ws-result-tag exact">EXACT</span>
          <div className="ws-result-value">{exactDisplay.value}</div>
          <div className="ws-result-meta">{exactDisplay.time} | {exactDisplay.rows}</div>
        </div>
      </div>

      {/* ─── Metrics Cards ─── */}
      <div className="ws-metrics-row">
        <div className="ws-metric-card">
          <div className="ws-metric-value speedup">{speedup > 0 ? `${speedup.toFixed(2)}X` : "—"}</div>
          <div className="ws-metric-label">Speedup</div>
        </div>
        <div className="ws-metric-card">
          <div className="ws-metric-value error">{errorPct > 0 ? `${errorPct.toFixed(1)}%` : "0.0%"}</div>
          <div className="ws-metric-label">Error</div>
        </div>
        <div className="ws-metric-card">
          <div className="ws-metric-value rows-used">{Math.round(sampleRate * 100)}%</div>
          <div className="ws-metric-label">Rows Used</div>
        </div>
        <div className="ws-metric-card">
          <div className="ws-metric-value method">{methodLabel}</div>
          <div className="ws-metric-label">Method</div>
        </div>
      </div>

      {/* ─── Full Data Table ─── */}
      {activeResult && activeResult.rows.length > 1 && (
        <div className="ws-full-table">
          <div className="panel-header">
            <h3>Full Results</h3>
            <div className="result-tabs">
              <button
                type="button"
                className={resultView === "approx" ? "active" : ""}
                onClick={() => onResultView("approx")}
                disabled={!queryResult?.approx}
              >
                Approx
              </button>
              <button
                type="button"
                className={resultView === "exact" ? "active" : ""}
                onClick={() => onResultView("exact")}
                disabled={!queryResult?.exact}
              >
                Exact
              </button>
            </div>
          </div>
          <div className="table-shell">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  {activeResult.schema.map((col) => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {activeResult.rows.map((row, i) => (
                  <tr key={i}>
                    <td>{i + 1}</td>
                    {activeResult.schema.map((col) => (
                      <td key={col}>{String(row[col] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}



/* ═══════════════════════════════════════════════════
   SHARED COMPONENTS
   ═══════════════════════════════════════════════════ */

function StatusPill({ status }: { status: SourceCard["status"] }) {
  return <span className={`status-pill ${status.toLowerCase().replace(" ", "-")}`}>{status}</span>;
}

function Field({
  label,
  value,
  onChange,
  type = "text",
  placeholder,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  type?: string;
  placeholder?: string;
}) {
  return (
    <label>
      {label}
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

/* ═══════════════════════════════════════════════════
   UTILITY FUNCTIONS
   ═══════════════════════════════════════════════════ */

function splitColumns(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function normalizeStatus(source: SourceConfig): CardStatus {
  const status = (source.status ?? "").toLowerCase();
  if (source.streaming || status.includes("sync")) {
    return "Syncing";
  }
  if (status.includes("fail") || status.includes("error") || status.includes("auth")) {
    return "Auth Failed";
  }
  return "Healthy";
}

function displayTableName(source: SourceConfig): string {
  if (source.kind === "postgres") {
    if (source.postgres_schema && source.postgres_schema !== "public") {
      return `${source.postgres_schema}.${source.postgres_table || source.table_name}`;
    }
    return source.postgres_table || source.table_name;
  }
  return source.table_name;
}

function formatRelativeTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const diffMs = Date.now() - date.getTime();
  const diffMinutes = Math.floor(diffMs / 60000);
  if (diffMinutes < 1) {
    return "just now";
  }
  if (diffMinutes < 60) {
    return `${diffMinutes} min${diffMinutes === 1 ? "" : "s"} ago`;
  }
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

function truncateSQL(sql: string, maxLen: number): string {
  const cleaned = sql.replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.substring(0, maxLen) + "...";
}

function stripWrappingQuotes(value: string): string {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1).trim();
  }
  return value;
}

function inferTableName(name: string, fallbackValue: string): string {
  const raw = (name || fallbackValue)
    .trim()
    .split(/[\\/]/)
    .pop()
    ?.replace(/\.[a-zA-Z0-9]+$/, "")
    .toLowerCase() ?? "";
  return raw.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
}

function chooseAvailableResultView(
  currentView: "approx" | "exact",
  result: RunQueryResponse
): "approx" | "exact" {
  if (currentView === "exact" && result.exact) {
    return "exact";
  }
  if (currentView === "approx" && result.approx) {
    return "approx";
  }
  if (result.exact) {
    return "exact";
  }
  return "approx";
}
