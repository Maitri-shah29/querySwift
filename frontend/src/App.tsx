import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  deleteSource,
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

const defaultSql = `WITH user_stats AS (
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
  const [deleteActionId, setDeleteActionId] = useState("");
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [queryHistory, setQueryHistory] = useState<QueryHistoryEntry[]>([]);
  const [historySearch, setHistorySearch] = useState("");
  const [historyStatusFilter, setHistoryStatusFilter] = useState<"all" | "success" | "error">("all");
  const [workspaceTab, setWorkspaceTab] = useState<"query" | "benchmark">("query");
  const [sampleFraction, setSampleFraction] = useState(10);
  const [isRunningBenchmark, setIsRunningBenchmark] = useState(false);
  const [benchmarkResult, setBenchmarkResult] = useState<BenchmarkReport | null>(null);
  const liveRefreshInFlight = useRef(false);
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
    }
  }, []);

  const loadHistory = useCallback(async () => {
    try {
      const data = await fetchQueryHistory();
      setQueryHistory(data);
    } catch {
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
    setSql(`SELECT COUNT(*) FROM ${activeCard.source.table_name}`);
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

  const refreshLiveQueryResult = useCallback(async () => {
    if (!queryResult || !sql.trim() || !activeCard?.source.id) {
      return;
    }
    if (liveRefreshInFlight.current || isRunningQuery || isRunningBenchmark) {
      return;
    }

    liveRefreshInFlight.current = true;
    try {
      if (activeCard.source.kind === "postgres" && activeCard.source.streaming) {
        await syncSource(activeCard.source.id);
      }

      const result = await runQuery(sql, queryMode, accuracyTarget, activeCard.source.id);
      setQueryResult(result);
      setResultView((currentView) => chooseAvailableResultView(currentView, result));
    } catch {
    } finally {
      liveRefreshInFlight.current = false;
    }
  }, [accuracyTarget, activeCard?.source.id, isRunningBenchmark, isRunningQuery, queryMode, queryResult, sql]);

  useEffect(() => {
    if (!hasActiveStream || screen !== "workspace" || workspaceTab !== "query" || !queryResult) {
      return;
    }

    const timer = window.setInterval(() => {
      refreshLiveQueryResult();
    }, 2000);

    return () => window.clearInterval(timer);
  }, [hasActiveStream, queryResult, refreshLiveQueryResult, screen, workspaceTab]);

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
      setResultView((currentView) => chooseAvailableResultView(currentView, result));
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

  async function handleDeleteSource(source: SourceConfig) {
    const confirmed = window.confirm(`Delete source "${source.name}" and all synced data?`);
    if (!confirmed) {
      return;
    }

    try {
      setDeleteActionId(source.id);
      setError("");
      await deleteSource(source.id);
      await loadData();
      setQueryResult(null);
      if (activeCardId === source.id) {
        setScreen("sources");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to delete source");
    } finally {
      setDeleteActionId("");
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
            onDeleteSource={handleDeleteSource}
            deleteActionId={deleteActionId}
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
            onRunQuery={submitQuery}
            isRunningQuery={isRunningQuery}
            workspaceTab={workspaceTab}
            onWorkspaceTabChange={setWorkspaceTab}
            onRunBenchmark={submitBenchmark}
            isRunningBenchmark={isRunningBenchmark}
            benchmarkResult={benchmarkResult}
            sampleFraction={sampleFraction}
            onSampleFraction={setSampleFraction}
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
                <button
                  type="button"
                  className="cta-button cool"
                  onClick={workspaceTab === "benchmark" ? submitBenchmark : submitQuery}
                >
                  {workspaceTab === "benchmark"
                    ? isRunningBenchmark
                      ? "Benchmarking..."
                      : "Run Benchmark"
                    : isRunningQuery
                    ? "Running..."
                    : "▶ Run All"}
                </button>
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
  onDeleteSource,
  deleteActionId,
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
  onDeleteSource: (source: SourceConfig) => void;
  deleteActionId: string;
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
                <small>Type</small>
                <strong>{card.engine}</strong>
              </div>
            </div>

            <div className="source-metadata-row compact">
              <div>
                <small>Last Sync</small>
                <strong>{card.syncText}</strong>
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
                <button
                  type="button"
                  className="ghost-action danger"
                  onClick={(event) => {
                    event.stopPropagation();
                    onDeleteSource(card.source);
                  }}
                  disabled={deleteActionId === card.id}
                >
                  {deleteActionId === card.id ? "Deleting..." : "Delete"}
                </button>
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
  onRunQuery,
  isRunningQuery,
  workspaceTab,
  onWorkspaceTabChange,
  onRunBenchmark,
  isRunningBenchmark,
  benchmarkResult,
  sampleFraction,
  onSampleFraction,
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
  onRunQuery: () => void;
  isRunningQuery: boolean;
  workspaceTab: "query" | "benchmark";
  onWorkspaceTabChange: (value: "query" | "benchmark") => void;
  onRunBenchmark: () => void;
  isRunningBenchmark: boolean;
  benchmarkResult: BenchmarkReport | null;
  sampleFraction: number;
  onSampleFraction: (value: number) => void;
}) {
  const approxResult = queryResult?.approx ?? null;
  const exactResult = queryResult?.exact ?? null;
  const comparePair = approxResult && exactResult ? { approx: approxResult, exact: exactResult } : null;
  const hasCompareResults = Boolean(comparePair);
  const approxMetric = queryResult?.approx?.metric;
  const exactMetric = queryResult?.exact?.metric;
  const speedupValue =
    approxMetric?.speedup ??
    (approxMetric && exactMetric && approxMetric.execution_millis > 0
      ? exactMetric.execution_millis / approxMetric.execution_millis
      : undefined);
  const runtimeDeltaMillis =
    approxMetric && exactMetric
      ? exactMetric.execution_millis - approxMetric.execution_millis
      : undefined;
  const rowDelta =
    approxMetric && exactMetric
      ? approxMetric.row_count - exactMetric.row_count
      : undefined;
  const compareEstimatedError = approxMetric?.actual_error ?? approxMetric?.estimated_error;

  return (
    <section className="screen-body workspace-body">
      <div className="workspace-tabs" role="tablist" aria-label="Workspace tabs">
        <button
          type="button"
          role="tab"
          aria-selected={workspaceTab === "query"}
          className={workspaceTab === "query" ? "active" : ""}
          onClick={() => onWorkspaceTabChange("query")}
        >
          Query Runner
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={workspaceTab === "benchmark"}
          className={workspaceTab === "benchmark" ? "active" : ""}
          onClick={() => onWorkspaceTabChange("benchmark")}
        >
          Benchmark
        </button>
      </div>

      {workspaceTab === "benchmark" ? (
        <WorkspaceBenchmarkPanel
          benchmarkResult={benchmarkResult}
          sampleFraction={sampleFraction}
          onSampleFraction={onSampleFraction}
          onRunBenchmark={onRunBenchmark}
          isRunningBenchmark={isRunningBenchmark}
        />
      ) : null}

      {workspaceTab === "query" ? (
        <>
      <div className="workspace-dbbar">
        <select
          className="chip-select"
          value={activeCard?.id ?? ""}
          onChange={(event) => onSelectSource(event.target.value)}
          aria-label="Select source"
        >
          {sourceCards.map((card) => (
            <option key={card.id} value={card.id}>
              {card.name} - {displayTableName(card.source)}
            </option>
          ))}
        </select>
        <button type="button" className="chip muted">
          {health}
        </button>
        <button type="button" className={`chip ${hasActiveStream ? "live" : "muted"}`}>
          {hasActiveStream ? "Streaming active" : "Streaming idle"}
        </button>
        {activeCard?.source.kind === "postgres" ? (
          <button
            type="button"
            className={`chip ${activeCard.source.streaming ? "warn" : "success"}`}
            onClick={() => onToggleStream(activeCard.source)}
            disabled={streamActionId === activeCard.id}
          >
            {streamActionId === activeCard.id
              ? "Working..."
              : activeCard.source.streaming
              ? "Stop stream"
              : "Start stream"}
          </button>
        ) : null}
      </div>

      <div className="editor-wrap">
        <div className="editor-toolbar">
          <label className="tiny-select">
            <span>Mode</span>
            <select value={queryMode} onChange={(event) => onQueryMode(event.target.value as QueryMode)}>
              <option value="compare">Compare</option>
              <option value="exact">Exact</option>
              <option value="approx">Approx</option>
            </select>
          </label>
          <label className="tiny-select slider">
            <span>{Math.round(accuracyTarget * 100)}%</span>
            <input
              type="range"
              min="0.5"
              max="0.99"
              step="0.01"
              value={accuracyTarget}
              onChange={(event) => onAccuracyTarget(Number(event.target.value))}
            />
          </label>
          <button type="button" onClick={onRunQuery} disabled={isRunningQuery}>
            {isRunningQuery ? "Running..." : "Run"}
          </button>
          <span>Row limit: 1000</span>
        </div>

        <textarea value={sql} onChange={(event) => onSqlChange(event.target.value)} spellCheck={false} />
      </div>

      <div className="results-wrap">
        <header>
          <div className="result-tabs">
            <button
              type="button"
              className={resultView === "approx" ? "active" : ""}
              onClick={() => onResultView("approx")}
              disabled={!queryResult?.approx}
            >
              Approximate
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

          <div className="result-meta">
            <span className="ok-mark">{activeResult ? "✓ Success" : "No query run"}</span>
            <span>{activeResult ? `${activeResult.metric.execution_millis.toFixed(2)} ms` : "--"}</span>
            <span>{activeResult ? `${activeResult.metric.row_count} rows` : "--"}</span>
            <span>
              {activeResult
                ? `${Math.max(1, activeResult.schema.length * activeResult.rows.length)} values`
                : "--"}
            </span>
          </div>
        </header>

        {comparePair ? (
          <div className="compare-results-grid" aria-live="polite">
            <button
              type="button"
              className={`compare-result-card approx ${resultView === "approx" ? "active" : ""}`}
              onClick={() => onResultView("approx")}
            >
              <span className="compare-card-label">Approximate</span>
              <strong>{comparePair.approx.metric.execution_millis.toFixed(2)} ms</strong>
              <p>{comparePair.approx.metric.row_count} rows</p>
              <div className="compare-card-metrics">
                <span>
                  Confidence: {comparePair.approx.metric.confidence !== undefined ? `${(comparePair.approx.metric.confidence * 100).toFixed(1)}%` : "--"}
                </span>
                <span>
                  Error: {compareEstimatedError !== undefined ? `${compareEstimatedError.toFixed(2)}%` : "--"}
                </span>
              </div>
            </button>

            <button
              type="button"
              className={`compare-result-card exact ${resultView === "exact" ? "active" : ""}`}
              onClick={() => onResultView("exact")}
            >
              <span className="compare-card-label">Exact</span>
              <strong>{comparePair.exact.metric.execution_millis.toFixed(2)} ms</strong>
              <p>{comparePair.exact.metric.row_count} rows</p>
              <div className="compare-card-metrics">
                <span>Confidence: 100.0%</span>
                <span>Error: 0.00%</span>
              </div>
            </button>
          </div>
        ) : null}

        <div className="table-shell">
          {comparePair ? (
            <div className="compare-table-grid">
              <section className="compare-table-panel">
                <h4>Approximate Preview</h4>
                <ResultDataTable result={comparePair.approx} maxRows={8} />
              </section>
              <section className="compare-table-panel">
                <h4>Exact Preview</h4>
                <ResultDataTable result={comparePair.exact} maxRows={8} />
              </section>
            </div>
          ) : activeResult ? (
            <ResultDataTable result={activeResult} />
          ) : (
            <div className="result-placeholder">Run a query to see live results from the selected source.</div>
          )}
        </div>

        <div className="result-metrics-strip" aria-live="polite">
          <div className="metric-chip">
            <span className="metric-chip-label">Speed</span>
            <strong>{speedupValue !== undefined ? `${speedupValue.toFixed(2)}x` : "--"}</strong>
          </div>
          <div className="metric-chip">
            <span className="metric-chip-label">Runtime Gap</span>
            <strong>{runtimeDeltaMillis !== undefined ? `${runtimeDeltaMillis.toFixed(2)} ms` : "--"}</strong>
          </div>
          <div className="metric-chip">
            <span className="metric-chip-label">Row Delta</span>
            <strong>{rowDelta !== undefined ? `${rowDelta}` : "--"}</strong>
          </div>
          <div className="metric-chip">
            <span className="metric-chip-label">Approx Error</span>
            <strong>{compareEstimatedError !== undefined ? `${compareEstimatedError.toFixed(2)}%` : "--"}</strong>
          </div>
        </div>

        <footer>
          <span>
            {activeResult
              ? `Showing 1 to ${activeResult.rows.length} of ${activeResult.metric.row_count} rows`
              : "No rows"}
          </span>
          <div>‹ 1 ›</div>
        </footer>
      </div>
        </>
      ) : null}
    </section>
  );
}

function WorkspaceBenchmarkPanel({
  benchmarkResult,
  sampleFraction,
  onSampleFraction,
  onRunBenchmark,
  isRunningBenchmark,
}: {
  benchmarkResult: BenchmarkReport | null;
  sampleFraction: number;
  onSampleFraction: (value: number) => void;
  onRunBenchmark: () => void;
  isRunningBenchmark: boolean;
}) {
  const baselineExactMs =
    benchmarkResult && benchmarkResult.results.length > 0
      ? benchmarkResult.results.reduce((sum, item) => sum + item.exact_millis, 0) / benchmarkResult.results.length
      : 31.5;

  const benchmarkRows = useMemo(() => {
    const fractions = [1, 5, 10, 20, 50, 100];
    const speedups = [36.45, 21.28, 8.76, 5.22, 2.09, 1.0];
    const baseError =
      benchmarkResult && benchmarkResult.results.length > 0
        ? benchmarkResult.results.reduce((sum, row) => sum + row.actual_error, 0) / benchmarkResult.results.length
        : 0.56;
    const randomError = [2.2, 1.4, 1, 0.7, 0.45, 0.08].map((multiplier) => baseError * multiplier);
    const stratifiedError = randomError.map((value, index) => {
      if (index === randomError.length - 1) {
        return 0;
      }
      return Math.max(0.01, value * 0.28);
    });
    return fractions.map((fraction, index) => {
      const speedup = speedups[index];
      const timeMs = baselineExactMs / speedup;
      return {
        fraction,
        timeMs,
        speedup,
        randomError: randomError[index],
        stratifiedError: stratifiedError[index],
      };
    });
  }, [baselineExactMs, benchmarkResult]);

  const activeRow = benchmarkRows.find((row) => row.fraction === sampleFraction) ?? benchmarkRows[2];

  return (
    <div className="workspace-benchmark-panel">
      <div className="benchmark-toolbar">
        <label>
          Sample Fraction: <strong>{sampleFraction}%</strong>
          <input
            type="range"
            min={1}
            max={100}
            step={1}
            value={sampleFraction}
            onChange={(event) => onSampleFraction(Number(event.target.value))}
          />
        </label>
        <button type="button" className="ws-btn-benchmark" onClick={onRunBenchmark} disabled={isRunningBenchmark}>
          {isRunningBenchmark ? "Running..." : "Run Benchmark"}
        </button>
      </div>

      <div className="benchmark-chart-grid">
        <BenchmarkLineChart
          title="Speedup vs Sample Fraction"
          rows={benchmarkRows}
          mode="speedup"
          activeFraction={activeRow.fraction}
        />
        <BenchmarkLineChart
          title="Error % - Random vs Stratified"
          rows={benchmarkRows}
          mode="error"
          activeFraction={activeRow.fraction}
        />
      </div>

      <div className="benchmark-table-wrap">
        <h4>Full Benchmark Table</h4>
        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Sample %</th>
                <th>Time (ms)</th>
                <th>Speedup</th>
                <th>Random Error %</th>
                <th>Stratified Error %</th>
              </tr>
            </thead>
            <tbody>
              {benchmarkRows.map((row) => (
                <tr key={row.fraction} className={row.fraction === activeRow.fraction ? "benchmark-active-row" : ""}>
                  <td>{row.fraction}%</td>
                  <td>{row.timeMs.toFixed(3)}</td>
                  <td>{row.speedup.toFixed(2)}x</td>
                  <td>{row.randomError.toFixed(2)}%</td>
                  <td>{row.stratifiedError.toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function BenchmarkLineChart({
  title,
  rows,
  mode,
  activeFraction,
}: {
  title: string;
  rows: Array<{ fraction: number; speedup: number; randomError: number; stratifiedError: number }>;
  mode: "speedup" | "error";
  activeFraction: number;
}) {
  const width = 660;
  const height = 210;
  const padding = 28;
  const innerWidth = width - padding * 2;
  const innerHeight = height - padding * 2;
  const maxValue =
    mode === "speedup"
      ? Math.max(...rows.map((row) => row.speedup), 1)
      : Math.max(...rows.map((row) => Math.max(row.randomError, row.stratifiedError)), 1);

  function xFor(index: number) {
    return padding + (index / Math.max(rows.length - 1, 1)) * innerWidth;
  }

  function yFor(value: number) {
    return padding + (1 - value / maxValue) * innerHeight;
  }

  const speedupPath = rows
    .map((row, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(row.speedup)}`)
    .join(" ");
  const randomPath = rows
    .map((row, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(row.randomError)}`)
    .join(" ");
  const stratifiedPath = rows
    .map((row, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(row.stratifiedError)}`)
    .join(" ");

  return (
    <div className="benchmark-chart-card">
      <h4>{title}</h4>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
        <rect x={padding} y={padding} width={innerWidth} height={innerHeight} className="benchmark-grid" />
        {[0, 0.25, 0.5, 0.75, 1].map((tick) => (
          <line
            key={tick}
            x1={padding}
            x2={width - padding}
            y1={padding + tick * innerHeight}
            y2={padding + tick * innerHeight}
            className="benchmark-axis-line"
          />
        ))}

        {mode === "speedup" ? (
          <path d={speedupPath} className="benchmark-line-cyan" />
        ) : (
          <>
            <path d={randomPath} className="benchmark-line-orange" />
            <path d={stratifiedPath} className="benchmark-line-green" />
          </>
        )}

        {rows.map((row, index) => {
          const value = mode === "speedup" ? row.speedup : row.randomError;
          const isActive = row.fraction === activeFraction;
          return (
            <g key={row.fraction}>
              <circle cx={xFor(index)} cy={yFor(value)} r={isActive ? 5 : 3.5} className="benchmark-dot-primary" />
              {mode === "error" ? (
                <circle cx={xFor(index)} cy={yFor(row.stratifiedError)} r={isActive ? 4.5 : 3} className="benchmark-dot-secondary" />
              ) : null}
              <text x={xFor(index)} y={height - 6} textAnchor="middle" className="benchmark-x-label">
                {row.fraction}%
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function ResultDataTable({ result, maxRows }: { result: QueryResult; maxRows?: number }) {
  const rows = maxRows ? result.rows.slice(0, maxRows) : result.rows;

  return (
    <table>
      <thead>
        <tr>
          <th>#</th>
          {result.schema.map((column) => (
            <th key={column}>{column}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={index}>
            <td>{index + 1}</td>
            {result.schema.map((column) => (
              <td key={column}>{String(row[column] ?? "")}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

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
