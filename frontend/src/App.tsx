import { useCallback, useEffect, useMemo, useState } from "react";
import {
  fetchHealth,
  fetchSources,
  importCSV,
  registerPostgres,
  runQuery,
  startStream,
  stopStream,
  syncSource,
} from "./api";
import type { QueryMode, QueryResult, RunQueryResponse, SourceConfig } from "./types";

type Screen = "sources" | "workspace";

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
  const [screen, setScreen] = useState<Screen>("sources");
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

  const loadData = useCallback(async () => {
    try {
      setIsLoading(true);
      setError("");
      const [healthPayload, sourcePayload] = await Promise.all([fetchHealth(), fetchSources()]);
      setHealth(
        healthPayload.ok
          ? `Backend ready - ${healthPayload.source_count} tables`
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

  useEffect(() => {
    loadData();
  }, [loadData]);

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
    if (filteredCards.length === 0) {
      setActiveCardId("");
      return;
    }
    if (!activeCardId || !filteredCards.some((card) => card.id === activeCardId)) {
      setActiveCardId(filteredCards[0].id);
    }
  }, [activeCardId, filteredCards]);

  const themeClass = screen === "workspace" ? "theme-workspace" : "theme-sources";
  const activeCard = useMemo(
    () => filteredCards.find((card) => card.id === activeCardId) ?? filteredCards[0],
    [activeCardId, filteredCards]
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
    }, 5000);
    return () => window.clearInterval(timer);
  }, [hasActiveStream, loadData]);

  async function submitQuery() {
    if (!sql.trim()) {
      setError("Please enter a SQL query.");
      return;
    }

    try {
      setIsRunningQuery(true);
      setError("");
      const result = await runQuery(sql, queryMode, accuracyTarget);
      setQueryResult(result);
      if (result.approx) {
        setResultView("approx");
      } else if (result.exact) {
        setResultView("exact");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed");
    } finally {
      setIsRunningQuery(false);
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
      setScreen("sources");
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
      setScreen("sources");
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

  return (
    <div className={`qs-root ${themeClass}`}>
      <div className="qs-shell">
        <aside className="qs-sidebar">
          <div>
            <div className="brand-row">
              <span className="brand-icon">⚡</span>
              <span className="brand-name">QuerySwift</span>
            </div>

            <button className="account-pill" type="button">
              <span className="account-avatar">A</span>
              <span>Acme Corp</span>
              <span className="caret">▾</span>
            </button>

            <nav className="nav-sections" aria-label="Navigation">
              <NavButton
                label="Query Workspace"
                icon="〉_"
                active={screen === "workspace"}
                onClick={() => setScreen("workspace")}
              />
              <NavButton
                label="Data Sources"
                icon="⛁"
                active={screen === "sources"}
                onClick={() => setScreen("sources")}
              />
            </nav>
          </div>

          <footer className="user-panel">
            <span className="user-avatar">JD</span>
            <div>
              <strong>John Doe</strong>
              <p>john@acmecorp.com</p>
            </div>
          </footer>
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
            ) : (
              <h2 className="top-title">Query Workspace</h2>
            )}

            <div className="topbar-actions">
              {screen === "sources" ? (
                <button type="button" className="cta-button warm" onClick={() => setConnectionOpen(true)}>
                  + Add Data Source
                </button>
              ) : (
                <button type="button" className="cta-button cool" onClick={submitQuery}>
                  {isRunningQuery ? "Running..." : "▶ Run All"}
                </button>
              )}
            </div>
          </header>

          {error ? <div className="inline-error">{error}</div> : null}

          {screen === "sources" ? (
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
          ) : (
            <WorkspaceView
              sql={sql}
              onSqlChange={setSql}
              activeCard={activeCard}
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
            />
          )}
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

function WorkspaceView({
  sql,
  onSqlChange,
  activeCard,
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
}: {
  sql: string;
  onSqlChange: (value: string) => void;
  activeCard?: SourceCard;
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
}) {
  return (
    <section className="screen-body workspace-body">
      <div className="workspace-dbbar">
        <button type="button" className="chip success">
          ● {activeCard?.name ?? "No source selected"}
        </button>
        <button type="button" className="chip muted">
          ◫ {activeCard ? displayTableName(activeCard.source) : "public"}
        </button>
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

        <div className="table-shell">
          {activeResult ? (
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  {activeResult.schema.map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {activeResult.rows.map((row, index) => (
                  <tr key={index}>
                    <td>{index + 1}</td>
                    {activeResult.schema.map((column) => (
                      <td key={column}>{String(row[column] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="result-placeholder">
              Run a query to see live results from your selected source.
            </div>
          )}
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
    </section>
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
