import { useEffect, useState } from "react";
import {
  fetchBenchmarks,
  fetchHealth,
  fetchSources,
  importCSV,
  registerPostgres,
  runBenchmark,
  runQuery,
  startStream,
  stopStream,
  syncSource,
} from "./api";
import type { BenchmarkReport, QueryMode, RunQueryResponse, SourceConfig } from "./types";

type TabKey = "sources" | "query" | "benchmarks" | "streaming";

const defaultSQL = "SELECT region, SUM(revenue) AS total_revenue, COUNT(*) AS total_rows FROM sales GROUP BY region";

export default function App() {
  const [tab, setTab] = useState<TabKey>("sources");
  const [health, setHealth] = useState("Checking backend...");
  const [error, setError] = useState("");
  const [sources, setSources] = useState<SourceConfig[]>([]);
  const [benchmarks, setBenchmarks] = useState<BenchmarkReport[]>([]);
  const [queryMode, setQueryMode] = useState<QueryMode>("compare");
  const [accuracyTarget, setAccuracyTarget] = useState(0.9);
  const [sql, setSQL] = useState(defaultSQL);
  const [queryResult, setQueryResult] = useState<RunQueryResponse | null>(null);
  const [csvForm, setCSVForm] = useState({ name: "CSV Dataset", table_name: "sales", file_path: "", stratify_columns: "region", sample_rate: 0.1 });
  const [pgForm, setPGForm] = useState({ name: "Postgres Dataset", table_name: "sales", postgres_dsn: "", postgres_schema: "public", postgres_table: "sales", primary_key: "id", watermark_column: "updated_at", poll_interval_seconds: 15, stratify_columns: "region", sample_rate: 0.1 });
  const [benchmarkForm, setBenchmarkForm] = useState({ name: "Demo benchmark", queries: defaultSQL, iterations: 3 });

  async function loadData() {
    try {
      setError("");
      const [healthPayload, sourcePayload, benchmarkPayload] = await Promise.all([fetchHealth(), fetchSources(), fetchBenchmarks()]);
      setHealth(healthPayload.ok ? `Backend ready, ${healthPayload.source_count} sources loaded` : "Backend unavailable");
      setSources(sourcePayload);
      setBenchmarks(benchmarkPayload);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setHealth("Backend unavailable");
      setError(message);
    }
  }

  useEffect(() => {
    loadData();
    const timer = window.setInterval(loadData, 10000);
    return () => window.clearInterval(timer);
  }, []);

  async function submitCSV() {
    try {
      setError("");
      await importCSV({ ...csvForm, stratify_columns: splitColumns(csvForm.stratify_columns) });
      await loadData();
      setTab("query");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to import CSV");
    }
  }

  async function submitPostgres() {
    try {
      setError("");
      await registerPostgres({ ...pgForm, stratify_columns: splitColumns(pgForm.stratify_columns) });
      await loadData();
      setTab("streaming");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to register Postgres source");
    }
  }

  async function submitQuery() {
    try {
      setError("");
      setQueryResult(await runQuery(sql, queryMode, accuracyTarget));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed");
    }
  }

  async function submitBenchmark() {
    try {
      setError("");
      await runBenchmark({
        name: benchmarkForm.name,
        iterations: benchmarkForm.iterations,
        accuracy_target: accuracyTarget,
        queries: benchmarkForm.queries.split("\n").map((query) => query.trim()).filter(Boolean),
      });
      await loadData();
      setTab("benchmarks");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Benchmark failed");
    }
  }

  const postgresSources = sources.filter((source) => source.kind === "postgres");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <p className="eyebrow">Approximate Query Engine</p>
        <h1>Exact vs approximate analytics on your desktop</h1>
        <p className="status-pill">{health}</p>
        <nav className="nav">
          <button className={tab === "sources" ? "active" : ""} onClick={() => setTab("sources")}>Data Sources</button>
          <button className={tab === "query" ? "active" : ""} onClick={() => setTab("query")}>Query Studio</button>
          <button className={tab === "benchmarks" ? "active" : ""} onClick={() => setTab("benchmarks")}>Benchmarks</button>
          <button className={tab === "streaming" ? "active" : ""} onClick={() => setTab("streaming")}>Streaming</button>
        </nav>
        {error ? <div className="error-box">{error}</div> : null}
      </aside>

      <main className="content">
        {tab === "sources" ? (
          <section className="panel-grid">
            <section className="panel">
              <h2>CSV Import</h2>
              <Field label="Display name" value={csvForm.name} onChange={(value) => setCSVForm({ ...csvForm, name: value })} />
              <Field label="Query table name" value={csvForm.table_name} onChange={(value) => setCSVForm({ ...csvForm, table_name: value })} />
              <Field label="CSV file path" value={csvForm.file_path} onChange={(value) => setCSVForm({ ...csvForm, file_path: value })} placeholder="C:\\data\\sales.csv" />
              <Field label="Stratify columns" value={csvForm.stratify_columns} onChange={(value) => setCSVForm({ ...csvForm, stratify_columns: value })} />
              <Field label="Sample rate" type="number" value={String(csvForm.sample_rate)} onChange={(value) => setCSVForm({ ...csvForm, sample_rate: Number(value) })} />
              <button onClick={submitCSV}>Import CSV into DuckDB</button>
            </section>

            <section className="panel">
              <h2>Live Postgres Source</h2>
              <Field label="Display name" value={pgForm.name} onChange={(value) => setPGForm({ ...pgForm, name: value })} />
              <Field label="Query table name" value={pgForm.table_name} onChange={(value) => setPGForm({ ...pgForm, table_name: value })} />
              <Field label="Postgres DSN" value={pgForm.postgres_dsn} onChange={(value) => setPGForm({ ...pgForm, postgres_dsn: value })} placeholder="postgres://user:pass@localhost:5432/dbname" />
              <Field label="Source schema" value={pgForm.postgres_schema} onChange={(value) => setPGForm({ ...pgForm, postgres_schema: value })} />
              <Field label="Source table" value={pgForm.postgres_table} onChange={(value) => setPGForm({ ...pgForm, postgres_table: value })} />
              <Field label="Primary key" value={pgForm.primary_key} onChange={(value) => setPGForm({ ...pgForm, primary_key: value })} />
              <Field label="Watermark column" value={pgForm.watermark_column} onChange={(value) => setPGForm({ ...pgForm, watermark_column: value })} />
              <Field label="Poll interval (seconds)" type="number" value={String(pgForm.poll_interval_seconds)} onChange={(value) => setPGForm({ ...pgForm, poll_interval_seconds: Number(value) })} />
              <Field label="Stratify columns" value={pgForm.stratify_columns} onChange={(value) => setPGForm({ ...pgForm, stratify_columns: value })} />
              <button onClick={submitPostgres}>Register and sync Postgres</button>
            </section>

            <section className="panel panel-wide">
              <h2>Registered Sources</h2>
              <SourceTable sources={sources} />
            </section>
          </section>
        ) : null}

        {tab === "query" ? (
          <section className="panel-grid query-layout">
            <section className="panel">
              <h2>Query Studio</h2>
              <label>
                Mode
                <select value={queryMode} onChange={(event) => setQueryMode(event.target.value as QueryMode)}>
                  <option value="compare">Compare exact vs approximate</option>
                  <option value="exact">Exact only</option>
                  <option value="approx">Approximate only</option>
                </select>
              </label>
              <label>
                Accuracy target ({Math.round(accuracyTarget * 100)}%)
                <input type="range" min="0.5" max="0.99" step="0.01" value={accuracyTarget} onChange={(event) => setAccuracyTarget(Number(event.target.value))} />
              </label>
              <label>
                SQL
                <textarea value={sql} onChange={(event) => setSQL(event.target.value)} rows={10} />
              </label>
              <button onClick={submitQuery}>Run Query</button>
            </section>

            <section className="panel panel-wide">
              <h2>Results Compare</h2>
              {!queryResult ? <p>Run a query to inspect exact and approximate results.</p> : null}
              {queryResult?.exact ? <ResultCard title="Exact Result" result={queryResult.exact} /> : null}
              {queryResult?.approx ? <ResultCard title="Approximate Result" result={queryResult.approx} /> : null}
            </section>
          </section>
        ) : null}

        {tab === "benchmarks" ? (
          <section className="panel-grid">
            <section className="panel">
              <h2>Run Benchmark</h2>
              <Field label="Report name" value={benchmarkForm.name} onChange={(value) => setBenchmarkForm({ ...benchmarkForm, name: value })} />
              <Field label="Iterations" type="number" value={String(benchmarkForm.iterations)} onChange={(value) => setBenchmarkForm({ ...benchmarkForm, iterations: Number(value) })} />
              <label>
                Benchmark queries
                <textarea rows={8} value={benchmarkForm.queries} onChange={(event) => setBenchmarkForm({ ...benchmarkForm, queries: event.target.value })} />
              </label>
              <button onClick={submitBenchmark}>Run Benchmark Suite</button>
            </section>

            <section className="panel panel-wide">
              <h2>Benchmark Dashboard</h2>
              {benchmarks.length === 0 ? <p>No benchmark reports yet.</p> : null}
              {benchmarks.map((report) => (
                <article className="benchmark-card" key={report.id}>
                  <h3>{report.name}</h3>
                  <p>{new Date(report.created_at).toLocaleString()} | {report.iterations} iterations | target {Math.round(report.accuracy_target * 100)}%</p>
                  <table>
                    <thead>
                      <tr>
                        <th>Query</th>
                        <th>Exact</th>
                        <th>Approx</th>
                        <th>Speedup</th>
                        <th>Estimated Error</th>
                        <th>Actual Error</th>
                      </tr>
                    </thead>
                    <tbody>
                      {report.results.map((result) => (
                        <tr key={`${report.id}-${result.query}`}>
                          <td>{result.query}</td>
                          <td>{result.exact_millis.toFixed(2)} ms</td>
                          <td>{result.approx_millis.toFixed(2)} ms</td>
                          <td>{result.speedup.toFixed(2)}x</td>
                          <td>{result.estimated_error.toFixed(2)}%</td>
                          <td>{result.actual_error.toFixed(2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </article>
              ))}
            </section>
          </section>
        ) : null}

        {tab === "streaming" ? (
          <section className="panel">
            <h2>Streaming Status</h2>
            {postgresSources.length === 0 ? <p>No Postgres sources registered yet.</p> : null}
            <div className="stream-list">
              {postgresSources.map((source) => (
                <article className="stream-card" key={source.id}>
                  <div>
                    <h3>{source.name}</h3>
                    <p>{source.postgres_schema}.{source.postgres_table} -&gt; {source.table_name}</p>
                    <p>Last sync: {source.last_sync_at ? new Date(source.last_sync_at).toLocaleString() : "Never"}</p>
                    <p>Last watermark: {source.last_watermark ?? "n/a"}</p>
                    <p>Poll every {source.poll_interval_seconds ?? 15}s | rows {source.raw_row_count} / sample {source.sample_row_count}</p>
                  </div>
                  <div className="stream-actions">
                    <button onClick={() => syncSource(source.id).then(loadData).catch(console.error)}>Sync now</button>
                    {source.streaming ? (
                      <button className="secondary" onClick={() => stopStream(source.id).then(loadData).catch(console.error)}>Stop stream</button>
                    ) : (
                      <button onClick={() => startStream(source.id).then(loadData).catch(console.error)}>Start stream</button>
                    )}
                  </div>
                </article>
              ))}
            </div>
          </section>
        ) : null}
      </main>
    </div>
  );
}

function splitColumns(value: string): string[] {
  return value.split(",").map((entry) => entry.trim()).filter(Boolean);
}

function Field({ label, value, onChange, type = "text", placeholder }: { label: string; value: string; onChange: (value: string) => void; type?: string; placeholder?: string; }) {
  return (
    <label>
      {label}
      <input type={type} value={value} placeholder={placeholder} onChange={(event) => onChange(event.target.value)} />
    </label>
  );
}

function SourceTable({ sources }: { sources: SourceConfig[] }) {
  if (sources.length === 0) {
    return <p>No sources registered yet.</p>;
  }
  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Type</th>
          <th>Table</th>
          <th>Status</th>
          <th>Sampling</th>
          <th>Rows</th>
          <th>Sample Rows</th>
          <th>Sample Rate</th>
        </tr>
      </thead>
      <tbody>
        {sources.map((source) => (
          <tr key={source.id}>
            <td>{source.name}</td>
            <td>{source.kind}</td>
            <td>{source.table_name}</td>
            <td>{source.streaming ? "streaming" : source.status ?? "ready"}</td>
            <td>{source.sampling_method ?? "uniform"}</td>
            <td>{source.raw_row_count}</td>
            <td>{source.sample_row_count}</td>
            <td>{Math.round((source.sample_rate ?? 0) * 100)}%</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ResultCard({ title, result }: { title: string; result: NonNullable<RunQueryResponse["exact"]>; }) {
  return (
    <article className="result-card">
      <h3>{title}</h3>
      <p>{result.metric.execution_millis.toFixed(2)} ms | {result.metric.row_count} rows | {typeof result.metric.speedup === "number" ? `${result.metric.speedup.toFixed(2)}x faster` : ""}</p>
      <p>{typeof result.metric.confidence === "number" ? `${Math.round(result.metric.confidence * 100)}% confidence` : ""} {typeof result.metric.estimated_error === "number" ? `| ${result.metric.estimated_error.toFixed(2)}% est. error` : ""} {typeof result.metric.actual_error === "number" ? `| ${result.metric.actual_error.toFixed(2)}% actual error` : ""}</p>
      <table>
        <thead>
          <tr>
            {result.schema.map((column) => <th key={column}>{column}</th>)}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, index) => (
            <tr key={index}>
              {result.schema.map((column) => <td key={column}>{String(row[column] ?? "")}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </article>
  );
}
