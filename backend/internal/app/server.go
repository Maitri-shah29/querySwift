package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/marcboeker/go-duckdb"
)

type Server struct {
	db            *sql.DB
	mu            sync.RWMutex
	writeMu       sync.Mutex
	sources       map[string]*SourceConfig
	streamCancels map[string]context.CancelFunc
}

func NewServer(dbPath string) (*Server, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	// DuckDB is embedded and much more stable with a single shared connection.
	// This prevents connection-level concurrency from invalidating the database.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	server := &Server{
		db:            db,
		sources:       map[string]*SourceConfig{},
		streamCancels: map[string]context.CancelFunc{},
	}
	if err := server.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	if err := server.loadSources(); err != nil {
		db.Close()
		return nil, err
	}
	return server, nil
}

func (s *Server) Close() error {
	s.mu.Lock()
	for _, cancel := range s.streamCancels {
		cancel()
	}
	s.mu.Unlock()
	return s.db.Close()
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /sources", s.handleListSources)
	mux.HandleFunc("POST /sources/csv/import", s.handleCSVImport)
	mux.HandleFunc("POST /sources/postgres/register", s.handleRegisterPostgres)
	mux.HandleFunc("POST /sources/", s.handleSourceActions)
	mux.HandleFunc("POST /queries/run", s.handleRunQuery)
	mux.HandleFunc("GET /queries/history", s.handleQueryHistory)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /benchmarks", s.handleListBenchmarks)
	mux.HandleFunc("POST /benchmarks/run", s.handleRunBenchmark)
	return corsMiddleware(mux)
}

func (s *Server) initSchema() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS aqe_sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            table_name TEXT NOT NULL,
            file_path TEXT,
            postgres_dsn TEXT,
            postgres_schema TEXT,
            postgres_table TEXT,
            primary_key TEXT,
            watermark_column TEXT,
            last_watermark TEXT,
            poll_interval_seconds INTEGER,
            stratify_columns TEXT,
            sampling_method TEXT,
            sample_rate DOUBLE,
            status TEXT,
            last_sync_at TIMESTAMP,
            raw_table TEXT NOT NULL,
            sample_table TEXT NOT NULL,
            raw_row_count BIGINT DEFAULT 0,
            sample_row_count BIGINT DEFAULT 0
        )`,
		`CREATE TABLE IF NOT EXISTS aqe_benchmarks (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            name TEXT NOT NULL,
            report_json TEXT NOT NULL
        )`,
		`CREATE TABLE IF NOT EXISTS aqe_query_history (
            id TEXT PRIMARY KEY,
            sql_text TEXT NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            source_name TEXT,
            exact_millis DOUBLE,
            approx_millis DOUBLE,
            row_count BIGINT DEFAULT 0,
            speedup DOUBLE,
            error_pct DOUBLE,
            created_at TIMESTAMP NOT NULL
        )`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return err
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE aqe_sources ADD COLUMN IF NOT EXISTS sampling_method TEXT`); err != nil {
		return err
	}
	return nil
}

func (s *Server) loadSources() error {
	rows, err := s.db.Query(`SELECT
        id, name, kind, table_name, file_path, postgres_dsn, postgres_schema, postgres_table,
        primary_key, watermark_column, last_watermark, poll_interval_seconds, stratify_columns,
        sampling_method, sample_rate, status, last_sync_at, raw_table, sample_table, raw_row_count, sample_row_count
        FROM aqe_sources ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	for rows.Next() {
		var source SourceConfig
		var stratify string
		var samplingMethod sql.NullString
		if err := rows.Scan(
			&source.ID,
			&source.Name,
			&source.Kind,
			&source.TableName,
			&source.FilePath,
			&source.PostgresDSN,
			&source.PostgresSchema,
			&source.PostgresTable,
			&source.PrimaryKey,
			&source.WatermarkColumn,
			&source.LastWatermark,
			&source.PollIntervalSeconds,
			&stratify,
			&samplingMethod,
			&source.SampleRate,
			&source.Status,
			&source.LastSyncAt,
			&source.RawTable,
			&source.SampleTable,
			&source.RawRowCount,
			&source.SampleRowCount,
		); err != nil {
			return err
		}
		source.StratifyColumns = splitCSVList(stratify)
		if samplingMethod.Valid {
			source.SamplingMethod = SamplingMethod(samplingMethod.String)
		}
		if source.SamplingMethod == "" {
			if len(source.StratifyColumns) > 0 {
				source.SamplingMethod = SamplingMethodStratified
			} else {
				source.SamplingMethod = SamplingMethodUniform
			}
		}
		s.sources[source.ID] = &source
	}
	return rows.Err()
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"source_count": len(s.snapshotSources()),
		"time":         time.Now().UTC(),
	})
}

func (s *Server) handleListSources(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.snapshotSources())
}

func (s *Server) handleCSVImport(w http.ResponseWriter, r *http.Request) {
	var req SourceConfig
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.FilePath == "" || req.TableName == "" {
		writeError(w, http.StatusBadRequest, errors.New("file_path and table_name are required"))
		return
	}

	source := normalizeSource(req, SourceKindCSV)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.importCSV(source); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, source)
}

func (s *Server) handleRegisterPostgres(w http.ResponseWriter, r *http.Request) {
	var req SourceConfig
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PostgresDSN == "" || req.PostgresTable == "" || req.TableName == "" {
		writeError(w, http.StatusBadRequest, errors.New("postgres_dsn, postgres_table, and table_name are required"))
		return
	}

	source := normalizeSource(req, SourceKindPostgres)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.fullSyncPostgres(source); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.refreshArtifacts(source); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, source)
}

func (s *Server) handleSourceActions(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/sources/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		writeError(w, http.StatusNotFound, errors.New("unknown source action"))
		return
	}

	sourceID := parts[0]
	if len(parts) == 2 && parts[1] == "sync" {
		source, err := s.SyncSource(r.Context(), sourceID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, source)
		return
	}

	if len(parts) == 3 && parts[1] == "stream" {
		switch parts[2] {
		case "start":
			source, err := s.StartStream(sourceID)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			writeJSON(w, http.StatusOK, source)
			return
		case "stop":
			source, err := s.StopStream(sourceID)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			writeJSON(w, http.StatusOK, source)
			return
		}
	}

	writeError(w, http.StatusNotFound, errors.New("unknown source action"))
}

func (s *Server) handleRunQuery(w http.ResponseWriter, r *http.Request) {
	var req RunQueryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.AccuracyTarget <= 0 || req.AccuracyTarget > 1 {
		req.AccuracyTarget = 0.9
	}

	response, err := s.RunQuery(r.Context(), req)
	if err != nil {
		s.saveQueryHistory(req.SQL, string(req.Mode), "error", "", 0, 0, 0, 0, 0)
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Save query to history
	var exactMs, approxMs, speedup, errorPct float64
	var rowCount int64
	var sourceName string
	if response.Exact != nil {
		exactMs = response.Exact.Metric.ExecutionMillis
		rowCount = response.Exact.Metric.RowCount
	}
	if response.Approx != nil {
		approxMs = response.Approx.Metric.ExecutionMillis
		speedup = response.Approx.Metric.Speedup
		if response.Approx.Metric.ActualError != nil {
			errorPct = *response.Approx.Metric.ActualError
		}
		if rowCount == 0 {
			rowCount = response.Approx.Metric.RowCount
		}
	}
	// Try to find source name from SQL
	parsed, parseErr := ParseAnalyticalSQL(req.SQL)
	if parseErr == nil {
		if src, srcErr := s.findSourceByTable(parsed.Table); srcErr == nil {
			sourceName = src.Name
		}
	}
	s.saveQueryHistory(req.SQL, string(req.Mode), "success", sourceName, exactMs, approxMs, rowCount, speedup, errorPct)

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) saveQueryHistory(sqlText, mode, status, sourceName string, exactMs, approxMs float64, rowCount int64, speedup, errorPct float64) {
	id := uuid.NewString()
	_, _ = s.db.Exec(
		`INSERT INTO aqe_query_history (id, sql_text, mode, status, source_name, exact_millis, approx_millis, row_count, speedup, error_pct, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, sqlText, mode, status, sourceName, exactMs, approxMs, rowCount, speedup, errorPct, time.Now().UTC(),
	)
}

func (s *Server) handleQueryHistory(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id, sql_text, mode, status, source_name, exact_millis, approx_millis, row_count, speedup, error_pct, created_at FROM aqe_query_history ORDER BY created_at DESC LIMIT 100`)
	if err != nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	defer rows.Close()

	type HistoryEntry struct {
		ID          string  `json:"id"`
		SQL         string  `json:"sql"`
		Mode        string  `json:"mode"`
		Status      string  `json:"status"`
		SourceName  string  `json:"source_name"`
		ExactMillis float64 `json:"exact_millis"`
		ApproxMillis float64 `json:"approx_millis"`
		RowCount    int64   `json:"row_count"`
		Speedup     float64 `json:"speedup"`
		ErrorPct    float64 `json:"error_pct"`
		CreatedAt   string  `json:"created_at"`
	}

	entries := []HistoryEntry{}
	for rows.Next() {
		var e HistoryEntry
		var createdAt time.Time
		if err := rows.Scan(&e.ID, &e.SQL, &e.Mode, &e.Status, &e.SourceName, &e.ExactMillis, &e.ApproxMillis, &e.RowCount, &e.Speedup, &e.ErrorPct, &createdAt); err != nil {
			continue
		}
		e.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		entries = append(entries, e)
	}
	writeJSON(w, http.StatusOK, entries)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]any{
		"total_queries":     0,
		"avg_runtime_ms":    0.0,
		"connected_sources": len(s.snapshotSources()),
		"success_count":     0,
		"error_count":       0,
	}

	var totalQueries int64
	var avgRuntime float64
	var successCount, errorCount int64

	_ = s.db.QueryRowContext(r.Context(), `SELECT COUNT(*), COALESCE(AVG(CASE WHEN exact_millis > 0 THEN exact_millis ELSE approx_millis END), 0) FROM aqe_query_history`).Scan(&totalQueries, &avgRuntime)
	_ = s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM aqe_query_history WHERE status = 'success'`).Scan(&successCount)
	_ = s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM aqe_query_history WHERE status = 'error'`).Scan(&errorCount)

	stats["total_queries"] = totalQueries
	stats["avg_runtime_ms"] = math.Round(avgRuntime*100) / 100
	stats["success_count"] = successCount
	stats["error_count"] = errorCount

	// Daily query counts for the last 7 days
	dailyCounts := []map[string]any{}
	dayRows, err := s.db.QueryContext(r.Context(), `
		SELECT 
			CAST(created_at AS DATE) AS day,
			COUNT(*) FILTER (WHERE status = 'success') AS success_count,
			COUNT(*) FILTER (WHERE status = 'error') AS error_count
		FROM aqe_query_history 
		WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
		GROUP BY CAST(created_at AS DATE)
		ORDER BY day
	`)
	if err == nil {
		defer dayRows.Close()
		for dayRows.Next() {
			var day time.Time
			var sc, ec int64
			if err := dayRows.Scan(&day, &sc, &ec); err == nil {
				dailyCounts = append(dailyCounts, map[string]any{
					"day":           day.Format("2006-01-02"),
					"success_count": sc,
					"error_count":   ec,
				})
			}
		}
	}
	stats["daily_counts"] = dailyCounts

	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleListBenchmarks(w http.ResponseWriter, r *http.Request) {
	reports, err := s.ListBenchmarks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, reports)
}

func (s *Server) handleRunBenchmark(w http.ResponseWriter, r *http.Request) {
	var req BenchmarkRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Queries) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("queries are required"))
		return
	}
	if req.Iterations <= 0 {
		req.Iterations = 1
	}
	if req.AccuracyTarget <= 0 || req.AccuracyTarget > 1 {
		req.AccuracyTarget = 0.9
	}

	report, err := s.RunBenchmark(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, report)
}

func (s *Server) StartStream(sourceID string) (*SourceConfig, error) {
	source, err := s.getSource(sourceID)
	if err != nil {
		return nil, err
	}
	if source.Kind != SourceKindPostgres {
		return nil, fmt.Errorf("streaming is only supported for postgres sources")
	}

	pollIntervalSeconds := source.PollIntervalSeconds
	if pollIntervalSeconds <= 0 {
		pollIntervalSeconds = 15
	}

	s.mu.Lock()
	if cancel, ok := s.streamCancels[sourceID]; ok {
		cancel()
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	s.streamCancels[sourceID] = cancel
	s.mu.Unlock()

	go func() {
		// Perform an immediate sync so users see updates as soon as streaming starts.
		_, _ = s.SyncSource(streamCtx, sourceID)

		ticker := time.NewTicker(time.Duration(pollIntervalSeconds) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				_, _ = s.SyncSource(streamCtx, sourceID)
			}
		}
	}()

	return s.getSource(sourceID)
}

func (s *Server) StopStream(sourceID string) (*SourceConfig, error) {
	s.mu.Lock()
	cancel, ok := s.streamCancels[sourceID]
	if ok {
		delete(s.streamCancels, sourceID)
	}
	s.mu.Unlock()
	if ok {
		cancel()
	}
	return s.getSource(sourceID)
}

func normalizeSource(req SourceConfig, kind SourceKind) *SourceConfig {
	id := req.ID
	if id == "" {
		id = uuid.NewString()
	}
	tableName := safeIdent(req.TableName)
	if tableName == "" {
		tableName = safeIdent(req.Name)
	}
	if tableName == "" {
		tableName = "dataset"
	}
	sampleRate := req.SampleRate
	if sampleRate <= 0 || sampleRate >= 1 {
		sampleRate = 0.1
	}
	poll := req.PollIntervalSeconds
	if poll <= 0 {
		poll = 15
	}
	stratifyColumns := normalizeIdentifiers(req.StratifyColumns)
	samplingMethod := SamplingMethodUniform
	if len(stratifyColumns) > 0 {
		samplingMethod = SamplingMethodStratified
	}
	return &SourceConfig{
		ID:                  id,
		Name:                defaultString(req.Name, tableName),
		Kind:                kind,
		TableName:           tableName,
		FilePath:            req.FilePath,
		PostgresDSN:         req.PostgresDSN,
		PostgresSchema:      strings.TrimSpace(strings.Trim(defaultString(req.PostgresSchema, "public"), `"`)),
		PostgresTable:       strings.TrimSpace(strings.Trim(req.PostgresTable, `"`)),
		PrimaryKey:          safeIdent(req.PrimaryKey),
		WatermarkColumn:     strings.TrimSpace(strings.Trim(req.WatermarkColumn, `"`)),
		PollIntervalSeconds: poll,
		StratifyColumns:     stratifyColumns,
		SamplingMethod:      samplingMethod,
		SampleRate:          sampleRate,
		Status:              "ready",
		RawTable:            tableName + "_raw",
		SampleTable:         tableName + "_sample",
	}
}

func (s *Server) importCSV(source *SourceConfig) error {
	sqlText := fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', HEADER=TRUE)",
		quoteIdent(source.RawTable),
		escapeLiteral(source.FilePath),
	)
	if _, err := s.db.Exec(sqlText); err != nil {
		return fmt.Errorf("import csv: %w", err)
	}
	return s.refreshArtifacts(source)
}

func (s *Server) SyncSource(ctx context.Context, sourceID string) (*SourceConfig, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	source, err := s.getSource(sourceID)
	if err != nil {
		return nil, err
	}

	switch source.Kind {
	case SourceKindCSV:
		if err := s.importCSV(source); err != nil {
			return nil, err
		}
	case SourceKindPostgres:
		if err := s.incrementalSyncPostgres(ctx, source); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported source kind %q", source.Kind)
	}
	return source, nil
}

func (s *Server) RunQuery(ctx context.Context, req RunQueryRequest) (*RunQueryResponse, error) {
	parsed, err := ParseAnalyticalSQL(req.SQL)
	if err != nil {
		return nil, err
	}

	var source *SourceConfig
	if req.SourceID != "" {
		source, err = s.getSource(req.SourceID)
		if err != nil {
			return nil, err
		}
	} else {
		source, err = s.findSourceByTable(parsed.Table)
		if err != nil {
			return nil, err
		}
	}

	response := &RunQueryResponse{Mode: req.Mode}
	switch req.Mode {
	case QueryModeExact:
		exact, err := s.executeParsedQuery(ctx, parsed, source.RawTable, false, source, req.AccuracyTarget)
		if err != nil {
			return nil, err
		}
		response.Exact = exact
	case QueryModeApprox:
		approx, err := s.executeParsedQuery(ctx, parsed, source.SampleTable, true, source, req.AccuracyTarget)
		if err != nil {
			return nil, err
		}
		response.Approx = approx
	case QueryModeCompare:
		exact, err := s.executeParsedQuery(ctx, parsed, source.RawTable, false, source, req.AccuracyTarget)
		if err != nil {
			return nil, err
		}
		approx, err := s.executeParsedQuery(ctx, parsed, source.SampleTable, true, source, req.AccuracyTarget)
		if err != nil {
			return nil, err
		}
		if approx.Metric.ExecutionMillis > 0 {
			approx.Metric.Speedup = exact.Metric.ExecutionMillis / approx.Metric.ExecutionMillis
		}
		actualError := computeActualError(exact.Rows, approx.Rows)
		approx.Metric.ActualError = &actualError
		response.Exact = exact
		response.Approx = approx
	default:
		return nil, fmt.Errorf("unsupported query mode %q", req.Mode)
	}
	return response, nil
}

func (s *Server) executeParsedQuery(ctx context.Context, parsed *ParsedQuery, table string, approximate bool, source *SourceConfig, accuracyTarget float64) (*QueryResult, error) {
	querySQL := BuildExactSQL(parsed, table)
	useHLL := approximate && IsHLLQueryEligible(parsed)
	if approximate {
		if useHLL {
			querySQL = BuildHLLSQL(parsed, source.RawTable)
		} else {
			querySQL = BuildApproxSQL(parsed, table)
		}
	}

	start := time.Now()
	rows, err := s.db.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultRows, schema, err := rowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	metric := QueryMetric{
		ExecutionMillis: float64(time.Since(start).Microseconds()) / 1000.0,
		RowCount:        int64(len(resultRows)),
	}
	if approximate {
		if useHLL {
			metric.SampleRate = 1
			// HyperLogLog typical relative standard error around 0.81% at ~16k registers.
			metric.EstimatedError = 0.81
			metric.Confidence = 0.95
		} else {
			metric.SampleRate = source.SampleRate
			metric.Confidence = math.Max(0.5, accuracyTarget)
			metric.EstimatedError = estimatedError(source.SampleRate, source.RawRowCount)
		}
	}

	return &QueryResult{Schema: schema, Rows: resultRows, Metric: metric}, nil
}

func (s *Server) RunBenchmark(ctx context.Context, req BenchmarkRunRequest) (*BenchmarkReport, error) {
	report := &BenchmarkReport{
		ID:             uuid.NewString(),
		Name:           defaultString(req.Name, "Benchmark Run"),
		CreatedAt:      time.Now().UTC(),
		Iterations:     req.Iterations,
		AccuracyTarget: req.AccuracyTarget,
	}

	for _, query := range req.Queries {
		var exactMillis float64
		var approxMillis float64
		var actualError float64
		var estimated float64
		var confidence float64

		for i := 0; i < req.Iterations; i++ {
			response, err := s.RunQuery(ctx, RunQueryRequest{SQL: query, Mode: QueryModeCompare, AccuracyTarget: req.AccuracyTarget})
			if err != nil {
				return nil, err
			}
			exactMillis += response.Exact.Metric.ExecutionMillis
			approxMillis += response.Approx.Metric.ExecutionMillis
			estimated = response.Approx.Metric.EstimatedError
			confidence = response.Approx.Metric.Confidence
			if response.Approx.Metric.ActualError != nil {
				actualError += *response.Approx.Metric.ActualError
			}
		}

		exactMillis /= float64(req.Iterations)
		approxMillis /= float64(req.Iterations)
		actualError /= float64(req.Iterations)
		speedup := 0.0
		if approxMillis > 0 {
			speedup = exactMillis / approxMillis
		}

		report.Results = append(report.Results, BenchmarkQueryResult{
			Query:            query,
			ExactMillis:      exactMillis,
			ApproxMillis:     approxMillis,
			Speedup:          speedup,
			EstimatedError:   estimated,
			ActualError:      actualError,
			ApproxConfidence: confidence,
		})
	}

	payload, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO aqe_benchmarks (id, created_at, name, report_json) VALUES (?, ?, ?, ?)`, report.ID, report.CreatedAt, report.Name, string(payload)); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Server) ListBenchmarks(ctx context.Context) ([]BenchmarkReport, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT report_json FROM aqe_benchmarks ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	reports := []BenchmarkReport{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var report BenchmarkReport
		if err := json.Unmarshal([]byte(payload), &report); err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

func (s *Server) refreshArtifacts(source *SourceConfig) error {
	if err := s.refreshSampleTable(source); err != nil {
		return err
	}

	rawCount, err := countRows(s.db, source.RawTable)
	if err != nil {
		return err
	}
	sampleCount, err := countRows(s.db, source.SampleTable)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	source.LastSyncAt = &now
	source.Status = "ready"
	source.RawRowCount = rawCount
	source.SampleRowCount = sampleCount
	if source.WatermarkColumn != "" {
		if value, err := queryMaxValue(s.db, source.RawTable, source.WatermarkColumn); err == nil {
			source.LastWatermark = value
		}
	}

	if err := s.saveSource(source); err != nil {
		return err
	}

	s.mu.Lock()
	s.sources[source.ID] = cloneSource(source)
	s.mu.Unlock()
	return nil
}

func (s *Server) refreshSampleTable(source *SourceConfig) error {
	if err := s.validateStratifyColumns(source); err != nil {
		return err
	}
	sqlText := buildSampleTableSQL(source)
	_, err := s.db.Exec(sqlText)
	return err
}

func (s *Server) saveSource(source *SourceConfig) error {
	_, err := s.db.Exec(`
        INSERT OR REPLACE INTO aqe_sources (
            id, name, kind, table_name, file_path, postgres_dsn, postgres_schema, postgres_table,
            primary_key, watermark_column, last_watermark, poll_interval_seconds, stratify_columns, sampling_method,
            sample_rate, status, last_sync_at, raw_table, sample_table, raw_row_count, sample_row_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
		source.ID,
		source.Name,
		string(source.Kind),
		source.TableName,
		source.FilePath,
		source.PostgresDSN,
		source.PostgresSchema,
		source.PostgresTable,
		source.PrimaryKey,
		source.WatermarkColumn,
		source.LastWatermark,
		source.PollIntervalSeconds,
		strings.Join(source.StratifyColumns, ","),
		string(source.SamplingMethod),
		source.SampleRate,
		source.Status,
		source.LastSyncAt,
		source.RawTable,
		source.SampleTable,
		source.RawRowCount,
		source.SampleRowCount,
	)
	return err
}

func (s *Server) fullSyncPostgres(source *SourceConfig) error {
	pg, err := sql.Open("pgx", source.PostgresDSN)
	if err != nil {
		return err
	}
	defer pg.Close()

	query := fmt.Sprintf("SELECT * FROM %s.%s", quoteExternalIdent(source.PostgresSchema), quoteExternalIdent(source.PostgresTable))
	rows, err := pg.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	_, err = s.copyRowsIntoDuckDB(source, rows, true)
	return err
}

func (s *Server) incrementalSyncPostgres(ctx context.Context, source *SourceConfig) error {
	if source.LastWatermark == "" || source.WatermarkColumn == "" {
		if err := s.fullSyncPostgres(source); err != nil {
			return err
		}
		return s.refreshArtifacts(source)
	}

	pg, err := sql.Open("pgx", source.PostgresDSN)
	if err != nil {
		return err
	}
	defer pg.Close()

	query := fmt.Sprintf(
		"SELECT * FROM %s.%s WHERE %s > $1 ORDER BY %s",
		quoteExternalIdent(source.PostgresSchema),
		quoteExternalIdent(source.PostgresTable),
		quoteExternalIdent(source.WatermarkColumn),
		quoteExternalIdent(source.WatermarkColumn),
	)
	rows, err := pg.QueryContext(ctx, query, source.LastWatermark)
	if err != nil {
		return err
	}
	defer rows.Close()

	changed, err := s.copyRowsIntoDuckDB(source, rows, false)
	if err != nil {
		return err
	}
	if changed == 0 {
		return nil
	}
	return s.refreshArtifacts(source)
}

func (s *Server) copyRowsIntoDuckDB(source *SourceConfig, rows *sql.Rows, replaceTable bool) (int, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if len(columns) == 0 {
		return 0, nil
	}

	if replaceTable {
		if _, err := s.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(source.RawTable))); err != nil {
			return 0, err
		}
		defs := make([]string, 0, len(columnTypes))
		for i, col := range columns {
			defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(col), mapPostgresToDuckDB(columnTypes[i].DatabaseTypeName())))
		}
		if _, err := s.db.Exec(fmt.Sprintf("CREATE TABLE %s (%s)", quoteIdent(source.RawTable), strings.Join(defs, ", "))); err != nil {
			return 0, err
		}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	targetTable := source.RawTable
	if !replaceTable && source.PrimaryKey != "" {
		targetTable = source.RawTable + "_staging"
		if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(targetTable))); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0", quoteIdent(targetTable), quoteIdent(source.RawTable))); err != nil {
			return 0, err
		}
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdent(targetTable), joinQuoted(columns), strings.Join(placeholders, ", "))
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	dest := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range dest {
		ptrs[i] = &dest[i]
	}

	count := 0
	for rows.Next() {
		for i := range dest {
			dest[i] = nil
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, err
		}
		args := make([]any, len(dest))
		for i, v := range dest {
			args[i] = normalizeDBValue(v)
		}
		if _, err := stmt.Exec(args...); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if targetTable != source.RawTable {
		deleteSQL := fmt.Sprintf(
			"DELETE FROM %s USING %s WHERE %s.%s = %s.%s",
			quoteIdent(source.RawTable),
			quoteIdent(targetTable),
			quoteIdent(source.RawTable),
			quoteIdent(source.PrimaryKey),
			quoteIdent(targetTable),
			quoteIdent(source.PrimaryKey),
		)
		if _, err := tx.Exec(deleteSQL); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", quoteIdent(source.RawTable), quoteIdent(targetTable))); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func mapPostgresToDuckDB(dbType string) string {
	switch strings.ToUpper(dbType) {
	case "INT2", "INT4", "INT8", "SERIAL", "BIGSERIAL":
		return "BIGINT"
	case "FLOAT4", "FLOAT8", "NUMERIC", "DECIMAL":
		return "DOUBLE"
	case "BOOL":
		return "BOOLEAN"
	case "DATE":
		return "DATE"
	case "TIMESTAMP", "TIMESTAMPTZ":
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

func (s *Server) getSource(sourceID string) (*SourceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	source, ok := s.sources[sourceID]
	if !ok {
		return nil, fmt.Errorf("source %q not found", sourceID)
	}
	copySource := *source
	copySource.StratifyColumns = append([]string(nil), source.StratifyColumns...)
	copySource.Streaming = s.streamCancels[sourceID] != nil
	return &copySource, nil
}

func (s *Server) findSourceByTable(table string) (*SourceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, source := range s.sources {
		if source.TableName == table {
			copySource := *source
			copySource.StratifyColumns = append([]string(nil), source.StratifyColumns...)
			copySource.Streaming = s.streamCancels[id] != nil
			return &copySource, nil
		}
	}
	return nil, fmt.Errorf("no source registered for table %q", table)
}

func (s *Server) snapshotSources() []*SourceConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*SourceConfig, 0, len(s.sources))
	for id, source := range s.sources {
		copySource := *source
		copySource.StratifyColumns = append([]string(nil), source.StratifyColumns...)
		copySource.Streaming = s.streamCancels[id] != nil
		result = append(result, &copySource)
	}
	return result
}

func countRows(db *sql.DB, table string) (int64, error) {
	var count int64
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(table))).Scan(&count)
	return count, err
}

func buildSampleTableSQL(source *SourceConfig) string {
	if len(source.StratifyColumns) == 0 {
		return fmt.Sprintf(`
            CREATE OR REPLACE TABLE %s AS
            SELECT *, (1.0 / %f) AS __aqe_weight
            FROM %s
            WHERE random() < %f
        `, quoteIdent(source.SampleTable), source.SampleRate, quoteIdent(source.RawTable), source.SampleRate)
	}

	parts := make([]string, 0, len(source.StratifyColumns))
	for _, col := range source.StratifyColumns {
		parts = append(parts, quoteIdent(col))
	}
	partition := strings.Join(parts, ", ")
	return fmt.Sprintf(`
        CREATE OR REPLACE TABLE %s AS
        WITH stratified AS (
            SELECT *,
                row_number() OVER (PARTITION BY %s ORDER BY random()) AS __aqe_rownum,
                count(*) OVER (PARTITION BY %s) AS __aqe_strata_count
            FROM %s
        )
        SELECT *,
            CASE
                WHEN __aqe_strata_count = 0 THEN 1.0
                ELSE __aqe_strata_count::DOUBLE / GREATEST(1, CEIL(__aqe_strata_count * %f))
            END AS __aqe_weight
        FROM stratified
        WHERE __aqe_rownum <= GREATEST(1, CEIL(__aqe_strata_count * %f))
    `, quoteIdent(source.SampleTable), partition, partition, quoteIdent(source.RawTable), source.SampleRate, source.SampleRate)
}

func (s *Server) validateStratifyColumns(source *SourceConfig) error {
	if len(source.StratifyColumns) == 0 {
		source.SamplingMethod = SamplingMethodUniform
		return nil
	}

	columns, err := tableColumns(s.db, source.RawTable)
	if err != nil {
		return err
	}
	columnSet := make(map[string]bool, len(columns))
	for _, column := range columns {
		columnSet[normalizeIdentifier(column)] = true
	}

	missing := make([]string, 0)
	for _, column := range source.StratifyColumns {
		if !columnSet[normalizeIdentifier(column)] {
			missing = append(missing, column)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("invalid stratify_columns for %s: %s", source.TableName, strings.Join(missing, ", "))
	}

	source.SamplingMethod = SamplingMethodStratified
	return nil
}

func tableColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE %s", quoteIdent(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var name string
		var columnType sql.NullString
		var nullable sql.NullString
		var key sql.NullString
		var defaultValue sql.NullString
		var extra sql.NullString
		if err := rows.Scan(&name, &columnType, &nullable, &key, &defaultValue, &extra); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

func queryMaxValue(db *sql.DB, table string, column string) (string, error) {
	var value sql.NullString
	err := db.QueryRow(fmt.Sprintf("SELECT MAX(%s)::TEXT FROM %s", quoteIdent(column), quoteIdent(table))).Scan(&value)
	if err != nil {
		return "", err
	}
	return value.String, nil
}

func rowsToMaps(rows *sql.Rows) ([]map[string]any, []string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	result := []map[string]any{}
	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = normalizeDBValue(values[i])
		}
		result = append(result, row)
	}
	return result, cols, rows.Err()
}

func computeActualError(exactRows, approxRows []map[string]any) float64 {
	if len(exactRows) == 0 || len(approxRows) == 0 {
		return 0
	}
	approxIndex := map[string]map[string]any{}
	for _, row := range approxRows {
		approxIndex[rowKey(row)] = row
	}

	total := 0.0
	count := 0
	for _, exact := range exactRows {
		approx, ok := approxIndex[rowKey(exact)]
		if !ok {
			continue
		}
		for key, exactValue := range exact {
			exactNumber, exactOK := toFloat(exactValue)
			approxNumber, approxOK := toFloat(approx[key])
			if !exactOK || !approxOK {
				continue
			}
			base := math.Max(math.Abs(exactNumber), 1)
			total += math.Abs(exactNumber-approxNumber) / base * 100
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func rowKey(row map[string]any) string {
	keys := make([]string, 0, len(row))
	for key, value := range row {
		if _, ok := toFloat(value); ok {
			continue
		}
		keys = append(keys, fmt.Sprintf("%s=%v", key, value))
	}
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

func estimatedError(sampleRate float64, rawRows int64) float64 {
	if sampleRate <= 0 || rawRows <= 0 {
		return 100
	}
	return math.Sqrt((1-sampleRate)/(sampleRate*float64(rawRows))) * 100
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func normalizeDBValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	default:
		return v
	}
}

func toFloat(v any) (float64, bool) {
	switch value := v.(type) {
	case int:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case json.Number:
		f, err := value.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
}

func normalizeIdentifiers(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if normalized := safeIdent(value); normalized != "" {
			out = append(out, normalized)
		}
	}
	return out
}

func joinQuoted(values []string) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, quoteIdent(value))
	}
	return strings.Join(out, ", ")
}

func splitCSVList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = normalizeIdentifier(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func cloneSource(source *SourceConfig) *SourceConfig {
	copySource := *source
	copySource.StratifyColumns = append([]string(nil), source.StratifyColumns...)
	if copySource.SamplingMethod == "" {
		if len(copySource.StratifyColumns) > 0 {
			copySource.SamplingMethod = SamplingMethodStratified
		} else {
			copySource.SamplingMethod = SamplingMethodUniform
		}
	}
	return &copySource
}
