package app

import "time"

type SourceKind string

const (
    SourceKindCSV      SourceKind = "csv"
    SourceKindPostgres SourceKind = "postgres"
)

type QueryMode string

const (
    QueryModeExact   QueryMode = "exact"
    QueryModeApprox  QueryMode = "approx"
    QueryModeCompare QueryMode = "compare"
)

type SourceConfig struct {
    ID                  string     `json:"id"`
    Name                string     `json:"name"`
    Kind                SourceKind `json:"kind"`
    TableName           string     `json:"table_name"`
    FilePath            string     `json:"file_path,omitempty"`
    PostgresDSN         string     `json:"postgres_dsn,omitempty"`
    PostgresSchema      string     `json:"postgres_schema,omitempty"`
    PostgresTable       string     `json:"postgres_table,omitempty"`
    PrimaryKey          string     `json:"primary_key,omitempty"`
    WatermarkColumn     string     `json:"watermark_column,omitempty"`
    LastWatermark       string     `json:"last_watermark,omitempty"`
    PollIntervalSeconds int        `json:"poll_interval_seconds,omitempty"`
    StratifyColumns     []string   `json:"stratify_columns,omitempty"`
    SampleRate          float64    `json:"sample_rate,omitempty"`
    Status              string     `json:"status,omitempty"`
    LastSyncAt          *time.Time `json:"last_sync_at,omitempty"`
    RawTable            string     `json:"raw_table"`
    SampleTable         string     `json:"sample_table"`
    RawRowCount         int64      `json:"raw_row_count"`
    SampleRowCount      int64      `json:"sample_row_count"`
    Streaming           bool       `json:"streaming"`
}

type RunQueryRequest struct {
    SQL            string    `json:"sql"`
    Mode           QueryMode `json:"mode"`
    AccuracyTarget float64   `json:"accuracy_target"`
}

type QueryMetric struct {
    ExecutionMillis float64  `json:"execution_millis"`
    RowCount        int64    `json:"row_count"`
    SampleRate      float64  `json:"sample_rate,omitempty"`
    EstimatedError  float64  `json:"estimated_error,omitempty"`
    Confidence      float64  `json:"confidence,omitempty"`
    Speedup         float64  `json:"speedup,omitempty"`
    ActualError     *float64 `json:"actual_error,omitempty"`
}

type QueryResult struct {
    Schema []string         `json:"schema"`
    Rows   []map[string]any `json:"rows"`
    Metric QueryMetric      `json:"metric"`
}

type RunQueryResponse struct {
    Mode   QueryMode    `json:"mode"`
    Exact  *QueryResult `json:"exact,omitempty"`
    Approx *QueryResult `json:"approx,omitempty"`
}

type BenchmarkRunRequest struct {
    Name           string   `json:"name"`
    Queries        []string `json:"queries"`
    Iterations     int      `json:"iterations"`
    AccuracyTarget float64  `json:"accuracy_target"`
}

type BenchmarkQueryResult struct {
    Query            string  `json:"query"`
    ExactMillis      float64 `json:"exact_millis"`
    ApproxMillis     float64 `json:"approx_millis"`
    Speedup          float64 `json:"speedup"`
    EstimatedError   float64 `json:"estimated_error"`
    ActualError      float64 `json:"actual_error"`
    ApproxConfidence float64 `json:"approx_confidence"`
}

type BenchmarkReport struct {
    ID             string                 `json:"id"`
    Name           string                 `json:"name"`
    CreatedAt      time.Time              `json:"created_at"`
    Iterations     int                    `json:"iterations"`
    AccuracyTarget float64                `json:"accuracy_target"`
    Results        []BenchmarkQueryResult `json:"results"`
}
