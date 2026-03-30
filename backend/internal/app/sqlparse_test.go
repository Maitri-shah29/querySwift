package app

import (
	"strings"
	"testing"
)

func TestParseAnalyticalSQL(t *testing.T) {
	parsed, err := ParseAnalyticalSQL(`SELECT region, SUM(revenue) AS total_revenue, COUNT(*) AS total_rows FROM sales GROUP BY region`)
	if err != nil {
		t.Fatalf("expected query to parse, got error: %v", err)
	}

	if parsed.Table != "sales" {
		t.Fatalf("expected table sales, got %s", parsed.Table)
	}
	if len(parsed.GroupBy) != 1 || parsed.GroupBy[0] != "region" {
		t.Fatalf("unexpected group by: %#v", parsed.GroupBy)
	}
	if len(parsed.Selects) != 3 {
		t.Fatalf("unexpected select count: %d", len(parsed.Selects))
	}
}

func TestParseAnalyticalSQLRejectsNonGroupColumn(t *testing.T) {
	_, err := ParseAnalyticalSQL(`SELECT region, city, SUM(revenue) FROM sales GROUP BY region`)
	if err == nil {
		t.Fatal("expected parser to reject non-grouped non-aggregate column")
	}
}

func TestParseAnalyticalSQLCountDistinct(t *testing.T) {
	parsed, err := ParseAnalyticalSQL(`SELECT COUNT(DISTINCT user_id) AS unique_users FROM sales`)
	if err != nil {
		t.Fatalf("expected count distinct to parse, got error: %v", err)
	}

	if len(parsed.Selects) != 1 {
		t.Fatalf("expected one select expression, got %d", len(parsed.Selects))
	}
	if parsed.Selects[0].Kind != "count_distinct" {
		t.Fatalf("expected count_distinct kind, got %s", parsed.Selects[0].Kind)
	}
	if parsed.Selects[0].Column != "user_id" {
		t.Fatalf("expected user_id column, got %s", parsed.Selects[0].Column)
	}
}

func TestBuildApproxSQL(t *testing.T) {
	parsed, err := ParseAnalyticalSQL(`SELECT region, AVG(revenue) AS avg_revenue, COUNT(*) FROM sales GROUP BY region`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sql := BuildApproxSQL(parsed, "sales_sample")
	if sql == "" {
		t.Fatal("expected non-empty sql")
	}
}

func TestBuildHLLSQL(t *testing.T) {
	parsed, err := ParseAnalyticalSQL(`SELECT region, COUNT(DISTINCT user_id) AS uu FROM sales GROUP BY region`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sql := BuildHLLSQL(parsed, "sales_raw")
	if !strings.Contains(sql, `approx_count_distinct("user_id") AS "uu"`) {
		t.Fatalf("expected approx_count_distinct in HLL sql, got: %s", sql)
	}
}

func TestIsHLLQueryEligible(t *testing.T) {
	eligible, err := ParseAnalyticalSQL(`SELECT COUNT(DISTINCT user_id) FROM sales`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !IsHLLQueryEligible(eligible) {
		t.Fatal("expected count distinct only query to be HLL eligible")
	}

	fallback, err := ParseAnalyticalSQL(`SELECT COUNT(DISTINCT user_id), SUM(revenue) FROM sales`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if IsHLLQueryEligible(fallback) {
		t.Fatal("expected mixed aggregates query to fallback to sampler path")
	}
}

func TestBuildSampleTableSQLUniform(t *testing.T) {
	sql := buildSampleTableSQL(&SourceConfig{
		RawTable:    "sales_raw",
		SampleTable: "sales_sample",
		SampleRate:  0.1,
	})

	if !strings.Contains(sql, `WHERE random() < 0.100000`) {
		t.Fatalf("expected uniform random filter, got: %s", sql)
	}
	if strings.Contains(sql, "PARTITION BY") {
		t.Fatalf("did not expect stratified partitioning in uniform sampling SQL: %s", sql)
	}
}

func TestBuildSampleTableSQLStratified(t *testing.T) {
	sql := buildSampleTableSQL(&SourceConfig{
		RawTable:        "sales_raw",
		SampleTable:     "sales_sample",
		SampleRate:      0.2,
		StratifyColumns: []string{"region", "channel"},
		SamplingMethod:  SamplingMethodStratified,
	})

	if !strings.Contains(sql, `PARTITION BY "region", "channel"`) {
		t.Fatalf("expected stratified partition clause, got: %s", sql)
	}
	if !strings.Contains(sql, "__aqe_strata_count") || !strings.Contains(sql, "__aqe_weight") {
		t.Fatalf("expected weighted stratified SQL, got: %s", sql)
	}
}

func TestQuoteExternalIdentPreservesCase(t *testing.T) {
	quoted := quoteExternalIdent(`PastPaper`)
	if quoted != `"PastPaper"` {
		t.Fatalf("expected mixed-case identifier to be preserved, got %s", quoted)
	}
}

func TestNormalizeSourcePreservesPostgresIdentifiers(t *testing.T) {
	source := normalizeSource(SourceConfig{
		Name:            "Past Papers",
		TableName:       "past_papers",
		PostgresSchema:  "public",
		PostgresTable:   "PastPaper",
		WatermarkColumn: "UpdatedAt",
	}, SourceKindPostgres)

	if source.PostgresTable != "PastPaper" {
		t.Fatalf("expected postgres table case to be preserved, got %s", source.PostgresTable)
	}
	if source.WatermarkColumn != "UpdatedAt" {
		t.Fatalf("expected watermark column case to be preserved, got %s", source.WatermarkColumn)
	}
}
