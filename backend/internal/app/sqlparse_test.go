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
