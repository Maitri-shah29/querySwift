package app

import "testing"

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
