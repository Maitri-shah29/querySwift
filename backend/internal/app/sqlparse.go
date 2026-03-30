package app

import (
	"fmt"
	"regexp"
	"strings"
)

var selectFromRegex = regexp.MustCompile(`(?is)^\s*select\s+(.*?)\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:group\s+by\s+(.*?))?\s*;?\s*$`)
var funcRegex = regexp.MustCompile(`(?is)^(count|sum|avg)\s*\(\s*(\*|[a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s*(?:as\s+([a-zA-Z_][a-zA-Z0-9_]*))?$`)

type SelectExpr struct {
	Kind   string
	Column string
	Alias  string
}

type ParsedQuery struct {
	Table   string
	GroupBy []string
	Selects []SelectExpr
}

func ParseAnalyticalSQL(sql string) (*ParsedQuery, error) {
	matches := selectFromRegex.FindStringSubmatch(strings.TrimSpace(sql))
	if len(matches) != 4 {
		return nil, fmt.Errorf("unsupported SQL: expected SELECT ... FROM ... [GROUP BY ...]")
	}

	selectClause := matches[1]
	table := matches[2]
	groupClause := strings.TrimSpace(matches[3])

	selectParts, err := splitCommaSeparated(selectClause)
	if err != nil {
		return nil, err
	}

	groupBy := []string{}
	if groupClause != "" {
		groupBy, err = splitCommaSeparated(groupClause)
		if err != nil {
			return nil, err
		}
		for i := range groupBy {
			groupBy[i] = normalizeIdentifier(groupBy[i])
		}
	}

	query := &ParsedQuery{
		Table:   normalizeIdentifier(table),
		GroupBy: groupBy,
	}

	groupSet := map[string]bool{}
	for _, col := range groupBy {
		groupSet[col] = true
	}

	for _, rawPart := range selectParts {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			return nil, fmt.Errorf("empty select expression")
		}

		funcMatch := funcRegex.FindStringSubmatch(strings.ToLower(part))
		if len(funcMatch) == 4 {
			alias := normalizeIdentifier(funcMatch[3])
			if alias == "" {
				if funcMatch[2] == "*" {
					alias = strings.ToLower(funcMatch[1]) + "_all"
				} else {
					alias = strings.ToLower(funcMatch[1]) + "_" + normalizeIdentifier(funcMatch[2])
				}
			}

			query.Selects = append(query.Selects, SelectExpr{
				Kind:   strings.ToLower(funcMatch[1]),
				Column: normalizeIdentifier(funcMatch[2]),
				Alias:  alias,
			})
			continue
		}

		col := normalizeIdentifier(part)
		if !groupSet[col] {
			return nil, fmt.Errorf("non-aggregate select column %q must appear in GROUP BY", col)
		}
		query.Selects = append(query.Selects, SelectExpr{
			Kind:   "group",
			Column: col,
			Alias:  col,
		})
	}

	hasAggregate := false
	for _, sel := range query.Selects {
		if sel.Kind != "group" {
			hasAggregate = true
			break
		}
	}
	if !hasAggregate {
		return nil, fmt.Errorf("at least one aggregate expression is required")
	}

	return query, nil
}

func BuildExactSQL(parsed *ParsedQuery, table string) string {
	parts := make([]string, 0, len(parsed.Selects))
	for _, sel := range parsed.Selects {
		switch sel.Kind {
		case "group":
			parts = append(parts, fmt.Sprintf("%s AS %s", quoteIdent(sel.Column), quoteIdent(sel.Alias)))
		case "count":
			parts = append(parts, fmt.Sprintf("COUNT(*) AS %s", quoteIdent(sel.Alias)))
		case "sum":
			parts = append(parts, fmt.Sprintf("SUM(%s) AS %s", quoteIdent(sel.Column), quoteIdent(sel.Alias)))
		case "avg":
			parts = append(parts, fmt.Sprintf("AVG(%s) AS %s", quoteIdent(sel.Column), quoteIdent(sel.Alias)))
		}
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(parts, ", "), quoteIdent(table))
	if len(parsed.GroupBy) > 0 {
		groupCols := make([]string, 0, len(parsed.GroupBy))
		for _, col := range parsed.GroupBy {
			groupCols = append(groupCols, quoteIdent(col))
		}
		sql += " GROUP BY " + strings.Join(groupCols, ", ")
	}
	return sql
}

func BuildApproxSQL(parsed *ParsedQuery, table string) string {
	parts := make([]string, 0, len(parsed.Selects))
	for _, sel := range parsed.Selects {
		switch sel.Kind {
		case "group":
			parts = append(parts, fmt.Sprintf("%s AS %s", quoteIdent(sel.Column), quoteIdent(sel.Alias)))
		case "count":
			parts = append(parts, fmt.Sprintf("SUM(__aqe_weight) AS %s", quoteIdent(sel.Alias)))
		case "sum":
			parts = append(parts, fmt.Sprintf("SUM(%s * __aqe_weight) AS %s", quoteIdent(sel.Column), quoteIdent(sel.Alias)))
		case "avg":
			parts = append(parts, fmt.Sprintf("SUM(%s * __aqe_weight) / NULLIF(SUM(__aqe_weight), 0) AS %s", quoteIdent(sel.Column), quoteIdent(sel.Alias)))
		}
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(parts, ", "), quoteIdent(table))
	if len(parsed.GroupBy) > 0 {
		groupCols := make([]string, 0, len(parsed.GroupBy))
		for _, col := range parsed.GroupBy {
			groupCols = append(groupCols, quoteIdent(col))
		}
		sql += " GROUP BY " + strings.Join(groupCols, ", ")
	}
	return sql
}

func splitCommaSeparated(input string) ([]string, error) {
	depth := 0
	start := 0
	var result []string
	for i, r := range input {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced parentheses")
			}
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(input[start:i]))
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced parentheses")
	}
	result = append(result, strings.TrimSpace(input[start:]))
	return result, nil
}

func normalizeIdentifier(s string) string {
	s = strings.TrimSpace(strings.Trim(s, `"`))
	s = strings.ToLower(s)
	return s
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(safeIdent(s), `"`, `""`) + `"`
}

func quoteExternalIdent(s string) string {
	s = strings.TrimSpace(strings.Trim(s, `"`))
	if s == "" {
		return `""`
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func safeIdent(s string) string {
	s = normalizeIdentifier(s)
	if s == "" {
		return "unnamed"
	}
	var out strings.Builder
	for i, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9' && i > 0) || r == '_' {
			out.WriteRune(r)
			continue
		}
		if i == 0 && r >= '0' && r <= '9' {
			out.WriteRune('_')
			out.WriteRune(r)
			continue
		}
		out.WriteRune('_')
	}
	return out.String()
}
