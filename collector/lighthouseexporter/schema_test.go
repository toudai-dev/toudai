package lighthouseexporter

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ddlColumns returns the column names of one CREATE TABLE in the product DDL.
func ddlColumns(t *testing.T, file, table string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "clickhouse", "schema", file))
	if err != nil {
		t.Fatal(err)
	}
	header := "CREATE TABLE IF NOT EXISTS lighthouse." + table + "\n(\n"
	_, body, ok := strings.Cut(string(raw), header)
	if !ok {
		t.Fatalf("%s: no %q statement", file, table)
	}
	body, _, _ = strings.Cut(body, "\n)\n")
	var cols []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		cols = append(cols, strings.Fields(line)[0])
	}
	return cols
}

// The exporter names the stored columns in its INSERT statements, so a column
// renamed or added in clickhouse/schema without the matching exporter change
// would only fail at the first ClickHouse insert.
func TestInsertColumnsMatchDDL(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		table   string
		columns []string
	}{
		{"canonical payload", "002-canonical-events.sql", "canonical_events", canonicalColumns},
		{"serving logs", "003-serving-logs.sql", "serving_logs", servingLogsColumns},
		{"serving spans", "004-serving-spans.sql", "serving_spans", servingSpansColumns},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := ddlColumns(t, tt.file, tt.table)
			got := append([]string(nil), tt.columns...)
			sort.Strings(want)
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("insert columns %v do not match DDL columns %v", got, want)
			}
		})
	}
}
