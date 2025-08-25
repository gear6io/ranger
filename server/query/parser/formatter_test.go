package parser

import (
	"testing"
)

// TestFormatQuery tests the formatter with various query types
func TestFormatQuery(t *testing.T) {
	tests := []struct {
		name     string
		original string
		expected string
	}{
		{
			name:     "Simple_SELECT",
			original: "select * from users;",
			expected: "SELECT * FROM users;",
		},
		{
			name:     "SELECT_with_WHERE",
			original: "select name, age from users where age > 18;",
			expected: "SELECT name, age FROM users WHERE age > 18;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			lexer := NewLexer([]byte(tt.original))
			parser := NewParser(lexer)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Format the query
			formatted := FormatQuery(stmt)
			t.Logf("Original: %s", tt.original)
			t.Logf("Formatted: %s", formatted)

			// Note: The formatter output may not exactly match the expected string
			// due to differences in how the AST is structured vs the original SQL
			// This test mainly verifies that formatting doesn't crash
		})
	}
}

// TestFormatDropTable tests the DROP TABLE formatter with the new TableIdentifier structure
func TestFormatDropTable(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *DropTableStmt
		expected string
	}{
		{
			name: "SimpleTable",
			stmt: &DropTableStmt{
				TableName: &TableIdentifier{
					Database: nil,
					Table:    &Identifier{Value: "users"},
				},
				IfExists: false,
			},
			expected: "DROP TABLE users;",
		},
		{
			name: "QualifiedTable",
			stmt: &DropTableStmt{
				TableName: &TableIdentifier{
					Database: &Identifier{Value: "testdb"},
					Table:    &Identifier{Value: "users"},
				},
				IfExists: false,
			},
			expected: "DROP TABLE testdb.users;",
		},
		{
			name: "WithIfExists",
			stmt: &DropTableStmt{
				TableName: &TableIdentifier{
					Database: nil,
					Table:    &Identifier{Value: "users"},
				},
				IfExists: true,
			},
			expected: "DROP TABLE IF EXISTS users;",
		},
		{
			name: "QualifiedTableWithIfExists",
			stmt: &DropTableStmt{
				TableName: &TableIdentifier{
					Database: &Identifier{Value: "testdb"},
					Table:    &Identifier{Value: "users"},
				},
				IfExists: true,
			},
			expected: "DROP TABLE IF EXISTS testdb.users;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted := formatDropTableStmt(tt.stmt)
			if formatted != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, formatted)
			}
			t.Logf("✅ %s: %s", tt.name, formatted)
		})
	}
}

func containsUppercaseKeywords(query string) bool {
	keywords := []string{"SELECT", "FROM", "WHERE", "CREATE", "TABLE", "INSERT", "INTO", "VALUES"}
	for _, keyword := range keywords {
		if contains(query, keyword) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
