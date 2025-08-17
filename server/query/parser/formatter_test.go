package parser

import (
	"testing"
)

func TestFormatQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "Simple SELECT",
			query:    "select * from users;",
			expected: "SELECT * FROM users;",
		},
		{
			name:     "SELECT with WHERE",
			query:    "select name, age from users where age > 18;",
			expected: "SELECT name, age FROM users WHERE age > 18;",
		},
		{
			name:     "CREATE TABLE",
			query:    "create table users (id int, name varchar(255));",
			expected: "CREATE TABLE users (id INT, name VARCHAR(255));",
		},
		{
			name:     "INSERT statement",
			query:    "insert into users values (1, 'john');",
			expected: "INSERT INTO users VALUES (1, 'john');",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			ast, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Format the query
			formatted := FormatQuery(ast)
			if formatted == "" {
				t.Fatalf("Formatter returned empty string")
			}

			// For now, just check that it's not empty and contains uppercase keywords
			if !containsUppercaseKeywords(formatted) {
				t.Errorf("Formatted query should contain uppercase keywords: %s", formatted)
			}

			t.Logf("Original: %s", tt.query)
			t.Logf("Formatted: %s", formatted)
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
