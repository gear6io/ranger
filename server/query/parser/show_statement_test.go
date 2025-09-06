package parser

import (
	"testing"
)

func TestParseShowColumns(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *ShowStmt
		hasError bool
	}{
		{
			name:  "SHOW COLUMNS FROM table",
			query: "SHOW COLUMNS FROM users;",
			expected: &ShowStmt{
				ShowType: SHOW_COLUMNS,
				TableName: &TableIdentifier{
					Table: &Identifier{Value: "users"},
				},
			},
			hasError: false,
		},
		{
			name:  "SHOW COLUMNS FROM database.table",
			query: "SHOW COLUMNS FROM testdb.users;",
			expected: &ShowStmt{
				ShowType: SHOW_COLUMNS,
				TableName: &TableIdentifier{
					Database: &Identifier{Value: "testdb"},
					Table:    &Identifier{Value: "users"},
				},
			},
			hasError: false,
		},
		{
			name:     "SHOW COLUMNS without FROM",
			query:    "SHOW COLUMNS;",
			expected: nil,
			hasError: true,
		},
		{
			name:     "SHOW COLUMNS FROM without table",
			query:    "SHOW COLUMNS FROM;",
			expected: nil,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer([]byte(tt.query))
			parser := NewParser(lexer)

			result, err := parser.Parse()

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Errorf("Expected ShowStmt, got %T", result)
				return
			}

			if showStmt.ShowType != tt.expected.ShowType {
				t.Errorf("Expected ShowType %v, got %v", tt.expected.ShowType, showStmt.ShowType)
			}

			if tt.expected.TableName != nil {
				if showStmt.TableName == nil {
					t.Errorf("Expected TableName, got nil")
					return
				}

				if showStmt.TableName.Table.Value != tt.expected.TableName.Table.Value {
					t.Errorf("Expected table name %s, got %s", tt.expected.TableName.Table.Value, showStmt.TableName.Table.Value)
				}

				if tt.expected.TableName.Database != nil {
					if showStmt.TableName.Database == nil {
						t.Errorf("Expected database name, got nil")
						return
					}
					if showStmt.TableName.Database.Value != tt.expected.TableName.Database.Value {
						t.Errorf("Expected database name %s, got %s", tt.expected.TableName.Database.Value, showStmt.TableName.Database.Value)
					}
				}
			}
		})
	}
}

func TestParseShowCreateTable(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected *ShowStmt
		hasError bool
	}{
		{
			name:  "SHOW CREATE TABLE table",
			query: "SHOW CREATE TABLE users;",
			expected: &ShowStmt{
				ShowType: SHOW_CREATE_TABLE,
				TableName: &TableIdentifier{
					Table: &Identifier{Value: "users"},
				},
			},
			hasError: false,
		},
		{
			name:  "SHOW CREATE TABLE database.table",
			query: "SHOW CREATE TABLE testdb.users;",
			expected: &ShowStmt{
				ShowType: SHOW_CREATE_TABLE,
				TableName: &TableIdentifier{
					Database: &Identifier{Value: "testdb"},
					Table:    &Identifier{Value: "users"},
				},
			},
			hasError: false,
		},
		{
			name:     "SHOW CREATE without TABLE",
			query:    "SHOW CREATE users;",
			expected: nil,
			hasError: true,
		},
		{
			name:     "SHOW CREATE TABLE without table name",
			query:    "SHOW CREATE TABLE;",
			expected: nil,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer([]byte(tt.query))
			parser := NewParser(lexer)

			result, err := parser.Parse()

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Errorf("Expected ShowStmt, got %T", result)
				return
			}

			if showStmt.ShowType != tt.expected.ShowType {
				t.Errorf("Expected ShowType %v, got %v", tt.expected.ShowType, showStmt.ShowType)
			}

			if tt.expected.TableName != nil {
				if showStmt.TableName == nil {
					t.Errorf("Expected TableName, got nil")
					return
				}

				if showStmt.TableName.Table.Value != tt.expected.TableName.Table.Value {
					t.Errorf("Expected table name %s, got %s", tt.expected.TableName.Table.Value, showStmt.TableName.Table.Value)
				}

				if tt.expected.TableName.Database != nil {
					if showStmt.TableName.Database == nil {
						t.Errorf("Expected database name, got nil")
						return
					}
					if showStmt.TableName.Database.Value != tt.expected.TableName.Database.Value {
						t.Errorf("Expected database name %s, got %s", tt.expected.TableName.Database.Value, showStmt.TableName.Database.Value)
					}
				}
			}
		})
	}
}

func TestShowTypeString(t *testing.T) {
	tests := []struct {
		showType ShowType
		expected string
	}{
		{SHOW_DATABASES, "DATABASES"},
		{SHOW_TABLES, "TABLES"},
		{SHOW_USERS, "USERS"},
		{SHOW_INDEXES, "INDEXES"},
		{SHOW_GRANTS, "GRANTS"},
		{SHOW_COLUMNS, "COLUMNS"},
		{SHOW_CREATE_TABLE, "CREATE TABLE"},
		{ShowType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.showType.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
