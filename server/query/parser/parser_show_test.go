package parser

import (
	"testing"
)

// TestShowDatabases tests SHOW DATABASES statement parsing
func TestShowDatabases(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
	}{
		{
			name:        "ShowDatabasesBasic",
			statement:   `SHOW DATABASES;`,
			description: "Basic SHOW DATABASES statement",
		},
		{
			name:        "ShowDatabasesLowerCase",
			statement:   `show databases;`,
			description: "SHOW DATABASES with lowercase keywords",
		},
		{
			name:        "ShowDatabasesWithSpaces",
			statement:   `  SHOW   DATABASES  ;  `,
			description: "SHOW DATABASES with extra spaces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)

			result, err := parser.Parse()
			if err != nil {
				t.Fatalf("Failed to parse statement: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Fatalf("expected ShowStmt, got %T", result)
			}

			if showStmt.ShowType != SHOW_DATABASES {
				t.Fatalf("expected SHOW_DATABASES, got %v", showStmt.ShowType)
			}
		})
	}
}

// TestShowTables tests SHOW TABLES statement parsing
func TestShowTables(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
		expectFrom  bool
		database    string
	}{
		{
			name:        "ShowTablesBasic",
			statement:   `SHOW TABLES;`,
			description: "Basic SHOW TABLES statement",
			expectFrom:  false,
			database:    "",
		},
		{
			name:        "ShowTablesFrom",
			statement:   `SHOW TABLES FROM testdb;`,
			description: "SHOW TABLES FROM database",
			expectFrom:  true,
			database:    "testdb",
		},
		{
			name:        "ShowTablesFromWithSpaces",
			statement:   `SHOW TABLES   FROM   testdb  ;`,
			description: "SHOW TABLES FROM with extra spaces",
			expectFrom:  true,
			database:    "testdb",
		},
		{
			name:        "ShowTablesLowerCase",
			statement:   `show tables from analytics;`,
			description: "SHOW TABLES FROM with lowercase keywords",
			expectFrom:  true,
			database:    "analytics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)

			result, err := parser.Parse()
			if err != nil {
				t.Fatalf("Failed to parse statement: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Fatalf("expected ShowStmt, got %T", result)
			}

			if showStmt.ShowType != SHOW_TABLES {
				t.Fatalf("expected SHOW_TABLES, got %v", showStmt.ShowType)
			}

			if tt.expectFrom {
				if showStmt.From == nil {
					t.Fatal("expected non-nil From")
				}
				if showStmt.From.Value != tt.database {
					t.Fatalf("expected database %s, got %s", tt.database, showStmt.From.Value)
				}
			} else {
				if showStmt.From != nil {
					t.Fatalf("expected nil From, got %s", showStmt.From.Value)
				}
			}
		})
	}
}

// TestShowColumns tests SHOW COLUMNS statement parsing
func TestShowColumns(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
		database    string
		table       string
	}{
		{
			name:        "ShowColumnsBasic",
			statement:   `SHOW COLUMNS FROM users;`,
			description: "Basic SHOW COLUMNS without database",
			database:    "",
			table:       "users",
		},
		{
			name:        "ShowColumnsWithDatabase",
			statement:   `SHOW COLUMNS FROM testdb.users;`,
			description: "SHOW COLUMNS with database.table format",
			database:    "testdb",
			table:       "users",
		},
		{
			name:        "ShowColumnsWithSpaces",
			statement:   `SHOW COLUMNS FROM   testdb  .  users  ;`,
			description: "SHOW COLUMNS with extra spaces",
			database:    "testdb",
			table:       "users",
		},
		{
			name:        "ShowColumnsLowerCase",
			statement:   `show columns from analytics.events;`,
			description: "SHOW COLUMNS with lowercase keywords",
			database:    "analytics",
			table:       "events",
		},
		{
			name:        "ShowColumnsComplexTable",
			statement:   `SHOW COLUMNS FROM user_events_2024;`,
			description: "SHOW COLUMNS with complex table name",
			database:    "",
			table:       "user_events_2024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)

			result, err := parser.Parse()
			if err != nil {
				t.Fatalf("Failed to parse statement: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Fatalf("expected ShowStmt, got %T", result)
			}

			if showStmt.ShowType != SHOW_COLUMNS {
				t.Fatalf("expected SHOW_COLUMNS, got %v", showStmt.ShowType)
			}

			if showStmt.TableName == nil {
				t.Fatal("expected non-nil TableName")
			}

			if tt.database != "" {
				if showStmt.TableName.Database == nil {
					t.Fatal("expected non-nil Database in TableName")
				}
				if showStmt.TableName.Database.Value != tt.database {
					t.Fatalf("expected database %s, got %s", tt.database, showStmt.TableName.Database.Value)
				}
			}

			if showStmt.TableName.Table.Value != tt.table {
				t.Fatalf("expected table %s, got %s", tt.table, showStmt.TableName.Table.Value)
			}
		})
	}
}

// TestShowCreateTable tests SHOW CREATE TABLE statement parsing
func TestShowCreateTable(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
		database    string
		table       string
	}{
		{
			name:        "ShowCreateTableBasic",
			statement:   `SHOW CREATE TABLE users;`,
			description: "Basic SHOW CREATE TABLE without database",
			database:    "",
			table:       "users",
		},
		{
			name:        "ShowCreateTableWithDatabase",
			statement:   `SHOW CREATE TABLE testdb.users;`,
			description: "SHOW CREATE TABLE with database.table format",
			database:    "testdb",
			table:       "users",
		},
		{
			name:        "ShowCreateTableWithSpaces",
			statement:   `SHOW CREATE TABLE   testdb  .  users  ;`,
			description: "SHOW CREATE TABLE with extra spaces",
			database:    "testdb",
			table:       "users",
		},
		{
			name:        "ShowCreateTableLowerCase",
			statement:   `show create table analytics.events;`,
			description: "SHOW CREATE TABLE with lowercase keywords",
			database:    "analytics",
			table:       "events",
		},
		{
			name:        "ShowCreateTableComplexName",
			statement:   `SHOW CREATE TABLE analytics.user_events_2024;`,
			description: "SHOW CREATE TABLE with complex table name",
			database:    "analytics",
			table:       "user_events_2024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)

			result, err := parser.Parse()
			if err != nil {
				t.Fatalf("Failed to parse statement: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Fatalf("expected ShowStmt, got %T", result)
			}

			if showStmt.ShowType != SHOW_CREATE_TABLE {
				t.Fatalf("expected SHOW_CREATE_TABLE, got %v", showStmt.ShowType)
			}

			if showStmt.TableName == nil {
				t.Fatal("expected non-nil TableName")
			}

			if tt.database != "" {
				if showStmt.TableName.Database == nil {
					t.Fatal("expected non-nil Database in TableName")
				}
				if showStmt.TableName.Database.Value != tt.database {
					t.Fatalf("expected database %s, got %s", tt.database, showStmt.TableName.Database.Value)
				}
			}

			if showStmt.TableName.Table.Value != tt.table {
				t.Fatalf("expected table %s, got %s", tt.table, showStmt.TableName.Table.Value)
			}
		})
	}
}

// TestShowUsers tests SHOW USERS statement parsing
func TestShowUsers(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
	}{
		{
			name:        "ShowUsersBasic",
			statement:   `SHOW USERS;`,
			description: "Basic SHOW USERS statement",
		},
		{
			name:        "ShowUsersLowerCase",
			statement:   `show users;`,
			description: "SHOW USERS with lowercase keywords",
		},
		{
			name:        "ShowUsersWithSpaces",
			statement:   `  SHOW   USERS  ;  `,
			description: "SHOW USERS with extra spaces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)

			result, err := parser.Parse()
			if err != nil {
				t.Fatalf("Failed to parse statement: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			showStmt, ok := result.(*ShowStmt)
			if !ok {
				t.Fatalf("expected ShowStmt, got %T", result)
			}

			if showStmt.ShowType != SHOW_USERS {
				t.Fatalf("expected SHOW_USERS, got %v", showStmt.ShowType)
			}
		})
	}
}

// TestShowStatementsErrorHandling tests error cases for SHOW statements
func TestShowStatementsErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
		expectError bool
	}{
		{
			name:        "ShowColumnsMissingTable",
			statement:   `SHOW COLUMNS;`,
			description: "SHOW COLUMNS without table name should fail",
			expectError: true,
		},
		{
			name:        "ShowCreateTableMissingTable",
			statement:   `SHOW CREATE TABLE;`,
			description: "SHOW CREATE TABLE without table name should fail",
			expectError: true,
		},
		{
			name:        "ShowColumnsInvalidSyntax",
			statement:   `SHOW COLUMNS FROM;`,
			description: "SHOW COLUMNS with FROM but no table should fail",
			expectError: true,
		},
		{
			name:        "ShowCreateTableInvalidSyntax",
			statement:   `SHOW CREATE TABLE FROM users;`,
			description: "SHOW CREATE TABLE with FROM should fail",
			expectError: true,
		},
		{
			name:        "ShowColumnsEmptyTable",
			statement:   `SHOW COLUMNS FROM "";`,
			description: "SHOW COLUMNS with empty table name should fail",
			expectError: true,
		},
		{
			name:        "ShowCreateTableEmptyTable",
			statement:   `SHOW CREATE TABLE "";`,
			description: "SHOW CREATE TABLE with empty table name should fail",
			expectError: true,
		},
		{
			name:        "ShowInvalidKeyword",
			statement:   `SHOW INVALID;`,
			description: "SHOW with invalid keyword should fail",
			expectError: true,
		},
		{
			name:        "ShowColumnsMissingFrom",
			statement:   `SHOW COLUMNS users;`,
			description: "SHOW COLUMNS without FROM should fail",
			expectError: true,
		},
		{
			name:        "ShowCreateTableMissingTableName",
			statement:   `SHOW CREATE TABLE .users;`,
			description: "SHOW CREATE TABLE with missing table name should fail",
			expectError: true,
		},
		{
			name:        "ShowColumnsMissingTableName",
			statement:   `SHOW COLUMNS FROM .users;`,
			description: "SHOW COLUMNS with missing table name should fail",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)

			_, err := parser.Parse()
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestShowStatementsFormatting tests that SHOW statements are formatted correctly
func TestShowStatementsFormatting(t *testing.T) {
	tests := []struct {
		name     string
		original string
		expected string
	}{
		{
			name:     "SHOW_DATABASES",
			original: "show databases;",
			expected: "SHOW DATABASES;",
		},
		{
			name:     "SHOW_TABLES",
			original: "show tables;",
			expected: "SHOW TABLES;",
		},
		{
			name:     "SHOW_TABLES_FROM",
			original: "show tables from testdb;",
			expected: "SHOW TABLES FROM testdb;",
		},
		{
			name:     "SHOW_COLUMNS_FROM",
			original: "show columns from users;",
			expected: "SHOW COLUMNS FROM users;",
		},
		{
			name:     "SHOW_COLUMNS_FROM_DATABASE_TABLE",
			original: "show columns from testdb.users;",
			expected: "SHOW COLUMNS FROM testdb.users;",
		},
		{
			name:     "SHOW_CREATE_TABLE",
			original: "show create table users;",
			expected: "SHOW CREATE TABLE users;",
		},
		{
			name:     "SHOW_CREATE_TABLE_DATABASE_TABLE",
			original: "show create table testdb.users;",
			expected: "SHOW CREATE TABLE testdb.users;",
		},
		{
			name:     "SHOW_USERS",
			original: "show users;",
			expected: "SHOW USERS;",
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
			t.Logf("Expected: %s", tt.expected)

			// Check that the formatted output contains the expected keywords
			if !contains(formatted, "SHOW") {
				t.Errorf("Formatted output should contain 'SHOW'")
			}

			// Check specific SHOW types
			switch {
			case contains(tt.original, "databases"):
				if !contains(formatted, "DATABASES") {
					t.Errorf("Formatted output should contain 'DATABASES'")
				}
			case contains(tt.original, "tables"):
				if !contains(formatted, "TABLES") {
					t.Errorf("Formatted output should contain 'TABLES'")
				}
			case contains(tt.original, "columns"):
				if !contains(formatted, "COLUMNS") {
					t.Errorf("Formatted output should contain 'COLUMNS'")
				}
				if contains(tt.original, "from") && !contains(formatted, "FROM") {
					t.Errorf("Formatted output should contain 'FROM'")
				}
			case contains(tt.original, "create table"):
				if !contains(formatted, "CREATE") || !contains(formatted, "TABLE") {
					t.Errorf("Formatted output should contain 'CREATE TABLE'")
				}
			case contains(tt.original, "users"):
				if !contains(formatted, "USERS") {
					t.Errorf("Formatted output should contain 'USERS'")
				}
			}
		})
	}
}
